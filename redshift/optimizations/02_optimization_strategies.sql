-- ============================================================
-- Redshift Query Optimization Strategies
--
-- This file documents the optimization decisions and provides
-- tuning queries for monitoring and maintaining performance.
-- ============================================================

-- ========================
-- 1. DISTRIBUTION STRATEGY
-- ========================
--
-- DISTKEY(user_key) on all fact tables:
--   - Most analytics queries join facts to dim_user or across fact tables
--   - Collocating by user_key eliminates data redistribution during joins
--   - fact_clickstream JOIN fact_orders ON user_key = local join (no network)
--
-- DISTSTYLE ALL on dimension tables:
--   - dim_user (~50K rows), dim_product (~5K rows), etc. are small
--   - Replicated to every node slice = zero redistribution on any join
--   - Trade-off: slightly more storage, but dimensions are tiny
--
-- DISTSTYLE EVEN on staging tables:
--   - Staging is bulk-loaded and queried once per ETL run
--   - Even distribution = fastest COPY performance

-- Monitor distribution skew:
SELECT
    "table" AS table_name,
    skew_rows,
    skew_sortkey1
FROM svv_table_info
WHERE "schema" IN ('dw', 'analytics')
ORDER BY skew_rows DESC;


-- ========================
-- 2. SORT KEY STRATEGY
-- ========================
--
-- COMPOUND SORTKEY on fact tables:
--   fact_clickstream: (date_key, event_type, user_key)
--     - date_key first: 90%+ queries filter by date range
--     - event_type second: common filter in funnel/behavior queries
--     - user_key third: grouping for user-level analytics
--
--   fact_orders: (date_key, user_key, order_status)
--     - Same date-first approach
--     - user_key for customer analytics
--     - order_status for completed vs cancelled filtering
--
--   fact_order_items: (date_key, product_key)
--     - Product performance queries = date range + product
--
-- Why COMPOUND over INTERLEAVED:
--   - Our queries nearly always include date_key as the leading filter
--   - Compound sort is more efficient when there's a dominant leading column
--   - Interleaved would help only if we often queried WITHOUT date

-- Check sort key effectiveness (zone map effectiveness):
SELECT
    t.table_id,
    t."table" AS table_name,
    NVL(s.num_qs, 0) AS queries,
    NVL(s.avg_pct_scanned, 100) AS avg_pct_scanned,
    NVL(s.max_pct_scanned, 100) AS max_pct_scanned
FROM svv_table_info t
LEFT JOIN (
    SELECT
        tbl AS table_id,
        COUNT(*) AS num_qs,
        AVG(pct_scanned) AS avg_pct_scanned,
        MAX(pct_scanned) AS max_pct_scanned
    FROM (
        SELECT
            tbl,
            ROUND(rows_pre_filter::DECIMAL / NULLIF(rows, 0) * 100, 2) AS pct_scanned
        FROM stl_scan
        WHERE type = 2  -- user tables only
          AND starttime > DATEADD(day, -7, GETDATE())
    )
    GROUP BY tbl
) s ON t.table_id = s.table_id
WHERE t."schema" = 'dw'
ORDER BY avg_pct_scanned DESC;


-- ========================
-- 3. COMPRESSION ENCODINGS
-- ========================
--
-- Selected encodings per column type:
--   - INTEGER/BIGINT keys: AZ64 (delta encoding, best for sorted integers)
--   - VARCHAR low-cardinality: BYTEDICT (event_type, status, tier)
--   - VARCHAR high-cardinality: LZO (names, URLs, emails)
--   - DECIMAL measures: AZ64 (good compression for numeric sequences)
--   - BOOLEAN: RAW (1 byte per value, no benefit from compression)
--   - TIMESTAMP: AZ64 (sequential timestamps compress well)
--
-- Verify compression is effective:
SELECT
    "column",
    type,
    encoding,
    num_values,
    ROUND(size::DECIMAL / NULLIF(num_values, 0) * 1000, 2) AS bytes_per_1000_rows
FROM svv_columns c
JOIN svv_table_info t ON c.table_name = t."table" AND c.table_schema = t."schema"
WHERE c.table_schema = 'dw'
  AND c.table_name = 'fact_clickstream'
ORDER BY bytes_per_1000_rows DESC;

-- Run ANALYZE COMPRESSION to get Redshift recommendations:
-- ANALYZE COMPRESSION dw.fact_clickstream;


-- ========================
-- 4. VACUUM & ANALYZE
-- ========================
-- Redshift needs regular maintenance:

-- VACUUM: Reclaims space from deleted rows, re-sorts data
-- Run after large deletes (daily ETL deletes + inserts)
VACUUM FULL dw.fact_clickstream;
VACUUM FULL dw.fact_orders;
VACUUM FULL dw.fact_order_items;

-- ANALYZE: Updates table statistics for the query planner
-- Run after significant data changes
ANALYZE dw.fact_clickstream;
ANALYZE dw.fact_orders;
ANALYZE dw.fact_order_items;
ANALYZE dw.fact_daily_user_sessions;

-- Check tables needing vacuum:
SELECT
    "table" AS table_name,
    unsorted AS pct_unsorted,
    empty AS pct_empty_blocks,
    stats_off
FROM svv_table_info
WHERE "schema" = 'dw'
  AND (unsorted > 5 OR empty > 20 OR stats_off > 5)
ORDER BY unsorted DESC;


-- ========================
-- 5. WORKLOAD MANAGEMENT (WLM)
-- ========================
-- Configured in aws_config.yaml:
--   etl_queue:     40% memory, 5 concurrency (for Glue/Airflow)
--   bi_queue:      40% memory, 15 concurrency (for QuickSight/Tableau)
--   default_queue: 20% memory, 5 concurrency (ad-hoc queries)
--
-- Assign users to queue groups:
-- CREATE GROUP etl_users;
-- CREATE GROUP bi_users;
-- ALTER GROUP etl_users ADD USER glue_etl_user;
-- ALTER GROUP bi_users ADD USER quicksight_user, tableau_user;


-- ========================
-- 6. QUERY OPTIMIZATION PATTERNS
-- ========================

-- GOOD: Use sort key as leading filter
-- The query planner can skip entire 1MB blocks via zone maps
SELECT date_key, SUM(order_total)
FROM dw.fact_orders
WHERE date_key BETWEEN 20240101 AND 20240131   -- Leverages sort key
GROUP BY date_key;

-- AVOID: Filter on non-sort-key column first
-- Forces full table scan
-- SELECT * FROM dw.fact_orders WHERE coupon_code = 'SAVE10';
-- FIX: If coupon queries are frequent, create a filtered MV

-- GOOD: Join on DISTKEY columns (collocated join)
SELECT fo.date_key, du.membership_tier, SUM(fo.order_total)
FROM dw.fact_orders fo
JOIN dw.dim_user du ON fo.user_key = du.user_key   -- Both DISTKEY(user_key)
GROUP BY fo.date_key, du.membership_tier;

-- GOOD: Use approximate functions for large-scale analytics
SELECT
    date_key,
    APPROXIMATE COUNT(DISTINCT user_key) AS approx_unique_users
FROM dw.fact_clickstream
GROUP BY date_key;


-- ========================
-- 7. MONITORING QUERIES
-- ========================

-- Identify slow queries (last 24 hours):
SELECT
    query,
    TRIM(querytxt) AS query_text,
    starttime,
    endtime,
    DATEDIFF(second, starttime, endtime) AS duration_sec,
    aborted
FROM stl_query
WHERE starttime > DATEADD(hour, -24, GETDATE())
  AND duration_sec > 30
  AND userid > 1  -- Exclude system queries
ORDER BY duration_sec DESC
LIMIT 20;

-- Identify disk-based queries (spilling to disk = slow):
SELECT
    query,
    step,
    rows,
    workmem,
    bytes,
    CASE WHEN is_diskbased = 't' THEN 'DISK' ELSE 'MEMORY' END AS execution
FROM svl_query_summary
WHERE query IN (
    SELECT query FROM stl_query
    WHERE starttime > DATEADD(hour, -24, GETDATE())
)
AND is_diskbased = 't'
ORDER BY bytes DESC;

-- Table storage utilization:
SELECT
    "schema",
    "table",
    size AS size_mb,
    tbl_rows,
    ROUND(size::DECIMAL / NULLIF(tbl_rows, 0) * 1000000, 2) AS mb_per_million_rows
FROM svv_table_info
WHERE "schema" IN ('dw', 'staging', 'analytics')
ORDER BY size DESC;
