-- ============================================================
-- Fact Tables (Star Schema)
--
-- Design principles:
--   - DISTKEY on the most common join column (user_key for user-centric queries)
--   - SORTKEY on timestamp columns for range-scan efficiency
--   - Additive measures that can be aggregated across all dimensions
--   - Foreign keys reference dimension surrogate keys
--   - Compression encodings chosen for numeric/date patterns
-- ============================================================


-- ---------- fact_clickstream ----------
-- Grain: One row per clickstream event
-- Expected volume: ~500K rows/day, ~180M rows/year
DROP TABLE IF EXISTS dw.fact_clickstream;
CREATE TABLE dw.fact_clickstream (
    -- Keys
    event_id            VARCHAR(36)     NOT NULL    ENCODE lzo,
    date_key            INTEGER         NOT NULL    ENCODE az64,
    time_key            INTEGER         NOT NULL    ENCODE az64,
    user_key            INTEGER         NOT NULL    ENCODE az64,
    product_key         INTEGER                     ENCODE az64,       -- NULL for non-product events
    device_key          INTEGER         NOT NULL    ENCODE az64,
    traffic_source_key  INTEGER         NOT NULL    ENCODE az64,

    -- Degenerate dimensions
    event_type          VARCHAR(50)     NOT NULL    ENCODE bytedict,
    session_id          VARCHAR(20)                 ENCODE lzo,
    page_url            VARCHAR(2048)               ENCODE lzo,

    -- Measures
    view_duration_sec   DECIMAL(8,1)                ENCODE az64,
    scroll_depth_pct    DECIMAL(5,1)                ENCODE az64,
    quantity            INTEGER                     ENCODE az64,
    search_results_count INTEGER                    ENCODE az64,
    cart_total          DECIMAL(10,2)               ENCODE az64,
    item_count          INTEGER                     ENCODE az64,

    -- Searchable metadata
    search_query        VARCHAR(256)                ENCODE lzo,
    page_name           VARCHAR(50)                 ENCODE bytedict,
    coupon_code         VARCHAR(30)                 ENCODE bytedict,

    -- Audit
    event_timestamp     TIMESTAMP       NOT NULL    ENCODE az64,
    etl_loaded_at       TIMESTAMP       DEFAULT GETDATE()   ENCODE az64,

    PRIMARY KEY (event_id)
)
DISTKEY (user_key)           -- Most queries filter/group by user
COMPOUND SORTKEY (date_key, event_type, user_key);
-- Sort by date first (range scans), then event type (filtering), then user (grouping)


-- ---------- fact_orders ----------
-- Grain: One row per order (header level)
-- Expected volume: ~8K rows/day, ~3M rows/year
DROP TABLE IF EXISTS dw.fact_orders;
CREATE TABLE dw.fact_orders (
    -- Keys
    order_id            VARCHAR(20)     NOT NULL    ENCODE lzo,        -- Degenerate dimension
    date_key            INTEGER         NOT NULL    ENCODE az64,
    time_key            INTEGER         NOT NULL    ENCODE az64,
    user_key            INTEGER         NOT NULL    ENCODE az64,
    shipping_key        INTEGER         NOT NULL    ENCODE az64,
    payment_key         INTEGER         NOT NULL    ENCODE az64,

    -- Degenerate dimensions
    order_status        VARCHAR(20)     NOT NULL    ENCODE bytedict,
    coupon_code         VARCHAR(30)                 ENCODE bytedict,

    -- Measures (all additive)
    subtotal            DECIMAL(12,2)   NOT NULL    ENCODE az64,
    discount_total      DECIMAL(10,2)   NOT NULL    ENCODE az64,
    shipping_cost       DECIMAL(8,2)    NOT NULL    ENCODE az64,
    tax_rate            DECIMAL(6,4)    NOT NULL    ENCODE az64,
    tax_amount          DECIMAL(10,2)   NOT NULL    ENCODE az64,
    order_total         DECIMAL(12,2)   NOT NULL    ENCODE az64,
    num_items           INTEGER         NOT NULL    ENCODE az64,

    -- Audit
    order_timestamp     TIMESTAMP       NOT NULL    ENCODE az64,
    etl_loaded_at       TIMESTAMP       DEFAULT GETDATE()   ENCODE az64,

    PRIMARY KEY (order_id)
)
DISTKEY (user_key)           -- Collocate with fact_clickstream for cross-fact queries
COMPOUND SORTKEY (date_key, user_key, order_status);


-- ---------- fact_order_items ----------
-- Grain: One row per line item in an order
-- Expected volume: ~20K rows/day, ~7M rows/year
DROP TABLE IF EXISTS dw.fact_order_items;
CREATE TABLE dw.fact_order_items (
    -- Keys
    line_item_id        VARCHAR(36)     NOT NULL    ENCODE lzo,
    order_id            VARCHAR(20)     NOT NULL    ENCODE lzo,
    date_key            INTEGER         NOT NULL    ENCODE az64,
    user_key            INTEGER         NOT NULL    ENCODE az64,
    product_key         INTEGER         NOT NULL    ENCODE az64,

    -- Measures (all additive)
    quantity            INTEGER         NOT NULL    ENCODE az64,
    unit_price          DECIMAL(10,2)   NOT NULL    ENCODE az64,
    line_total          DECIMAL(10,2)   NOT NULL    ENCODE az64,
    discount_amount     DECIMAL(10,2)   NOT NULL    ENCODE az64,
    net_amount          DECIMAL(10,2)   NOT NULL    ENCODE az64,       -- line_total - discount_amount

    -- Audit
    etl_loaded_at       TIMESTAMP       DEFAULT GETDATE()   ENCODE az64,

    PRIMARY KEY (line_item_id)
)
DISTKEY (user_key)
COMPOUND SORTKEY (date_key, product_key);


-- ---------- fact_daily_user_sessions ----------
-- Grain: One row per user per day (aggregated from clickstream)
-- This is a periodic snapshot fact table for session-level analytics
DROP TABLE IF EXISTS dw.fact_daily_user_sessions;
CREATE TABLE dw.fact_daily_user_sessions (
    -- Keys
    date_key            INTEGER         NOT NULL    ENCODE az64,
    user_key            INTEGER         NOT NULL    ENCODE az64,
    device_key          INTEGER         NOT NULL    ENCODE az64,
    traffic_source_key  INTEGER         NOT NULL    ENCODE az64,

    -- Measures
    session_count       INTEGER         NOT NULL    ENCODE az64,
    total_page_views    INTEGER         NOT NULL    ENCODE az64,
    total_product_views INTEGER         NOT NULL    ENCODE az64,
    total_add_to_cart   INTEGER         NOT NULL    ENCODE az64,
    total_searches      INTEGER         NOT NULL    ENCODE az64,
    total_events        INTEGER         NOT NULL    ENCODE az64,
    avg_view_duration   DECIMAL(8,1)                ENCODE az64,
    max_scroll_depth    DECIMAL(5,1)                ENCODE az64,

    -- Derived flags
    did_add_to_cart     BOOLEAN         NOT NULL    ENCODE raw,
    did_begin_checkout  BOOLEAN         NOT NULL    ENCODE raw,
    did_purchase        BOOLEAN         NOT NULL    ENCODE raw,

    -- Audit
    etl_loaded_at       TIMESTAMP       DEFAULT GETDATE()   ENCODE az64
)
DISTKEY (user_key)
COMPOUND SORTKEY (date_key, user_key);
