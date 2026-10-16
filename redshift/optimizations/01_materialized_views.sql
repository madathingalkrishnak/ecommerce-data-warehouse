-- ============================================================
-- Materialized Views for Analytics
-- Pre-computed aggregations for common BI queries.
-- Refreshed daily by the Airflow DAG after fact table loads.
-- ============================================================


-- ---------- Daily Revenue Summary ----------
-- Used by: Revenue dashboard, executive reporting
CREATE MATERIALIZED VIEW analytics.mv_daily_revenue AS
SELECT
    d.full_date,
    d.day_name,
    d.month_name,
    d.quarter_name,
    d.year,
    d.is_weekend,
    d.is_holiday,
    COUNT(DISTINCT fo.order_id)                     AS total_orders,
    COUNT(DISTINCT fo.user_key)                     AS unique_customers,
    SUM(fo.order_total)                             AS gross_revenue,
    SUM(fo.discount_total)                          AS total_discounts,
    SUM(fo.shipping_cost)                           AS total_shipping,
    SUM(fo.tax_amount)                              AS total_tax,
    SUM(fo.order_total - fo.discount_total)         AS net_revenue,
    AVG(fo.order_total)                             AS avg_order_value,
    SUM(fo.num_items)                               AS total_items_sold,
    AVG(fo.num_items)                               AS avg_items_per_order
FROM dw.fact_orders fo
JOIN dw.dim_date d ON fo.date_key = d.date_key
GROUP BY d.full_date, d.day_name, d.month_name, d.quarter_name,
         d.year, d.is_weekend, d.is_holiday
ORDER BY d.full_date;


-- ---------- Product Performance ----------
-- Used by: Product analytics, merchandising team
CREATE MATERIALIZED VIEW analytics.mv_product_performance AS
SELECT
    dp.product_id,
    dp.product_name,
    dp.category,
    dp.subcategory,
    dp.brand,
    dp.price_tier,
    dp.rating_tier,
    d.year,
    d.month_number,
    d.quarter_name,
    -- Sales metrics
    COUNT(DISTINCT foi.order_id)                    AS order_count,
    SUM(foi.quantity)                               AS units_sold,
    SUM(foi.line_total)                             AS gross_sales,
    SUM(foi.net_amount)                             AS net_sales,
    SUM(foi.discount_amount)                        AS total_discounts,
    AVG(foi.unit_price)                             AS avg_selling_price,
    -- Engagement metrics (from clickstream)
    COUNT(DISTINCT CASE WHEN fc.event_type = 'product_view' THEN fc.event_id END) AS view_count,
    COUNT(DISTINCT CASE WHEN fc.event_type = 'add_to_cart' THEN fc.event_id END)  AS add_to_cart_count,
    -- Conversion rate: views to purchases
    CASE
        WHEN COUNT(DISTINCT CASE WHEN fc.event_type = 'product_view' THEN fc.event_id END) > 0
        THEN ROUND(
            COUNT(DISTINCT foi.order_id)::DECIMAL /
            COUNT(DISTINCT CASE WHEN fc.event_type = 'product_view' THEN fc.event_id END) * 100, 2
        )
        ELSE 0
    END AS view_to_purchase_rate
FROM dw.dim_product dp
LEFT JOIN dw.fact_order_items foi   ON dp.product_key = foi.product_key
LEFT JOIN dw.dim_date d             ON foi.date_key = d.date_key
LEFT JOIN dw.fact_clickstream fc    ON dp.product_key = fc.product_key
                                   AND fc.date_key = d.date_key
WHERE dp.is_current = TRUE
GROUP BY dp.product_id, dp.product_name, dp.category, dp.subcategory,
         dp.brand, dp.price_tier, dp.rating_tier,
         d.year, d.month_number, d.quarter_name;


-- ---------- User Funnel Analysis ----------
-- Used by: Marketing, growth team
CREATE MATERIALIZED VIEW analytics.mv_user_funnel AS
SELECT
    d.full_date,
    d.year,
    d.month_number,
    d.week_of_year,
    du.membership_tier,
    du.age_bracket,
    du.gender,
    du.state,
    -- Funnel stages
    COUNT(DISTINCT CASE WHEN fc.event_type = 'page_view' THEN fc.user_key END)       AS visitors,
    COUNT(DISTINCT CASE WHEN fc.event_type = 'product_view' THEN fc.user_key END)    AS product_viewers,
    COUNT(DISTINCT CASE WHEN fc.event_type = 'add_to_cart' THEN fc.user_key END)     AS cart_adders,
    COUNT(DISTINCT CASE WHEN fc.event_type = 'begin_checkout' THEN fc.user_key END)  AS checkout_starters,
    COUNT(DISTINCT fo.user_key)                                                       AS purchasers,
    -- Conversion rates
    CASE
        WHEN COUNT(DISTINCT CASE WHEN fc.event_type = 'page_view' THEN fc.user_key END) > 0
        THEN ROUND(
            COUNT(DISTINCT fo.user_key)::DECIMAL /
            COUNT(DISTINCT CASE WHEN fc.event_type = 'page_view' THEN fc.user_key END) * 100, 2
        )
        ELSE 0
    END AS overall_conversion_rate,
    -- Revenue
    COALESCE(SUM(fo.order_total), 0)                AS revenue,
    COALESCE(AVG(fo.order_total), 0)                AS avg_order_value
FROM dw.fact_clickstream fc
JOIN dw.dim_date d       ON fc.date_key = d.date_key
JOIN dw.dim_user du      ON fc.user_key = du.user_key
LEFT JOIN dw.fact_orders fo ON du.user_key = fo.user_key AND fo.date_key = fc.date_key
GROUP BY d.full_date, d.year, d.month_number, d.week_of_year,
         du.membership_tier, du.age_bracket, du.gender, du.state;


-- ---------- Traffic Source Performance ----------
-- Used by: Marketing, acquisition team
CREATE MATERIALIZED VIEW analytics.mv_traffic_sources AS
SELECT
    d.full_date,
    d.year,
    d.month_number,
    dts.traffic_source,
    dts.channel_group,
    dd.device_type,
    -- Traffic metrics
    COUNT(DISTINCT fc.session_id)                    AS sessions,
    COUNT(DISTINCT fc.user_key)                      AS unique_users,
    COUNT(fc.event_id)                               AS total_events,
    AVG(fc.view_duration_sec)                        AS avg_view_duration,
    -- Engagement
    SUM(CASE WHEN fc.event_type = 'product_view' THEN 1 ELSE 0 END) AS product_views,
    SUM(CASE WHEN fc.event_type = 'add_to_cart' THEN 1 ELSE 0 END)  AS add_to_carts,
    SUM(CASE WHEN fc.event_type = 'search' THEN 1 ELSE 0 END)       AS searches,
    -- Bounce rate (single-event sessions)
    ROUND(
        SUM(CASE WHEN sess.event_count = 1 THEN 1 ELSE 0 END)::DECIMAL /
        NULLIF(COUNT(DISTINCT fc.session_id), 0) * 100, 2
    ) AS bounce_rate
FROM dw.fact_clickstream fc
JOIN dw.dim_date d                   ON fc.date_key = d.date_key
JOIN dw.dim_traffic_source dts       ON fc.traffic_source_key = dts.traffic_source_key
JOIN dw.dim_device dd                ON fc.device_key = dd.device_key
LEFT JOIN (
    SELECT session_id, COUNT(*) AS event_count
    FROM dw.fact_clickstream
    GROUP BY session_id
) sess ON fc.session_id = sess.session_id
GROUP BY d.full_date, d.year, d.month_number,
         dts.traffic_source, dts.channel_group, dd.device_type;
