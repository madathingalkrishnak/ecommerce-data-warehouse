-- ============================================================
-- E-Commerce KPI Queries for BI Dashboard
-- Connect these to QuickSight/Tableau as data sources
-- ============================================================


-- ========================
-- EXECUTIVE DASHBOARD
-- ========================

-- KPI 1: Revenue Overview (current month vs previous month)
SELECT
    'current_month' AS period,
    SUM(gross_revenue) AS revenue,
    SUM(total_orders) AS orders,
    SUM(unique_customers) AS customers,
    AVG(avg_order_value) AS aov,
    SUM(total_items_sold) AS items_sold
FROM analytics.mv_daily_revenue
WHERE full_date >= DATE_TRUNC('month', CURRENT_DATE)
UNION ALL
SELECT
    'previous_month',
    SUM(gross_revenue),
    SUM(total_orders),
    SUM(unique_customers),
    AVG(avg_order_value),
    SUM(total_items_sold)
FROM analytics.mv_daily_revenue
WHERE full_date >= DATE_TRUNC('month', DATEADD(month, -1, CURRENT_DATE))
  AND full_date < DATE_TRUNC('month', CURRENT_DATE);


-- KPI 2: Daily Revenue Trend (last 90 days)
SELECT
    full_date,
    gross_revenue,
    net_revenue,
    total_orders,
    avg_order_value,
    -- 7-day moving average
    AVG(gross_revenue) OVER (ORDER BY full_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS revenue_7d_avg,
    -- Year-over-year comparison
    LAG(gross_revenue, 365) OVER (ORDER BY full_date) AS revenue_yoy
FROM analytics.mv_daily_revenue
WHERE full_date >= DATEADD(day, -90, CURRENT_DATE)
ORDER BY full_date;


-- KPI 3: Revenue by Category (current month)
SELECT
    dp.category,
    dp.subcategory,
    SUM(foi.net_amount) AS net_revenue,
    SUM(foi.quantity) AS units_sold,
    COUNT(DISTINCT foi.order_id) AS order_count,
    AVG(foi.unit_price) AS avg_price,
    ROUND(SUM(foi.discount_amount) / NULLIF(SUM(foi.line_total), 0) * 100, 2) AS discount_rate_pct
FROM dw.fact_order_items foi
JOIN dw.dim_product dp ON foi.product_key = dp.product_key
JOIN dw.dim_date d ON foi.date_key = d.date_key
WHERE d.full_date >= DATE_TRUNC('month', CURRENT_DATE)
GROUP BY dp.category, dp.subcategory
ORDER BY net_revenue DESC;


-- ========================
-- CONVERSION FUNNEL DASHBOARD
-- ========================

-- KPI 4: Daily Conversion Funnel
SELECT
    full_date,
    SUM(visitors) AS visitors,
    SUM(product_viewers) AS product_viewers,
    SUM(cart_adders) AS cart_adders,
    SUM(checkout_starters) AS checkout_starters,
    SUM(purchasers) AS purchasers,
    -- Step conversion rates
    ROUND(SUM(product_viewers)::DECIMAL / NULLIF(SUM(visitors), 0) * 100, 2) AS visit_to_view_rate,
    ROUND(SUM(cart_adders)::DECIMAL / NULLIF(SUM(product_viewers), 0) * 100, 2) AS view_to_cart_rate,
    ROUND(SUM(checkout_starters)::DECIMAL / NULLIF(SUM(cart_adders), 0) * 100, 2) AS cart_to_checkout_rate,
    ROUND(SUM(purchasers)::DECIMAL / NULLIF(SUM(checkout_starters), 0) * 100, 2) AS checkout_to_purchase_rate,
    ROUND(SUM(purchasers)::DECIMAL / NULLIF(SUM(visitors), 0) * 100, 2) AS overall_conversion_rate
FROM analytics.mv_user_funnel
WHERE full_date >= DATEADD(day, -30, CURRENT_DATE)
GROUP BY full_date
ORDER BY full_date;


-- KPI 5: Funnel by User Segment
SELECT
    membership_tier,
    age_bracket,
    SUM(visitors) AS visitors,
    SUM(purchasers) AS purchasers,
    ROUND(SUM(purchasers)::DECIMAL / NULLIF(SUM(visitors), 0) * 100, 2) AS conversion_rate,
    SUM(revenue) AS total_revenue,
    ROUND(SUM(revenue) / NULLIF(SUM(purchasers), 0), 2) AS revenue_per_customer
FROM analytics.mv_user_funnel
WHERE full_date >= DATEADD(day, -30, CURRENT_DATE)
GROUP BY membership_tier, age_bracket
ORDER BY conversion_rate DESC;


-- ========================
-- TRAFFIC & MARKETING DASHBOARD
-- ========================

-- KPI 6: Traffic Source Performance
SELECT
    traffic_source,
    channel_group,
    device_type,
    SUM(sessions) AS sessions,
    SUM(unique_users) AS unique_users,
    SUM(product_views) AS product_views,
    SUM(add_to_carts) AS add_to_carts,
    AVG(avg_view_duration) AS avg_session_duration,
    AVG(bounce_rate) AS avg_bounce_rate,
    ROUND(SUM(product_views)::DECIMAL / NULLIF(SUM(sessions), 0), 2) AS views_per_session
FROM analytics.mv_traffic_sources
WHERE full_date >= DATEADD(day, -30, CURRENT_DATE)
GROUP BY traffic_source, channel_group, device_type
ORDER BY sessions DESC;


-- KPI 7: Device Type Breakdown
SELECT
    device_type,
    SUM(sessions) AS sessions,
    SUM(unique_users) AS users,
    AVG(bounce_rate) AS bounce_rate,
    ROUND(SUM(sessions)::DECIMAL / SUM(SUM(sessions)) OVER () * 100, 2) AS session_share_pct
FROM analytics.mv_traffic_sources
WHERE full_date >= DATEADD(day, -30, CURRENT_DATE)
GROUP BY device_type
ORDER BY sessions DESC;


-- ========================
-- PRODUCT ANALYTICS DASHBOARD
-- ========================

-- KPI 8: Top 20 Products by Revenue
SELECT
    product_name,
    category,
    subcategory,
    brand,
    SUM(net_sales) AS net_revenue,
    SUM(units_sold) AS units_sold,
    SUM(order_count) AS orders,
    AVG(avg_selling_price) AS avg_price,
    AVG(view_to_purchase_rate) AS conversion_rate
FROM analytics.mv_product_performance
WHERE year = EXTRACT(YEAR FROM CURRENT_DATE)
  AND month_number = EXTRACT(MONTH FROM CURRENT_DATE)
GROUP BY product_name, category, subcategory, brand
ORDER BY net_revenue DESC
LIMIT 20;


-- KPI 9: Category Performance Heatmap (revenue by category x month)
SELECT
    category,
    month_number,
    quarter_name,
    SUM(net_sales) AS revenue,
    SUM(units_sold) AS units,
    SUM(view_count) AS views,
    AVG(view_to_purchase_rate) AS conversion_rate
FROM analytics.mv_product_performance
WHERE year = EXTRACT(YEAR FROM CURRENT_DATE)
GROUP BY category, month_number, quarter_name
ORDER BY category, month_number;


-- ========================
-- CUSTOMER ANALYTICS DASHBOARD
-- ========================

-- KPI 10: Customer Cohort Analysis (by registration month)
SELECT
    DATE_TRUNC('month', du.registration_date) AS cohort_month,
    d.month_number AS purchase_month,
    DATEDIFF(month, du.registration_date, d.full_date) AS months_since_registration,
    COUNT(DISTINCT fo.user_key) AS active_customers,
    SUM(fo.order_total) AS revenue,
    AVG(fo.order_total) AS avg_order_value
FROM dw.fact_orders fo
JOIN dw.dim_user du ON fo.user_key = du.user_key
JOIN dw.dim_date d ON fo.date_key = d.date_key
WHERE d.year = EXTRACT(YEAR FROM CURRENT_DATE)
GROUP BY cohort_month, d.month_number,
         DATEDIFF(month, du.registration_date, d.full_date)
ORDER BY cohort_month, months_since_registration;


-- KPI 11: Customer RFM Segmentation
WITH rfm AS (
    SELECT
        du.user_key,
        du.user_id,
        du.membership_tier,
        DATEDIFF(day, MAX(d.full_date), CURRENT_DATE) AS recency_days,
        COUNT(DISTINCT fo.order_id) AS frequency,
        SUM(fo.order_total) AS monetary
    FROM dw.fact_orders fo
    JOIN dw.dim_user du ON fo.user_key = du.user_key
    JOIN dw.dim_date d ON fo.date_key = d.date_key
    WHERE fo.order_status = 'completed'
      AND d.full_date >= DATEADD(year, -1, CURRENT_DATE)
    GROUP BY du.user_key, du.user_id, du.membership_tier
)
SELECT
    CASE
        WHEN recency_days <= 30 AND frequency >= 5 AND monetary >= 500 THEN 'Champions'
        WHEN recency_days <= 30 AND frequency >= 3 THEN 'Loyal Customers'
        WHEN recency_days <= 60 AND frequency >= 2 THEN 'Potential Loyalists'
        WHEN recency_days <= 30 AND frequency = 1 THEN 'New Customers'
        WHEN recency_days BETWEEN 60 AND 120 THEN 'At Risk'
        WHEN recency_days > 120 THEN 'Hibernating'
        ELSE 'Other'
    END AS rfm_segment,
    COUNT(*) AS customer_count,
    AVG(recency_days) AS avg_recency,
    AVG(frequency) AS avg_frequency,
    AVG(monetary) AS avg_monetary,
    SUM(monetary) AS total_revenue
FROM rfm
GROUP BY rfm_segment
ORDER BY total_revenue DESC;


-- KPI 12: Geographic Distribution
SELECT
    du.state,
    COUNT(DISTINCT fo.user_key) AS customers,
    SUM(fo.order_total) AS revenue,
    AVG(fo.order_total) AS avg_order_value,
    COUNT(fo.order_id) AS total_orders
FROM dw.fact_orders fo
JOIN dw.dim_user du ON fo.user_key = du.user_key
JOIN dw.dim_date d ON fo.date_key = d.date_key
WHERE d.full_date >= DATEADD(day, -30, CURRENT_DATE)
GROUP BY du.state
ORDER BY revenue DESC;
