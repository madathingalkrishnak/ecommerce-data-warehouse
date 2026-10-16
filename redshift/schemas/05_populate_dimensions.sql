-- ============================================================
-- Populate Static Dimensions
-- Run once during initial setup. dim_date and dim_time are
-- pre-generated; other small dimensions are reference data.
-- ============================================================

-- ---------- Populate dim_date (2020-01-01 to 2030-12-31) ----------
-- Uses a recursive CTE to generate date series
INSERT INTO dw.dim_date
WITH RECURSIVE date_series AS (
    SELECT '2020-01-01'::DATE AS dt
    UNION ALL
    SELECT DATEADD(day, 1, dt)
    FROM date_series
    WHERE dt < '2030-12-31'
)
SELECT
    TO_CHAR(dt, 'YYYYMMDD')::INTEGER                           AS date_key,
    dt                                                          AS full_date,
    EXTRACT(DOW FROM dt)::SMALLINT                              AS day_of_week,
    TO_CHAR(dt, 'Day')                                          AS day_name,
    EXTRACT(DAY FROM dt)::SMALLINT                              AS day_of_month,
    EXTRACT(DOY FROM dt)::SMALLINT                              AS day_of_year,
    EXTRACT(WEEK FROM dt)::SMALLINT                             AS week_of_year,
    EXTRACT(WEEK FROM dt)::SMALLINT                             AS iso_week,
    EXTRACT(MONTH FROM dt)::SMALLINT                            AS month_number,
    TO_CHAR(dt, 'Month')                                        AS month_name,
    EXTRACT(QUARTER FROM dt)::SMALLINT                          AS quarter,
    'Q' || EXTRACT(QUARTER FROM dt)::VARCHAR                    AS quarter_name,
    EXTRACT(YEAR FROM dt)::SMALLINT                             AS year,
    EXTRACT(YEAR FROM dt)::SMALLINT                             AS fiscal_year,
    EXTRACT(QUARTER FROM dt)::SMALLINT                          AS fiscal_quarter,
    CASE WHEN EXTRACT(DOW FROM dt) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    -- US Federal Holidays (simplified - month/day based)
    CASE
        WHEN EXTRACT(MONTH FROM dt) = 1 AND EXTRACT(DAY FROM dt) = 1 THEN TRUE     -- New Year
        WHEN EXTRACT(MONTH FROM dt) = 7 AND EXTRACT(DAY FROM dt) = 4 THEN TRUE     -- Independence Day
        WHEN EXTRACT(MONTH FROM dt) = 12 AND EXTRACT(DAY FROM dt) = 25 THEN TRUE   -- Christmas
        WHEN EXTRACT(MONTH FROM dt) = 11 AND EXTRACT(DAY FROM dt) BETWEEN 22 AND 28
             AND EXTRACT(DOW FROM dt) = 4 THEN TRUE                                 -- Thanksgiving
        ELSE FALSE
    END AS is_holiday,
    CASE
        WHEN EXTRACT(MONTH FROM dt) = 1 AND EXTRACT(DAY FROM dt) = 1 THEN 'New Year''s Day'
        WHEN EXTRACT(MONTH FROM dt) = 7 AND EXTRACT(DAY FROM dt) = 4 THEN 'Independence Day'
        WHEN EXTRACT(MONTH FROM dt) = 12 AND EXTRACT(DAY FROM dt) = 25 THEN 'Christmas Day'
        WHEN EXTRACT(MONTH FROM dt) = 11 AND EXTRACT(DAY FROM dt) BETWEEN 22 AND 28
             AND EXTRACT(DOW FROM dt) = 4 THEN 'Thanksgiving'
        ELSE NULL
    END AS holiday_name
FROM date_series;


-- ---------- Populate dim_time (every minute of the day) ----------
INSERT INTO dw.dim_time
WITH RECURSIVE time_series AS (
    SELECT 0 AS h, 0 AS m
    UNION ALL
    SELECT
        CASE WHEN m = 59 THEN h + 1 ELSE h END,
        CASE WHEN m = 59 THEN 0 ELSE m + 1 END
    FROM time_series
    WHERE NOT (h = 23 AND m = 59)
)
SELECT
    (h * 100 + m)                                               AS time_key,
    h::SMALLINT                                                 AS hour,
    m::SMALLINT                                                 AS minute,
    CASE WHEN h = 0 THEN 12 WHEN h > 12 THEN h - 12 ELSE h END::SMALLINT AS hour_12,
    CASE WHEN h < 12 THEN 'AM' ELSE 'PM' END                   AS am_pm,
    CASE
        WHEN h BETWEEN 5 AND 11 THEN 'Morning'
        WHEN h BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN h BETWEEN 17 AND 20 THEN 'Evening'
        ELSE 'Night'
    END                                                         AS time_period
FROM time_series;


-- ---------- Populate dim_traffic_source ----------
INSERT INTO dw.dim_traffic_source (traffic_source, channel_group) VALUES
    ('organic',     'Organic'),
    ('paid_search', 'Paid'),
    ('social',      'Organic'),
    ('email',       'Owned'),
    ('direct',      'Direct'),
    ('referral',    'Earned');


-- ---------- Populate dim_device ----------
INSERT INTO dw.dim_device (device_type, browser) VALUES
    ('desktop', 'Chrome'),
    ('desktop', 'Firefox'),
    ('desktop', 'Safari'),
    ('desktop', 'Edge'),
    ('mobile',  'Safari Mobile'),
    ('mobile',  'Chrome Mobile'),
    ('mobile',  'Samsung Internet'),
    ('tablet',  'Safari'),
    ('tablet',  'Chrome'),
    ('tablet',  'Firefox');


-- ---------- Populate dim_shipping ----------
INSERT INTO dw.dim_shipping (shipping_method, shipping_tier) VALUES
    ('standard',     'Economy'),
    ('express',      'Standard'),
    ('next_day',     'Premium'),
    ('store_pickup', 'Free');


-- ---------- Populate dim_payment ----------
INSERT INTO dw.dim_payment (payment_method, payment_category) VALUES
    ('credit_card',  'Card'),
    ('debit_card',   'Card'),
    ('paypal',       'Digital Wallet'),
    ('apple_pay',    'Digital Wallet'),
    ('google_pay',   'Digital Wallet'),
    ('gift_card',    'Other');
