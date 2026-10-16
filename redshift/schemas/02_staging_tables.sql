-- ============================================================
-- Staging Tables
-- Raw data landing zone - loaded from S3 via COPY command.
-- No constraints or distribution optimization; focus is fast bulk load.
-- Data is truncated and reloaded on each pipeline run.
-- ============================================================

-- Staging: Raw clickstream events
DROP TABLE IF EXISTS staging.clickstream_events;
CREATE TABLE staging.clickstream_events (
    event_id            VARCHAR(36)     NOT NULL,
    event_type          VARCHAR(50)     NOT NULL,
    event_timestamp     TIMESTAMP       NOT NULL,
    user_id             VARCHAR(16),
    session_id          VARCHAR(20),
    device_type         VARCHAR(20),
    browser             VARCHAR(50),
    traffic_source      VARCHAR(30),
    page_url            VARCHAR(2048),
    referrer_url        VARCHAR(2048),
    ip_address          VARCHAR(45),
    user_agent          VARCHAR(512),
    page_name           VARCHAR(50),
    product_id          VARCHAR(20),
    quantity            INTEGER,
    view_duration_sec   DECIMAL(8,1),
    scroll_depth_pct    DECIMAL(5,1),
    search_query        VARCHAR(256),
    results_count       INTEGER,
    cart_total           DECIMAL(10,2),
    item_count          INTEGER,
    coupon_code         VARCHAR(30),
    discount_pct        INTEGER,
    filter_type         VARCHAR(30),
    filter_value        VARCHAR(100),
    year                INTEGER,
    month               INTEGER,
    day                 INTEGER
)
DISTSTYLE EVEN
SORTKEY (event_timestamp);

-- Staging: Raw user profiles
DROP TABLE IF EXISTS staging.users;
CREATE TABLE staging.users (
    user_id             VARCHAR(16)     NOT NULL,
    email               VARCHAR(256),
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    age                 INTEGER,
    age_bracket         VARCHAR(10),
    gender              VARCHAR(30),
    state               VARCHAR(2),
    city                VARCHAR(100),
    zip_code            VARCHAR(10),
    registration_date   DATE,
    membership_tier     VARCHAR(20),
    is_active           BOOLEAN,
    preferred_device    VARCHAR(20),
    preferred_payment   VARCHAR(20)
)
DISTSTYLE ALL;  -- Small table, replicate to all nodes

-- Staging: Raw products
DROP TABLE IF EXISTS staging.products;
CREATE TABLE staging.products (
    product_id          VARCHAR(20)     NOT NULL,
    product_name        VARCHAR(256),
    category            VARCHAR(50),
    subcategory         VARCHAR(50),
    brand               VARCHAR(100),
    base_price          DECIMAL(10,2),
    current_price       DECIMAL(10,2),
    rating              DECIMAL(3,1),
    review_count        INTEGER,
    in_stock            BOOLEAN,
    weight_kg           DECIMAL(6,2),
    created_at          DATE,
    is_prime_eligible   BOOLEAN
)
DISTSTYLE ALL;  -- Small table, replicate to all nodes

-- Staging: Raw orders
DROP TABLE IF EXISTS staging.orders;
CREATE TABLE staging.orders (
    order_id            VARCHAR(20)     NOT NULL,
    user_id             VARCHAR(16)     NOT NULL,
    order_timestamp     TIMESTAMP       NOT NULL,
    status              VARCHAR(20),
    subtotal            DECIMAL(12,2),
    discount_total      DECIMAL(10,2),
    shipping_cost       DECIMAL(8,2),
    shipping_method     VARCHAR(20),
    tax_rate            DECIMAL(6,4),
    tax_amount          DECIMAL(10,2),
    order_total         DECIMAL(12,2),
    payment_method      VARCHAR(20),
    coupon_code         VARCHAR(30),
    num_items           INTEGER,
    year                INTEGER,
    month               INTEGER,
    day                 INTEGER
)
DISTSTYLE EVEN
SORTKEY (order_timestamp);

-- Staging: Raw order line items
DROP TABLE IF EXISTS staging.order_line_items;
CREATE TABLE staging.order_line_items (
    order_id            VARCHAR(20)     NOT NULL,
    line_item_id        VARCHAR(36)     NOT NULL,
    product_id          VARCHAR(20)     NOT NULL,
    quantity            INTEGER,
    unit_price          DECIMAL(10,2),
    line_total          DECIMAL(10,2),
    discount_amount     DECIMAL(10,2)
)
DISTSTYLE EVEN;
