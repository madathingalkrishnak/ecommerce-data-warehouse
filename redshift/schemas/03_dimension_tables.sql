-- ============================================================
-- Dimension Tables (Star Schema)
--
-- Design principles:
--   - Surrogate keys (integer) for all dimensions for join performance
--   - Natural/business keys preserved for traceability
--   - SCD Type 1 (overwrite) for most dimensions
--   - DISTSTYLE ALL for small dimensions (< 1M rows)
--   - Compression encodings chosen based on data characteristics
-- ============================================================

-- ---------- dim_date ----------
-- Pre-populated date dimension covering 2020-2030
DROP TABLE IF EXISTS dw.dim_date;
CREATE TABLE dw.dim_date (
    date_key            INTEGER         NOT NULL    ENCODE az64,       -- YYYYMMDD format
    full_date           DATE            NOT NULL    ENCODE az64,
    day_of_week         SMALLINT        NOT NULL    ENCODE az64,       -- 0=Monday
    day_name            VARCHAR(10)     NOT NULL    ENCODE bytedict,
    day_of_month        SMALLINT        NOT NULL    ENCODE az64,
    day_of_year         SMALLINT        NOT NULL    ENCODE az64,
    week_of_year        SMALLINT        NOT NULL    ENCODE az64,
    iso_week            SMALLINT        NOT NULL    ENCODE az64,
    month_number        SMALLINT        NOT NULL    ENCODE az64,
    month_name          VARCHAR(10)     NOT NULL    ENCODE bytedict,
    quarter             SMALLINT        NOT NULL    ENCODE az64,
    quarter_name        VARCHAR(5)      NOT NULL    ENCODE bytedict,
    year                SMALLINT        NOT NULL    ENCODE az64,
    fiscal_year         SMALLINT        NOT NULL    ENCODE az64,       -- Assuming calendar = fiscal
    fiscal_quarter      SMALLINT        NOT NULL    ENCODE az64,
    is_weekend          BOOLEAN         NOT NULL    ENCODE raw,
    is_holiday          BOOLEAN         NOT NULL    ENCODE raw,        -- US federal holidays
    holiday_name        VARCHAR(50)                 ENCODE lzo,
    PRIMARY KEY (date_key)
)
DISTSTYLE ALL
SORTKEY (full_date);


-- ---------- dim_time ----------
-- 24*60 = 1440 rows covering every minute of the day
DROP TABLE IF EXISTS dw.dim_time;
CREATE TABLE dw.dim_time (
    time_key            INTEGER         NOT NULL    ENCODE az64,       -- HHMM format
    hour                SMALLINT        NOT NULL    ENCODE az64,
    minute              SMALLINT        NOT NULL    ENCODE az64,
    hour_12             SMALLINT        NOT NULL    ENCODE az64,
    am_pm               VARCHAR(2)      NOT NULL    ENCODE bytedict,
    time_period         VARCHAR(20)     NOT NULL    ENCODE bytedict,   -- Morning, Afternoon, Evening, Night
    PRIMARY KEY (time_key)
)
DISTSTYLE ALL
SORTKEY (time_key);


-- ---------- dim_user ----------
DROP TABLE IF EXISTS dw.dim_user;
CREATE TABLE dw.dim_user (
    user_key            INTEGER         IDENTITY(1,1)   ENCODE az64,
    user_id             VARCHAR(16)     NOT NULL         ENCODE lzo,    -- Natural key
    email               VARCHAR(256)                     ENCODE lzo,
    first_name          VARCHAR(100)                     ENCODE lzo,
    last_name           VARCHAR(100)                     ENCODE lzo,
    age                 SMALLINT                         ENCODE az64,
    age_bracket         VARCHAR(10)                      ENCODE bytedict,
    gender              VARCHAR(30)                      ENCODE bytedict,
    state               VARCHAR(2)                       ENCODE bytedict,
    city                VARCHAR(100)                     ENCODE lzo,
    zip_code            VARCHAR(10)                      ENCODE lzo,
    registration_date   DATE                             ENCODE az64,
    membership_tier     VARCHAR(20)                      ENCODE bytedict,
    is_active           BOOLEAN                          ENCODE raw,
    preferred_device    VARCHAR(20)                      ENCODE bytedict,
    preferred_payment   VARCHAR(20)                      ENCODE bytedict,
    -- Metadata
    effective_date      DATE            DEFAULT CURRENT_DATE   ENCODE az64,
    is_current          BOOLEAN         DEFAULT TRUE           ENCODE raw,
    PRIMARY KEY (user_key)
)
DISTSTYLE ALL     -- ~50K users; replicate for fast joins
SORTKEY (user_id);


-- ---------- dim_product ----------
DROP TABLE IF EXISTS dw.dim_product;
CREATE TABLE dw.dim_product (
    product_key         INTEGER         IDENTITY(1,1)   ENCODE az64,
    product_id          VARCHAR(20)     NOT NULL         ENCODE lzo,    -- Natural key
    product_name        VARCHAR(256)                     ENCODE lzo,
    category            VARCHAR(50)                      ENCODE bytedict,
    subcategory         VARCHAR(50)                      ENCODE bytedict,
    brand               VARCHAR(100)                     ENCODE lzo,
    base_price          DECIMAL(10,2)                    ENCODE az64,
    current_price       DECIMAL(10,2)                    ENCODE az64,
    price_tier          VARCHAR(20)                      ENCODE bytedict,   -- Derived: Low/Mid/High/Premium
    rating              DECIMAL(3,1)                     ENCODE az64,
    rating_tier         VARCHAR(10)                      ENCODE bytedict,   -- Derived: Poor/Fair/Good/Excellent
    review_count        INTEGER                          ENCODE az64,
    in_stock            BOOLEAN                          ENCODE raw,
    weight_kg           DECIMAL(6,2)                     ENCODE az64,
    is_prime_eligible   BOOLEAN                          ENCODE raw,
    created_at          DATE                             ENCODE az64,
    -- Metadata
    effective_date      DATE            DEFAULT CURRENT_DATE   ENCODE az64,
    is_current          BOOLEAN         DEFAULT TRUE           ENCODE raw,
    PRIMARY KEY (product_key)
)
DISTSTYLE ALL     -- ~5K products; replicate for fast joins
SORTKEY (product_id);


-- ---------- dim_traffic_source ----------
DROP TABLE IF EXISTS dw.dim_traffic_source;
CREATE TABLE dw.dim_traffic_source (
    traffic_source_key  INTEGER         IDENTITY(1,1)   ENCODE az64,
    traffic_source      VARCHAR(30)     NOT NULL         ENCODE bytedict,
    channel_group       VARCHAR(30)                      ENCODE bytedict,   -- Paid, Organic, Direct
    PRIMARY KEY (traffic_source_key)
)
DISTSTYLE ALL
SORTKEY (traffic_source);


-- ---------- dim_device ----------
DROP TABLE IF EXISTS dw.dim_device;
CREATE TABLE dw.dim_device (
    device_key          INTEGER         IDENTITY(1,1)   ENCODE az64,
    device_type         VARCHAR(20)     NOT NULL         ENCODE bytedict,
    browser             VARCHAR(50)     NOT NULL         ENCODE bytedict,
    PRIMARY KEY (device_key)
)
DISTSTYLE ALL
SORTKEY (device_type);


-- ---------- dim_shipping ----------
DROP TABLE IF EXISTS dw.dim_shipping;
CREATE TABLE dw.dim_shipping (
    shipping_key        INTEGER         IDENTITY(1,1)   ENCODE az64,
    shipping_method     VARCHAR(20)     NOT NULL         ENCODE bytedict,
    shipping_tier       VARCHAR(20)                      ENCODE bytedict,   -- Economy, Standard, Premium
    PRIMARY KEY (shipping_key)
)
DISTSTYLE ALL
SORTKEY (shipping_method);


-- ---------- dim_payment ----------
DROP TABLE IF EXISTS dw.dim_payment;
CREATE TABLE dw.dim_payment (
    payment_key         INTEGER         IDENTITY(1,1)   ENCODE az64,
    payment_method      VARCHAR(20)     NOT NULL         ENCODE bytedict,
    payment_category    VARCHAR(20)                      ENCODE bytedict,   -- Card, Digital Wallet, Other
    PRIMARY KEY (payment_key)
)
DISTSTYLE ALL
SORTKEY (payment_method);
