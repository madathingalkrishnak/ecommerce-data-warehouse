-- ============================================================
-- Load Dimension Data from Staging to DW (SCD Type 1 - Upsert)
-- Run by Airflow after staging tables are loaded from S3.
-- Uses MERGE pattern: update existing, insert new records.
-- ============================================================

-- ---------- Load dim_user ----------
-- Delete + Insert pattern (Redshift doesn't support MERGE natively pre-RA3)
BEGIN TRANSACTION;

-- Update existing users
UPDATE dw.dim_user
SET
    email = s.email,
    first_name = s.first_name,
    last_name = s.last_name,
    age = s.age,
    age_bracket = s.age_bracket,
    gender = s.gender,
    state = s.state,
    city = s.city,
    zip_code = s.zip_code,
    membership_tier = s.membership_tier,
    is_active = s.is_active,
    preferred_device = s.preferred_device,
    preferred_payment = s.preferred_payment,
    effective_date = CURRENT_DATE
FROM staging.users s
WHERE dw.dim_user.user_id = s.user_id;

-- Insert new users
INSERT INTO dw.dim_user (
    user_id, email, first_name, last_name, age, age_bracket,
    gender, state, city, zip_code, registration_date,
    membership_tier, is_active, preferred_device, preferred_payment
)
SELECT
    s.user_id, s.email, s.first_name, s.last_name, s.age, s.age_bracket,
    s.gender, s.state, s.city, s.zip_code, s.registration_date,
    s.membership_tier, s.is_active, s.preferred_device, s.preferred_payment
FROM staging.users s
LEFT JOIN dw.dim_user d ON s.user_id = d.user_id
WHERE d.user_id IS NULL;

COMMIT;


-- ---------- Load dim_product ----------
BEGIN TRANSACTION;

UPDATE dw.dim_product
SET
    product_name = s.product_name,
    category = s.category,
    subcategory = s.subcategory,
    brand = s.brand,
    base_price = s.base_price,
    current_price = s.current_price,
    price_tier = CASE
        WHEN s.current_price < 25 THEN 'Low'
        WHEN s.current_price < 100 THEN 'Mid'
        WHEN s.current_price < 500 THEN 'High'
        ELSE 'Premium'
    END,
    rating = s.rating,
    rating_tier = CASE
        WHEN s.rating < 2.0 THEN 'Poor'
        WHEN s.rating < 3.5 THEN 'Fair'
        WHEN s.rating < 4.5 THEN 'Good'
        ELSE 'Excellent'
    END,
    review_count = s.review_count,
    in_stock = s.in_stock,
    weight_kg = s.weight_kg,
    is_prime_eligible = s.is_prime_eligible,
    effective_date = CURRENT_DATE
FROM staging.products s
WHERE dw.dim_product.product_id = s.product_id;

INSERT INTO dw.dim_product (
    product_id, product_name, category, subcategory, brand,
    base_price, current_price, price_tier, rating, rating_tier,
    review_count, in_stock, weight_kg, is_prime_eligible, created_at
)
SELECT
    s.product_id, s.product_name, s.category, s.subcategory, s.brand,
    s.base_price, s.current_price,
    CASE
        WHEN s.current_price < 25 THEN 'Low'
        WHEN s.current_price < 100 THEN 'Mid'
        WHEN s.current_price < 500 THEN 'High'
        ELSE 'Premium'
    END,
    s.rating,
    CASE
        WHEN s.rating < 2.0 THEN 'Poor'
        WHEN s.rating < 3.5 THEN 'Fair'
        WHEN s.rating < 4.5 THEN 'Good'
        ELSE 'Excellent'
    END,
    s.review_count, s.in_stock, s.weight_kg, s.is_prime_eligible, s.created_at
FROM staging.products s
LEFT JOIN dw.dim_product d ON s.product_id = d.product_id
WHERE d.product_id IS NULL;

COMMIT;
