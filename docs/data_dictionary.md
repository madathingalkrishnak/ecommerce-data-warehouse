# Data Dictionary

## Overview
This document describes every table in the e-commerce data warehouse, organized by schema layer.

---

## Schema: `staging`
Landing zone for raw data loaded from S3 via COPY. Tables are truncated and reloaded daily.

### staging.clickstream_events
| Column | Type | Description |
|--------|------|-------------|
| event_id | VARCHAR(36) | UUID of the clickstream event (PK) |
| event_type | VARCHAR(50) | Type: page_view, product_view, add_to_cart, search, etc. |
| event_timestamp | TIMESTAMP | When the event occurred |
| user_id | VARCHAR(16) | Natural key linking to the user |
| session_id | VARCHAR(20) | Browser session identifier |
| device_type | VARCHAR(20) | desktop, mobile, tablet |
| browser | VARCHAR(50) | Browser name (Chrome, Safari, etc.) |
| traffic_source | VARCHAR(30) | How the user arrived: organic, paid_search, social, etc. |
| page_url | VARCHAR(2048) | Full URL of the page |
| referrer_url | VARCHAR(2048) | Referring page URL (null for direct) |
| ip_address | VARCHAR(45) | Client IP address |
| user_agent | VARCHAR(512) | Browser user agent string |
| page_name | VARCHAR(50) | Page identifier for page_view events |
| product_id | VARCHAR(20) | Product involved (product_view, add_to_cart events) |
| quantity | INTEGER | Quantity for cart events |
| view_duration_sec | DECIMAL(8,1) | Time spent viewing a product |
| scroll_depth_pct | DECIMAL(5,1) | How far the user scrolled (0-100%) |
| search_query | VARCHAR(256) | User's search text |
| results_count | INTEGER | Number of search results returned |
| cart_total | DECIMAL(10,2) | Cart value at begin_checkout |
| item_count | INTEGER | Items in cart at begin_checkout |
| coupon_code | VARCHAR(30) | Applied coupon code |
| discount_pct | INTEGER | Coupon discount percentage |
| filter_type | VARCHAR(30) | Filter category applied |
| filter_value | VARCHAR(100) | Filter value selected |
| year | INTEGER | Partition key |
| month | INTEGER | Partition key |
| day | INTEGER | Partition key |

### staging.users
| Column | Type | Description |
|--------|------|-------------|
| user_id | VARCHAR(16) | Unique user identifier (PK) |
| email | VARCHAR(256) | User email address |
| first_name | VARCHAR(100) | First name |
| last_name | VARCHAR(100) | Last name |
| age | INTEGER | User's age |
| age_bracket | VARCHAR(10) | Age group: 18-24, 25-34, etc. |
| gender | VARCHAR(30) | Self-reported gender |
| state | VARCHAR(2) | US state code |
| city | VARCHAR(100) | City name |
| zip_code | VARCHAR(10) | Postal code |
| registration_date | DATE | When the user created their account |
| membership_tier | VARCHAR(20) | Free, Silver, Gold, Platinum |
| is_active | BOOLEAN | Whether account is active |
| preferred_device | VARCHAR(20) | Most-used device type |
| preferred_payment | VARCHAR(20) | Default payment method |

### staging.orders
| Column | Type | Description |
|--------|------|-------------|
| order_id | VARCHAR(20) | Order identifier, e.g., ORD-ABC123 (PK) |
| user_id | VARCHAR(16) | Customer who placed the order |
| order_timestamp | TIMESTAMP | When the order was placed |
| status | VARCHAR(20) | completed, processing, shipped, cancelled, returned |
| subtotal | DECIMAL(12,2) | Sum of line item totals before discounts |
| discount_total | DECIMAL(10,2) | Total discount applied |
| shipping_cost | DECIMAL(8,2) | Shipping charge |
| shipping_method | VARCHAR(20) | standard, express, next_day, store_pickup |
| tax_rate | DECIMAL(6,4) | Applied tax rate (e.g., 0.0825) |
| tax_amount | DECIMAL(10,2) | Calculated tax amount |
| order_total | DECIMAL(12,2) | Final order total (subtotal - discount + shipping + tax) |
| payment_method | VARCHAR(20) | credit_card, paypal, apple_pay, etc. |
| coupon_code | VARCHAR(30) | Coupon code used (null if none) |
| num_items | INTEGER | Number of line items |
| year/month/day | INTEGER | Partition keys |

### staging.order_line_items
| Column | Type | Description |
|--------|------|-------------|
| order_id | VARCHAR(20) | Parent order ID (FK) |
| line_item_id | VARCHAR(36) | Unique line item UUID (PK) |
| product_id | VARCHAR(20) | Product purchased (FK) |
| quantity | INTEGER | Units purchased |
| unit_price | DECIMAL(10,2) | Price per unit at time of purchase |
| line_total | DECIMAL(10,2) | quantity * unit_price |
| discount_amount | DECIMAL(10,2) | Discount on this line item |

---

## Schema: `dw` (Data Warehouse - Dimensional Model)

### Dimension Tables

#### dw.dim_date
Pre-populated calendar dimension (2020-2030). Join on date_key = YYYYMMDD integer.

#### dw.dim_time
1,440 rows (every minute). Join on time_key = HHMM integer.

#### dw.dim_user
SCD Type 1 (overwrite). Enriched user profiles with surrogate key.

#### dw.dim_product
SCD Type 1. Products with derived columns: price_tier (Low/Mid/High/Premium) and rating_tier (Poor/Fair/Good/Excellent).

#### dw.dim_traffic_source, dim_device, dim_shipping, dim_payment
Small reference dimensions with surrogate keys.

### Fact Tables

#### dw.fact_clickstream
**Grain:** One row per clickstream event.
**Volume:** ~500K rows/day.
**Distribution:** DISTKEY(user_key).
**Sort:** COMPOUND(date_key, event_type, user_key).

#### dw.fact_orders
**Grain:** One row per order (header).
**Volume:** ~8K rows/day.
**Distribution:** DISTKEY(user_key).
**Sort:** COMPOUND(date_key, user_key, order_status).

#### dw.fact_order_items
**Grain:** One row per line item.
**Volume:** ~20K rows/day.
**Distribution:** DISTKEY(user_key).
**Sort:** COMPOUND(date_key, product_key).

#### dw.fact_daily_user_sessions
**Grain:** One row per user per day (periodic snapshot).
**Purpose:** Pre-aggregated session-level metrics for faster user analytics.

---

## Schema: `analytics`
Materialized views refreshed daily after fact loads.

| View | Purpose | Key Metrics |
|------|---------|-------------|
| mv_daily_revenue | Executive revenue dashboard | gross/net revenue, orders, AOV |
| mv_product_performance | Product analytics & merchandising | units sold, view-to-purchase rate |
| mv_user_funnel | Conversion funnel by segment | funnel stage counts, conversion rates |
| mv_traffic_sources | Marketing channel performance | sessions, bounce rate, engagement |
