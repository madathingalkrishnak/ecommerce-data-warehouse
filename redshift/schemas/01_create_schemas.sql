-- ============================================================
-- E-Commerce Data Warehouse - Schema Setup
-- Creates separate schemas for staging, dimensional model, and analytics
-- ============================================================

-- Staging schema: holds raw data loaded from S3 via COPY
CREATE SCHEMA IF NOT EXISTS staging;

-- Data warehouse schema: star schema with facts & dimensions
CREATE SCHEMA IF NOT EXISTS dw;

-- Analytics schema: materialized views, aggregation tables, BI-ready views
CREATE SCHEMA IF NOT EXISTS analytics;

-- Data quality schema: audit logs and quality check results
CREATE SCHEMA IF NOT EXISTS data_quality;

-- Grant permissions (adjust roles as needed for your team)
-- GRANT USAGE ON SCHEMA dw TO GROUP analysts;
-- GRANT SELECT ON ALL TABLES IN SCHEMA dw TO GROUP analysts;
-- GRANT USAGE ON SCHEMA analytics TO GROUP analysts;
-- GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO GROUP analysts;
