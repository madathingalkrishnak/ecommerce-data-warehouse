"""Tests that validate SQL schema files are syntactically sound and consistent."""
import os
import re
import pytest

SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "redshift", "schemas")
OPTIMIZATION_DIR = os.path.join(os.path.dirname(__file__), "..", "redshift", "optimizations")


def _read_sql(filename, directory=SQL_DIR):
    path = os.path.join(directory, filename)
    with open(path) as f:
        return f.read()


class TestSchemaFiles:
    """Verify all expected SQL schema files exist and are non-empty."""

    EXPECTED = [
        "01_create_schemas.sql",
        "02_staging_tables.sql",
        "03_dimension_tables.sql",
        "04_fact_tables.sql",
        "05_populate_dimensions.sql",
        "06_load_dimensions_from_staging.sql",
    ]

    @pytest.mark.parametrize("filename", EXPECTED)
    def test_file_exists_and_nonempty(self, filename):
        path = os.path.join(SQL_DIR, filename)
        assert os.path.isfile(path), f"{filename} not found"
        assert os.path.getsize(path) > 100, f"{filename} seems too small"


class TestCreateSchemas:
    def test_creates_required_schemas(self):
        sql = _read_sql("01_create_schemas.sql")
        for schema in ["staging", "dw", "analytics", "data_quality"]:
            assert schema in sql, f"Schema '{schema}' not found"


class TestStagingTables:
    def test_expected_tables(self):
        sql = _read_sql("02_staging_tables.sql")
        for table in ["clickstream_events", "users", "products", "orders", "order_line_items"]:
            assert table in sql, f"Staging table '{table}' not found"


class TestDimensionTables:
    def test_expected_dimensions(self):
        sql = _read_sql("03_dimension_tables.sql")
        for dim in ["dim_date", "dim_time", "dim_user", "dim_product",
                     "dim_traffic_source", "dim_device", "dim_shipping", "dim_payment"]:
            assert dim in sql, f"Dimension '{dim}' not found"

    def test_surrogate_keys(self):
        sql = _read_sql("03_dimension_tables.sql")
        # surrogate keys should use IDENTITY
        assert "IDENTITY" in sql, "No IDENTITY surrogate keys found"


class TestFactTables:
    def test_expected_facts(self):
        sql = _read_sql("04_fact_tables.sql")
        for fact in ["fact_clickstream", "fact_orders", "fact_order_items", "fact_daily_user_sessions"]:
            assert fact in sql, f"Fact table '{fact}' not found"

    def test_distkey_defined(self):
        sql = _read_sql("04_fact_tables.sql")
        assert "DISTKEY" in sql, "No DISTKEY found in fact tables"

    def test_sortkey_defined(self):
        sql = _read_sql("04_fact_tables.sql")
        assert "SORTKEY" in sql, "No SORTKEY found in fact tables"


class TestMaterializedViews:
    def test_expected_views(self):
        sql = _read_sql("01_materialized_views.sql", OPTIMIZATION_DIR)
        for mv in ["mv_daily_revenue", "mv_product_performance",
                    "mv_user_funnel", "mv_traffic_sources"]:
            assert mv in sql, f"Materialized view '{mv}' not found"


class TestKPIQueries:
    def test_file_exists(self):
        path = os.path.join(os.path.dirname(__file__), "..", "dashboards", "kpi_queries.sql")
        assert os.path.isfile(path)

    def test_has_all_kpi_sections(self):
        path = os.path.join(os.path.dirname(__file__), "..", "dashboards", "kpi_queries.sql")
        with open(path) as f:
            sql = f.read()
        for kpi in ["KPI 1", "KPI 2", "KPI 3", "KPI 4", "KPI 5", "KPI 6",
                     "KPI 7", "KPI 8", "KPI 9", "KPI 10", "KPI 11", "KPI 12"]:
            assert kpi in sql, f"{kpi} not found in kpi_queries.sql"
