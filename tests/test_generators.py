"""Tests for data generators."""
import pytest
from datetime import datetime
from data_generator.generators import (
    generate_users,
    generate_products,
    generate_clickstream_events,
    generate_purchases,
)
from data_generator.config import (
    PRODUCT_CATEGORIES,
    EVENT_TYPES,
    DEVICES,
    TRAFFIC_SOURCES,
    MEMBERSHIP_TIERS,
    PAYMENT_METHODS,
    SHIPPING_METHODS,
)


# ── Users ──────────────────────────────────────────────────────────────


class TestGenerateUsers:
    def test_correct_count(self):
        users = generate_users(100)
        assert len(users) == 100

    def test_user_fields(self):
        users = generate_users(5)
        required = {
            "user_id", "email", "first_name", "last_name",
            "age", "age_bracket", "gender", "state", "city",
            "zip_code", "registration_date", "membership_tier",
            "is_active", "preferred_device", "preferred_payment",
        }
        for u in users:
            assert required.issubset(u.keys()), f"Missing keys: {required - u.keys()}"

    def test_user_id_unique(self):
        users = generate_users(200)
        ids = [u["user_id"] for u in users]
        assert len(ids) == len(set(ids))

    def test_age_bracket_matches_age(self):
        users = generate_users(50)
        for u in users:
            age = u["age"]
            bracket = u["age_bracket"]
            if bracket == "65+":
                assert age >= 65, f"age {age} not in bracket {bracket}"
            else:
                lo, hi = map(int, bracket.split("-"))
                assert lo <= age <= hi, f"age {age} not in bracket {bracket}"

    def test_membership_tier_valid(self):
        users = generate_users(50)
        valid = set(MEMBERSHIP_TIERS.keys())
        for u in users:
            assert u["membership_tier"] in valid


# ── Products ───────────────────────────────────────────────────────────


class TestGenerateProducts:
    def test_correct_count(self):
        products = generate_products(50)
        assert len(products) == 50

    def test_product_fields(self):
        products = generate_products(5)
        required = {
            "product_id", "product_name", "category", "subcategory",
            "brand", "base_price", "current_price", "rating",
            "review_count", "in_stock",
        }
        for p in products:
            assert required.issubset(p.keys()), f"Missing keys: {required - p.keys()}"

    def test_product_id_unique(self):
        products = generate_products(100)
        ids = [p["product_id"] for p in products]
        assert len(ids) == len(set(ids))

    def test_category_valid(self):
        products = generate_products(50)
        valid_cats = set(PRODUCT_CATEGORIES.keys())
        for p in products:
            assert str(p["category"]) in valid_cats

    def test_price_positive(self):
        products = generate_products(100)
        for p in products:
            assert p["current_price"] > 0
            assert p["base_price"] > 0

    def test_rating_range(self):
        products = generate_products(50)
        for p in products:
            assert 1.0 <= p["rating"] <= 5.0


# ── Clickstream Events ────────────────────────────────────────────────


class TestGenerateClickstreamEvents:
    @pytest.fixture
    def sample_data(self):
        users = generate_users(20)
        products = generate_products(10)
        return users, products

    def test_generates_events(self, sample_data):
        users, products = sample_data
        target = datetime(2024, 1, 15)
        events = list(generate_clickstream_events(users, products, target, 100))
        # Count is adjusted by seasonality multiplier, so check range
        assert 50 <= len(events) <= 200

    def test_event_fields(self, sample_data):
        users, products = sample_data
        events = list(generate_clickstream_events(users, products, datetime(2024, 1, 15), 10))
        required = {"event_id", "event_type", "event_timestamp", "user_id", "session_id"}
        for e in events:
            assert required.issubset(e.keys())

    def test_event_type_valid(self, sample_data):
        users, products = sample_data
        events = list(generate_clickstream_events(users, products, datetime(2024, 1, 15), 50))
        valid = set(EVENT_TYPES.keys())
        for e in events:
            assert e["event_type"] in valid

    def test_event_timestamp_on_target_date(self, sample_data):
        users, products = sample_data
        target = datetime(2024, 3, 10)
        events = list(generate_clickstream_events(users, products, target, 20))
        for e in events:
            ts = e["event_timestamp"]
            if isinstance(ts, str):
                ts = datetime.fromisoformat(ts)
            assert ts.date() == target.date()

    def test_device_type_valid(self, sample_data):
        users, products = sample_data
        events = list(generate_clickstream_events(users, products, datetime(2024, 1, 15), 50))
        valid = set(DEVICES.keys())
        for e in events:
            assert e["device_type"] in valid


# ── Purchases ──────────────────────────────────────────────────────────


class TestGeneratePurchases:
    @pytest.fixture
    def sample_data(self):
        users = generate_users(30)
        products = generate_products(15)
        return users, products

    def test_generates_orders(self, sample_data):
        users, products = sample_data
        orders = list(generate_purchases(users, products, datetime(2024, 6, 1), 20))
        # Count is adjusted by seasonality multiplier, so check range
        assert 10 <= len(orders) <= 40

    def test_order_fields(self, sample_data):
        users, products = sample_data
        orders = list(generate_purchases(users, products, datetime(2024, 6, 1), 5))
        required = {
            "order_id", "user_id", "order_timestamp", "status",
            "subtotal", "order_total", "payment_method", "shipping_method",
            "line_items",
        }
        for o in orders:
            assert required.issubset(o.keys()), f"Missing: {required - o.keys()}"

    def test_order_has_line_items(self, sample_data):
        users, products = sample_data
        orders = list(generate_purchases(users, products, datetime(2024, 6, 1), 10))
        for o in orders:
            assert len(o["line_items"]) >= 1

    def test_line_item_fields(self, sample_data):
        users, products = sample_data
        orders = list(generate_purchases(users, products, datetime(2024, 6, 1), 5))
        required = {"order_id", "line_item_id", "product_id", "quantity", "unit_price", "line_total"}
        for o in orders:
            for li in o["line_items"]:
                assert required.issubset(li.keys())

    def test_order_total_positive(self, sample_data):
        users, products = sample_data
        orders = list(generate_purchases(users, products, datetime(2024, 6, 1), 20))
        for o in orders:
            assert o["order_total"] > 0

    def test_payment_method_valid(self, sample_data):
        users, products = sample_data
        orders = list(generate_purchases(users, products, datetime(2024, 6, 1), 10))
        valid = set(PAYMENT_METHODS.keys())
        for o in orders:
            assert o["payment_method"] in valid

    def test_shipping_method_valid(self, sample_data):
        users, products = sample_data
        orders = list(generate_purchases(users, products, datetime(2024, 6, 1), 10))
        valid = set(SHIPPING_METHODS.keys())
        for o in orders:
            assert o["shipping_method"] in valid

    def test_status_valid(self, sample_data):
        users, products = sample_data
        orders = list(generate_purchases(users, products, datetime(2024, 6, 1), 30))
        valid = {"completed", "processing", "shipped", "cancelled", "returned"}
        for o in orders:
            assert o["status"] in valid
