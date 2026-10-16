"""Tests for data generation configuration."""
import pytest
from data_generator.config import (
    PRODUCT_CATEGORIES,
    EVENT_TYPES,
    DEVICES,
    BROWSERS,
    TRAFFIC_SOURCES,
    MEMBERSHIP_TIERS,
    PAYMENT_METHODS,
    SHIPPING_METHODS,
    MONTHLY_TRAFFIC_MULTIPLIER,
)


class TestProductCategories:
    def test_has_categories(self):
        assert len(PRODUCT_CATEGORIES) >= 5

    def test_category_structure(self):
        for cat, info in PRODUCT_CATEGORIES.items():
            assert "subcategories" in info, f"{cat} missing subcategories"
            assert "price_range" in info, f"{cat} missing price_range"
            assert "weight" in info, f"{cat} missing weight"
            assert len(info["subcategories"]) >= 1
            lo, hi = info["price_range"]
            assert lo < hi, f"{cat}: price range inverted"

    def test_category_weights_positive(self):
        for cat, info in PRODUCT_CATEGORIES.items():
            assert info["weight"] > 0, f"{cat} has non-positive weight"


class TestWeightedDicts:
    """Weighted choice dicts should have entries and weights that sum > 0."""

    @pytest.mark.parametrize("name,items", [
        ("DEVICES", DEVICES),
        ("TRAFFIC_SOURCES", TRAFFIC_SOURCES),
        ("PAYMENT_METHODS", PAYMENT_METHODS),
        ("SHIPPING_METHODS", SHIPPING_METHODS),
    ])
    def test_non_empty(self, name, items):
        assert len(items) >= 2, f"{name} too few entries"

    @pytest.mark.parametrize("name,items", [
        ("DEVICES", DEVICES),
        ("TRAFFIC_SOURCES", TRAFFIC_SOURCES),
        ("PAYMENT_METHODS", PAYMENT_METHODS),
        ("SHIPPING_METHODS", SHIPPING_METHODS),
    ])
    def test_weights_positive(self, name, items):
        total = sum(items.values())
        assert total > 0, f"{name} weights sum to {total}"


class TestEventTypes:
    def test_non_empty(self):
        assert len(EVENT_TYPES) >= 2

    def test_each_has_weight(self):
        for name, info in EVENT_TYPES.items():
            assert "weight" in info, f"{name} missing weight"
            assert info["weight"] > 0


class TestBrowsers:
    def test_has_device_types(self):
        assert len(BROWSERS) >= 2

    def test_each_device_has_browsers(self):
        for device, browsers in BROWSERS.items():
            assert len(browsers) >= 1, f"{device} has no browsers"
            total = sum(browsers.values())
            assert total > 0


class TestMembershipTiers:
    def test_has_tiers(self):
        assert len(MEMBERSHIP_TIERS) >= 2

    def test_free_tier_exists(self):
        assert "Free" in MEMBERSHIP_TIERS


class TestSeasonality:
    def test_twelve_months(self):
        assert len(MONTHLY_TRAFFIC_MULTIPLIER) == 12

    def test_multipliers_positive(self):
        for m in MONTHLY_TRAFFIC_MULTIPLIER.values():
            assert m > 0
