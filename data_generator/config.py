"""
Configuration constants for synthetic data generation.
Defines product categories, user demographics, event types,
and volume parameters for realistic e-commerce data.
"""

import os
from datetime import datetime, timedelta

# --- Time Range ---
DATA_START_DATE = datetime(2024, 1, 1)
DATA_END_DATE = datetime(2024, 12, 31)

# --- Volume Parameters ---
NUM_USERS = 50_000
NUM_PRODUCTS = 5_000
NUM_CLICKSTREAM_EVENTS_PER_DAY = 500_000
NUM_PURCHASES_PER_DAY = 8_000
BATCH_SIZE = 100_000  # For chunked writing

# --- Output ---
OUTPUT_DIR = os.environ.get("DATA_OUTPUT_DIR", "/tmp/ecommerce_data")
OUTPUT_FORMAT = "parquet"  # parquet or json

# --- Product Categories (hierarchical) ---
PRODUCT_CATEGORIES = {
    "Electronics": {
        "subcategories": ["Smartphones", "Laptops", "Tablets", "Headphones", "Cameras", "Smart Home"],
        "price_range": (29.99, 2499.99),
        "weight": 0.25,
    },
    "Clothing": {
        "subcategories": ["Men's Wear", "Women's Wear", "Kids", "Shoes", "Accessories"],
        "price_range": (9.99, 299.99),
        "weight": 0.20,
    },
    "Home & Kitchen": {
        "subcategories": ["Furniture", "Kitchen Appliances", "Bedding", "Decor", "Storage"],
        "price_range": (14.99, 1499.99),
        "weight": 0.15,
    },
    "Books": {
        "subcategories": ["Fiction", "Non-Fiction", "Textbooks", "Comics", "Audiobooks"],
        "price_range": (4.99, 79.99),
        "weight": 0.10,
    },
    "Sports & Outdoors": {
        "subcategories": ["Exercise Equipment", "Outdoor Gear", "Team Sports", "Cycling", "Water Sports"],
        "price_range": (9.99, 999.99),
        "weight": 0.10,
    },
    "Beauty & Health": {
        "subcategories": ["Skincare", "Makeup", "Hair Care", "Supplements", "Personal Care"],
        "price_range": (5.99, 199.99),
        "weight": 0.10,
    },
    "Toys & Games": {
        "subcategories": ["Board Games", "Video Games", "Action Figures", "Educational", "Puzzles"],
        "price_range": (9.99, 299.99),
        "weight": 0.05,
    },
    "Grocery": {
        "subcategories": ["Snacks", "Beverages", "Organic", "Pantry", "Fresh"],
        "price_range": (2.99, 49.99),
        "weight": 0.05,
    },
}

# --- User Demographics ---
AGE_DISTRIBUTION = {
    "18-24": 0.15,
    "25-34": 0.30,
    "35-44": 0.25,
    "45-54": 0.15,
    "55-64": 0.10,
    "65+": 0.05,
}

GENDER_DISTRIBUTION = {"Male": 0.48, "Female": 0.48, "Non-binary": 0.03, "Prefer not to say": 0.01}

US_STATES = [
    "CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI",
    "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI",
    "CO", "MN", "SC", "AL", "LA", "KY", "OR", "OK", "CT", "UT",
]

MEMBERSHIP_TIERS = {"Free": 0.50, "Silver": 0.25, "Gold": 0.15, "Platinum": 0.10}

# --- Clickstream Event Types ---
EVENT_TYPES = {
    "page_view": {"weight": 0.40, "pages": ["home", "category", "product", "search", "cart", "checkout", "account", "deals"]},
    "product_view": {"weight": 0.25, "metadata": ["view_duration_sec", "scroll_depth_pct"]},
    "add_to_cart": {"weight": 0.12, "metadata": ["quantity"]},
    "remove_from_cart": {"weight": 0.03, "metadata": ["quantity"]},
    "search": {"weight": 0.10, "metadata": ["search_query", "results_count"]},
    "add_to_wishlist": {"weight": 0.04, "metadata": ["source_page"]},
    "apply_coupon": {"weight": 0.02, "metadata": ["coupon_code", "discount_pct"]},
    "begin_checkout": {"weight": 0.03, "metadata": ["cart_total", "item_count"]},
    "filter_apply": {"weight": 0.01, "metadata": ["filter_type", "filter_value"]},
}

# --- Device & Channel Distribution ---
DEVICES = {"desktop": 0.35, "mobile": 0.50, "tablet": 0.15}
BROWSERS = {
    "desktop": {"Chrome": 0.60, "Firefox": 0.15, "Safari": 0.15, "Edge": 0.10},
    "mobile": {"Safari Mobile": 0.45, "Chrome Mobile": 0.45, "Samsung Internet": 0.10},
    "tablet": {"Safari": 0.55, "Chrome": 0.35, "Firefox": 0.10},
}
TRAFFIC_SOURCES = {"organic": 0.30, "paid_search": 0.20, "social": 0.15, "email": 0.15, "direct": 0.12, "referral": 0.08}

# --- Payment & Shipping ---
PAYMENT_METHODS = {"credit_card": 0.45, "debit_card": 0.20, "paypal": 0.15, "apple_pay": 0.10, "google_pay": 0.05, "gift_card": 0.05}
SHIPPING_METHODS = {"standard": 0.50, "express": 0.25, "next_day": 0.15, "store_pickup": 0.10}
ORDER_STATUSES = {"completed": 0.82, "processing": 0.05, "shipped": 0.05, "cancelled": 0.05, "returned": 0.03}

# --- Search Queries (for realistic clickstream) ---
SAMPLE_SEARCH_QUERIES = [
    "wireless headphones", "laptop deals", "running shoes", "organic coffee",
    "yoga mat", "smart watch", "winter jacket", "protein powder",
    "gaming keyboard", "air fryer", "standing desk", "vitamin d",
    "bluetooth speaker", "face moisturizer", "hiking boots", "board game",
    "phone case", "water bottle", "backpack", "sunscreen",
]

COUPON_CODES = ["SAVE10", "WELCOME20", "FLASH15", "SUMMER25", "LOYALTY30", "FREESHIP", "VIP40"]

# --- Seasonality Multipliers (by month) ---
MONTHLY_TRAFFIC_MULTIPLIER = {
    1: 0.85, 2: 0.80, 3: 0.90, 4: 0.95, 5: 1.00, 6: 1.05,
    7: 1.10, 8: 1.00, 9: 0.95, 10: 1.05, 11: 1.40, 12: 1.60,  # Holiday spike
}
