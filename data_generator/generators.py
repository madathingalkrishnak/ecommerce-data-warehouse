"""
Core data generators for synthetic e-commerce data.
Produces users, products, clickstream events, and purchase transactions
with realistic distributions, seasonality, and correlations.
"""

import uuid
import random
import hashlib
from datetime import datetime, timedelta
from typing import Generator

import numpy as np
from faker import Faker

from .config import (
    PRODUCT_CATEGORIES, AGE_DISTRIBUTION, GENDER_DISTRIBUTION,
    US_STATES, MEMBERSHIP_TIERS, EVENT_TYPES, DEVICES, BROWSERS,
    TRAFFIC_SOURCES, PAYMENT_METHODS, SHIPPING_METHODS, ORDER_STATUSES,
    SAMPLE_SEARCH_QUERIES, COUPON_CODES, MONTHLY_TRAFFIC_MULTIPLIER,
    NUM_USERS, NUM_PRODUCTS,
)

fake = Faker("en_US")
Faker.seed(42)
np.random.seed(42)
random.seed(42)


def _weighted_choice(options: dict) -> str:
    """Pick a random key from a dict of {option: weight}."""
    keys = list(options.keys())
    weights = [options[k] for k in keys]
    return np.random.choice(keys, p=np.array(weights) / sum(weights))


def _generate_user_id(index: int) -> str:
    """Deterministic user ID from index."""
    return hashlib.md5(f"user_{index}".encode()).hexdigest()[:16]


def _generate_product_id(index: int) -> str:
    """Deterministic product ID from index."""
    return f"PROD-{index:06d}"


# ---------- USER GENERATOR ----------

def generate_users(num_users: int = NUM_USERS) -> list[dict]:
    """Generate synthetic user profiles with demographics."""
    users = []
    age_brackets = list(AGE_DISTRIBUTION.keys())
    age_weights = list(AGE_DISTRIBUTION.values())

    for i in range(num_users):
        age_bracket = np.random.choice(age_brackets, p=age_weights)
        # Map bracket to actual age
        low, high = (int(x.replace("+", "")) for x in age_bracket.split("-")) if "-" in age_bracket else (65, 85)
        age = random.randint(low, high)

        registration_date = fake.date_between(
            start_date=datetime(2020, 1, 1),
            end_date=datetime(2024, 6, 30),
        )

        user = {
            "user_id": _generate_user_id(i),
            "email": fake.unique.email(),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "age": age,
            "age_bracket": age_bracket,
            "gender": _weighted_choice(GENDER_DISTRIBUTION),
            "state": random.choice(US_STATES),
            "city": fake.city(),
            "zip_code": fake.zipcode(),
            "registration_date": registration_date.isoformat(),
            "membership_tier": _weighted_choice(MEMBERSHIP_TIERS),
            "is_active": random.random() < 0.85,
            "preferred_device": _weighted_choice(DEVICES),
            "preferred_payment": _weighted_choice(PAYMENT_METHODS),
        }
        users.append(user)

    return users


# ---------- PRODUCT GENERATOR ----------

def generate_products(num_products: int = NUM_PRODUCTS) -> list[dict]:
    """Generate synthetic product catalog."""
    products = []
    categories = list(PRODUCT_CATEGORIES.keys())
    cat_weights = [PRODUCT_CATEGORIES[c]["weight"] for c in categories]

    for i in range(num_products):
        category = np.random.choice(categories, p=np.array(cat_weights) / sum(cat_weights))
        cat_config = PRODUCT_CATEGORIES[category]
        subcategory = random.choice(cat_config["subcategories"])

        price_low, price_high = cat_config["price_range"]
        # Log-normal distribution for more realistic pricing
        base_price = round(np.random.lognormal(
            mean=np.log((price_low + price_high) / 4),
            sigma=0.6
        ), 2)
        base_price = max(price_low, min(price_high, base_price))

        product = {
            "product_id": _generate_product_id(i),
            "product_name": f"{fake.word().capitalize()} {subcategory} {fake.word().capitalize()}",
            "category": category,
            "subcategory": subcategory,
            "brand": fake.company(),
            "base_price": base_price,
            "current_price": round(base_price * random.uniform(0.8, 1.0), 2),  # Some on sale
            "rating": round(random.uniform(1.0, 5.0), 1),
            "review_count": int(np.random.exponential(scale=50)),
            "in_stock": random.random() < 0.92,
            "weight_kg": round(random.uniform(0.1, 25.0), 2),
            "created_at": fake.date_between(
                start_date=datetime(2022, 1, 1),
                end_date=datetime(2024, 6, 30),
            ).isoformat(),
            "is_prime_eligible": random.random() < 0.70,
        }
        products.append(product)

    return products


# ---------- CLICKSTREAM GENERATOR ----------

def generate_clickstream_events(
    users: list[dict],
    products: list[dict],
    target_date: datetime,
    events_per_day: int,
) -> Generator[dict, None, None]:
    """
    Generate clickstream events for a specific day.
    Yields events one at a time for memory efficiency.
    """
    month = target_date.month
    multiplier = MONTHLY_TRAFFIC_MULTIPLIER.get(month, 1.0)
    adjusted_count = int(events_per_day * multiplier)

    # Hour-of-day distribution (more traffic in evening)
    hour_weights = np.array([
        0.01, 0.005, 0.003, 0.002, 0.003, 0.005,  # 0-5 AM
        0.01, 0.03, 0.05, 0.07, 0.08, 0.08,        # 6-11 AM
        0.07, 0.06, 0.06, 0.06, 0.07, 0.08,        # 12-5 PM
        0.09, 0.10, 0.09, 0.07, 0.04, 0.02,        # 6-11 PM
    ])
    hour_weights = hour_weights / hour_weights.sum()

    active_users = [u for u in users if u["is_active"]]
    product_ids = [p["product_id"] for p in products]
    event_types = list(EVENT_TYPES.keys())
    event_weights = [EVENT_TYPES[e]["weight"] for e in event_types]

    for _ in range(adjusted_count):
        user = random.choice(active_users)
        hour = int(np.random.choice(range(24), p=hour_weights))
        minute = random.randint(0, 59)
        second = random.randint(0, 59)

        event_timestamp = target_date.replace(
            hour=hour, minute=minute, second=second,
            microsecond=random.randint(0, 999999),
        )

        event_type = np.random.choice(event_types, p=np.array(event_weights) / sum(event_weights))
        device = _weighted_choice(DEVICES)

        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "event_timestamp": event_timestamp.isoformat(),
            "user_id": user["user_id"],
            "session_id": hashlib.md5(
                f"{user['user_id']}_{target_date.date()}_{random.randint(1, 5)}".encode()
            ).hexdigest()[:20],
            "device_type": device,
            "browser": _weighted_choice(BROWSERS[device]),
            "traffic_source": _weighted_choice(TRAFFIC_SOURCES),
            "page_url": _build_page_url(event_type, products),
            "referrer_url": fake.url() if random.random() < 0.3 else None,
            "ip_address": fake.ipv4(),
            "user_agent": fake.user_agent(),
        }

        # Add event-specific metadata
        if event_type == "page_view":
            event["page_name"] = random.choice(EVENT_TYPES["page_view"]["pages"])
        elif event_type == "product_view":
            event["product_id"] = random.choice(product_ids)
            event["view_duration_sec"] = round(random.uniform(3, 300), 1)
            event["scroll_depth_pct"] = round(random.uniform(10, 100), 1)
        elif event_type in ("add_to_cart", "remove_from_cart"):
            event["product_id"] = random.choice(product_ids)
            event["quantity"] = random.randint(1, 5)
        elif event_type == "search":
            event["search_query"] = random.choice(SAMPLE_SEARCH_QUERIES)
            event["results_count"] = random.randint(0, 500)
        elif event_type == "add_to_wishlist":
            event["product_id"] = random.choice(product_ids)
        elif event_type == "apply_coupon":
            event["coupon_code"] = random.choice(COUPON_CODES)
            event["discount_pct"] = random.choice([10, 15, 20, 25, 30])
        elif event_type == "begin_checkout":
            event["cart_total"] = round(random.uniform(15, 500), 2)
            event["item_count"] = random.randint(1, 10)
        elif event_type == "filter_apply":
            event["filter_type"] = random.choice(["price", "brand", "rating", "category", "color"])
            event["filter_value"] = fake.word()

        # Date partition keys
        event["year"] = target_date.year
        event["month"] = target_date.month
        event["day"] = target_date.day

        yield event


def _build_page_url(event_type: str, products: list[dict]) -> str:
    """Build a realistic page URL based on event type."""
    base = "https://shop.example.com"
    if event_type == "page_view":
        page = random.choice(["", "/deals", "/categories", "/account", "/cart"])
        return f"{base}{page}"
    elif event_type in ("product_view", "add_to_cart", "add_to_wishlist"):
        product = random.choice(products)
        slug = product["product_name"].lower().replace(" ", "-")
        return f"{base}/product/{slug}"
    elif event_type == "search":
        query = random.choice(SAMPLE_SEARCH_QUERIES).replace(" ", "+")
        return f"{base}/search?q={query}"
    elif event_type == "begin_checkout":
        return f"{base}/checkout"
    return f"{base}/"


# ---------- PURCHASE TRANSACTION GENERATOR ----------

def generate_purchases(
    users: list[dict],
    products: list[dict],
    target_date: datetime,
    purchases_per_day: int,
) -> Generator[dict, None, None]:
    """
    Generate purchase transactions for a specific day.
    Includes line items, shipping, and payment details.
    """
    month = target_date.month
    multiplier = MONTHLY_TRAFFIC_MULTIPLIER.get(month, 1.0)
    adjusted_count = int(purchases_per_day * multiplier)

    active_users = [u for u in users if u["is_active"]]
    in_stock_products = [p for p in products if p["in_stock"]]

    for _ in range(adjusted_count):
        user = random.choice(active_users)
        order_id = f"ORD-{uuid.uuid4().hex[:12].upper()}"

        # Generate 1-6 line items per order
        num_items = np.random.choice([1, 2, 3, 4, 5, 6], p=[0.35, 0.30, 0.18, 0.10, 0.05, 0.02])
        selected_products = random.sample(in_stock_products, min(num_items, len(in_stock_products)))

        line_items = []
        subtotal = 0.0
        for product in selected_products:
            quantity = random.randint(1, 3)
            item_price = product["current_price"]
            line_total = round(item_price * quantity, 2)
            subtotal += line_total

            line_items.append({
                "order_id": order_id,
                "line_item_id": str(uuid.uuid4()),
                "product_id": product["product_id"],
                "quantity": quantity,
                "unit_price": item_price,
                "line_total": line_total,
                "discount_amount": round(line_total * random.choice([0, 0, 0, 0.10, 0.15, 0.20]), 2),
            })

        # Compute order totals
        total_discount = sum(item["discount_amount"] for item in line_items)
        shipping_method = _weighted_choice(SHIPPING_METHODS)
        shipping_cost = {"standard": 5.99, "express": 12.99, "next_day": 24.99, "store_pickup": 0.0}[shipping_method]
        tax_rate = round(random.uniform(0.05, 0.10), 4)
        tax_amount = round((subtotal - total_discount) * tax_rate, 2)
        order_total = round(subtotal - total_discount + shipping_cost + tax_amount, 2)

        hour_probs = np.array([
            0.01, 0.005, 0.003, 0.002, 0.003, 0.005,
            0.015, 0.03, 0.05, 0.07, 0.08, 0.08,
            0.07, 0.06, 0.06, 0.06, 0.07, 0.08,
            0.09, 0.10, 0.08, 0.06, 0.03, 0.015,
        ])
        hour_probs = hour_probs / hour_probs.sum()
        hour = int(np.random.choice(range(24), p=hour_probs))

        order_timestamp = target_date.replace(
            hour=hour,
            minute=random.randint(0, 59),
            second=random.randint(0, 59),
        )

        order = {
            "order_id": order_id,
            "user_id": user["user_id"],
            "order_timestamp": order_timestamp.isoformat(),
            "status": _weighted_choice(ORDER_STATUSES),
            "subtotal": round(subtotal, 2),
            "discount_total": round(total_discount, 2),
            "shipping_cost": shipping_cost,
            "shipping_method": shipping_method,
            "tax_rate": tax_rate,
            "tax_amount": tax_amount,
            "order_total": order_total,
            "payment_method": user.get("preferred_payment", _weighted_choice(PAYMENT_METHODS)),
            "coupon_code": random.choice(COUPON_CODES) if random.random() < 0.15 else None,
            "num_items": len(line_items),
            "line_items": line_items,
            # Partition keys
            "year": target_date.year,
            "month": target_date.month,
            "day": target_date.day,
        }

        yield order
