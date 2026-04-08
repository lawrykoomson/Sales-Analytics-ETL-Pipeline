"""
Sales Data Generator
====================
Creates a realistic SQLite sales database with 5 related tables.
This is the DATA SOURCE that the ETL pipeline reads from.

Tables created:
    customers   — 500 Ghana retail customers
    products    — 100 products across 6 categories
    salespersons — 20 sales staff
    orders      — 50,000 sales orders
    order_items — line items per order

Author: Lawrence Koomson
GitHub: github.com/lawrykoomson
"""

import sqlite3
import random
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

DB_PATH = Path("data/raw/sales_database.db")

# Ghana regions and cities
GHANA_REGIONS = {
    "Greater Accra": ["Accra", "Tema", "Kasoa", "Madina", "Achimota"],
    "Ashanti":       ["Kumasi", "Obuasi", "Ejisu", "Konongo"],
    "Western":       ["Takoradi", "Tarkwa", "Axim", "Prestea"],
    "Eastern":       ["Koforidua", "Nkawkaw", "Suhum", "Akim Oda"],
    "Central":       ["Cape Coast", "Winneba", "Assin Fosu", "Mankessim"],
    "Northern":      ["Tamale", "Yendi", "Savelugu"],
    "Volta":         ["Ho", "Hohoe", "Keta", "Aflao"],
}

PRODUCT_CATEGORIES = {
    "Electronics":    [("Smartphone", 800, 2500), ("Laptop", 2500, 6000),
                       ("Tablet", 600, 1800), ("Earbuds", 80, 350),
                       ("Smart Watch", 300, 900), ("Power Bank", 60, 200),
                       ("Router", 150, 450), ("USB Hub", 40, 120)],
    "Food & Grocery": [("Rice (50kg)", 380, 420), ("Cooking Oil (5L)", 85, 110),
                       ("Sugar (50kg)", 320, 380), ("Flour (50kg)", 260, 310),
                       ("Canned Tomatoes", 8, 15), ("Milo (1kg)", 45, 65)],
    "Clothing":       [("Men's Shirt", 80, 250), ("Women's Dress", 120, 400),
                       ("Jeans", 150, 350), ("T-Shirt", 40, 120),
                       ("Sneakers", 180, 600), ("Sandals", 60, 180)],
    "Home & Kitchen": [("Blender", 180, 450), ("Rice Cooker", 220, 550),
                       ("Frying Pan", 80, 200), ("Bed Sheet Set", 150, 400),
                       ("Water Dispenser", 400, 900), ("Fan", 180, 450)],
    "Health & Beauty": [("Body Lotion", 25, 80), ("Hair Relaxer", 30, 90),
                        ("Face Cream", 35, 120), ("Perfume", 80, 350),
                        ("Vitamins", 45, 150), ("Sanitizer", 15, 45)],
    "Stationery":     [("Notebook (Pack)", 25, 60), ("Pen Set", 15, 45),
                       ("Printer Paper (Ream)", 55, 85), ("Calculator", 40, 150),
                       ("Stapler", 20, 60), ("File Folders", 10, 30)],
}

PAYMENT_METHODS = ["Mobile Money", "Cash", "Bank Transfer", "POS Card", "Credit"]
ORDER_STATUSES  = ["Completed", "Completed", "Completed", "Returned", "Pending"]
SALESPERSON_NAMES = [
    "Kwame Asante", "Abena Mensah", "Kofi Boateng", "Akosua Darko",
    "Yaw Amoah", "Ama Owusu", "Kweku Adjei", "Efua Ansah",
    "Kojo Frimpong", "Adwoa Sarpong", "Nana Acheampong", "Araba Quaye",
    "Fiifi Turkson", "Maame Asare", "Kwabena Ofori", "Akua Bonsu",
    "Kwadwo Appiah", "Esi Tetteh", "Yoofi Agyeman", "Abla Dziedzic",
]


def create_database():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    print("Creating sales database...")

    # ── CUSTOMERS TABLE
    cur.execute("DROP TABLE IF EXISTS customers")
    cur.execute("""
        CREATE TABLE customers (
            customer_id   INTEGER PRIMARY KEY,
            first_name    TEXT NOT NULL,
            last_name     TEXT NOT NULL,
            email         TEXT UNIQUE,
            phone         TEXT,
            region        TEXT,
            city          TEXT,
            gender        TEXT,
            age           INTEGER,
            joined_date   TEXT
        )
    """)

    first_names_m = ["Kwame","Kofi","Yaw","Kweku","Kojo","Kwabena","Kwadwo","Fiifi",
                     "Nana","Yoofi","Ato","Dela","Edem","Sena","Selorm"]
    first_names_f = ["Abena","Akosua","Ama","Efua","Adwoa","Akua","Araba","Maame",
                     "Esi","Abla","Afua","Afia","Ewurama","Adjoa","Akua"]
    last_names    = ["Mensah","Asante","Boateng","Darko","Amoah","Owusu","Adjei",
                     "Ansah","Frimpong","Sarpong","Tetteh","Quaye","Ofori","Bonsu",
                     "Appiah","Acheampong","Turkson","Asare","Agyeman","Dziedzic"]

    random.seed(42)
    customers = []
    for i in range(1, 501):
        gender = random.choice(["M", "F"])
        fname  = random.choice(first_names_m if gender == "M" else first_names_f)
        lname  = random.choice(last_names)
        region = random.choice(list(GHANA_REGIONS.keys()))
        city   = random.choice(GHANA_REGIONS[region])
        joined = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1460))
        customers.append((
            i, fname, lname,
            f"{fname.lower()}.{lname.lower()}{i}@gmail.com",
            f"02{random.choice(['4','5','3'])}{random.randint(1000000,9999999)}",
            region, city, gender,
            random.randint(18, 65),
            joined.strftime("%Y-%m-%d")
        ))
    cur.executemany("INSERT INTO customers VALUES (?,?,?,?,?,?,?,?,?,?)", customers)
    print(f"  ✓ {len(customers)} customers created")

    # ── PRODUCTS TABLE
    cur.execute("DROP TABLE IF EXISTS products")
    cur.execute("""
        CREATE TABLE products (
            product_id    INTEGER PRIMARY KEY,
            product_name  TEXT NOT NULL,
            category      TEXT,
            unit_price    REAL,
            cost_price    REAL,
            stock_qty     INTEGER
        )
    """)

    products = []
    pid = 1
    for category, items in PRODUCT_CATEGORIES.items():
        for name, min_p, max_p in items:
            price = round(random.uniform(min_p, max_p), 2)
            cost  = round(price * random.uniform(0.55, 0.75), 2)
            stock = random.randint(20, 500)
            products.append((pid, name, category, price, cost, stock))
            pid += 1
    cur.executemany("INSERT INTO products VALUES (?,?,?,?,?,?)", products)
    print(f"  ✓ {len(products)} products created")

    # ── SALESPERSONS TABLE
    cur.execute("DROP TABLE IF EXISTS salespersons")
    cur.execute("""
        CREATE TABLE salespersons (
            salesperson_id  INTEGER PRIMARY KEY,
            full_name       TEXT NOT NULL,
            region          TEXT,
            hire_date       TEXT,
            base_salary_ghs REAL
        )
    """)

    regions_list = list(GHANA_REGIONS.keys())
    salespersons = []
    for i, name in enumerate(SALESPERSON_NAMES, 1):
        hired = datetime(2019, 1, 1) + timedelta(days=random.randint(0, 1460))
        salespersons.append((
            i, name,
            regions_list[i % len(regions_list)],
            hired.strftime("%Y-%m-%d"),
            round(random.uniform(1800, 4500), 2)
        ))
    cur.executemany("INSERT INTO salespersons VALUES (?,?,?,?,?)", salespersons)
    print(f"  ✓ {len(salespersons)} salespersons created")

    # ── ORDERS TABLE
    cur.execute("DROP TABLE IF EXISTS orders")
    cur.execute("""
        CREATE TABLE orders (
            order_id        INTEGER PRIMARY KEY,
            customer_id     INTEGER,
            salesperson_id  INTEGER,
            order_date      TEXT,
            order_status    TEXT,
            payment_method  TEXT,
            delivery_region TEXT,
            FOREIGN KEY (customer_id)    REFERENCES customers(customer_id),
            FOREIGN KEY (salesperson_id) REFERENCES salespersons(salesperson_id)
        )
    """)

    orders = []
    for i in range(1, 50001):
        order_date = datetime(2023, 1, 1) + timedelta(
            days=random.randint(0, 730),
            hours=random.randint(7, 21),
            minutes=random.randint(0, 59)
        )
        region = random.choice(list(GHANA_REGIONS.keys()))
        orders.append((
            i,
            random.randint(1, 500),
            random.randint(1, 20),
            order_date.strftime("%Y-%m-%d %H:%M:%S"),
            random.choices(ORDER_STATUSES, weights=[70, 70, 70, 10, 5])[0],
            random.choice(PAYMENT_METHODS),
            region
        ))
    cur.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?)", orders)
    print(f"  ✓ {len(orders):,} orders created")

    # ── ORDER ITEMS TABLE
    cur.execute("DROP TABLE IF EXISTS order_items")
    cur.execute("""
        CREATE TABLE order_items (
            item_id     INTEGER PRIMARY KEY,
            order_id    INTEGER,
            product_id  INTEGER,
            quantity    INTEGER,
            unit_price  REAL,
            discount_pct REAL,
            FOREIGN KEY (order_id)   REFERENCES orders(order_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        )
    """)

    items = []
    iid   = 1
    for order_id in range(1, 50001):
        num_items = random.choices([1, 2, 3, 4], weights=[50, 30, 15, 5])[0]
        chosen    = random.sample(range(1, pid), min(num_items, pid-1))
        for product_id in chosen:
            product   = products[product_id - 1]
            unit_price = product[3]
            discount   = random.choices([0, 5, 10, 15], weights=[60, 20, 15, 5])[0]
            qty        = random.randint(1, 5)
            items.append((iid, order_id, product_id, qty, unit_price, discount))
            iid += 1
    cur.executemany("INSERT INTO order_items VALUES (?,?,?,?,?,?)", items)
    print(f"  ✓ {len(items):,} order line items created")

    conn.commit()
    conn.close()

    print(f"\n✅ Sales database created at: {DB_PATH}")
    print(f"   Total records: {500 + len(products) + 20 + 50000 + len(items):,}")
    return str(DB_PATH)


if __name__ == "__main__":
    create_database()