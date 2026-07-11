#!/usr/bin/env python3
import os
import sys
import time
import sqlite3
import random
import uuid
from datetime import datetime, timezone

# Add project root to sys path
try:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
except NameError:
    sys.path.append("/Volumes/workspace/bronze/raw")
    sys.path.append("/Volumes/workspace/bronze/raw/scripts")
from utility.env_loader import load_env

load_env()

# Load configurations with fallback defaults
DATABASE_PATH = os.getenv("DATABASE_PATH", "data/source_oltp.db")
INTERVAL_SECONDS = float(os.getenv("SIMULATOR_INTERVAL_SECONDS", "5"))
RANDOM_SEED = os.getenv("SIMULATOR_RANDOM_SEED")

if RANDOM_SEED:
    try:
        random.seed(int(RANDOM_SEED))
        print(f"Random seed set to {RANDOM_SEED}")
    except ValueError:
        pass

# Ensure containing directory exists
os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)

# List of mock data items
FIRST_NAMES = ["Alice", "Bob", "Charlie", "Diana", "Ethan", "Fiona", "George", "Hannah", "Ian", "Julia"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
STATES = ["NY", "CA", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"]
STATUSES = ["Pending", "Shipped", "Delivered", "Cancelled"]

def get_db_connection():
    # Setup connection timeout to handle locks gracefully
    conn = sqlite3.connect(DATABASE_PATH, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def get_utc_timestamp():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

def initialize_database():
    print(f"Initializing operational source database at {DATABASE_PATH}...")
    conn = get_db_connection()
    cursor = conn.cursor()

    # Create tables
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        customer_id TEXT PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        state TEXT,
        created_at TEXT,
        updated_at TEXT
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        order_id TEXT PRIMARY KEY,
        customer_id TEXT,
        order_amount REAL,
        order_status TEXT,
        created_at TEXT,
        updated_at TEXT,
        FOREIGN KEY(customer_id) REFERENCES customers(customer_id) ON DELETE SET NULL
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS cdc_event_log (
        _row_sequence_id INTEGER PRIMARY KEY AUTOINCREMENT,
        entity_type TEXT NOT NULL,
        _change_type TEXT NOT NULL,
        _commit_timestamp TEXT NOT NULL,
        customer_id TEXT,
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        state TEXT,
        order_id TEXT,
        order_amount REAL,
        order_status TEXT,
        created_at TEXT,
        updated_at TEXT
    );
    """)

    # Create Triggers for customers
    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS customers_after_insert
    AFTER INSERT ON customers
    BEGIN
        INSERT INTO cdc_event_log (
            entity_type, _change_type, _commit_timestamp,
            customer_id, first_name, last_name, email, state,
            created_at, updated_at
        ) VALUES (
            'customers', 'INSERT', STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'),
            NEW.customer_id, NEW.first_name, NEW.last_name, NEW.email, NEW.state,
            NEW.created_at, NEW.updated_at
        );
    END;
    """)

    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS customers_after_update
    AFTER UPDATE ON customers
    BEGIN
        INSERT INTO cdc_event_log (
            entity_type, _change_type, _commit_timestamp,
            customer_id, first_name, last_name, email, state,
            created_at, updated_at
        ) VALUES (
            'customers', 'UPDATE_BEFORE', STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'),
            OLD.customer_id, OLD.first_name, OLD.last_name, OLD.email, OLD.state,
            OLD.created_at, OLD.updated_at
        );
        INSERT INTO cdc_event_log (
            entity_type, _change_type, _commit_timestamp,
            customer_id, first_name, last_name, email, state,
            created_at, updated_at
        ) VALUES (
            'customers', 'UPDATE_AFTER', STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'),
            NEW.customer_id, NEW.first_name, NEW.last_name, NEW.email, NEW.state,
            NEW.created_at, NEW.updated_at
        );
    END;
    """)

    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS customers_after_delete
    AFTER DELETE ON customers
    BEGIN
        INSERT INTO cdc_event_log (
            entity_type, _change_type, _commit_timestamp,
            customer_id, first_name, last_name, email, state,
            created_at, updated_at
        ) VALUES (
            'customers', 'DELETE', STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'),
            OLD.customer_id, OLD.first_name, OLD.last_name, OLD.email, OLD.state,
            OLD.created_at, OLD.updated_at
        );
    END;
    """)

    # Create Triggers for orders
    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS orders_after_insert
    AFTER INSERT ON orders
    BEGIN
        INSERT INTO cdc_event_log (
            entity_type, _change_type, _commit_timestamp,
            order_id, customer_id, order_amount, order_status,
            created_at, updated_at
        ) VALUES (
            'orders', 'INSERT', STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'),
            NEW.order_id, NEW.customer_id, NEW.order_amount, NEW.order_status,
            NEW.created_at, NEW.updated_at
        );
    END;
    """)

    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS orders_after_update
    AFTER UPDATE ON orders
    BEGIN
        INSERT INTO cdc_event_log (
            entity_type, _change_type, _commit_timestamp,
            order_id, customer_id, order_amount, order_status,
            created_at, updated_at
        ) VALUES (
            'orders', 'UPDATE_BEFORE', STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'),
            OLD.order_id, OLD.customer_id, OLD.order_amount, OLD.order_status,
            OLD.created_at, OLD.updated_at
        );
        INSERT INTO cdc_event_log (
            entity_type, _change_type, _commit_timestamp,
            order_id, customer_id, order_amount, order_status,
            created_at, updated_at
        ) VALUES (
            'orders', 'UPDATE_AFTER', STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'),
            NEW.order_id, NEW.customer_id, NEW.order_amount, NEW.order_status,
            NEW.created_at, NEW.updated_at
        );
    END;
    """)

    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS orders_after_delete
    AFTER DELETE ON orders
    BEGIN
        INSERT INTO cdc_event_log (
            entity_type, _change_type, _commit_timestamp,
            order_id, customer_id, order_amount, order_status,
            created_at, updated_at
        ) VALUES (
            'orders', 'DELETE', STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'),
            OLD.order_id, OLD.customer_id, OLD.order_amount, OLD.order_status,
            OLD.created_at, OLD.updated_at
        );
    END;
    """)

    conn.commit()

    # Pre-populate with initial seeds if empty
    cursor.execute("SELECT COUNT(*) FROM customers;")
    if cursor.fetchone()[0] == 0:
        print("Pre-populating initial mock customers and orders...")
        initial_customers = []
        for _ in range(5):
            c_id = str(uuid.uuid4())
            f_name = random.choice(FIRST_NAMES)
            l_name = random.choice(LAST_NAMES)
            email = f"{f_name.lower()}.{l_name.lower()}@example.com"
            state = random.choice(STATES)
            ts = get_utc_timestamp()
            cursor.execute(
                "INSERT INTO customers VALUES (?, ?, ?, ?, ?, ?, ?);",
                (c_id, f_name, l_name, email, state, ts, ts)
            )
            initial_customers.append(c_id)
        
        for _ in range(5):
            o_id = str(uuid.uuid4())
            c_id = random.choice(initial_customers)
            amount = round(random.uniform(10.0, 500.0), 2)
            status = "Pending"
            ts = get_utc_timestamp()
            cursor.execute(
                "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?);",
                (o_id, c_id, amount, status, ts, ts)
            )
        conn.commit()
    conn.close()

def generate_random_mutation():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Find existing customers and orders
    cursor.execute("SELECT customer_id FROM customers;")
    customers = [r[0] for r in cursor.fetchall()]

    cursor.execute("SELECT order_id, customer_id, order_status FROM orders;")
    orders = [r for r in cursor.fetchall()]

    mutation_type = random.choice([
        "INSERT_CUSTOMER", "UPDATE_CUSTOMER", "DELETE_CUSTOMER",
        "INSERT_ORDER", "UPDATE_ORDER", "DELETE_ORDER"
    ])

    ts = get_utc_timestamp()

    try:
        if mutation_type == "INSERT_CUSTOMER":
            c_id = str(uuid.uuid4())
            f_name = random.choice(FIRST_NAMES)
            l_name = random.choice(LAST_NAMES)
            email = f"{f_name.lower()}.{l_name.lower()}@example.com"
            state = random.choice(STATES)
            cursor.execute(
                "INSERT INTO customers VALUES (?, ?, ?, ?, ?, ?, ?);",
                (c_id, f_name, l_name, email, state, ts, ts)
            )
            print(f"[{ts}] Simulated Mutation: INSERT CUSTOMER {c_id} ({f_name} {l_name}, {state})")

        elif mutation_type == "UPDATE_CUSTOMER" and customers:
            c_id = random.choice(customers)
            # Randomly update email or state
            choice = random.choice(["email", "state"])
            if choice == "email":
                cursor.execute("SELECT first_name, last_name FROM customers WHERE customer_id = ?;", (c_id,))
                fn, ln = cursor.fetchone()
                new_email = f"{fn.lower()}.{ln.lower()}{random.randint(10,99)}@example.com"
                cursor.execute("UPDATE customers SET email = ?, updated_at = ? WHERE customer_id = ?;", (new_email, ts, c_id))
                print(f"[{ts}] Simulated Mutation: UPDATE CUSTOMER {c_id} set email = {new_email}")
            else:
                new_state = random.choice(STATES)
                cursor.execute("UPDATE customers SET state = ?, updated_at = ? WHERE customer_id = ?;", (new_state, ts, c_id))
                print(f"[{ts}] Simulated Mutation: UPDATE CUSTOMER {c_id} set state = {new_state}")

        elif mutation_type == "DELETE_CUSTOMER" and customers:
            # Pick a customer to delete
            c_id = random.choice(customers)
            cursor.execute("DELETE FROM customers WHERE customer_id = ?;", (c_id,))
            print(f"[{ts}] Simulated Mutation: DELETE CUSTOMER {c_id}")

        elif mutation_type == "INSERT_ORDER" and customers:
            o_id = str(uuid.uuid4())
            c_id = random.choice(customers)
            amount = round(random.uniform(5.0, 1000.0), 2)
            status = "Pending"
            cursor.execute(
                "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?);",
                (o_id, c_id, amount, status, ts, ts)
            )
            print(f"[{ts}] Simulated Mutation: INSERT ORDER {o_id} for Customer {c_id} of amount ${amount}")

        elif mutation_type == "UPDATE_ORDER" and orders:
            o_id, c_id, status = random.choice(orders)
            # Progress status
            if status == "Pending":
                next_status = random.choice(["Shipped", "Cancelled"])
            elif status == "Shipped":
                next_status = "Delivered"
            else:
                # Delievered/Cancelled cannot progress, just change amount slightly
                next_status = status
            
            if next_status != status:
                cursor.execute("UPDATE orders SET order_status = ?, updated_at = ? WHERE order_id = ?;", (next_status, ts, o_id))
                print(f"[{ts}] Simulated Mutation: UPDATE ORDER {o_id} status progress {status} -> {next_status}")
            else:
                new_amount = round(random.uniform(5.0, 1000.0), 2)
                cursor.execute("UPDATE orders SET order_amount = ?, updated_at = ? WHERE order_id = ?;", (new_amount, ts, o_id))
                print(f"[{ts}] Simulated Mutation: UPDATE ORDER {o_id} amount modified to ${new_amount}")

        elif mutation_type == "DELETE_ORDER" and orders:
            o_id, _, _ = random.choice(orders)
            cursor.execute("DELETE FROM orders WHERE order_id = ?;", (o_id,))
            print(f"[{ts}] Simulated Mutation: DELETE ORDER {o_id}")

        conn.commit()
    except sqlite3.OperationalError as e:
        print(f"SQLite lock/concurrency error: {e}. Retrying next interval...")
    finally:
        conn.close()

def main():
    initialize_database()
    print("Simulation workload engine is running. Press Ctrl+C to terminate.")
    try:
        while True:
            # Perform 1 to 5 random mutations per cycle
            num_mutations = random.randint(1, 5)
            for _ in range(num_mutations):
                generate_random_mutation()
            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nSimulation workload engine stopped.")
        sys.exit(0)

if __name__ == "__main__":
    main()
