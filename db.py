import sqlite3
import time

DB_PATH = "orders.db"


def get_connection():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT UNIQUE,
            user TEXT,
            item TEXT,
            quantity INTEGER,
            amount REAL,
            status TEXT,
            created_at REAL,
            updated_at REAL
        )
        """
    )
    conn.commit()
    conn.close()


def create_order(order_id, user, item, quantity, amount, status):
    ts = time.time()
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO orders (order_id, user, item, quantity, amount, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (order_id, user, item, quantity, amount, status, ts, ts),
    )
    conn.commit()
    conn.close()


def update_order_status(order_id, status):
    ts = time.time()
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE orders SET status = ?, updated_at = ? WHERE order_id = ?",
        (status, ts, order_id),
    )
    conn.commit()
    conn.close()


def get_all_orders():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM orders ORDER BY created_at DESC")
    rows = cur.fetchall()
    conn.close()
    return rows
