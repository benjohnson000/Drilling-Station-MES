"""
Model layer for the MES prototype.

Algorithm:
1. Store orders, machine events, RFID events, machine state logs, and completed cycles in SQLite.
2. Provide simple helper methods for creating, reading, and updating orders.
3. Provide helper methods for dashboard KPIs and recent activity.
4. Keep all database code separate from OPC UA, controller logic, and Flask.
"""

from __future__ import annotations

import random
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any

def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

@dataclass
class Order:
    order_id: Optional[int]
    task_code: int
    quantity: int
    completed_quantity: int
    status: str
    created_at: str
    started_at: Optional[str]
    completed_at: Optional[str]

class MESDatabase:
    VALID_STATUSES = {"Pending", "In Progress", "Completed", "Failed", "Cancelled"}
    VALID_TASK_CODES = {1, 2, 3}

    def __init__(self, db_name: str = "mes_prototype.db") -> None:
        self.conn = sqlite3.connect(db_name, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()

######################### Table Setup #########################

    def _create_tables(self) -> None:
        cursor = self.conn.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                order_id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_code INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                completed_quantity INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS cycles (
                cycle_id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER,
                task_code INTEGER,
                cycle_start TEXT NOT NULL,
                cycle_end TEXT NOT NULL,
                cycle_seconds REAL NOT NULL,
                result TEXT NOT NULL
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS machine_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                event_type TEXT NOT NULL,
                message TEXT NOT NULL
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS machine_state_log (
                state_id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                plc_connected INTEGER NOT NULL,
                operating_mode TEXT,
                await_app INTEGER NOT NULL,
                app_run INTEGER NOT NULL,
                app_done INTEGER NOT NULL,
                release_cmd INTEGER NOT NULL,
                active_order_id INTEGER,
                state_label TEXT NOT NULL
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS rfid_events (
                rfid_event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                order_id INTEGER,
                operation TEXT NOT NULL,
                addr_tag INTEGER NOT NULL,
                length INTEGER NOT NULL,
                raw_data TEXT NOT NULL
            )
            """
        )

        self.conn.commit()

######################### Validate Parameters #########################

    def _validate_task_code(self, task_code: int) -> None:
        if task_code not in self.VALID_TASK_CODES:
            raise ValueError(f"Invalid task code: {task_code}")

    def _validate_quantity(self, quantity: int) -> None:
        if quantity < 1:
            raise ValueError("quantity must be at least 1")

    def _validate_status(self, status: str) -> None:
        if status not in self.VALID_STATUSES:
            raise ValueError(f"Invalid order status: {status}")

######################### Create Orders #########################

    def create_order(self, task_code: int, quantity: int = 1) -> int:
        self._validate_task_code(task_code)
        self._validate_quantity(quantity)

        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT INTO orders (
                task_code, quantity, completed_quantity,
                status, created_at, started_at, completed_at
            )
            VALUES (?, ?, 0, 'Pending', ?, NULL, NULL)
            """,
            (task_code, quantity, now_str()),
        )
        self.conn.commit()
        return int(cursor.lastrowid)

    def generate_random_order(self) -> int:
        task_code = random.choice([1, 2, 3])
        quantity = random.randint(1, 4)
        return self.create_order(task_code, quantity)

    def generate_random_orders(self, count: int) -> List[int]:
        if count < 1:
            raise ValueError("count must be at least 1")

        order_ids = []
        for _ in range(count):
            order_ids.append(self.generate_random_order())
        return order_ids

######################### Get Orders #########################

    def get_order_by_id(self, order_id: int) -> Optional[Order]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM orders WHERE order_id = ?", (order_id,))
        row = cursor.fetchone()
        return self._row_to_order(row) if row else None

    def get_next_pending_order(self) -> Optional[Order]:
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT * FROM orders
            WHERE status IN ('Pending', 'In Progress')
            ORDER BY order_id ASC
            LIMIT 1
            """
        )
        row = cursor.fetchone()
        return self._row_to_order(row) if row else None

    def get_orders_by_status(self, status: str) -> List[Order]:
        self._validate_status(status)

        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT * FROM orders WHERE status = ? ORDER BY order_id DESC",
            (status,),
        )
        return [self._row_to_order(row) for row in cursor.fetchall()]

    def list_orders(self) -> List[Order]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM orders ORDER BY order_id DESC")
        return [self._row_to_order(row) for row in cursor.fetchall()]

    def list_recent_orders(self, limit: int = 20) -> List[Order]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM orders ORDER BY order_id DESC LIMIT ?", (limit,))
        return [self._row_to_order(row) for row in cursor.fetchall()]

######################### Manage Orders #########################

    def start_order_if_needed(self, order_id: int) -> None:
        cursor = self.conn.cursor()
        cursor.execute(
            """
            UPDATE orders
            SET status = CASE WHEN status = 'Pending' THEN 'In Progress' ELSE status END,
                started_at = CASE WHEN started_at IS NULL THEN ? ELSE started_at END
            WHERE order_id = ?
            """,
            (now_str(), order_id),
        )
        self.conn.commit()

    def update_order_status(self, order_id: int, status: str) -> None:
        self._validate_status(status)

        cursor = self.conn.cursor()
        cursor.execute(
            """
            UPDATE orders
            SET status = ?
            WHERE order_id = ?
            """,
            (status, order_id),
        )
        self.conn.commit()

    def mark_one_part_completed(self, order_id: int) -> None:
        order = self.get_order_by_id(order_id)
        if order is None:
            return

        new_completed = min(order.completed_quantity + 1, order.quantity)
        new_status = "Completed" if new_completed >= order.quantity else "Pending"
        completed_at = now_str() if new_status == "Completed" else None

        cursor = self.conn.cursor()
        cursor.execute(
            """
            UPDATE orders
            SET completed_quantity = ?, status = ?, completed_at = ?
            WHERE order_id = ?
            """,
            (new_completed, new_status, completed_at, order_id),
        )
        self.conn.commit()

    def delete_order(self, order_id: int) -> None:
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM orders WHERE order_id = ?", (order_id,))
        self.conn.commit()

    def clear_all_orders(self) -> None:
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM orders")
        self.conn.commit()

######################### Machine Events/Logs/etc. #########################

    def add_cycle(
        self,
        order_id: Optional[int],
        task_code: Optional[int],
        cycle_start: str,
        cycle_end: str,
        cycle_seconds: float,
        result: str = "Completed",
    ) -> None:
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT INTO cycles (order_id, task_code, cycle_start, cycle_end, cycle_seconds, result)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (order_id, task_code, cycle_start, cycle_end, cycle_seconds, result),
        )
        self.conn.commit()

    def add_event(self, event_type: str, message: str) -> None:
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO machine_events (timestamp, event_type, message) VALUES (?, ?, ?)",
            (now_str(), event_type, message),
        )
        self.conn.commit()

    def add_rfid_event(
        self,
        order_id: Optional[int],
        operation: str,
        addr_tag: int,
        length: int,
        raw_data: List[int],
    ) -> None:
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT INTO rfid_events (timestamp, order_id, operation, addr_tag, length, raw_data)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (now_str(), order_id, operation, addr_tag, length, ",".join(str(x) for x in raw_data)),
        )
        self.conn.commit()

    def log_machine_state(self, snapshot: Dict[str, Any]) -> None:
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT INTO machine_state_log
            (timestamp, plc_connected, operating_mode, await_app, app_run, app_done, release_cmd, active_order_id, state_label)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                now_str(),
                1 if snapshot.get("plc_connected") else 0,
                snapshot.get("operating_mode"),
                1 if snapshot.get("await_app") else 0,
                1 if snapshot.get("app_run") else 0,
                1 if snapshot.get("app_done") else 0,
                1 if snapshot.get("release") else 0,
                snapshot.get("active_order_id"),
                snapshot.get("state_label", "Unknown"),
            ),
        )
        self.conn.commit()

    def get_recent_events(self, limit: int = 20) -> List[sqlite3.Row]:
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT * FROM machine_events ORDER BY event_id DESC LIMIT ?",
            (limit,),
        )
        return cursor.fetchall()

    def get_recent_cycles(self, limit: int = 20) -> List[sqlite3.Row]:
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT * FROM cycles ORDER BY cycle_id DESC LIMIT ?",
            (limit,),
        )
        return cursor.fetchall()

    def get_recent_rfid_events(self, limit: int = 20) -> List[sqlite3.Row]:
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT * FROM rfid_events ORDER BY rfid_event_id DESC LIMIT ?",
            (limit,),
        )
        return cursor.fetchall()

    def get_latest_machine_state(self) -> Optional[sqlite3.Row]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM machine_state_log ORDER BY state_id DESC LIMIT 1")
        return cursor.fetchone()

######################### Performance Metrics #########################

    def get_kpis(self) -> Dict[str, Any]:
        cursor = self.conn.cursor()

        cursor.execute("SELECT COUNT(*) AS count FROM orders")
        total_orders = cursor.fetchone()["count"]

        cursor.execute("SELECT COUNT(*) AS count FROM orders WHERE status = 'Completed'")
        completed_orders = cursor.fetchone()["count"]

        cursor.execute("SELECT COUNT(*) AS count FROM orders WHERE status IN ('Pending', 'In Progress')")
        open_orders = cursor.fetchone()["count"]

        cursor.execute("SELECT SUM(quantity) AS total_parts FROM orders")
        total_parts = cursor.fetchone()["total_parts"]

        cursor.execute("SELECT SUM(completed_quantity) AS completed_parts FROM orders")
        completed_parts = cursor.fetchone()["completed_parts"]

        cursor.execute("SELECT AVG(cycle_seconds) AS avg_cycle FROM cycles")
        avg_cycle = cursor.fetchone()["avg_cycle"]

        cursor.execute("SELECT MAX(cycle_seconds) AS max_cycle FROM cycles")
        max_cycle = cursor.fetchone()["max_cycle"]

        cursor.execute("SELECT MIN(cycle_seconds) AS min_cycle FROM cycles")
        min_cycle = cursor.fetchone()["min_cycle"]

        cursor.execute("SELECT COUNT(*) AS count FROM cycles")
        completed_cycles = cursor.fetchone()["count"]

        cursor.execute(
            """
            SELECT COUNT(*) AS count
            FROM machine_events
            WHERE event_type = 'downtime_start'
            """
        )
        downtime_events = cursor.fetchone()["count"]

        cursor.execute(
            """
            SELECT COUNT(*) AS count
            FROM rfid_events
            """
        )
        rfid_event_count = cursor.fetchone()["count"]

        cursor.execute(
            """
            SELECT timestamp
            FROM machine_state_log
            ORDER BY state_id DESC LIMIT 1
            """
        )
        row = cursor.fetchone()
        last_update = row["timestamp"] if row else "No data"

        return {
            "total_orders": total_orders,
            "completed_orders": completed_orders,
            "open_orders": open_orders,
            "total_parts": total_parts or 0,
            "completed_parts": completed_parts or 0,
            "avg_cycle_seconds": round(avg_cycle, 2) if avg_cycle is not None else None,
            "max_cycle_seconds": round(max_cycle, 2) if max_cycle is not None else None,
            "min_cycle_seconds": round(min_cycle, 2) if min_cycle is not None else None,
            "completed_cycles": completed_cycles,
            "downtime_events": downtime_events,
            "rfid_event_count": rfid_event_count,
            "last_update": last_update,
        }

######################### Helpers #########################

    def close(self) -> None:
        self.conn.close()

    def _row_to_order(self, row: sqlite3.Row) -> Order:
        return Order(
            order_id=row["order_id"],
            task_code=row["task_code"],
            quantity=row["quantity"],
            completed_quantity=row["completed_quantity"],
            status=row["status"],
            created_at=row["created_at"],
            started_at=row["started_at"],
            completed_at=row["completed_at"],
        )