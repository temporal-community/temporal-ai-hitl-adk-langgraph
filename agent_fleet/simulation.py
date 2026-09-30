"""
Shared simulation state for the Meltdown ice cream delivery demo.

Manages driver positions, order status, and agent events.
Backed by SQLite WAL mode for cross-process sharing between the
Temporal worker and FastAPI server.
"""

from __future__ import annotations

import time
from typing import Any

import aiosqlite

from agent_fleet.config import FLEET_DB_PATH
from agent_fleet.locations import WAREHOUSE
from agent_fleet.models import (
    Coords,
    Driver,
    DriverStatus,
    Order,
    OrderPriority,
    OrderStatus,
)

# Orders per driver, as shown by the Fleet tool (x/2). Keep in sync with DRIVER_CAPACITY
# in workflows.py, which enforces it (kept literal here to avoid importing the heavy
# workflows module into FleetState). The schema default below matches it too.
DRIVER_CAPACITY = 2

_SCHEMA = """\
CREATE TABLE IF NOT EXISTS drivers (
    driver_id TEXT PRIMARY KEY,
    lat REAL NOT NULL,
    lng REAL NOT NULL,
    battery_pct REAL NOT NULL DEFAULT 100.0,
    status TEXT NOT NULL DEFAULT 'idle',
    capacity INTEGER NOT NULL DEFAULT 2,
    disconnected INTEGER NOT NULL DEFAULT 0,
    recovering INTEGER NOT NULL DEFAULT 0,
    status_before_disconnect TEXT NOT NULL DEFAULT 'idle',
    warmup_hidden INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS driver_orders (
    driver_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    PRIMARY KEY (driver_id, order_id)
);

CREATE TABLE IF NOT EXISTS driver_path_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    driver_id TEXT NOT NULL,
    lat REAL NOT NULL,
    lng REAL NOT NULL,
    t REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    hotel TEXT NOT NULL,
    label TEXT NOT NULL,
    priority TEXT NOT NULL DEFAULT 'standard',
    servings INTEGER NOT NULL DEFAULT 1,
    delivery_lat REAL NOT NULL,
    delivery_lng REAL NOT NULL,
    assigned_driver_id TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    deadline_minutes INTEGER NOT NULL DEFAULT 45,
    degraded INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS order_status_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL,
    message TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_name TEXT NOT NULL,
    event_type TEXT NOT NULL,
    content TEXT NOT NULL,
    timestamp REAL NOT NULL,
    summary TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS event_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    t REAL NOT NULL,
    msg TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_health (
    agent_name TEXT PRIMARY KEY,
    online INTEGER NOT NULL DEFAULT 1
)
"""


class FleetState:
    """Fleet state backed by SQLite WAL for cross-process sharing.

    In production, this would be Redis or Postgres. SQLite with WAL mode
    gives the same pattern (separate reader/writer processes) with zero
    infrastructure for the demo.
    """

    def __init__(self) -> None:
        self._conn: aiosqlite.Connection | None = None
        self._db_path = FLEET_DB_PATH

    async def _get_conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            self._conn = await aiosqlite.connect(self._db_path)
            self._conn.row_factory = aiosqlite.Row
            await self._conn.execute("PRAGMA journal_mode=WAL")
            await self._conn.execute("PRAGMA synchronous=NORMAL")
            await self._conn.execute("PRAGMA busy_timeout=5000")
            await self._create_tables()
            await self._seed_initial_state()
        return self._conn

    async def _create_tables(self) -> None:
        conn = self._conn
        for statement in _SCHEMA.split(";"):
            s = statement.strip()
            if s:
                await conn.execute(s)
        await conn.commit()

    async def _seed_initial_state(self) -> None:
        conn = self._conn
        # Keep in sync with DRIVER_IDS in workflows.py (kept literal here to avoid importing
        # the heavy workflows module into FleetState). 4 drivers — the deliberate squeeze.
        for letter in ["a", "b", "c", "d"]:
            did = f"driver-{letter}"
            # Explicit capacity: an older fleet.db may still carry the old DEFAULT 3, and
            # CREATE TABLE IF NOT EXISTS won't change it.
            await conn.execute(
                "INSERT OR IGNORE INTO drivers (driver_id, lat, lng, capacity) VALUES (?, ?, ?, ?)",
                (did, WAREHOUSE.lat, WAREHOUSE.lng, DRIVER_CAPACITY),
            )
        # INSERT OR IGNORE skips existing rows, so also migrate rows an older version seeded.
        # Nothing changes capacity at runtime, so this only rewrites stale rows.
        await conn.execute(
            "UPDATE drivers SET capacity = ? WHERE capacity != ?",
            (DRIVER_CAPACITY, DRIVER_CAPACITY),
        )
        for agent in ("fleet_agent", "customer_agent", "resolver"):
            await conn.execute(
                "INSERT OR IGNORE INTO agent_health (agent_name, online) VALUES (?, 1)",
                (agent,),
            )
        await conn.commit()

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def reset(self) -> None:
        """Reset simulation to initial state for a fresh demo run."""
        conn = await self._get_conn()
        for table in [
            "driver_path_history",
            "driver_orders",
            "order_status_log",
            "agent_events",
            "event_log",
            "orders",
            "drivers",
            "agent_health",
        ]:
            await conn.execute(f"DELETE FROM {table}")  # noqa: S608
        await conn.commit()
        await self._seed_initial_state()

    # --- Warmup visibility ---

    async def set_drivers_warmup_hidden(self, driver_ids: list[str], hidden: bool = True) -> None:
        """Hide/show drivers during warmup phase."""
        conn = await self._get_conn()
        for did in driver_ids:
            await conn.execute(
                "UPDATE drivers SET warmup_hidden=? WHERE driver_id=?",
                (1 if hidden else 0, did),
            )
        await conn.commit()

    # --- Per-driver disconnect / reconnect ---

    async def disconnect_driver(self, driver_id: str) -> None:
        """Mark a single driver as disconnected (UI projection only)."""
        conn = await self._get_conn()
        await conn.execute(
            "UPDATE drivers SET status_before_disconnect=status, disconnected=1, "
            "status='disconnected' WHERE driver_id=?",
            (driver_id,),
        )
        await self._log_event(conn, f"[DISCONNECT] Driver {driver_id} lost connection")
        await conn.commit()

    async def reconnect_driver(self, driver_id: str) -> None:
        """Clear disconnect flag and enter per-driver recovery phase (UI projection only)."""
        conn = await self._get_conn()
        await conn.execute(
            "UPDATE drivers SET disconnected=0, recovering=1, "
            "status=status_before_disconnect WHERE driver_id=?",
            (driver_id,),
        )
        await self._log_event(conn, f"[RECONNECT] Driver {driver_id} reconnecting — replaying...")
        await conn.commit()

    async def mark_driver_recovery_complete(self, driver_id: str) -> None:
        """Clear the per-driver recovery flag after replay completes."""
        conn = await self._get_conn()
        await conn.execute(
            "UPDATE drivers SET recovering=0 WHERE driver_id=?",
            (driver_id,),
        )
        await self._log_event(conn, f"[RECONNECT] Driver {driver_id} replay complete — resumed")
        await conn.commit()

    async def is_driver_disconnected(self, driver_id: str) -> bool:
        conn = await self._get_conn()
        async with conn.execute(
            "SELECT disconnected FROM drivers WHERE driver_id=?", (driver_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return bool(row["disconnected"]) if row else False

    # --- Per-agent health ---

    async def disconnect_agent(self, agent_name: str) -> None:
        """Mark a specific agent as offline."""
        conn = await self._get_conn()
        await conn.execute("UPDATE agent_health SET online=0 WHERE agent_name=?", (agent_name,))
        await self._log_event(conn, f"[AGENT OFFLINE] {agent_name} disconnected")
        await conn.commit()

    async def reconnect_agent(self, agent_name: str) -> None:
        """Bring a specific agent back online (UI projection only)."""
        conn = await self._get_conn()
        await conn.execute("UPDATE agent_health SET online=1 WHERE agent_name=?", (agent_name,))
        await self._log_event(conn, f"[AGENT ONLINE] {agent_name} reconnected")
        await conn.commit()

    async def is_agent_online(self, agent_name: str) -> bool:
        conn = await self._get_conn()
        async with conn.execute(
            "SELECT online FROM agent_health WHERE agent_name=?", (agent_name,)
        ) as cursor:
            row = await cursor.fetchone()
            return bool(row["online"]) if row else True

    async def is_agent_disconnected(self, agent_name: str) -> bool:
        return not await self.is_agent_online(agent_name)

    async def get_agent_health(self) -> dict[str, bool]:
        conn = await self._get_conn()
        result: dict[str, bool] = {}
        async with conn.execute("SELECT * FROM agent_health") as cursor:
            async for row in cursor:
                result[row["agent_name"]] = bool(row["online"])
        return result

    # --- Driver operations ---

    async def update_driver_position(self, driver_id: str, lat: float, lng: float) -> None:
        conn = await self._get_conn()
        await conn.execute(
            "UPDATE drivers SET lat=?, lng=? WHERE driver_id=?", (lat, lng, driver_id)
        )
        await conn.execute(
            "INSERT INTO driver_path_history (driver_id, lat, lng, t) VALUES (?, ?, ?, ?)",
            (driver_id, lat, lng, time.time()),
        )
        await conn.commit()

    async def set_driver_status(self, driver_id: str, status: DriverStatus) -> None:
        conn = await self._get_conn()
        await conn.execute(
            "UPDATE drivers SET status=? WHERE driver_id=?", (status.value, driver_id)
        )
        await self._log_event(conn, f"Driver {driver_id} -> {status.value}")
        await conn.commit()

    async def clear_driver_path_history(self, driver_id: str) -> None:
        """Clear path history for a driver — called at start of each delivery leg."""
        conn = await self._get_conn()
        await conn.execute("DELETE FROM driver_path_history WHERE driver_id=?", (driver_id,))
        await conn.commit()

    async def get_driver_position(self, driver_id: str) -> tuple[float, float]:
        conn = await self._get_conn()
        async with conn.execute(
            "SELECT lat, lng FROM drivers WHERE driver_id=?", (driver_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return (row["lat"], row["lng"])

    async def driver_exists(self, driver_id: str) -> bool:
        conn = await self._get_conn()
        async with conn.execute("SELECT 1 FROM drivers WHERE driver_id=?", (driver_id,)) as cursor:
            row = await cursor.fetchone()
            return row is not None

    async def get_driver(self, driver_id: str) -> Driver | None:
        conn = await self._get_conn()
        async with conn.execute("SELECT * FROM drivers WHERE driver_id=?", (driver_id,)) as cursor:
            row = await cursor.fetchone()
            if row is None:
                return None

        # Current orders
        orders: list[str] = []
        async with conn.execute(
            "SELECT order_id FROM driver_orders WHERE driver_id=?", (driver_id,)
        ) as cursor:
            async for orow in cursor:
                orders.append(orow["order_id"])

        # Path history
        path: list[dict[str, float]] = []
        async with conn.execute(
            "SELECT lat, lng, t FROM driver_path_history WHERE driver_id=? ORDER BY id",
            (driver_id,),
        ) as cursor:
            async for prow in cursor:
                path.append({"lat": prow["lat"], "lng": prow["lng"], "t": prow["t"]})

        return Driver(
            driver_id=row["driver_id"],
            position=Coords(lat=row["lat"], lng=row["lng"]),
            battery_pct=row["battery_pct"],
            status=DriverStatus(row["status"]),
            capacity=row["capacity"],
            current_orders=orders,
            path_history=path,
            disconnected=bool(row["disconnected"]),
            recovering=bool(row["recovering"]),
            status_before_disconnect=DriverStatus(row["status_before_disconnect"]),
        )

    async def get_all_drivers(self) -> list[Driver]:
        """Get all drivers with their current state."""
        conn = await self._get_conn()
        drivers = []
        async with conn.execute("SELECT * FROM drivers") as cursor:
            async for row in cursor:
                drivers.append(
                    Driver(
                        driver_id=row["driver_id"],
                        position=Coords(lat=row["lat"], lng=row["lng"]),
                        battery_pct=row["battery_pct"],
                        status=DriverStatus(row["status"]),
                        capacity=row["capacity"],
                        current_orders=[],
                        path_history=[],
                        disconnected=bool(row["disconnected"]),
                        recovering=bool(row["recovering"]),
                        status_before_disconnect=DriverStatus(row["status_before_disconnect"]),
                    )
                )
        return drivers

    async def get_order(self, order_id: str) -> Order | None:
        conn = await self._get_conn()
        async with conn.execute("SELECT * FROM orders WHERE order_id=?", (order_id,)) as cursor:
            row = await cursor.fetchone()
            if row is None:
                return None

        # Status log
        status_log: list[str] = []
        async with conn.execute(
            "SELECT message FROM order_status_log WHERE order_id=? ORDER BY id", (order_id,)
        ) as cursor:
            async for slrow in cursor:
                status_log.append(slrow["message"])

        return Order(
            order_id=row["order_id"],
            hotel=row["hotel"],
            label=row["label"],
            priority=OrderPriority(row["priority"]),
            servings=row["servings"],
            delivery_coords=Coords(lat=row["delivery_lat"], lng=row["delivery_lng"]),
            assigned_driver_id=row["assigned_driver_id"],
            status=OrderStatus(row["status"]),
            deadline_minutes=row["deadline_minutes"],
            status_log=status_log,
        )

    # --- Order operations ---

    async def register_order(
        self,
        order_id: str,
        hotel: str,
        label: str,
        priority: str,
        servings: int,
        delivery_coords: Coords,
        deadline_minutes: int,
    ) -> None:
        """Register a new dynamically-generated order."""
        conn = await self._get_conn()
        cursor = await conn.execute(
            "INSERT OR IGNORE INTO orders "
            "(order_id, hotel, label, priority, servings, delivery_lat, delivery_lng, "
            "deadline_minutes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                order_id,
                hotel,
                label,
                priority,
                servings,
                delivery_coords.lat,
                delivery_coords.lng,
                deadline_minutes,
            ),
        )
        if cursor.rowcount > 0:
            await self._log_event(conn, f"New order {order_id}: {label}")
        await conn.commit()

    async def assign_order_to_driver(
        self, driver_id: str, order_id: str, *, degraded: bool = False
    ) -> bool:
        """Assign a single order to a driver.

        Won't overwrite terminal states (CANCELLED/DELIVERED).
        """
        conn = await self._get_conn()
        cursor = await conn.execute(
            "UPDATE orders SET assigned_driver_id=?, status=?, degraded=? "
            "WHERE order_id=? AND status NOT IN (?, ?)",
            (
                driver_id,
                OrderStatus.ASSIGNED.value,
                1 if degraded else 0,
                order_id,
                OrderStatus.CANCELLED.value,
                OrderStatus.DELIVERED.value,
            ),
        )
        if cursor.rowcount > 0:
            # Clear any prior owner first so a re-assignment (e.g. the ADK re-reason flow)
            # can't leave the order owned by two drivers in the join table.
            await conn.execute(
                "DELETE FROM driver_orders WHERE order_id=? AND driver_id<>?",
                (order_id, driver_id),
            )
            await conn.execute(
                "INSERT OR IGNORE INTO driver_orders (driver_id, order_id) VALUES (?, ?)",
                (driver_id, order_id),
            )
            msg = f"Assigned to {driver_id}"
            if degraded:
                msg += " (degraded — no fleet visibility)"
            await conn.execute(
                "INSERT INTO order_status_log (order_id, message) VALUES (?, ?)",
                (order_id, msg),
            )
            await self._log_event(conn, f"Order {order_id} assigned to {driver_id}")
        await conn.commit()
        return cursor.rowcount > 0

    # Terminal states — once an order reaches these, no other status can overwrite
    _TERMINAL_STATUSES = {OrderStatus.CANCELLED.value, OrderStatus.DELIVERED.value}

    async def update_order_status(self, order_id: str, status: OrderStatus, note: str = "") -> None:
        conn = await self._get_conn()
        # Terminal states can only be set explicitly, never overwritten
        if status.value not in self._TERMINAL_STATUSES:
            cursor = await conn.execute(
                "UPDATE orders SET status=? WHERE order_id=? AND status NOT IN (?, ?)",
                (status.value, order_id, OrderStatus.CANCELLED.value, OrderStatus.DELIVERED.value),
            )
            # Only log if status actually changed (avoids phantom entries
            # for cancelled/delivered orders whose activities are still running)
            if cursor.rowcount > 0:
                if note:
                    await conn.execute(
                        "INSERT INTO order_status_log (order_id, message) VALUES (?, ?)",
                        (order_id, note),
                    )
                msg = f"Order {order_id} -> {status.value}" + (f": {note}" if note else "")
                await self._log_event(conn, msg)
        else:
            await conn.execute(
                "UPDATE orders SET status=? WHERE order_id=?",
                (status.value, order_id),
            )
            if note:
                await conn.execute(
                    "INSERT INTO order_status_log (order_id, message) VALUES (?, ?)",
                    (order_id, note),
                )
            msg = f"Order {order_id} -> {status.value}" + (f": {note}" if note else "")
            await self._log_event(conn, msg)
        await conn.commit()

    async def complete_order_delivery(self, driver_id: str, order_id: str) -> tuple[bool, int]:
        """Mark an order delivered and remove it from the driver's active queue.

        Returns (delivered, remaining_count). delivered=False when cancel won
        the race (order already terminal).
        """
        conn = await self._get_conn()
        # Atomic: only set DELIVERED if not already terminal
        cursor = await conn.execute(
            "UPDATE orders SET status=? WHERE order_id=? AND status NOT IN (?, ?)",
            (
                OrderStatus.DELIVERED.value,
                order_id,
                OrderStatus.CANCELLED.value,
                OrderStatus.DELIVERED.value,
            ),
        )
        delivered = cursor.rowcount > 0
        if delivered:
            await conn.execute(
                "INSERT INTO order_status_log (order_id, message) VALUES (?, ?)",
                (order_id, "Delivered successfully!"),
            )
            await self._log_event(
                conn,
                f"Order {order_id} delivered",
            )
        await conn.execute(
            "DELETE FROM driver_orders WHERE driver_id=? AND order_id=?",
            (driver_id, order_id),
        )
        await conn.commit()
        # Return remaining order count for this driver
        async with conn.execute(
            "SELECT COUNT(*) as cnt FROM driver_orders WHERE driver_id=?", (driver_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return delivered, row["cnt"]

    async def is_order_terminal(self, order_id: str) -> bool:
        """Return True if the order is in a terminal state (DELIVERED/CANCELLED).

        Used by deliver_order to detect retries after a successful DB commit —
        skips re-setting DELIVERING status on the already-finished order, which
        would otherwise overwrite IDLE and leave the driver visibly stuck.
        """
        status = await self.get_order_status(order_id)
        if status is None:
            return False
        return status in self._TERMINAL_STATUSES

    async def get_order_status(self, order_id: str) -> str | None:
        """Return the order's current status string, or None if not found.

        deliver_order uses this to distinguish DELIVERED (return success so
        the workflow replays the parent signal) from CANCELLED (return
        failure so the workflow skips the order_delivered signal).
        """
        conn = await self._get_conn()
        async with conn.execute(
            "SELECT status FROM orders WHERE order_id=?", (order_id,)
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            return None
        return row["status"]

    async def get_order_driver(self, order_id: str) -> str | None:
        conn = await self._get_conn()
        async with conn.execute(
            "SELECT assigned_driver_id FROM orders WHERE order_id=?", (order_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row is None:
                return None
            return row["assigned_driver_id"]

    async def get_driver_orders(self, driver_id: str) -> list[str]:
        conn = await self._get_conn()
        result: list[str] = []
        async with conn.execute(
            "SELECT order_id FROM driver_orders WHERE driver_id=?", (driver_id,)
        ) as cursor:
            async for row in cursor:
                result.append(row["order_id"])
        return result

    async def update_order_delivery(
        self, order_id: str, new_lat: float, new_lng: float, new_hotel: str | None = None
    ) -> None:
        """Update delivery coordinates (and optionally hotel) for an order (customer change).

        Skips if order is already in a terminal state.
        """
        conn = await self._get_conn()
        if new_hotel:
            # Don't modify terminal orders
            cursor = await conn.execute(
                "UPDATE orders SET delivery_lat=?, delivery_lng=?, hotel=?, "
                "label=label || ' → ' || ? "
                "WHERE order_id=? AND status NOT IN (?, ?)",
                (
                    new_lat,
                    new_lng,
                    new_hotel,
                    new_hotel,
                    order_id,
                    OrderStatus.CANCELLED.value,
                    OrderStatus.DELIVERED.value,
                ),
            )
            note = f"Rerouted to {new_hotel}"
        else:
            cursor = await conn.execute(
                "UPDATE orders SET delivery_lat=?, delivery_lng=? "
                "WHERE order_id=? AND status NOT IN (?, ?)",
                (
                    new_lat,
                    new_lng,
                    order_id,
                    OrderStatus.CANCELLED.value,
                    OrderStatus.DELIVERED.value,
                ),
            )
            note = f"Delivery address updated to ({new_lat:.4f}, {new_lng:.4f})"
        if cursor.rowcount > 0:
            await conn.execute(
                "INSERT INTO order_status_log (order_id, message) VALUES (?, ?)",
                (order_id, note),
            )
        await conn.commit()

    async def cancel_order(self, order_id: str) -> None:
        conn = await self._get_conn()
        # Atomic: only cancel if not already terminal (idempotent on retry)
        cursor = await conn.execute(
            "UPDATE orders SET status=? WHERE order_id=? AND status NOT IN (?, ?)",
            (
                OrderStatus.CANCELLED.value,
                order_id,
                OrderStatus.DELIVERED.value,
                OrderStatus.CANCELLED.value,
            ),
        )
        if cursor.rowcount > 0:
            await conn.execute(
                "INSERT INTO order_status_log (order_id, message) VALUES (?, ?)",
                (order_id, "Cancelled by customer"),
            )
            # Delete by order_id only — no stale-read race with concurrent assignment
            await conn.execute(
                "DELETE FROM driver_orders WHERE order_id=?",
                (order_id,),
            )
            await self._log_event(conn, f"Order {order_id} cancelled")
        await conn.commit()

    # --- Agent events (for UI panel) ---

    async def publish_agent_event(
        self, agent_name: str, event_type: str, content: str, summary: str = ""
    ) -> None:
        conn = await self._get_conn()
        ts = time.time()
        await conn.execute(
            "INSERT INTO agent_events (agent_name, event_type, content, timestamp, summary) "
            "VALUES (?, ?, ?, ?, ?)",
            (agent_name, event_type, content, ts, summary),
        )
        await self._log_event(conn, f"[{agent_name}] {event_type}: {content[:80]}")
        await conn.commit()

    # --- Query ---

    async def snapshot(self) -> dict[str, Any]:
        """Return full state as JSON-serializable dict (for frontend)."""
        conn = await self._get_conn()

        # Drivers (skip warmup-hidden so frontend doesn't render D/E early)
        drivers: dict[str, Any] = {}
        async with conn.execute("SELECT * FROM drivers WHERE warmup_hidden=0") as cursor:
            async for row in cursor:
                did = row["driver_id"]
                # Get current orders
                orders: list[str] = []
                async with conn.execute(
                    "SELECT order_id FROM driver_orders WHERE driver_id=?", (did,)
                ) as oc:
                    async for orow in oc:
                        orders.append(orow["order_id"])
                # Get path history
                path: list[dict[str, float]] = []
                async with conn.execute(
                    "SELECT lat, lng, t FROM driver_path_history WHERE driver_id=? ORDER BY id",
                    (did,),
                ) as pc:
                    async for prow in pc:
                        path.append({"lat": prow["lat"], "lng": prow["lng"], "t": prow["t"]})

                drivers[did] = {
                    "driver_id": did,
                    "position": {"lat": row["lat"], "lng": row["lng"]},
                    "battery_pct": row["battery_pct"],
                    "status": row["status"],
                    "capacity": row["capacity"],
                    "current_orders": orders,
                    "path_history": path,
                    "disconnected": bool(row["disconnected"]),
                    "recovering": bool(row["recovering"]),
                }

        # Orders
        orders_dict: dict[str, Any] = {}
        async with conn.execute("SELECT * FROM orders") as cursor:
            async for row in cursor:
                oid = row["order_id"]
                status_log: list[str] = []
                async with conn.execute(
                    "SELECT message FROM order_status_log WHERE order_id=? ORDER BY id",
                    (oid,),
                ) as slc:
                    async for slrow in slc:
                        status_log.append(slrow["message"])
                orders_dict[oid] = {
                    "order_id": oid,
                    "hotel": row["hotel"],
                    "label": row["label"],
                    "priority": row["priority"],
                    "servings": row["servings"],
                    "delivery_coords": {
                        "lat": row["delivery_lat"],
                        "lng": row["delivery_lng"],
                    },
                    "assigned_driver_id": row["assigned_driver_id"],
                    "status": row["status"],
                    "deadline_minutes": row["deadline_minutes"],
                    "degraded": bool(row["degraded"]),
                    "status_log": status_log,
                }

        # Agent events
        events: list[dict[str, Any]] = []
        async with conn.execute("SELECT * FROM agent_events ORDER BY id") as cursor:
            async for row in cursor:
                events.append(
                    {
                        "agent_name": row["agent_name"],
                        "event_type": row["event_type"],
                        "content": row["content"],
                        "timestamp": row["timestamp"],
                        "summary": row["summary"],
                    }
                )

        # Event log (newest 500, reversed to chronological order)
        log: list[dict[str, Any]] = []
        async with conn.execute(
            "SELECT t, msg FROM event_log ORDER BY id DESC LIMIT 500"
        ) as cursor:
            async for row in cursor:
                log.append({"t": row["t"], "msg": row["msg"]})
        log.reverse()

        # Agent health
        health: dict[str, bool] = {}
        async with conn.execute("SELECT * FROM agent_health") as cursor:
            async for row in cursor:
                health[row["agent_name"]] = bool(row["online"])

        return {
            "drivers": drivers,
            "orders": orders_dict,
            "agent_events": events,
            "event_log": log,
            "agent_health": health,
        }

    async def get_fleet_summary(self) -> str:
        """Return a text summary of fleet state for LLM consumption.

        Skips warmup-hidden drivers so the LLM only sees available drivers.
        """
        conn = await self._get_conn()
        lines = ["=== Fleet Status ==="]

        async with conn.execute("SELECT * FROM drivers WHERE warmup_hidden=0") as cursor:
            async for row in cursor:
                did = row["driver_id"]
                # Get current orders for this driver
                order_ids: list[str] = []
                async with conn.execute(
                    "SELECT order_id FROM driver_orders WHERE driver_id=?", (did,)
                ) as oc:
                    async for orow in oc:
                        order_ids.append(orow["order_id"])
                orders_str = ", ".join(order_ids) if order_ids else "none"
                disconnect_tag = " **DISCONNECTED**" if row["disconnected"] else ""
                recovering_tag = " [recovering]" if row["recovering"] else ""
                cap_used = len(order_ids)
                cap_total = row["capacity"]
                lines.append(
                    f"  {did}: status={row['status']}, "
                    f"pos=({row['lat']:.4f}, {row['lng']:.4f}), "
                    f"capacity={cap_used}/{cap_total}, "
                    f"orders=[{orders_str}]"
                    f"{disconnect_tag}{recovering_tag}"
                )

        lines.append("=== Agent Health ===")
        async with conn.execute("SELECT * FROM agent_health") as cursor:
            async for row in cursor:
                status = "ONLINE" if row["online"] else "OFFLINE"
                lines.append(f"  {row['agent_name']}: {status}")

        lines.append("=== Orders ===")
        async with conn.execute("SELECT * FROM orders") as cursor:
            async for row in cursor:
                lines.append(
                    f"  {row['order_id']}: {row['hotel']} ({row['label']}), "
                    f"priority={row['priority']}, status={row['status']}, "
                    f"Driver={row['assigned_driver_id'] or 'unassigned'}, "
                    f"deadline={row['deadline_minutes']}min"
                )

        return "\n".join(lines)

    async def get_order_priorities_summary(self) -> str:
        """Return order priority details for Customer Agent consumption."""
        conn = await self._get_conn()
        lines = ["=== Order Priorities ==="]

        async with conn.execute("SELECT * FROM orders") as cursor:
            async for row in cursor:
                driver_status = ""
                if row["assigned_driver_id"]:
                    async with conn.execute(
                        "SELECT disconnected FROM drivers WHERE driver_id=?",
                        (row["assigned_driver_id"],),
                    ) as dc:
                        drow = await dc.fetchone()
                        if drow and drow["disconnected"]:
                            driver_status = f" **DRIVER {row['assigned_driver_id']} DISCONNECTED**"
                lines.append(
                    f"  {row['order_id']}: {row['hotel']} — "
                    f"{row['priority'].upper()}, "
                    f"{row['servings']} servings, "
                    f"deadline={row['deadline_minutes']}min, "
                    f"status={row['status']}{driver_status}"
                )

        return "\n".join(lines)

    # --- Internals ---

    async def _log_event(self, conn: aiosqlite.Connection, msg: str) -> None:
        await conn.execute("INSERT INTO event_log (t, msg) VALUES (?, ?)", (time.time(), msg))


# Singleton — shared across worker and server
fleet = FleetState()
