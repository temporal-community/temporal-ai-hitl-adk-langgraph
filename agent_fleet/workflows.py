"""
Temporal workflows for the Meltdown ice cream delivery demo.

MeltdownDemoWorkflow — main orchestrator. Starts driver and order-generation
child workflows. Owns driver state, runs multi-agent assignment per order,
and signals the chosen driver. Handles customer-change signals concurrently.

OrderGenerationWorkflow — child workflow that generates orders on a timer
and signals the parent with each new order for assignment.

DriverRouteWorkflow — per-driver continuous delivery loop (child workflow).
Waits for orders via signal, picks up at the shop, delivers, repeats.
Uses cancellation scopes for workflow-driven disconnect handling.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from google.adk.runners import Runner
    from google.adk.sessions import InMemorySessionService
    from google.genai.types import Content, Part
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.types import Command
    from temporalio.contrib.langgraph import graph

    from agent_fleet.activities import (
        deliver_order,
        execute_customer_change,
        generate_order,
        get_route_polyline,
        navigate_to,
        pickup_orders,
        publish_agent_event,
        publish_agent_events_batch,
        register_assignment,
        set_driver_idle,
        set_warmup_hidden,
        sync_driver_position,
    )
    from agent_fleet.agents import create_assessment_team_agent, create_order_assignment_agent
    from agent_fleet.config import GATE_ESCALATION_SECONDS
    from agent_fleet.langgraph_agents import DISPATCH_ONLY_GRAPH_NAME, GRAPH_NAME
    from agent_fleet.locations import WAREHOUSE
    from agent_fleet.models import (
        AdkAssessmentOutput,
        AgentDisconnectInput,
        CustomerChangeInput,
        DeliverInput,
        DriverDisconnectInput,
        DriverRouteInput,
        DriverRouteOrder,
        DriverSnapshot,
        ExecuteCustomerChangeInput,
        GenerateOrderInput,
        LgDispatchInput,
        LgDispatchOutput,
        MeltdownDemoInput,
        NavigateInput,
        NavigateOutput,
        OrderAssignmentResult,
        OrderDeliveredInput,
        OrderGenerationInput,
        OrderUpdateInput,
        PickupInput,
        PublishAgentEventInput,
        ReasonAboutAssignmentInput,
        ReasonAboutAssignmentOutput,
    )
    from agent_fleet.queues import AGENTS_QUEUE, DELIVERY_QUEUE

FAST_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
)
NAV_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=3),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
)

MAX_ORDERS = 50
# Steady-state spacing between orders. The langgraph tab runs the full 3-agent team
# per order (Fleet ∥ Customer → Dispatch, each a Gemini activity), so the cadence is
# tuned for that heavier mode — slower than ADK needs, but watchable and within the
# worker's LLM throughput. Tune against a live rehearsal.
ORDER_INTERVAL_SECONDS = 12
# Initial burst so the first few drivers pick up multi-order batches. Gentler than the
# old 8-at-1s flood: at 8 orders/sec, langgraph mode fires ~24 concurrent Gemini calls
# at the worker. Aligned to the warmup window (drivers D-E are hidden for the first
# _WARMUP_ORDERS), so the burst fills A-C without overwhelming the agent team.
WARMUP_BURST_ORDERS = 5
WARMUP_BURST_SECONDS = 2

# Cross-framework tab: each order spawns TWO child workflows (an ADK assess child + a
# LangGraph dispatch child), so the full 50-order auto-flow floods the Temporal UI with
# ~100 short-lived executions. The per-order model is intentional (each agent unit is its
# own durable workflow) — we just generate fewer, SLOWER orders and skip the warmup burst
# so the demo stays legible AND stays alive long enough to interactively drop a high-value
# order and watch the ask_human gate (the demo tears down once order-gen completes, so the
# cap also bounds runtime — keep it high enough to outlast the talk). 50 @ 22s ≈ 18 min.
# The slow interval (not the total) bounds concurrent Gemini load, so raising the total just
# lengthens the run, it doesn't add peak load. Tunable; ADK/LangGraph tabs are unaffected.
CROSSFRAMEWORK_MAX_ORDERS = 50
CROSSFRAMEWORK_ORDER_INTERVAL_SECONDS = 22
CROSSFRAMEWORK_WARMUP_BURST_ORDERS = 3  # mild quick-start; no 5-order flood

# 4 drivers × 2 orders = 8 slots — a deliberate squeeze so the fleet occasionally runs
# tight, making the agents' capacity reasoning (and the "commit scarce capacity" escalation)
# actually fire. NOTE: capacity is a per-driver ORDER COUNT (batch limit), not servings —
# a large order takes one slot, same as a small one.
DRIVER_CAPACITY = 2
DRIVER_IDS = ["driver-a", "driver-b", "driver-c", "driver-d"]
# Hidden during warmup so the first orders fill A–C before D comes online.
WARMUP_HIDDEN = ["driver-d"]

# Pattern B — orders at/above this value are routed to the dispatch agent, which
# DECIDES whether to escalate to a human. This is a cheap, token-saving routing
# pre-filter (non-platinum orders top out ~$1,950), NOT the escalation decision —
# that stays the agent's prompt-driven call.
GATE_REVIEW_VALUE = 2000
# GATE_ESCALATION_SECONDS, the primary approver window before escalation, lives in config.py
# so .env can change it. Intentionally short for the demo (30 s); production systems would
# normally wait minutes or hours.

# Long-lived entity workflows keep their history bounded by periodically continuing-as-new.
# A driver runs deliveries indefinitely, so its event history would otherwise grow until it
# hit Temporal's hard cap (~50K events / 50MB). When history crosses this threshold AND the
# driver is at a clean quiescent point (idle at base, nothing pending), DriverRouteWorkflow
# continue-as-news, carrying forward ONLY its live state (identity, position, lifetime
# delivery count) — history resets to ~0 while the workflow ID stays "route-driver-x", so the
# parent keeps signaling it transparently. 10K leaves headroom under the 50K cap. Tests
# override this low (via DriverRouteInput.history_threshold) to force a continue-as-new fast.
DRIVER_HISTORY_CONTINUE_AS_NEW = 10_000

# Same discipline for the ORCHESTRATOR (MeltdownDemoWorkflow). DORMANT in this demo: the run is
# bounded by order generation (~50 orders ≈ <2K events), so it completes long before this fires.
# It's wired so that a parent which DID run indefinitely would keep its own history bounded by
# continuing-as-new at a quiescent point, re-acquiring its long-lived children by id. Tests set
# it low (via MeltdownDemoInput.history_threshold) to exercise the decision helpers.
PARENT_HISTORY_CONTINUE_AS_NEW = 10_000


@dataclass
class PendingHold:
    """Per-order HITL hold state on a DriverRouteWorkflow.

    Replaces the prior single-slot fields (`_update_pending_order`,
    `_update_decision`, `_update_new_*`). Those collapsed any concurrent
    hold onto one slot, so a second `update_pending` for a different
    order on the same driver would overwrite the first — dropping the
    first order's approved decision silently. Keying holds by order_id
    prevents that entirely: each order has its own slot, and signals
    (update_pending, resolve_update) target a specific order.
    """

    decision: str | None = None  # "cancel", "address_change", "release"
    new_lat: float | None = None
    new_lng: float | None = None
    new_hotel: str | None = None


# --- Per-driver continuous delivery workflow ---


@workflow.defn
class DriverRouteWorkflow:
    """
    Continuous delivery loop for a single Driver.

    Waits for orders via signal, picks up at the shop, delivers to hotel,
    then returns to waiting. Loops until told to stop.

    Disconnect handling uses the Temporal-native retry pattern:
    - Activities check FleetState for disconnect status on each heartbeat
    - When disconnected, activities fail — Temporal retries with backoff
    - When reconnected, the next retry succeeds and delivery resumes
    - No workflow-side cancellation needed
    """

    def __init__(self) -> None:
        self._pending_orders: list[DriverRouteOrder] = []
        self._stop = False
        self._is_disconnected: bool = False
        self._current_lat: float = 0.0
        self._current_lng: float = 0.0
        self._driver_id: str = ""
        self._status: str = "idle"
        self._current_orders: list[str] = []
        self._path_history: list[dict] = []
        self._is_recovering: bool = False
        self._position_sync_needed: bool = False
        # HITL state: one hold per order_id. Each entry is created when
        # update_pending arrives for that order and populated when
        # resolve_update arrives. Removed when the hold is processed by the
        # delivery loop (or when cancel_order removes the order from the
        # queue). See PendingHold docstring for why this replaced the prior
        # single-slot design.
        self._active_order_id: str | None = None
        self._pending_holds: dict[str, PendingHold] = {}
        self._cancel_pending: bool = False
        self._batch_orders: list[DriverRouteOrder] = []
        # Lifetime delivery count — survives continue-as-new (carried via DriverRouteInput).
        self._delivered_total: int = 0
        self._history_threshold: int = DRIVER_HISTORY_CONTINUE_AS_NEW

    # --- Signals ---

    @workflow.signal
    async def add_order(self, order: DriverRouteOrder) -> None:
        self._pending_orders.append(order)
        self._current_orders.append(order.order_id)

    @workflow.signal
    async def stop(self) -> None:
        self._stop = True

    @workflow.signal
    async def driver_disconnected(self, inp: DriverDisconnectInput) -> None:
        self._is_disconnected = True
        self._status = "disconnected"
        workflow.logger.info(
            f"Driver {inp.driver_id} disconnected — activities will fail and retry"
        )

    @workflow.signal
    async def driver_reconnected(self, inp: DriverDisconnectInput) -> None:
        self._is_disconnected = False
        self._position_sync_needed = True
        workflow.logger.info(f"Driver {inp.driver_id} reconnected — resuming")

    @workflow.signal
    async def update_pending(self, inp: OrderUpdateInput) -> None:
        """Register a HITL hold for this order. Idempotent — multiple sends
        for the same order are a no-op. Each order gets its own slot in
        _pending_holds so concurrent holds for different orders on the same
        driver don't collide.
        """
        if inp.order_id not in self._pending_holds:
            self._pending_holds[inp.order_id] = PendingHold()
        workflow.logger.info(f"Order {inp.order_id} — update pending, will hold before delivery")

    @workflow.signal
    async def resolve_update(self, inp: OrderUpdateInput) -> None:
        """Fill in the HITL decision for a specific order's hold.

        Drops if there's no matching hold (stale resolution, e.g. the order
        was cancelled out of the queue by cancel_order before this arrived,
        or the parent is sending resolve_update for a different change than
        the child is tracking). With per-order holds the drop is strictly
        "no hold to resolve"; it can never accidentally clobber another
        order's decision.
        """
        hold = self._pending_holds.get(inp.order_id)
        if hold is None:
            workflow.logger.info(
                f"Order {inp.order_id} — resolve_update ignored (no matching hold)"
            )
            return
        hold.decision = inp.change_type  # "cancel", "address_change", "release"
        if inp.new_lat is not None:
            hold.new_lat = inp.new_lat
        if inp.new_lng is not None:
            hold.new_lng = inp.new_lng
        if inp.new_hotel is not None:
            hold.new_hotel = inp.new_hotel
        workflow.logger.info(f"Order {inp.order_id} — update resolved: {inp.change_type}")

    @workflow.signal
    async def cancel_order(self, inp: OrderUpdateInput) -> None:
        """Cancel an order — works for pending and batched orders.

        Also removes any HITL hold tracked for this order, so a later
        delivery loop iteration doesn't wait on a hold for an order that
        no longer exists in the queue.
        """
        # Check pending orders
        before = len(self._pending_orders)
        self._pending_orders = [o for o in self._pending_orders if o.order_id != inp.order_id]
        if len(self._pending_orders) < before:
            workflow.logger.info(f"Order {inp.order_id} cancelled — removed from pending queue")
            try:
                self._current_orders.remove(inp.order_id)
            except ValueError:
                pass
            self._pending_holds.pop(inp.order_id, None)
            return
        # Check batched orders
        before = len(self._batch_orders)
        self._batch_orders = [o for o in self._batch_orders if o.order_id != inp.order_id]
        if len(self._batch_orders) < before:
            workflow.logger.info(f"Order {inp.order_id} cancelled — removed from batch")
            try:
                self._current_orders.remove(inp.order_id)
            except ValueError:
                pass
            self._pending_holds.pop(inp.order_id, None)
            return
        # Active order cancel is now handled by resolve_update("cancel")
        workflow.logger.info(f"Order {inp.order_id} not in pending/batch — handled by HITL flow")

    @workflow.signal
    async def update_order(self, inp: OrderUpdateInput) -> None:
        """Update delivery coordinates (and hotel label) for pending/batched orders."""
        for order in self._pending_orders + self._batch_orders:
            if order.order_id == inp.order_id:
                if inp.new_lat is not None and inp.new_lng is not None:
                    order.delivery_lat = inp.new_lat
                    order.delivery_lng = inp.new_lng
                if inp.new_hotel is not None:
                    order.hotel = inp.new_hotel
                workflow.logger.info(f"Order {inp.order_id} updated — new destination")
                return
        # Active order reroute is now handled by resolve_update("address_change")
        workflow.logger.info(f"Order {inp.order_id} not in pending/batch — handled by HITL flow")

    # --- Queries ---

    @workflow.query
    def get_position(self) -> dict:
        """Return current driver position. Used by parent workflow for driver snapshots."""
        return {"lat": self._current_lat, "lng": self._current_lng}

    @workflow.query
    def get_status(self) -> dict:
        return {
            "driver_id": self._driver_id,
            "lat": self._current_lat,
            "lng": self._current_lng,
            "status": self._status,
            "is_disconnected": self._is_disconnected,
            "is_recovering": self._is_recovering,
            "current_orders": list(self._current_orders),
            "active_order_id": self._active_order_id,
            "path_history": list(self._path_history),
            "pending_orders": len(self._pending_orders),
            "pending_hold_order_ids": list(self._pending_holds.keys()),
            "lifetime_deliveries": self._delivered_total,
        }

    # --- Helpers ---

    async def _execute_navigate(
        self, driver_id: str, nav_input: NavigateInput, summary: str = ""
    ) -> NavigateOutput:
        """Execute navigate_to — Temporal retries on failure (including disconnect).

        The activity checks FleetState for disconnect status on each heartbeat.
        When disconnected, it fails. Temporal retries with backoff (NAV_RETRY).
        When reconnected, the next retry succeeds and navigation resumes.
        No workflow-side cancellation needed — this is the Temporal-native pattern.
        """
        return await workflow.execute_activity(
            navigate_to,
            nav_input,
            task_queue=DELIVERY_QUEUE,
            summary=summary,
            schedule_to_close_timeout=timedelta(minutes=10),
            start_to_close_timeout=timedelta(seconds=120),
            heartbeat_timeout=timedelta(seconds=15),
            retry_policy=NAV_RETRY,
        )

    # --- Main entry ---

    @workflow.run
    async def run(self, inp: DriverRouteInput) -> str:
        driver_id = inp.driver_id
        self._driver_id = driver_id
        delivered: list[str] = []  # order ids delivered in THIS run (resets each generation)
        # Lifetime delivery count + history threshold survive continue-as-new. getattr keeps
        # this replay-safe for runs started before these input fields existed.
        self._delivered_total = getattr(inp, "delivered_total", 0)
        self._history_threshold = (
            getattr(inp, "history_threshold", 0) or DRIVER_HISTORY_CONTINUE_AS_NEW
        )
        # Resume at the carried-over position on continue-as-new; a fresh driver (0.0/0.0)
        # starts at the warehouse.
        seed_lat = getattr(inp, "current_lat", 0.0)
        seed_lng = getattr(inp, "current_lng", 0.0)
        if seed_lat and seed_lng:
            self._current_lat, self._current_lng = seed_lat, seed_lng
        else:
            self._current_lat, self._current_lng = WAREHOUSE.lat, WAREHOUSE.lng

        while not self._stop:
            # Long-lived entity pattern: continue-as-new to keep history bounded. We do it
            # ONLY at a clean quiescent point — top of the loop, idle, nothing pending — so the
            # carried state fully captures the driver. History resets; the workflow ID is
            # unchanged, so the parent's signals keep flowing to the new generation. See
            # DRIVER_HISTORY_CONTINUE_AS_NEW. (continue_as_new raises, so nothing runs after.)
            if (
                not self._pending_orders
                and not self._pending_holds
                and not self._is_disconnected
                and workflow.info().get_current_history_length() >= self._history_threshold
            ):
                workflow.logger.info(
                    f"{driver_id} continue-as-new at {self._delivered_total} lifetime "
                    f"deliveries (history={workflow.info().get_current_history_length()})"
                )
                workflow.continue_as_new(
                    DriverRouteInput(
                        driver_id=driver_id,
                        current_lat=self._current_lat,
                        current_lng=self._current_lng,
                        delivered_total=self._delivered_total,
                        history_threshold=self._history_threshold,
                    )
                )
            # Wait for an order to arrive or stop signal
            try:
                await workflow.wait_condition(
                    lambda: len(self._pending_orders) > 0 or self._stop,
                    timeout=timedelta(minutes=10),
                )
            except TimeoutError:
                continue

            if self._stop:
                break

            # Brief pause to let concurrent assignments land before collecting.
            # The real batching happens naturally: orders assigned while the
            # driver is navigating to Ziggy's accumulate in _pending_orders
            # and get scooped into the batch at pickup time.
            await workflow.sleep(timedelta(seconds=2))

            # --- Position sync after reconnect ---
            if self._position_sync_needed:
                self._position_sync_needed = False
                pos = await workflow.execute_activity(
                    sync_driver_position,
                    driver_id,
                    task_queue=DELIVERY_QUEUE,
                    start_to_close_timeout=timedelta(seconds=10),
                    retry_policy=FAST_RETRY,
                )
                self._current_lat, self._current_lng = pos[0], pos[1]
                workflow.logger.info(f"Position synced to ({pos[0]:.4f}, {pos[1]:.4f})")

            # --- Batch pickup: collect all pending orders, drive to shop once ---
            self._batch_orders = []
            while self._pending_orders:
                self._batch_orders.append(self._pending_orders.pop(0))
            if not self._batch_orders:
                continue
            order_ids_str = ", ".join(
                f"#{o.order_id.split('-', 1)[-1]}" for o in self._batch_orders
            )

            # Navigate to shop for pickup (skip if already there)
            at_shop = (
                abs(self._current_lat - WAREHOUSE.lat) < 0.001
                and abs(self._current_lng - WAREHOUSE.lng) < 0.001
            )
            if not at_shop:
                self._status = "en_route_pickup"
                pickup_waypoints = await workflow.execute_activity(
                    get_route_polyline,
                    args=[self._current_lat, self._current_lng, WAREHOUSE.lat, WAREHOUSE.lng],
                    task_queue=DELIVERY_QUEUE,
                    summary=f"[{order_ids_str}] Calculating route to Ziggy's",
                    schedule_to_close_timeout=timedelta(minutes=5),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=FAST_RETRY,
                )
                nav_result = await self._execute_navigate(
                    driver_id,
                    NavigateInput(
                        driver_id=driver_id,
                        order_id=self._batch_orders[0].order_id,
                        target_lat=WAREHOUSE.lat,
                        target_lng=WAREHOUSE.lng,
                        leg="pickup",
                        steps=15,
                        waypoints=pickup_waypoints,
                        start_lat=self._current_lat,
                        start_lng=self._current_lng,
                    ),
                    summary=f"[{order_ids_str}] Driving to Ziggy's",
                )
                self._current_lat = nav_result.final_lat
                self._current_lng = nav_result.final_lng
                self._path_history.append(
                    {"lat": nav_result.final_lat, "lng": nav_result.final_lng}
                )

            # Scoop up any orders that arrived while driving to Ziggy's
            while self._pending_orders:
                self._batch_orders.append(self._pending_orders.pop(0))

            # All orders may have been cancelled during navigation
            if not self._batch_orders:
                await workflow.execute_activity(
                    set_driver_idle,
                    driver_id,
                    task_queue=DELIVERY_QUEUE,
                    start_to_close_timeout=timedelta(seconds=10),
                    retry_policy=FAST_RETRY,
                )
                self._status = "idle"
                continue

            order_ids_str = ", ".join(
                f"#{o.order_id.split('-', 1)[-1]}" for o in self._batch_orders
            )

            # Batch pickup all orders at once
            self._status = "picking_up"
            await workflow.execute_activity(
                pickup_orders,
                PickupInput(
                    driver_id=driver_id,
                    order_ids=[o.order_id for o in self._batch_orders],
                ),
                task_queue=DELIVERY_QUEUE,
                summary=f"[{order_ids_str}] Loading {len(self._batch_orders)} order(s) at Ziggy's",
                schedule_to_close_timeout=timedelta(minutes=5),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=NAV_RETRY,
            )
            # Position is at warehouse after pickup
            self._current_lat, self._current_lng = WAREHOUSE.lat, WAREHOUSE.lng

            # --- Deliver each order sequentially ---
            while self._batch_orders:
                order = self._batch_orders.pop(0)
                self._active_order_id = order.order_id
                self._cancel_pending = False
                onum = order.order_id.split("-", 1)[-1]

                # Position sync after reconnect (may have happened mid-batch)
                if self._position_sync_needed:
                    self._position_sync_needed = False
                    pos = await workflow.execute_activity(
                        sync_driver_position,
                        driver_id,
                        task_queue=DELIVERY_QUEUE,
                        start_to_close_timeout=timedelta(seconds=10),
                        retry_policy=FAST_RETRY,
                    )
                    self._current_lat, self._current_lng = pos[0], pos[1]

                # Check for cancel before navigating
                if self._cancel_pending:
                    workflow.logger.info(f"Order {order.order_id} cancelled — skipping")
                    self._active_order_id = None
                    self._cancel_pending = False
                    try:
                        self._current_orders.remove(order.order_id)
                    except ValueError:
                        pass
                    continue

                # Navigate to hotel
                self._status = "en_route_delivery"
                delivery_waypoints = await workflow.execute_activity(
                    get_route_polyline,
                    args=[
                        self._current_lat,
                        self._current_lng,
                        order.delivery_lat,
                        order.delivery_lng,
                    ],
                    task_queue=DELIVERY_QUEUE,
                    summary=f"[#{onum}] Calculating route to {order.hotel}",
                    schedule_to_close_timeout=timedelta(minutes=5),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=FAST_RETRY,
                )

                nav_result = await self._execute_navigate(
                    driver_id,
                    NavigateInput(
                        driver_id=driver_id,
                        order_id=order.order_id,
                        target_lat=order.delivery_lat,
                        target_lng=order.delivery_lng,
                        leg="delivery",
                        steps=30,
                        waypoints=delivery_waypoints,
                        start_lat=self._current_lat,
                        start_lng=self._current_lng,
                    ),
                    summary=f"[#{onum}] Driving to {order.hotel}",
                )
                self._current_lat = nav_result.final_lat
                self._current_lng = nav_result.final_lng
                self._path_history.append(
                    {"lat": nav_result.final_lat, "lng": nav_result.final_lng}
                )

                # --- HITL hold: check for pending customer change ---
                # Server signals update_pending directly (same pattern as
                # disconnect). No grace period needed — signal arrives
                # before any parent processing delay.
                if order.order_id in self._pending_holds:
                    self._status = "awaiting_update"
                    workflow.logger.info(
                        f"[{order.order_id}] Holding at {order.hotel} — awaiting HITL decision"
                    )
                    # _stop escape: if the demo is shutting down while this
                    # driver is parked awaiting approval, exit cleanly instead
                    # of blocking forever. The parent's _wait_for_approval
                    # returns None on shutdown without sending resolve_update,
                    # so without this the child would hang and the parent's
                    # `await handle` join would wait on the child indefinitely.
                    await workflow.wait_condition(
                        lambda: (
                            (
                                (h := self._pending_holds.get(order.order_id)) is not None
                                and h.decision is not None
                            )
                            or self._stop
                        )
                    )
                    if self._stop:
                        break

                # Process decision if one was made FOR THIS ORDER
                hold = self._pending_holds.get(order.order_id)
                if hold is not None and hold.decision is not None:
                    decision = hold.decision
                    new_lat = hold.new_lat
                    new_lng = hold.new_lng
                    new_hotel = hold.new_hotel
                    # Pop AFTER capturing so no other coroutine can see a
                    # half-cleared hold.
                    self._pending_holds.pop(order.order_id, None)

                    if decision == "cancel":
                        workflow.logger.info(f"[{order.order_id}] HITL cancel approved")
                        self._cancel_pending = True
                    elif decision == "address_change":
                        # Reroute: update destination and re-navigate
                        if new_lat is not None and new_lng is not None:
                            # If the order was still in pending/batch when the
                            # change was approved, update_order already applied
                            # the new coords — the batch loop navigated to the
                            # new destination on its first trip. Skip the
                            # otherwise-redundant re-navigation here.
                            already_at_new_destination = (
                                abs(order.delivery_lat - new_lat) < 0.0001
                                and abs(order.delivery_lng - new_lng) < 0.0001
                            )
                            order.delivery_lat = new_lat
                            order.delivery_lng = new_lng
                            if new_hotel is not None:
                                order.hotel = new_hotel
                            if already_at_new_destination:
                                workflow.logger.info(
                                    f"[{order.order_id}] HITL reroute approved — "
                                    f"coords already applied, skipping redundant nav"
                                )
                            else:
                                workflow.logger.info(
                                    f"[{order.order_id}] HITL reroute approved — "
                                    f"navigating to new destination"
                                )
                                # Navigate to new destination
                                reroute_waypoints = await workflow.execute_activity(
                                    get_route_polyline,
                                    args=[
                                        self._current_lat,
                                        self._current_lng,
                                        order.delivery_lat,
                                        order.delivery_lng,
                                    ],
                                    task_queue=DELIVERY_QUEUE,
                                    summary=f"[#{onum}] Rerouting to new destination",
                                    schedule_to_close_timeout=timedelta(minutes=5),
                                    start_to_close_timeout=timedelta(seconds=30),
                                    retry_policy=FAST_RETRY,
                                )
                                nav_result = await self._execute_navigate(
                                    driver_id,
                                    NavigateInput(
                                        driver_id=driver_id,
                                        order_id=order.order_id,
                                        target_lat=order.delivery_lat,
                                        target_lng=order.delivery_lng,
                                        leg="delivery",
                                        steps=30,
                                        waypoints=reroute_waypoints,
                                        start_lat=self._current_lat,
                                        start_lng=self._current_lng,
                                    ),
                                    summary=f"[#{onum}] Rerouting to new destination",
                                )
                                self._current_lat = nav_result.final_lat
                                self._current_lng = nav_result.final_lng
                    else:
                        workflow.logger.info(
                            f"[{order.order_id}] HITL change rejected — delivering normally"
                        )

                # Deliver (skip if cancelled)
                if not self._cancel_pending:
                    self._status = "delivering"
                    deliver_result = await workflow.execute_activity(
                        deliver_order,
                        DeliverInput(
                            driver_id=driver_id,
                            order_id=order.order_id,
                        ),
                        task_queue=DELIVERY_QUEUE,
                        summary=f"[#{onum}] Order delivered to {order.hotel}",
                        schedule_to_close_timeout=timedelta(minutes=5),
                        start_to_close_timeout=timedelta(seconds=30),
                        retry_policy=NAV_RETRY,
                    )

                    if deliver_result.success:
                        delivered.append(order.order_id)
                        self._delivered_total += 1  # lifetime count survives continue-as-new

                        # Signal parent — guarded so a terminated parent (e.g.
                        # during demo reset) doesn't raise and fail this child
                        # mid-delivery. info().parent.workflow_id avoids
                        # coupling to a hardcoded id.
                        try:
                            parent_info = workflow.info().parent
                            if parent_info is not None:
                                parent = workflow.get_external_workflow_handle(
                                    parent_info.workflow_id
                                )
                                await parent.signal(
                                    "order_delivered",
                                    OrderDeliveredInput(
                                        driver_id=driver_id,
                                        order_id=order.order_id,
                                        delivery_lat=order.delivery_lat,
                                        delivery_lng=order.delivery_lng,
                                    ),
                                )
                        except Exception as e:
                            workflow.logger.warning(
                                f"Could not signal parent with order_delivered for "
                                f"{order.order_id}: {e}"
                            )
                    else:
                        # deliver_order returned success=False, meaning a
                        # cancel beat us to it. Don't tell the parent we
                        # delivered — the order is CANCELLED, not DELIVERED.
                        workflow.logger.info(
                            f"[{order.order_id}] Delivery did not commit "
                            f"(cancelled before/during activity) — skipping parent signal"
                        )
                else:
                    workflow.logger.info(f"[{order.order_id}] Delivery skipped — cancelled")

                self._active_order_id = None
                self._cancel_pending = False
                # Clean up any hold tied to this order. Usually the hold was
                # already popped when its decision was processed inside the
                # HITL block above — but there's a race where update_pending
                # arrives AFTER the HITL gate evaluated False (no hold yet)
                # but BEFORE deliver_order completed. That creates a late
                # PendingHold(decision=None) that nothing else clears,
                # leaking the order_id into pending_hold_order_ids for the
                # lifetime of the workflow. Popping here closes the race.
                self._pending_holds.pop(order.order_id, None)

                # Remove from current_orders tracking
                try:
                    self._current_orders.remove(order.order_id)
                except ValueError:
                    pass

            # All orders in batch delivered — drive back to base if not already there
            at_warehouse = (
                abs(self._current_lat - WAREHOUSE.lat) < 0.001
                and abs(self._current_lng - WAREHOUSE.lng) < 0.001
            )
            workflow.logger.info(
                f"Batch done. pos=({self._current_lat:.4f}, {self._current_lng:.4f}), "
                f"at_warehouse={at_warehouse}"
            )
            if not at_warehouse:
                return_waypoints = await workflow.execute_activity(
                    get_route_polyline,
                    args=[self._current_lat, self._current_lng, WAREHOUSE.lat, WAREHOUSE.lng],
                    task_queue=DELIVERY_QUEUE,
                    summary="Calculating return to Ziggy's",
                    schedule_to_close_timeout=timedelta(minutes=5),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=FAST_RETRY,
                )
                nav_result = await self._execute_navigate(
                    driver_id,
                    NavigateInput(
                        driver_id=driver_id,
                        order_id="return",
                        target_lat=WAREHOUSE.lat,
                        target_lng=WAREHOUSE.lng,
                        leg="pickup",
                        steps=15,
                        waypoints=return_waypoints,
                        start_lat=self._current_lat,
                        start_lng=self._current_lng,
                    ),
                    summary="Returning to Ziggy's",
                )
                self._current_lat = nav_result.final_lat
                self._current_lng = nav_result.final_lng

            self._status = "idle"
            self._path_history.clear()
            # Update FleetState so the UI shows idle + clear trail
            await workflow.execute_activity(
                set_driver_idle,
                driver_id,
                task_queue=DELIVERY_QUEUE,
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=FAST_RETRY,
            )

        return (
            f"Driver {driver_id} completed {self._delivered_total} deliveries "
            f"(this run: {delivered})"
        )


# --- Order generation child workflow ---


@workflow.defn
class OrderGenerationWorkflow:
    """Generates orders on a timer and signals parent with each new order.

    The parent workflow owns driver state and handles assignment.
    This workflow is purely a timer + order generator.
    """

    def __init__(self) -> None:
        self._stop = False

    @workflow.signal
    async def stop(self) -> None:
        self._stop = True

    @workflow.query
    def get_status(self) -> dict:
        return {"stop": self._stop}

    @workflow.run
    async def run(self, inp: OrderGenerationInput) -> str:
        for order_num in range(1, inp.max_orders + 1):
            if self._stop:
                break

            # Generate a new order
            order = await workflow.execute_activity(
                generate_order,
                GenerateOrderInput(order_number=order_num),
                task_queue=DELIVERY_QUEUE,
                summary=f"[#{order_num}] Generate order",
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=FAST_RETRY,
            )

            # Signal parent with the new order — guarded (same rationale as
            # DriverRouteWorkflow.order_delivered): a terminated parent during
            # demo reset must not fail this child.
            try:
                parent_info = workflow.info().parent
                if parent_info is not None:
                    parent = workflow.get_external_workflow_handle(parent_info.workflow_id)
                    await parent.signal(
                        "new_order",
                        OrderAssignmentResult(
                            order_id=order.order_id,
                            hotel=order.hotel,
                            delivery_lat=order.delivery_lat,
                            delivery_lng=order.delivery_lng,
                            driver_id="",  # Not yet assigned — parent handles assignment
                            reasoning_summary="",
                            priority=order.priority,
                            servings=order.servings,
                            deadline_minutes=order.deadline_minutes,
                            event=order.event,
                            order_value=order.order_value,
                        ),
                    )
            except Exception as e:
                workflow.logger.warning(
                    f"Could not signal parent with new order {order.order_id}: {e}"
                )

            workflow.logger.info(
                f"Order {order_num}/{inp.max_orders}: {order.order_id} signaled to parent"
            )

            # Wait before next order — gentle initial burst to fill driver batches,
            # then normal cadence (tuned for the heavier langgraph agent-team mode).
            # Burst size is per-input (crossframework passes a smaller one); getattr keeps
            # replay clean for workflows started before the field existed.
            burst = getattr(inp, "warmup_burst_orders", WARMUP_BURST_ORDERS)
            if order_num < inp.max_orders:
                if order_num <= burst:
                    await workflow.sleep(timedelta(seconds=WARMUP_BURST_SECONDS))
                else:
                    base = inp.order_interval_seconds
                    jitter = int(workflow.random().random() * base * 0.6)  # 0–60% jitter
                    interval = base + jitter - int(base * 0.3)  # center around base
                    await workflow.sleep(timedelta(seconds=max(5, interval)))

        return f"Order generation complete — {inp.max_orders} orders generated"


# --- Cross-framework agent child workflows (3rd tab) ---
#
# The cross-framework tab proves that only Temporal can orchestrate ACROSS agent
# frameworks: Fleet+Customer run on ADK (AdkAssessmentWorkflow), the Dispatch agent
# runs on LangGraph (LgDispatchWorkflow), and the parent (MeltdownDemoWorkflow) joins
# them as child workflows. Each child is its own Temporal history in the UI — the
# visible cross-framework boundary. Children DECIDE; the parent APPLIES (owns driver
# state, signals drivers); driver workflows EXECUTE.


@workflow.defn
class AdkAssessmentWorkflow:
    """ADK framework child: Fleet ∥ Customer assessment ONLY (no dispatch).

    Mirrors the front half of MeltdownDemoWorkflow._run_adk_assignment, but runs the
    assessment-only ParallelAgent and returns just the two assessment strings. Runs on
    the parent's task queue (where GoogleAdkPlugin is registered). Returns plain strings
    — never ADK objects — so the result crosses the child boundary cleanly.
    """

    @workflow.run
    async def run(self, inp: ReasonAboutAssignmentInput) -> AdkAssessmentOutput:
        workflow.logger.info(f"ADK assessment child for {inp.order_id}")
        agent = create_assessment_team_agent()

        session_service = InMemorySessionService()
        runner = Runner(
            agent=agent,
            app_name="meltdown_demo",
            session_service=session_service,
        )
        session = await session_service.create_session(
            app_name="meltdown_demo",
            user_id="workflow",
        )

        agent_status_lines = []
        if inp.disconnected_agents:
            for name in inp.disconnected_agents:
                agent_status_lines.append(f"⚠️ {name} is OFFLINE — compensate with available data.")
        agent_context = "\n".join(agent_status_lines) + "\n\n" if agent_status_lines else ""

        # Keep the "Order ID:" / "Venue:" lines — _build_summary parses them for UI labels.
        prompt = (
            f"{agent_context}"
            f"NEW ORDER — assess it:\n"
            f"Order ID: {inp.order_id}\n"
            f"Venue: {inp.hotel}\n"
            f"Event: {inp.event}\n"
            f"Priority: {inp.priority}\n"
            f"Servings: {inp.servings}\n"
            f"Deadline: {inp.deadline_minutes} minutes\n"
            f"Coordinates: ({inp.delivery_lat}, {inp.delivery_lng})\n\n"
            f"Fleet Agent: assess capacity and recommend the best driver. "
            f"Customer Agent: assess priority and urgency."
        )

        async for _ in runner.run_async(
            user_id="workflow",
            session_id=session.id,
            new_message=Content(parts=[Part(text=prompt)]),
        ):
            pass

        updated_session = await session_service.get_session(
            app_name="meltdown_demo",
            user_id="workflow",
            session_id=session.id,
        )
        state = updated_session.state or {}
        out = AdkAssessmentOutput(
            fleet_assessment=(state.get("fleet_assessment") or "").strip(),
            customer_assessment=(state.get("customer_assessment") or "").strip(),
        )
        workflow.logger.info(f"ADK assessment child done for {inp.order_id}")
        return out


@workflow.defn
class LgDispatchWorkflow:
    """LangGraph framework child: the Dispatch agent, with its OWN in-loop HITL.

    Seeded with the ADK-produced assessments, it runs the dispatch-only graph and may
    call ask_human mid-loop. The human signals THIS workflow directly (`answer_dispatch`);
    the durable wait is a Temporal signal + wait_condition + Command(resume) — interrupt()
    only suspends the graph, Temporal history is the durability. Returns the decision.
    """

    def __init__(self) -> None:
        self._answer: str | None = None
        self._pending_question: dict | None = None
        self._stop: bool = False
        self._approver_tier: str = "primary"

    @workflow.signal
    async def answer_dispatch(self, decision: str) -> None:
        """Human answers this dispatch agent's ask_human. The child IS the order, so no
        order_id is needed — resolves the durable interrupt the graph is parked on."""
        self._answer = decision

    @workflow.signal
    async def stop(self) -> None:
        """Demo shutdown escape — lets a parked run return a HOLD-default cleanly."""
        self._stop = True

    @workflow.query
    def pending_question(self) -> dict | None:
        """The ask_human payload the agent is currently parked on (None if not parked).
        The server polls this to render the approval card and find this child."""
        return self._pending_question

    @workflow.run
    async def run(self, inp: LgDispatchInput) -> LgDispatchOutput:
        workflow.logger.info(f"LangGraph dispatch child for {inp.order_id}")
        state = {
            "order_id": inp.order_id,
            "venue": inp.venue,
            "order_value": inp.order_value,
            "servings": inp.servings,
            "deadline_minutes": inp.deadline_minutes,
            "proposed_driver_id": inp.proposed_driver_id,
            "drivers_available": inp.drivers_available,
            "drivers_total": inp.drivers_total,
            "pending_orders": inp.pending_orders,
            # Seed the ADK assessments — dispatch_reason reads these directly.
            "fleet_assessment": inp.fleet_assessment,
            "customer_assessment": inp.customer_assessment,
            "eligible_drivers": inp.eligible_drivers,  # the agent picks from these
        }
        compiled = graph(DISPATCH_ONLY_GRAPH_NAME).compile(checkpointer=InMemorySaver())
        config = {"configurable": {"thread_id": workflow.info().workflow_id}}

        result = await compiled.ainvoke(state, config=config)
        rejected = False
        # Drive any in-loop ask_human interrupts the dispatch agent raises.
        while result.get("__interrupt__"):
            self._pending_question = result["__interrupt__"][0].value
            # Primary approver window. If it lapses with no answer, escalate to a backup
            # approver (surface the tier so the UI/Temporal shows it) and keep waiting on
            # the same durable primitive. The timer survives a worker crash like the wait.
            try:
                await workflow.wait_condition(
                    lambda: self._answer is not None or self._stop,
                    timeout=timedelta(seconds=GATE_ESCALATION_SECONDS),
                )
            except TimeoutError:
                self._approver_tier = "backup"
                workflow.logger.info(
                    f"Approval timed out for {inp.order_id}, escalating to backup approver"
                )
                workflow.set_current_details(
                    "Primary approver timed out, escalated to backup: "
                    f"{inp.venue} (${inp.order_value:,})"
                )
                if isinstance(self._pending_question, dict):
                    self._pending_question = {
                        **self._pending_question,
                        "approver_tier": "backup",
                        "escalated": True,
                    }
                await workflow.wait_condition(lambda: self._answer is not None or self._stop)
            if self._stop:
                self._pending_question = None
                workflow.logger.info(f"Dispatch child stopping while parked, HOLD {inp.order_id}")
                return LgDispatchOutput(
                    decision="HOLD",
                    reasoning="Held: demo shut down before approval",
                    asked_human=True,
                )
            answer = self._answer
            self._answer = None
            self._pending_question = None
            self._approver_tier = "primary"
            if answer == "reject":
                rejected = True
            result = await compiled.ainvoke(Command(resume=answer), config=config)

        # Robust decision: trust the human's answer (rejected flag) over the graph's
        # free-text dispatch_decision (Gemini may return it empty).
        raw = (result.get("dispatch_decision") or "").strip()
        asked = bool(result.get("asked_human"))
        if not raw:
            raw = (
                "Held — human rejected"
                if rejected
                else ("Approved by human — dispatching" if asked else "Within policy — dispatching")
            )
        hold = rejected or "HOLD" in raw.upper()
        workflow.logger.info(
            f"LangGraph dispatch child decided {inp.order_id}: {'HOLD' if hold else 'DISPATCH'}"
        )
        return LgDispatchOutput(
            decision="HOLD" if hold else "DISPATCH",
            driver_id=(result.get("chosen_driver") or "").strip(),  # the agent's pick
            reasoning=raw,
            asked_human=asked,
        )


# --- Main demo orchestrator ---


@workflow.defn
class MeltdownDemoWorkflow:
    """
    Orchestrates the Meltdown demo with continuous order flow.

    Starts 3 driver child workflows, generates orders on a timer,
    runs multi-agent reasoning per order, and signals the chosen driver.
    Handles customer-change signals concurrently.

    Owns driver state: positions, order assignments, disconnect status.
    Activities receive this state as inputs — they never read FleetState
    for decision-making.
    """

    def __init__(self) -> None:
        self._pending_changes: list[CustomerChangeInput] = []
        self._pending_approvals: list[bool] = []
        self._pending_new_orders: list[OrderAssignmentResult] = []
        self._routes_done: bool = False
        self._disconnected_drivers: set[str] = set()
        self._disconnected_agents: set[str] = set()
        # Workflow-owned driver state
        self._driver_orders: dict[str, list[str]] = {}
        self._driver_last_position: dict[str, tuple[float, float]] = {}
        self._orders_generated: int = 0
        self._route_handles: dict = {}
        self._order_gen_handle: workflow.ChildWorkflowHandle | None = None
        # Order tracking for queries
        self._orders: dict[str, dict] = {}
        self._agent_health: dict[str, bool] = {
            "fleet_agent": True,
            "customer_agent": True,
            "resolver": True,
        }
        # Pattern B — LangGraph in-loop ask_human state
        self._langgraph_tasks: list = []  # concurrent _run_langgraph_assignment tasks
        self._pending_dispatch: dict[str, dict] = {}  # order_id -> question (awaiting human)
        # Agent→human in-loop HITL: human answers to an agent's ask_human, keyed by order.
        self._dispatch_answers: dict[str, str] = {}
        # which framework dispatches orders: "adk" | "langgraph" | "crossframework"
        self._dispatch_mode: str = "adk"
        # Cross-framework mode: live LgDispatchWorkflow child handles, keyed by order_id, so
        # shutdown can signal them to stop. The dispatch CHILD owns its own answer_dispatch
        # signal + pending_question query (the human signals the agent's own workflow).
        self._dispatch_children: dict[str, workflow.ChildWorkflowHandle] = {}
        # Per-order re-reason counter so cross-framework re-reason spawns fresh child ids
        # (Temporal rejects a duplicate id for an already-closed workflow).
        self._rereason_count: dict[str, int] = {}
        # Continue-as-new threshold for THIS orchestrator (see PARENT_HISTORY_CONTINUE_AS_NEW).
        self._history_threshold: int = PARENT_HISTORY_CONTINUE_AS_NEW

    # --- Signals ---

    @workflow.signal
    async def customer_change(self, change: CustomerChangeInput) -> None:
        self._pending_changes.append(change)

    @workflow.signal
    async def answer_dispatch(self, order_id: str, decision: str) -> None:
        """Human answers an agent's in-loop ask_human for a specific order (LangGraph path).
        Resolves the durable interrupt the dispatch team is parked on.
        """
        self._dispatch_answers[order_id] = decision

    @workflow.signal
    async def change_approved(self, approved: bool) -> None:
        self._pending_approvals.append(approved)

    @workflow.signal
    async def driver_disconnected(self, inp: DriverDisconnectInput) -> None:
        self._disconnected_drivers.add(inp.driver_id)
        workflow.logger.info(f"Driver {inp.driver_id} disconnected — activities will retry")

    @workflow.signal
    async def driver_reconnected(self, inp: DriverDisconnectInput) -> None:
        self._disconnected_drivers.discard(inp.driver_id)
        workflow.logger.info(f"Driver {inp.driver_id} reconnected — resuming")

    @workflow.signal
    async def agent_disconnected(self, inp: AgentDisconnectInput) -> None:
        self._disconnected_agents.add(inp.agent_name)
        self._agent_health[inp.agent_name] = False
        workflow.logger.info(f"Agent {inp.agent_name} disconnected")

    @workflow.signal
    async def agent_reconnected(self, inp: AgentDisconnectInput) -> None:
        self._disconnected_agents.discard(inp.agent_name)
        self._agent_health[inp.agent_name] = True
        workflow.logger.info(f"Agent {inp.agent_name} reconnected")

    @workflow.signal
    async def order_delivered(self, inp: OrderDeliveredInput) -> None:
        """Signaled by DriverRouteWorkflow when a delivery completes."""
        driver_id = inp.driver_id
        before = len(self._driver_orders.get(driver_id, []))
        if driver_id in self._driver_orders:
            if inp.order_id in self._driver_orders[driver_id]:
                self._driver_orders[driver_id].remove(inp.order_id)
        after = len(self._driver_orders.get(driver_id, []))
        self._driver_last_position[driver_id] = (inp.delivery_lat, inp.delivery_lng)
        if inp.order_id in self._orders:
            self._orders[inp.order_id]["status"] = "delivered"
        workflow.logger.info(
            f"Order {inp.order_id} delivered by {driver_id} (orders: {before} → {after})"
        )

    @workflow.signal
    async def new_order(self, order: OrderAssignmentResult) -> None:
        """Signaled by OrderGenerationWorkflow with each new order to assign."""
        self._pending_new_orders.append(order)

    @workflow.signal
    async def set_dispatch_mode(self, mode: str) -> None:
        """UI tab selects which framework dispatches orders: 'adk' or 'langgraph'."""
        self._dispatch_mode = mode
        workflow.logger.info(f"Dispatch mode → {mode}")

    # --- Queries ---

    @workflow.query
    def get_status(self) -> dict:
        return {
            "routes_done": self._routes_done,
            "orders_generated": self._orders_generated,
            "pending_changes": len(self._pending_changes),
            "disconnected_drivers": list(self._disconnected_drivers),
            "disconnected_agents": list(self._disconnected_agents),
            "driver_orders": {cid: list(oids) for cid, oids in self._driver_orders.items()},
            "orders": dict(self._orders),
            "agent_health": dict(self._agent_health),
            "pending_dispatch": dict(self._pending_dispatch),
        }

    # --- Helpers ---

    # First N orders use only 3 drivers so A-C warm up with single
    # deliveries while D-E accumulate batched orders naturally.
    _WARMUP_ORDERS = 5

    def _build_driver_snapshots(self) -> list[DriverSnapshot]:
        """Build driver snapshots from workflow state for activity inputs."""
        snapshots = []
        warming_up = self._orders_generated <= self._WARMUP_ORDERS
        for driver_id in DRIVER_IDS:
            # During warmup, hide drivers D-E so agents only assign to A-C
            if warming_up and driver_id in WARMUP_HIDDEN:
                continue
            pos = self._driver_last_position.get(driver_id, (WAREHOUSE.lat, WAREHOUSE.lng))
            order_count = len(self._driver_orders.get(driver_id, []))
            snapshots.append(
                DriverSnapshot(
                    driver_id=driver_id,
                    lat=pos[0],
                    lng=pos[1],
                    status="disconnected" if driver_id in self._disconnected_drivers else "active",
                    capacity=DRIVER_CAPACITY,
                    current_order_count=order_count,
                    is_disconnected=driver_id in self._disconnected_drivers,
                )
            )
        return snapshots

    def _eligible_drivers(self) -> list[str]:
        """Connected, under-capacity drivers (D-E hidden during warmup)."""
        warming_up = self._orders_generated <= self._WARMUP_ORDERS
        return [
            d
            for d in DRIVER_IDS
            if d in self._route_handles
            and d not in self._disconnected_drivers
            and len(self._driver_orders.get(d, [])) < DRIVER_CAPACITY
            and not (warming_up and d in WARMUP_HIDDEN)
        ]

    def _least_loaded_driver(self) -> str:
        """Least-loaded eligible driver (fleet-spread); falls back to driver-a."""
        eligible = self._eligible_drivers()
        if eligible:
            return min(eligible, key=lambda d: len(self._driver_orders.get(d, [])))
        return DRIVER_IDS[0]

    # --- Main entry ---

    @workflow.run
    async def run(self, inp: MeltdownDemoInput) -> str:
        workflow.logger.info(f"Meltdown demo starting (escalation={inp.escalation_enabled})")
        # Seed dispatch mode from the active tab at start so the first orders don't race a
        # later set_dispatch_mode signal ("the tab chooses the framework for all orders").
        # getattr-with-default so a workflow STARTED before this field existed still replays
        # cleanly (Temporal replays old inputs against new code — don't read new fields raw).
        self._dispatch_mode = getattr(inp, "dispatch_mode", "adk")
        self._history_threshold = (
            getattr(inp, "history_threshold", 0) or PARENT_HISTORY_CONTINUE_AS_NEW
        )
        # Long-lived ORCHESTRATOR: a continued generation reseeds its live state and RE-ACQUIRES
        # its still-running children by id instead of restarting them (they survived this
        # parent's continue-as-new because they were started ParentClosePolicy.ABANDON). DORMANT
        # in the demo — the bounded run never reaches the threshold; this path is exercised by
        # unit tests of the pure helpers. See _build_continue_as_new_input / _apply_continuation.
        continued = workflow.info().continued_run_id is not None

        if continued:
            self._apply_continuation(inp)
            self._route_handles = {
                d: workflow.get_external_workflow_handle(f"route-{d}") for d in DRIVER_IDS
            }
            self._order_gen_handle = workflow.get_external_workflow_handle("order-generation")
        else:
            # Fresh start (the demo path) — initialize driver state and start the children.
            for driver_id in DRIVER_IDS:
                self._driver_orders[driver_id] = []
                self._driver_last_position[driver_id] = (WAREHOUSE.lat, WAREHOUSE.lng)

            # Hide drivers D-E in FleetState during warmup so
            # tool_get_fleet_status only shows A-C to the LLM
            await workflow.execute_activity(
                set_warmup_hidden,
                args=[WARMUP_HIDDEN, True],
                task_queue=DELIVERY_QUEUE,
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=FAST_RETRY,
            )

            # Start driver child workflows. ParentClosePolicy.ABANDON so a parent
            # continue-as-new does NOT terminate these long-lived drivers; the demo stops them
            # explicitly at shutdown, so normal teardown is unchanged.
            self._route_handles = {}
            for driver_id in DRIVER_IDS:
                handle = await workflow.start_child_workflow(
                    DriverRouteWorkflow.run,
                    DriverRouteInput(driver_id=driver_id),
                    id=f"route-{driver_id}",
                    static_summary=f"{driver_id} — delivery loop",
                    parent_close_policy=workflow.ParentClosePolicy.ABANDON,
                )
                self._route_handles[driver_id] = handle

            # Start order generation as a child workflow. Cross-framework mode generates fewer,
            # slower orders (each order = 2 child workflows) so the Temporal UI stays legible;
            # the ADK/LangGraph tabs keep the full fleet flow. Seeded from the start-time mode.
            if self._dispatch_mode == "crossframework":
                gen_max = min(inp.max_orders, CROSSFRAMEWORK_MAX_ORDERS)
                gen_interval = CROSSFRAMEWORK_ORDER_INTERVAL_SECONDS
                gen_burst = CROSSFRAMEWORK_WARMUP_BURST_ORDERS
            else:
                gen_max = inp.max_orders
                gen_interval = ORDER_INTERVAL_SECONDS
                gen_burst = WARMUP_BURST_ORDERS
            self._order_gen_handle = await workflow.start_child_workflow(
                OrderGenerationWorkflow.run,
                OrderGenerationInput(
                    max_orders=gen_max,
                    order_interval_seconds=gen_interval,
                    warmup_burst_orders=gen_burst,
                ),
                id="order-generation",
                static_summary="Order generation + agent assignment",
                parent_close_policy=workflow.ParentClosePolicy.ABANDON,
            )

        # Process new orders and customer changes concurrently
        order_task = asyncio.create_task(self._process_new_orders())
        change_task = asyncio.create_task(self._process_customer_changes())

        # Wait for order generation to complete — but continue-as-new if THIS parent's own
        # history grows past the threshold at a quiescent point (dormant in the demo).
        await self._await_order_gen_or_continue(continued)

        # Drain any remaining orders that arrived via signal before stopping
        while self._pending_new_orders:
            order = self._pending_new_orders.pop(0)
            await self._assign_order(order)

        # Stop all drivers and concurrent loops
        self._routes_done = True
        for handle in self._route_handles.values():
            try:
                await handle.signal(DriverRouteWorkflow.stop)
            except Exception:
                pass

        # Let in-flight LangGraph assignment tasks finish committing/rejecting before we go.
        # _routes_done is set, so a task parked on ask_human exits cleanly via
        # _await_dispatch_answer's escape; an actively-reasoning task gets a grace window to
        # commit. Only stragglers past the window are cancelled. (Exceptions are surfaced by
        # the per-task done callback in _assign_order, not swallowed.)
        if self._langgraph_tasks:
            # Cross-framework mode parks its durable wait INSIDE the dispatch child, so
            # _routes_done can't unblock it — signal each parked child to stop so it returns
            # a HOLD-default and the parent task's `await lg_handle` unblocks cleanly.
            for handle in list(self._dispatch_children.values()):
                try:
                    await handle.signal(LgDispatchWorkflow.stop)
                except Exception:
                    pass
            try:
                await workflow.wait_condition(
                    lambda: all(t.done() for t in self._langgraph_tasks),
                    timeout=timedelta(seconds=30),
                )
            except TimeoutError:
                workflow.logger.warning("LangGraph tasks still running at shutdown — cancelling")
            for task in self._langgraph_tasks:
                if not task.done():
                    task.cancel()
        self._pending_dispatch.clear()

        await change_task
        await order_task

        # Wait for drivers to finish current deliveries
        results = []
        for driver_id, handle in self._route_handles.items():
            try:
                result = await handle
                results.append(result)
            except Exception as e:
                results.append(f"{driver_id}: {e}")

        return f"Meltdown demo complete. Results: {results}"

    # --- Continue-as-new (long-lived orchestrator; dormant in the demo) ---

    def _parent_quiescent(self) -> bool:
        """True when the parent holds no in-flight assignment work — the only point at which it
        is safe to continue-as-new without losing an in-progress decision: nothing waiting to be
        assigned, no dispatch parked on a human, and every fire-and-forget assignment task done.
        Pure/synchronous so it can be unit-tested without a worker."""
        return (
            not self._pending_new_orders
            and not self._pending_dispatch
            and all(t.done() for t in self._langgraph_tasks)
        )

    def _parent_should_continue_as_new(self, history_length: int) -> bool:
        """Continue-as-new only at a quiescent point, once this parent's own history crosses the
        threshold. DORMANT in the demo (threshold high; the bounded run finishes first). Pure so
        it can be unit-tested by passing a history length."""
        return self._parent_quiescent() and history_length >= self._history_threshold

    def _build_continue_as_new_input(self) -> MeltdownDemoInput:
        """Snapshot the parent's LIVE state for the next generation: the capacity ledger,
        counters, and mode. NOT carried: child handles (re-acquired by id) and per-order UI
        metadata (rebuilt as orders flow). max_orders=0 because order generation is a surviving
        child — the new generation re-acquires it, it does not restart it. Pure → unit-testable."""
        return MeltdownDemoInput(
            escalation_enabled=False,
            max_orders=0,
            dispatch_mode=self._dispatch_mode,
            history_threshold=self._history_threshold,
            driver_orders={d: list(v) for d, v in self._driver_orders.items()},
            orders_generated=self._orders_generated,
            rereason_counts=dict(self._rereason_count),
        )

    def _apply_continuation(self, inp: MeltdownDemoInput) -> None:
        """Reseed the parent's live state from a carried input on a continued run. Pure →
        unit-testable. (getattr-read so a pre-field run still replays.)"""
        carried_orders = getattr(inp, "driver_orders", {}) or {}
        self._driver_orders = {d: list(carried_orders.get(d, [])) for d in DRIVER_IDS}
        self._driver_last_position = {d: (WAREHOUSE.lat, WAREHOUSE.lng) for d in DRIVER_IDS}
        self._orders_generated = getattr(inp, "orders_generated", 0)
        self._rereason_count = dict(getattr(inp, "rereason_counts", {}) or {})

    async def _drain_order_gen(self) -> str:
        """Await the order-generation child to completion (fresh runs only — the handle is an
        awaitable child there)."""
        return await self._order_gen_handle

    async def _await_order_gen_or_continue(self, continued: bool) -> None:
        """Wait for order generation to finish, but continue-as-new if this parent's own history
        grows past PARENT_HISTORY_CONTINUE_AS_NEW at a quiescent point — the long-lived-
        orchestrator analog of the driver's continue-as-new. DORMANT in the demo: the run
        completes long before the threshold, so this behaves exactly like the previous bare
        `await self._order_gen_handle`.

        Fresh runs hold an awaitable child handle; drain it in a task so we can also poll for a
        continue-as-new opportunity. On a continued run the handle is external (no awaitable
        result), so completion is driven by the continue-as-new / stop path (dormant)."""
        gen = None
        if not continued and self._order_gen_handle is not None:
            gen = asyncio.create_task(self._drain_order_gen())
        while True:
            try:
                await workflow.wait_condition(
                    lambda: gen is not None and gen.done(),
                    timeout=timedelta(seconds=30),
                )
            except TimeoutError:
                pass
            if gen is not None and gen.done():
                gen.result()  # propagate any error from the order-gen child
                return
            if self._parent_should_continue_as_new(workflow.info().get_current_history_length()):
                if gen is not None and not gen.done():
                    gen.cancel()
                workflow.logger.info("Parent continue-as-new (history bound) — handing off")
                workflow.continue_as_new(self._build_continue_as_new_input())

    # --- ADK inline assignment (live mode) ---

    async def _run_adk_assignment(
        self, inp: ReasonAboutAssignmentInput
    ) -> ReasonAboutAssignmentOutput:
        """Run ADK agents inline in the workflow.

        Each LLM call and tool call is a separate Temporal activity via
        TemporalModel + activity_tool. This gives per-call durability and
        visibility in the Temporal UI. Failures propagate — Temporal retries.
        """
        workflow.logger.info(f"Running ADK assignment for {inp.order_id}")
        agent = create_order_assignment_agent()

        session_service = InMemorySessionService()
        runner = Runner(
            agent=agent,
            app_name="meltdown_demo",
            session_service=session_service,
        )

        session = await session_service.create_session(
            app_name="meltdown_demo",
            user_id="workflow",
        )

        # Build agent status context
        agent_status_lines = []
        if inp.disconnected_agents:
            for agent in inp.disconnected_agents:
                agent_status_lines.append(f"⚠️ {agent} is OFFLINE — compensate with available data.")
        agent_context = "\n".join(agent_status_lines) + "\n\n" if agent_status_lines else ""

        prompt = (
            f"{agent_context}"
            f"NEW ORDER — assign to the best driver:\n"
            f"Order ID: {inp.order_id}\n"
            f"Venue: {inp.hotel}\n"
            f"Event: {inp.event}\n"
            f"Priority: {inp.priority}\n"
            f"Servings: {inp.servings}\n"
            f"Deadline: {inp.deadline_minutes} minutes\n"
            f"Coordinates: ({inp.delivery_lat}, {inp.delivery_lng})\n\n"
            f"Assess fleet capacity and customer priority, then the resolver "
            f"MUST call tool_submit_assignment with the driver_id and reasoning."
        )

        events_count = 0
        async for event in runner.run_async(
            user_id="workflow",
            session_id=session.id,
            new_message=Content(parts=[Part(text=prompt)]),
        ):
            events_count += 1

        updated_session = await session_service.get_session(
            app_name="meltdown_demo",
            user_id="workflow",
            session_id=session.id,
        )

        state = updated_session.state or {}
        assignment_dict = state.get("assignment")
        if not assignment_dict:
            workflow.logger.warning(
                "ADK did not call tool_submit_assignment — LLM instruction failure"
            )
            assignment_dict = {"driver_id": "", "reasoning_summary": ""}

        driver_id = assignment_dict["driver_id"]
        reasoning = assignment_dict.get("reasoning_summary", "ADK assignment")

        # Extract agent outputs for UI summary events
        fleet_output = state.get("fleet_assessment", "")
        customer_output = state.get("customer_assessment", "")

        # Build short summary events — first sentence only
        def _first_sentence(text: str) -> str:
            text = text.strip()
            for sep in [".", "\n"]:
                idx = text.find(sep)
                if idx > 0:
                    return text[: idx + 1].strip()
            return text[:80]

        events = []
        if fleet_output:
            events.append(
                {
                    "agent_name": "fleet_agent",
                    "event_type": "assessment",
                    "content": fleet_output,
                    "summary": _first_sentence(fleet_output),
                }
            )
        if customer_output:
            events.append(
                {
                    "agent_name": "customer_agent",
                    "event_type": "assessment",
                    "content": customer_output,
                    "summary": _first_sentence(customer_output),
                }
            )
        # Extract order number from order_id (e.g. "order-3" → "3")
        order_number = inp.order_id.split("-", 1)[-1] if "-" in inp.order_id else inp.order_id
        hotel = inp.hotel

        events.append(
            {
                "agent_name": "resolver",
                "event_type": "plan",
                "content": f"Order #{order_number} → {driver_id} — {hotel} — {reasoning}",
                "summary": f"Order #{order_number} → {driver_id} — {hotel}",
            }
        )

        # If Fleet Agent was disconnected, publish a Dispatch Agent note about the gap
        if "fleet_agent" in inp.disconnected_agents:
            events.append(
                {
                    "agent_name": "resolver",
                    "event_type": "assessment",
                    "content": "Fleet Agent offline — assigned with customer data only",
                    "summary": "Fleet Agent offline — customer data only",
                }
            )

        workflow.logger.info(f"ADK assignment complete: {events_count} events, driver={driver_id}")
        return ReasonAboutAssignmentOutput(
            driver_id=driver_id,
            reasoning_summary=reasoning,
            agent_events=events,
        )

    # --- New order processing (triggered by signals from OrderGenerationWorkflow) ---

    async def _process_new_orders(self) -> None:
        """Process new orders as they arrive via signal from OrderGenerationWorkflow."""
        while not self._routes_done:
            await workflow.wait_condition(
                lambda: len(self._pending_new_orders) > 0 or self._routes_done,
            )

            if self._routes_done:
                break

            while self._pending_new_orders:
                order = self._pending_new_orders.pop(0)
                await self._assign_order(order)

    async def _assign_order(self, order: OrderAssignmentResult) -> None:
        """Run ADK assignment for a new order and signal the chosen driver."""
        onum = order.order_id.split("-", 1)[-1]
        self._orders_generated += 1

        # End warmup: unhide drivers D-E when warmup orders are done
        if self._orders_generated == self._WARMUP_ORDERS + 1:
            await workflow.execute_activity(
                set_warmup_hidden,
                args=[WARMUP_HIDDEN, False],
                task_queue=DELIVERY_QUEUE,
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=FAST_RETRY,
            )
            workflow.logger.info("Warmup complete — drivers D-E now visible")

        # Track order in workflow state
        self._orders[order.order_id] = {
            "order_id": order.order_id,
            "hotel": order.hotel,
            "priority": order.priority,
            "servings": order.servings,
            "delivery_lat": order.delivery_lat,
            "delivery_lng": order.delivery_lng,
            "assigned_driver_id": None,
            "status": "pending",
            "deadline_minutes": order.deadline_minutes,
            # Re-reasons after an address change read these back (_rereason_crossframework,
            # _rereason_order); without them the agents saw "revised order" at $0.
            "event": order.event,
            "order_value": order.order_value,
        }

        # Build driver snapshots from workflow state — passed to activity as input
        driver_snapshots = self._build_driver_snapshots()

        # The active UI tab sets the dispatch framework. In "langgraph" mode the
        # multi-agent dispatch decision runs INLINE in this workflow (mirroring the ADK
        # path) — agent nodes execute as Temporal activities recorded in this history,
        # NOT a per-order child. Run it concurrently so the order loop and the fleet keep
        # moving while the agents (and possibly a human) deliberate. "adk" mode uses the
        # ADK assignment path below.
        if self._dispatch_mode == "langgraph":
            task = asyncio.create_task(
                self._run_langgraph_assignment(order, self._least_loaded_driver(), onum)
            )
            task.add_done_callback(self._on_langgraph_task_done)
            self._langgraph_tasks.append(task)
            return

        # Cross-framework mode: Temporal orchestrates across frameworks — an ADK child
        # produces the assessments, a LangGraph child makes the dispatch decision. Run
        # concurrently (reusing _langgraph_tasks so the existing shutdown drain covers it)
        # so the order loop and fleet keep moving while the agents (and maybe a human)
        # deliberate in their child workflows.
        if self._dispatch_mode == "crossframework":
            task = asyncio.create_task(
                self._run_crossframework_assignment(order, self._least_loaded_driver(), onum)
            )
            task.add_done_callback(self._on_langgraph_task_done)
            self._langgraph_tasks.append(task)
            return

        assignment_input = ReasonAboutAssignmentInput(
            order_id=order.order_id,
            hotel=order.hotel,
            delivery_lat=order.delivery_lat,
            delivery_lng=order.delivery_lng,
            priority=order.priority,
            servings=order.servings,
            deadline_minutes=order.deadline_minutes,
            event=order.event,
            driver_snapshots=driver_snapshots,
            disconnected_agents=list(self._disconnected_agents),
        )

        assignment = await self._run_adk_assignment(assignment_input)
        # Determine if this is a degraded assignment (Fleet Agent offline)
        fleet_offline = "fleet_agent" in self._disconnected_agents

        # Publish summary events to FleetState — single batched local activity
        if assignment.agent_events:
            await workflow.execute_local_activity(
                publish_agent_events_batch,
                [
                    PublishAgentEventInput(
                        agent_name=evt["agent_name"],
                        event_type=evt["event_type"],
                        content=evt["content"],
                        summary=evt.get("summary", ""),
                    )
                    for evt in assignment.agent_events
                ],
                start_to_close_timeout=timedelta(seconds=10),
            )

        # Validate and reassign if chosen driver is invalid, full, or disconnected
        driver_id = assignment.driver_id
        warming_up = self._orders_generated <= self._WARMUP_ORDERS
        needs_reassign = (
            driver_id not in self._route_handles
            or driver_id in self._disconnected_drivers
            or len(self._driver_orders.get(driver_id, [])) >= DRIVER_CAPACITY
            or (warming_up and driver_id in WARMUP_HIDDEN)
        )

        if needs_reassign:
            original = driver_id
            reassigned = False
            for fallback_id in DRIVER_IDS:
                if fallback_id == original:
                    continue
                if warming_up and fallback_id in WARMUP_HIDDEN:
                    continue
                if fallback_id in self._disconnected_drivers:
                    continue
                if fallback_id not in self._route_handles:
                    continue
                if len(self._driver_orders.get(fallback_id, [])) < DRIVER_CAPACITY:
                    driver_id = fallback_id
                    reassigned = True
                    reason = (
                        "invalid"
                        if original not in self._route_handles
                        else (
                            "disconnected"
                            if original in self._disconnected_drivers
                            else "at capacity"
                        )
                    )
                    workflow.logger.warning(
                        f"Reassigning {order.order_id}: {original} is {reason} → {driver_id}"
                    )
                    break

            if not reassigned:
                # All drivers unavailable — keep original driver_id so the
                # order is queued on its route handle (delivered on reconnect)
                driver_id = original if original in self._route_handles else DRIVER_IDS[0]
                workflow.logger.warning(
                    f"No available drivers — queuing {order.order_id} on {driver_id}"
                )

        # Spread load across the fleet: prefer the least-loaded eligible driver so the
        # whole fleet stays active. The agents still reason and publish their assessment;
        # this only rebalances the final destination.
        eligible = self._eligible_drivers()
        if eligible:
            least = min(eligible, key=lambda d: len(self._driver_orders.get(d, [])))
            if len(self._driver_orders.get(least, [])) < len(
                self._driver_orders.get(driver_id, [])
            ):
                driver_id = least

        # Routine (human-initiated path): commit the ADK assignment immediately.
        await self._commit_assignment(order, driver_id, fleet_offline, onum)

    async def _signal_driver(self, driver_id: str, signal, arg) -> None:
        """Signal a driver by workflow id (not a cached child handle).

        Two reasons this matters: (1) addressing by id routes to the driver's CURRENT run, so
        it survives a driver continue-as-new (a cached ChildWorkflowHandle pins the original,
        now-closed run → "not found"); (2) it's guarded, so a driver that's already gone —
        completed at shutdown, terminated, or mid-continue-as-new — never crashes the parent.
        Mirrors the guarded child→parent signals."""
        try:
            handle = workflow.get_external_workflow_handle(f"route-{driver_id}")
            await handle.signal(signal, arg)
        except Exception as e:
            name = getattr(signal, "__name__", signal)
            workflow.logger.warning(f"Could not signal {driver_id}.{name} (driver gone): {e}")

    async def _commit_assignment(
        self, order: OrderAssignmentResult, driver_id: str, fleet_offline: bool, onum: str
    ) -> None:
        """Register the assignment in FleetState and signal the chosen driver."""
        # Register final assignment AFTER capacity check
        assigned = await workflow.execute_activity(
            register_assignment,
            args=[driver_id, order.order_id, fleet_offline],
            task_queue=AGENTS_QUEUE,
            summary=f"[#{onum}] Dispatch Agent — {order.order_id} → {driver_id}"
            + (" (degraded)" if fleet_offline else ""),
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=FAST_RETRY,
        )

        # Skip workflow state + child signal if DB write was a no-op
        # (order already cancelled/delivered/assigned)
        if not assigned:
            workflow.logger.info(f"{order.order_id} assignment skipped — already terminal")
            return

        # Update workflow-owned driver state (skip if already assigned —
        # prevents duplicate signals when fallback re-assigns to same driver)
        already_assigned = (
            driver_id in self._driver_orders and order.order_id in self._driver_orders[driver_id]
        )
        if driver_id in self._driver_orders and not already_assigned:
            self._driver_orders[driver_id].append(order.order_id)

        # Update order tracking
        if order.order_id in self._orders:
            self._orders[order.order_id]["assigned_driver_id"] = driver_id
            self._orders[order.order_id]["status"] = "assigned"

        # Signal the chosen driver (skip if already assigned)
        if already_assigned:
            workflow.logger.info(f"{order.order_id} already on {driver_id} — skipping signal")
        elif driver_id in self._route_handles:
            await self._signal_driver(
                driver_id,
                DriverRouteWorkflow.add_order,
                DriverRouteOrder(
                    order_id=order.order_id,
                    hotel=order.hotel,
                    delivery_lat=order.delivery_lat,
                    delivery_lng=order.delivery_lng,
                ),
            )

        workflow.logger.info(f"Order {self._orders_generated}: {order.order_id} → {driver_id}")

    def _on_langgraph_task_done(self, task: asyncio.Task) -> None:
        """Surface failures from the fire-and-forget LangGraph assignment tasks instead of
        letting them be swallowed (the exception is retrieved here so it can't go unobserved).
        """
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            workflow.logger.warning(f"LangGraph assignment task failed: {exc!r}")

    async def _run_langgraph_assignment(
        self, order: OrderAssignmentResult, driver_id: str, onum: str
    ) -> None:
        """Run the LangGraph multi-agent dispatch decision INLINE in this workflow.

        The langgraph-tab counterpart to _run_adk_assignment: the Fleet ∥ Customer →
        Dispatch graph runs in THIS workflow's context (nodes execute as Temporal
        activities recorded in this history), not a per-order child. Agents call ask_human
        as an IN-LOOP tool — that suspends the graph on a durable interrupt(); we surface
        the question, wait for the human's `answer_dispatch` signal, and resume the graph
        via Command(resume=answer). The answer flows back into the agent's reasoning.
        """
        available = sum(
            1
            for d in DRIVER_IDS
            if d not in self._disconnected_drivers
            and len(self._driver_orders.get(d, [])) < DRIVER_CAPACITY
        )
        state = {
            "order_id": order.order_id,
            "venue": order.hotel,
            "order_value": order.order_value,
            "servings": order.servings,
            "deadline_minutes": order.deadline_minutes,
            "proposed_driver_id": driver_id,
            "drivers_available": available,
            "drivers_total": len(DRIVER_IDS),
            "pending_orders": len(self._pending_new_orders),
            "eligible_drivers": self._eligible_drivers(),  # the agent picks from these
        }
        compiled = graph(GRAPH_NAME).compile(checkpointer=InMemorySaver())
        config = {"configurable": {"thread_id": f"{workflow.info().workflow_id}-{order.order_id}"}}

        result = await compiled.ainvoke(state, config=config)
        rejected = False
        # Drive any in-loop ask_human interrupts the agents raise.
        while result.get("__interrupt__"):
            payload = result["__interrupt__"][0].value
            self._pending_dispatch[order.order_id] = payload
            if order.order_id in self._orders:
                self._orders[order.order_id]["status"] = "awaiting_dispatch_approval"
            await workflow.execute_local_activity(
                publish_agent_event,
                PublishAgentEventInput(
                    agent_name="dispatch_gate",
                    event_type="approval_gate",
                    content=(
                        f"{payload.get('agent', 'Agent')} is asking a human about "
                        f"{order.order_id} (${order.order_value:,}): {payload.get('question', '')}"
                    ),
                    summary=f"{order.order_id} — agent asked a human",
                ),
                start_to_close_timeout=timedelta(seconds=10),
            )
            answer = await self._await_dispatch_answer(order.order_id)
            self._pending_dispatch.pop(order.order_id, None)
            if answer is None:  # demo shutting down — exit cleanly
                return
            if answer == "reject":
                rejected = True
            result = await compiled.ainvoke(Command(resume=answer), config=config)

        # Robust decision: the workflow knows the human's answer (rejected flag), so don't
        # depend on the graph's free-text dispatch_decision (Gemini may return it empty).
        raw = (result.get("dispatch_decision") or "").strip()
        asked = bool(result.get("asked_human"))
        if not raw:
            raw = (
                "Held — human rejected"
                if rejected
                else ("Approved by human — dispatching" if asked else "Within policy — dispatching")
            )
        hold = rejected or "HOLD" in raw.upper()
        # The Dispatch agent picks the driver (submit_dispatch); honor it if still assignable,
        # else fall back to the least-loaded driver chosen when the order arrived.
        chosen = (result.get("chosen_driver") or "").strip()
        final_driver = chosen if chosen in self._eligible_drivers() else driver_id
        await self._publish_langgraph_reasoning(
            result, raw, order=order, driver_id=final_driver, hold=hold
        )

        if hold:
            workflow.logger.info(f"[#{onum}] LangGraph dispatch held {order.order_id}")
            # Only a human "reject" is a supervisor rejection. Any other HOLD is the Dispatch
            # agent's own call, which can come right after a human approved.
            agent_hold = ""
            if not rejected:
                agent_hold = "after a human approved it" if asked else "without asking a human"
            await self._reject_order(order, onum, agent_hold=agent_hold)
        else:
            workflow.logger.info(
                f"[#{onum}] LangGraph dispatch → {final_driver}"
                + (f" (agent chose {chosen})" if chosen else " (fallback)")
            )
            await self._commit_assignment(order, final_driver, False, onum)

    async def _await_dispatch_answer(self, order_id: str) -> str | None:
        """Durable wait for a human's answer to an agent's in-loop ask_human (the
        `answer_dispatch` signal). Returns None if the demo shuts down while parked, so the
        team task can exit cleanly instead of blocking the parent's teardown.
        """
        # Primary approver window. On timeout, escalate to a backup approver (mark the
        # pending entry so the UI shows it) and wait unbounded for the backup's answer.
        try:
            await workflow.wait_condition(
                lambda: order_id in self._dispatch_answers or self._routes_done,
                timeout=timedelta(seconds=GATE_ESCALATION_SECONDS),
            )
        except TimeoutError:
            workflow.logger.info(
                f"Approval timed out for {order_id}, escalating to backup approver"
            )
            entry = self._pending_dispatch.get(order_id)
            if isinstance(entry, dict):
                entry["approver_tier"] = "backup"
                entry["escalated"] = True
            await workflow.wait_condition(
                lambda: order_id in self._dispatch_answers or self._routes_done,
            )
        if self._routes_done:
            return None
        if order_id in self._dispatch_answers:
            return self._dispatch_answers.pop(order_id)
        return None

    async def _publish_langgraph_reasoning(
        self,
        result: dict,
        dispatch_note: str,
        order: OrderAssignmentResult | None = None,
        driver_id: str = "",
        hold: bool = False,
    ) -> None:
        """Surface the LangGraph/cross-framework team's reasoning in the Fleet/Customer/Dispatch
        panels. When `order` is given, the Dispatch (resolver) card shows the concrete
        assignment — `Order #N → driver-X — venue` — mirroring the ADK path, instead of just
        the terse decision text."""
        fleet = (result.get("fleet_assessment") or "").strip()
        cust = (result.get("customer_assessment") or "").strip()
        dispatch_note = dispatch_note or "Dispatching."
        if order is not None:
            onum = order.order_id.split("-", 1)[-1] if "-" in order.order_id else order.order_id
            if hold:
                resolver_content = f"Order #{onum} — {order.hotel} — HELD: {dispatch_note}"
                resolver_summary = f"Order #{onum} — {order.hotel} — held"
            else:
                resolver_content = f"Order #{onum} → {driver_id} — {order.hotel} — {dispatch_note}"
                resolver_summary = f"Order #{onum} → {driver_id} — {order.hotel}"
        else:
            resolver_content = dispatch_note
            resolver_summary = dispatch_note[:90]
        await workflow.execute_local_activity(
            publish_agent_events_batch,
            [
                PublishAgentEventInput(
                    agent_name="fleet_agent",
                    event_type="assessment",
                    content=fleet,
                    summary=fleet[:90],
                ),
                PublishAgentEventInput(
                    agent_name="customer_agent",
                    event_type="assessment",
                    content=cust,
                    summary=cust[:90],
                ),
                PublishAgentEventInput(
                    agent_name="resolver",
                    event_type="plan",
                    content=resolver_content,
                    summary=resolver_summary,
                ),
            ],
            start_to_close_timeout=timedelta(seconds=10),
        )

    async def _reject_order(
        self, order: OrderAssignmentResult, onum: str, agent_hold: str = ""
    ) -> None:
        """Don't commit fleet capacity. By default a supervisor rejected the high-value order.
        A non-empty `agent_hold` means the Dispatch agent chose HOLD itself, and says when
        (e.g. "after a human approved it")."""
        if agent_hold:
            content = f"Dispatch agent held order {order.order_id} (${order.order_value:,}) "
            content += f"{agent_hold} — not dispatched, fleet capacity preserved."
            summary = f"{order.order_id} held by Dispatch agent — not dispatched"
        else:
            content = f"Supervisor rejected high-value order {order.order_id} "
            content += f"(${order.order_value:,}) — not dispatched, fleet capacity preserved."
            summary = f"{order.order_id} rejected — not dispatched"
        if order.order_id in self._orders:
            self._orders[order.order_id]["status"] = "rejected"
        # Reflect the rejection in FleetState so the order shows as cancelled in the UI.
        await workflow.execute_activity(
            execute_customer_change,
            ExecuteCustomerChangeInput(order_id=order.order_id, change_type="cancel"),
            task_queue=DELIVERY_QUEUE,
            summary=f"[#{onum}] Dispatch rejected — cancel {order.order_id}",
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=FAST_RETRY,
        )
        await workflow.execute_local_activity(
            publish_agent_event,
            PublishAgentEventInput(
                agent_name="dispatch_gate",
                event_type="change_rejected",
                content=content,
                summary=summary,
            ),
            start_to_close_timeout=timedelta(seconds=10),
        )
        workflow.logger.info(f"[#{onum}] {order.order_id} not dispatched: {summary}")

    # --- Cross-framework assignment (3rd tab): ADK assess child ∥ LangGraph dispatch child ---

    async def _run_crossframework_assignment(
        self, order: OrderAssignmentResult, driver_id: str, onum: str
    ) -> None:
        """Temporal orchestrates ACROSS frameworks: an ADK child produces the Fleet+Customer
        assessments, then a LangGraph child (which owns its own ask_human HITL) makes the
        dispatch decision. The children DECIDE; this parent APPLIES the result via the same
        _commit_assignment / _reject_order used by the other tabs (the parent owns driver
        state and signals the DriverRouteWorkflow). Sequential: the dispatch child needs the
        assessments as input. Child ids `assess-`/`dispatch-<order_id>` give each framework its
        own visible Temporal history — the cross-framework boundary.
        """
        await self._dispatch_via_children(order, driver_id, onum, suffix="")

    async def _dispatch_via_children(
        self,
        order: OrderAssignmentResult,
        driver_id: str,
        onum: str,
        suffix: str,
        apply: bool = True,
    ) -> None:
        """Shared core for cross-framework dispatch — used for the initial assignment and for
        human→agent re-reason (which passes a revision `suffix` to get fresh child ids).

        apply=True commits/rejects via the parent (initial assignment). apply=False only
        re-reasons + publishes (re-reason: the held driver reroutes via the caller's
        update_order/resolve_update signals, so committing here would double-assign)."""
        available = sum(
            1
            for d in DRIVER_IDS
            if d not in self._disconnected_drivers
            and len(self._driver_orders.get(d, [])) < DRIVER_CAPACITY
        )

        # 1. ADK framework child — Fleet ∥ Customer assessment only.
        adk_handle = await workflow.start_child_workflow(
            AdkAssessmentWorkflow.run,
            ReasonAboutAssignmentInput(
                order_id=order.order_id,
                hotel=order.hotel,
                delivery_lat=order.delivery_lat,
                delivery_lng=order.delivery_lng,
                priority=order.priority,
                servings=order.servings,
                deadline_minutes=order.deadline_minutes,
                event=order.event,
                driver_snapshots=self._build_driver_snapshots(),
                disconnected_agents=list(self._disconnected_agents),
            ),
            id=f"assess-{order.order_id}{suffix}",
            static_summary=f"[#{onum}] ADK framework — Fleet ∥ Customer assessment",
        )
        assessment = await adk_handle

        # 2. LangGraph framework child — dispatch decision + its own ask_human HITL.
        child_id = f"dispatch-{order.order_id}{suffix}"
        lg_handle = await workflow.start_child_workflow(
            LgDispatchWorkflow.run,
            LgDispatchInput(
                order_id=order.order_id,
                venue=order.hotel,
                order_value=order.order_value,
                servings=order.servings,
                deadline_minutes=order.deadline_minutes,
                proposed_driver_id=driver_id,
                drivers_available=available,
                drivers_total=len(DRIVER_IDS),
                pending_orders=len(self._pending_new_orders),
                fleet_assessment=assessment.fleet_assessment,
                customer_assessment=assessment.customer_assessment,
                eligible_drivers=self._eligible_drivers(),  # the LG dispatch agent picks from these
            ),
            id=child_id,
            static_summary=f"[#{onum}] LangGraph framework — dispatch decision",
        )
        self._dispatch_children[order.order_id] = lg_handle
        # Roll-up for the server: child_id + order context. The actual ask_human question
        # lives on the child (server queries LgDispatchWorkflow.pending_question per entry).
        self._pending_dispatch[order.order_id] = {
            "child_id": child_id,
            "order_id": order.order_id,
            "venue": order.hotel,
            "order_value": order.order_value,
            "via_child": True,
        }
        if order.order_id in self._orders:
            self._orders[order.order_id]["status"] = "awaiting_dispatch_approval"
        try:
            result = await lg_handle
        finally:
            self._dispatch_children.pop(order.order_id, None)
            self._pending_dispatch.pop(order.order_id, None)

        # 3. Surface reasoning in the agent panels, then APPLY (parent owns driver state).
        # The LangGraph dispatch agent picks the driver; honor it if still assignable, else
        # fall back to the least-loaded driver chosen when the order arrived.
        chosen = (result.driver_id or "").strip()
        final_driver = chosen if chosen in self._eligible_drivers() else driver_id
        await self._publish_langgraph_reasoning(
            # Reuse the assessments the parent already holds (from the ADK child) instead of
            # re-reading them off the dispatch result — keeps LgDispatchOutput thin.
            {
                "fleet_assessment": assessment.fleet_assessment,
                "customer_assessment": assessment.customer_assessment,
            },
            result.reasoning,
            order=order,
            driver_id=final_driver,
            hold=(result.decision == "HOLD"),
        )
        if not apply:
            return
        if result.decision == "HOLD":
            workflow.logger.info(f"[#{onum}] cross-framework held {order.order_id} (human)")
            await self._reject_order(order, onum)
        else:
            workflow.logger.info(
                f"[#{onum}] cross-framework dispatch → {final_driver}"
                + (f" (agent chose {chosen})" if chosen else " (fallback)")
            )
            await self._commit_assignment(order, final_driver, False, onum)

    async def _rereason_crossframework(self, order_id: str, note: str) -> None:
        """Human→agent HITL across frameworks: the human approved a new location, so re-run
        the CROSS-FRAMEWORK flow — ADK Fleet+Customer reassess the new spot, then the LangGraph
        Dispatch agent re-decides — and publish that reassessment to the agent panels. Like
        _rereason_order (the ADK-tab version), this is reasoning-only: the held driver reroutes
        via the caller's update_order/resolve_update signals, so we pass apply=False. Reads the
        order's CURRENT coords, so the caller must update them first. Fresh child ids per
        revision avoid colliding with the original assess-/dispatch- children.
        """
        o = self._orders.get(order_id)
        if o is None:
            return
        await workflow.execute_local_activity(
            publish_agent_event,
            PublishAgentEventInput(
                agent_name="customer_agent",
                event_type="customer_request",
                content=(
                    f"Human revised {order_id}: {note} — agents re-reasoning across frameworks."
                ),
                summary=f"Human revised {order_id} — re-reasoning",
            ),
            start_to_close_timeout=timedelta(seconds=10),
        )
        self._rereason_count[order_id] = self._rereason_count.get(order_id, 0) + 1
        suffix = f"-rev{self._rereason_count[order_id]}"
        onum = order_id.split("-", 1)[-1]
        revised = OrderAssignmentResult(
            order_id=order_id,
            hotel=o["hotel"],
            delivery_lat=o["delivery_lat"],
            delivery_lng=o["delivery_lng"],
            driver_id="",
            reasoning_summary="",
            priority=o.get("priority", "standard"),
            servings=o.get("servings", 0),
            deadline_minutes=o.get("deadline_minutes", 0),
            event=o.get("event", "revised order"),
            order_value=o.get("order_value", 0),
        )
        await self._dispatch_via_children(
            revised, self._least_loaded_driver(), onum, suffix=suffix, apply=False
        )

    # --- Human→agent HITL (ADK): re-reason an order when a human revises its location ---

    async def _rereason_order(self, order_id: str, note: str) -> None:
        """Human→agent HITL, IN the ADK reasoning loop: the human approved a new location,
        so feed the revised order back to the ADK assignment team — Fleet recomputes ETAs to
        the new spot, Customer re-reads priority, Dispatch reassesses — and publish that
        reassessment to the agent panels. The caller applies the operational result (the
        held driver reroutes to the new destination). Reads the order's *current* coords, so
        the caller must update them first.
        """
        order = self._orders.get(order_id)
        if order is None:
            return
        await workflow.execute_local_activity(
            publish_agent_event,
            PublishAgentEventInput(
                agent_name="customer_agent",
                event_type="customer_request",
                content=f"Human revised {order_id}: {note} — agents re-reasoning the assignment.",
                summary=f"Human revised {order_id} — re-reasoning",
            ),
            start_to_close_timeout=timedelta(seconds=10),
        )
        assignment = await self._run_adk_assignment(
            ReasonAboutAssignmentInput(
                order_id=order_id,
                hotel=order["hotel"],
                delivery_lat=order["delivery_lat"],
                delivery_lng=order["delivery_lng"],
                priority=order.get("priority", "standard"),
                servings=order.get("servings", 0),
                deadline_minutes=order.get("deadline_minutes", 0),
                event=order.get("event", "revised order"),
                driver_snapshots=self._build_driver_snapshots(),
                disconnected_agents=list(self._disconnected_agents),
            )
        )
        if assignment.agent_events:
            await workflow.execute_local_activity(
                publish_agent_events_batch,
                [
                    PublishAgentEventInput(
                        agent_name=e["agent_name"],
                        event_type=e["event_type"],
                        content=e["content"],
                        summary=e.get("summary", ""),
                    )
                    for e in assignment.agent_events
                ],
                start_to_close_timeout=timedelta(seconds=10),
            )

    # --- Signal processing loop ---

    def _has_pending_signal(self) -> bool:
        return len(self._pending_changes) > 0

    async def _process_customer_changes(self) -> None:
        while not self._routes_done:
            await workflow.wait_condition(
                lambda: self._has_pending_signal() or self._routes_done,
            )

            if self._routes_done:
                break

            await self._drain_pending_signals()

    async def _drain_pending_signals(self) -> None:
        # Serial processing. With the child's HITL state now keyed per-order
        # (self._pending_holds), concurrent drain would be safe from the
        # overwrite bug that motivated the original serial revert. Keeping
        # serial anyway because it's simpler and matches the demo flow
        # (changes submitted one at a time); the only cost is that driver B
        # stays parked at its HITL hold while driver A's change awaits human
        # approval, which is a minor UX delay in practice.
        while self._pending_changes:
            change = self._pending_changes.pop(0)
            await self._process_customer_change(change)

    async def _wait_for_approval(self) -> bool | None:
        """Pull the next approval off _pending_approvals, or None if the demo
        shuts down while waiting.

        _drain_pending_signals processes serially, so only one caller is ever
        parked here at a time — the loop exists solely to let _routes_done
        (demo shutdown) unblock a parked change without requiring an approval.
        Returns None in that case so the caller can exit cleanly.
        """
        while not self._routes_done:
            await workflow.wait_condition(
                lambda: len(self._pending_approvals) > 0 or self._routes_done,
            )
            if self._routes_done:
                return None
            if self._pending_approvals:
                return self._pending_approvals.pop(0)
        return None

    # --- Customer change handling ---

    async def _process_customer_change(self, change: CustomerChangeInput) -> None:
        cnum = change.order_id.split("-", 1)[-1]

        # Signal child FIRST to hold delivery — before any local activities
        # that could delay the signal past the child's grace period
        driver_id_before_wait = self._find_driver_for_order(change.order_id)
        driver_id = driver_id_before_wait
        if driver_id and driver_id in self._route_handles:
            await self._signal_driver(
                driver_id,
                DriverRouteWorkflow.update_pending,
                OrderUpdateInput(
                    order_id=change.order_id,
                    change_type=change.change_type,
                ),
            )

        await workflow.execute_local_activity(
            publish_agent_event,
            PublishAgentEventInput(
                agent_name="customer_agent",
                event_type="customer_request",
                content=(
                    f"Customer change request for {change.order_id}: "
                    f"{change.change_type} — {change.new_details}"
                ),
            ),
            start_to_close_timeout=timedelta(seconds=10),
        )

        # Wait for human approval — workflow pauses here, order generation
        # and other activities keep running. _wait_for_approval returns None
        # if the demo shuts down (routes_done) while we're parked, so a
        # pending change can't block the parent's teardown on `await handle`.
        approved = await self._wait_for_approval()
        if approved is None:
            return

        # Re-resolve driver_id — order may have been assigned during the wait
        driver_id = self._find_driver_for_order(change.order_id)

        if approved:
            await workflow.execute_activity(
                execute_customer_change,
                ExecuteCustomerChangeInput(
                    order_id=change.order_id,
                    change_type=change.change_type,
                    new_lat=change.new_lat,
                    new_lng=change.new_lng,
                    new_hotel=change.new_hotel,
                ),
                task_queue=DELIVERY_QUEUE,
                summary=f"[#{cnum}] Dispatch Agent — execute change",
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=FAST_RETRY,
            )

            # Human → agent, IN the reasoning loop: for an address change, update the order
            # record to the new location and let the ADK team RE-REASON it (Fleet recomputes
            # ETAs, Dispatch reassesses) before the held driver reroutes below. The human's
            # approved location is the new input the agents reason over.
            if change.change_type == "address_change" and change.new_lat is not None:
                o = self._orders.get(change.order_id)
                if o is not None:
                    o["delivery_lat"] = change.new_lat
                    o["delivery_lng"] = change.new_lng
                    if change.new_hotel is not None:
                        o["hotel"] = change.new_hotel
                if self._dispatch_mode == "crossframework":
                    await self._rereason_crossframework(change.order_id, change.new_details)
                else:
                    await self._rereason_order(change.order_id, change.new_details)

            # Signal child with the approved decision.
            # Send update_pending again ONLY if the driver changed during the
            # approval wait (order was unassigned at submission and got
            # assigned while the human was deciding). With per-order holds
            # this send is always safe — it creates a new hold entry for
            # this specific order on the new driver without touching any
            # other order's hold state.
            if driver_id and driver_id in self._route_handles:
                if driver_id != driver_id_before_wait:
                    await self._signal_driver(
                        driver_id,
                        DriverRouteWorkflow.update_pending,
                        OrderUpdateInput(
                            order_id=change.order_id,
                            change_type=change.change_type,
                        ),
                    )
                # For pending/batched orders: also send cancel_order or
                # update_order so the order is immediately removed/updated
                # without a wasted navigation trip
                if change.change_type == "cancel":
                    await self._signal_driver(
                        driver_id,
                        DriverRouteWorkflow.cancel_order,
                        OrderUpdateInput(
                            order_id=change.order_id,
                            change_type=change.change_type,
                        ),
                    )
                elif change.change_type == "address_change":
                    await self._signal_driver(
                        driver_id,
                        DriverRouteWorkflow.update_order,
                        OrderUpdateInput(
                            order_id=change.order_id,
                            change_type=change.change_type,
                            new_lat=change.new_lat,
                            new_lng=change.new_lng,
                            new_hotel=change.new_hotel,
                        ),
                    )
                # Resolve the HITL hold for active orders
                await self._signal_driver(
                    driver_id,
                    DriverRouteWorkflow.resolve_update,
                    OrderUpdateInput(
                        order_id=change.order_id,
                        change_type=change.change_type,
                        new_lat=change.new_lat,
                        new_lng=change.new_lng,
                        new_hotel=change.new_hotel,
                    ),
                )
                if change.change_type == "cancel":
                    try:
                        self._driver_orders[driver_id].remove(change.order_id)
                    except (KeyError, ValueError):
                        pass
            await workflow.execute_local_activity(
                publish_agent_event,
                PublishAgentEventInput(
                    agent_name="resolver",
                    event_type="change_executed",
                    content=(
                        f"Customer change approved and executed for "
                        f"{change.order_id}: {change.new_details}"
                    ),
                ),
                start_to_close_timeout=timedelta(seconds=10),
            )
        else:
            # Rejected — signal child to release (deliver normally)
            if driver_id and driver_id in self._route_handles:
                await self._signal_driver(
                    driver_id,
                    DriverRouteWorkflow.resolve_update,
                    OrderUpdateInput(
                        order_id=change.order_id,
                        change_type="release",
                    ),
                )
            await workflow.execute_local_activity(
                publish_agent_event,
                PublishAgentEventInput(
                    agent_name="resolver",
                    event_type="change_rejected",
                    content=f"Customer change rejected for {change.order_id}",
                ),
                start_to_close_timeout=timedelta(seconds=10),
            )

    def _find_driver_for_order(self, order_id: str) -> str | None:
        """Find which driver has a given order."""
        for driver_id, orders in self._driver_orders.items():
            if order_id in orders:
                return driver_id
        return None
