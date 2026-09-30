"""
Temporal activities for the Meltdown ice cream delivery demo.

Each activity is a discrete, retryable unit of work. Activities handle:
- Driver navigation with heartbeats
- Order pickup/delivery
- Fleet status queries (for LLM agents)
- Customer change execution
"""

import asyncio
import math

import httpx
from temporalio import activity

from agent_fleet.config import GOOGLE_MAPS_API_KEY
from agent_fleet.locations import generate_random_order
from agent_fleet.models import (
    DeliverInput,
    DeliverOutput,
    DriverStatus,
    ExecuteCustomerChangeInput,
    ExecuteCustomerChangeOutput,
    GenerateOrderInput,
    GenerateOrderOutput,
    GetFleetStatusInput,
    GetFleetStatusOutput,
    GetOrderPrioritiesInput,
    GetOrderPrioritiesOutput,
    LegType,
    NavigateInput,
    NavigateOutput,
    OrderStatus,
    PickupInput,
    PickupOutput,
    PublishAgentEventInput,
    PublishAgentEventOutput,
)
from agent_fleet.simulation import fleet

# --- Polyline decoding and route fetching ---


def decode_polyline(encoded: str) -> list[tuple[float, float]]:
    """Decode a Google Maps encoded polyline string into (lat, lng) tuples."""
    points = []
    index = 0
    lat = 0
    lng = 0

    while index < len(encoded):
        # Decode latitude
        shift = 0
        result = 0
        while True:
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        dlat = ~(result >> 1) if (result & 1) else (result >> 1)
        lat += dlat

        # Decode longitude
        shift = 0
        result = 0
        while True:
            b = ord(encoded[index]) - 63
            index += 1
            result |= (b & 0x1F) << shift
            shift += 5
            if b < 0x20:
                break
        dlng = ~(result >> 1) if (result & 1) else (result >> 1)
        lng += dlng

        points.append((lat / 1e5, lng / 1e5))

    return points


@activity.defn
async def get_route_polyline(
    origin_lat: float,
    origin_lng: float,
    dest_lat: float,
    dest_lng: float,
) -> list[dict[str, float]]:
    """Fetch route waypoints from Google Maps Directions API (decoded polyline).

    Returns a list of {"lat": float, "lng": float} waypoints.
    Failures propagate to Temporal's retry mechanism.
    """
    origin = f"{origin_lat},{origin_lng}"
    destination = f"{dest_lat},{dest_lng}"
    url = "https://maps.googleapis.com/maps/api/directions/json"
    params = {
        "origin": origin,
        "destination": destination,
        "key": GOOGLE_MAPS_API_KEY,
        "mode": "driving",
    }

    # The API key rides in the query string, so errors name only the status or error type:
    # raise_for_status() would put the full URL in the activity failure (Event History, the
    # Temporal UI, the worker log). `from None` keeps the httpx error out of the failure chain.
    async with httpx.AsyncClient(timeout=8.0) as client:
        try:
            resp = await client.get(url, params=params)
        except httpx.RequestError as e:
            raise RuntimeError(f"Maps Directions API request failed: {type(e).__name__}") from None
        if not resp.is_success:
            raise RuntimeError(f"Maps Directions API HTTP {resp.status_code}")
        data = resp.json()

    if data.get("status") != "OK" or not data.get("routes"):
        raise RuntimeError(f"Maps Directions API returned status: {data.get('status')}")

    encoded = data["routes"][0]["overview_polyline"]["points"]
    decoded = decode_polyline(encoded)
    activity.logger.info(f"[NAV] Google Maps polyline: {len(decoded)} points")
    return [{"lat": lat, "lng": lng} for lat, lng in decoded]


# --- Flat-signature tool activities (called by ADK agents via activity_tool) ---


@activity.defn
async def tool_get_fleet_status() -> str:
    """Check current fleet state: Driver positions, cooler conditions, orders.

    Fails when Fleet Agent is disconnected — Temporal retries until reconnected.
    """
    if await fleet.is_agent_disconnected("fleet_agent"):
        raise RuntimeError("Fleet Agent is disconnected — tool unavailable")
    return await fleet.get_fleet_summary()


@activity.defn
async def tool_get_order_priorities() -> str:
    """Check order priority details: VIP vs standard, deadlines, servings."""
    return await fleet.get_order_priorities_summary()


@activity.defn
async def tool_search_venue_events(venue: str) -> str:
    """Search for current events at a delivery venue (Gemini + Google Search grounding) that
    might raise an order's urgency — the LangGraph Customer agent's analog of the ADK
    GoogleSearchTool, so both frameworks' Customer agents have the same tools.

    Args:
        venue: the venue / hotel name (e.g. "Moscone Center").
    """
    from google import genai
    from google.genai import types

    from agent_fleet.config import DEFAULT_MODEL, GOOGLE_API_KEY, LLM_MAX_RETRIES

    if not GOOGLE_API_KEY:
        return f"Event search unavailable for {venue}."
    try:
        # One attempt, no client-side retries (google-genai counts the first request).
        client = genai.Client(
            api_key=GOOGLE_API_KEY,
            http_options=types.HttpOptions(
                retry_options=types.HttpRetryOptions(attempts=LLM_MAX_RETRIES + 1)
            ),
        )
        resp = await asyncio.to_thread(
            client.models.generate_content,
            model=DEFAULT_MODEL,
            contents=(
                f"In one short sentence: any notable events today/tonight at or near {venue} in "
                "San Francisco that would increase catering urgency? "
                "If none, say 'no notable events'."
            ),
            config=types.GenerateContentConfig(
                tools=[types.Tool(google_search=types.GoogleSearch())]
            ),
        )
        return (resp.text or "").strip() or f"No notable events found for {venue}."
    except Exception as e:
        activity.logger.warning(f"venue-events search failed for {venue}: {e}")
        return f"Event search unavailable for {venue}."


@activity.defn
async def tool_get_route_info(
    origin_lat: float,
    origin_lng: float,
    destination_lat: float,
    destination_lng: float,
    destination_name: str = "",
    origin_name: str = "",
) -> str:
    """Get driving route info between two points using Google Maps Directions API.

    Fails when Fleet Agent is disconnected — Temporal retries until reconnected.
    Returns distance, duration, and step-by-step directions.
    Use this to assess reroute feasibility and ETAs for Driver dispatching.
    Failures propagate to Temporal's retry mechanism.

    Args:
        origin_lat: Starting latitude
        origin_lng: Starting longitude
        destination_lat: Destination latitude
        destination_lng: Destination longitude
        destination_name: Human-readable name of the destination (e.g. "Fairmont San Francisco")
        origin_name: Human-readable name of the origin (e.g. "driver-c")
    """
    if await fleet.is_agent_disconnected("fleet_agent"):
        raise RuntimeError("Fleet Agent is disconnected — tool unavailable")

    import re

    origin = f"{origin_lat},{origin_lng}"
    destination = f"{destination_lat},{destination_lng}"
    url = "https://maps.googleapis.com/maps/api/directions/json"
    params = {
        "origin": origin,
        "destination": destination,
        "key": GOOGLE_MAPS_API_KEY,
        "mode": "driving",
    }

    # Key-safe errors, same as get_route_polyline above.
    async with httpx.AsyncClient(timeout=8.0) as client:
        try:
            resp = await client.get(url, params=params)
        except httpx.RequestError as e:
            raise RuntimeError(f"Maps Directions API request failed: {type(e).__name__}") from None
        if not resp.is_success:
            raise RuntimeError(f"Maps Directions API HTTP {resp.status_code}")
        data = resp.json()

    if data.get("status") != "OK" or not data.get("routes"):
        raise RuntimeError(f"Maps Directions API returned status: {data.get('status')}")

    route = data["routes"][0]
    leg = route["legs"][0]
    distance = leg["distance"]["text"]
    duration = leg["duration"]["text"]
    eta_minutes = max(1, leg["duration"]["value"] // 60)

    steps = []
    for i, step in enumerate(leg["steps"][:5], 1):
        instruction = step["html_instructions"]
        instruction = re.sub(r"<[^>]+>", " ", instruction).strip()
        steps.append(f"  {i}. {instruction} ({step['distance']['text']})")

    dest_label = destination_name or f"({destination_lat:.4f}, {destination_lng:.4f})"
    steps_text = "\n".join(steps)
    return (
        f"Route to {dest_label}:\n"
        f"  Distance: {distance}\n"
        f"  ETA: {duration}\n"
        f"  ETA_MINUTES: {eta_minutes}\n"
        f"  Key directions:\n{steps_text}"
    )


# --- Core delivery activities ---


@activity.defn
async def generate_order(inp: GenerateOrderInput) -> GenerateOrderOutput:
    """Generate a random order from the venue pool and register it in fleet state."""
    order_data = generate_random_order(inp.order_number)

    await fleet.register_order(
        order_id=order_data["order_id"],
        hotel=order_data["hotel"],
        label=order_data["label"],
        priority=order_data["priority"],
        servings=order_data["servings"],
        delivery_coords=order_data["coords"],
        deadline_minutes=order_data["deadline_minutes"],
    )

    activity.logger.info(f"Generated {order_data['order_id']}: {order_data['label']}")
    return GenerateOrderOutput(
        order_id=order_data["order_id"],
        hotel=order_data["hotel"],
        label=order_data["label"],
        priority=order_data["priority"],
        servings=order_data["servings"],
        delivery_lat=order_data["coords"].lat,
        delivery_lng=order_data["coords"].lng,
        deadline_minutes=order_data["deadline_minutes"],
        event=order_data["event"],
        order_value=order_data["order_value"],
    )


@activity.defn
async def register_assignment(driver_id: str, order_id: str, degraded: bool = False) -> bool:
    """Register an ADK-decided assignment in fleet state (UI projection).

    Returns True if assignment was written, False if order was already
    in a terminal state (cancelled/delivered/assigned).
    """
    return await fleet.assign_order_to_driver(driver_id, order_id, degraded=degraded)


@activity.defn
async def navigate_to(inp: NavigateInput) -> NavigateOutput:
    """
    Simulate Driver navigation by interpolating position over N steps.

    The driver always completes navigation (truck keeps moving on the road).
    Disconnect is checked at start (fail-fast on retry while still disconnected)
    and at end (simulates "arrived but can't report back"). Temporal retries
    until reconnected.
    """
    # Fail-fast on retry if still disconnected — don't re-drive the whole route
    if await fleet.is_driver_disconnected(inp.driver_id):
        raise RuntimeError(f"Driver {inp.driver_id} still disconnected — waiting for reconnect")

    leg = inp.leg if isinstance(inp.leg, str) else str(inp.leg)
    status = (
        DriverStatus.EN_ROUTE_PICKUP
        if leg == LegType.PICKUP.value
        else DriverStatus.EN_ROUTE_DELIVERY
    )
    await fleet.set_driver_status(inp.driver_id, status)
    # Skip order status update for return-to-base trips (no real order)
    if inp.order_id and inp.order_id != "return":
        await fleet.update_order_status(
            inp.order_id,
            OrderStatus.IN_TRANSIT,
            f"En route to {leg}",
        )

    # Clear path history at start of delivery leg so trail shows only this leg
    if inp.leg == "delivery":
        await fleet.clear_driver_path_history(inp.driver_id)

    # Read actual position from FleetState — handles retry after disconnect where
    # the driver may have moved (completed navigation) but the workflow didn't get
    # the result. On first attempt this matches the workflow's position. On retry
    # after disconnect it picks up from where the truck actually is on the map.
    start_lat, start_lng = await fleet.get_driver_position(inp.driver_id)

    # If already at destination (e.g., retry after completing navigation but failing
    # the end check), skip driving — just report arrival.
    dist_to_target = math.sqrt(
        (start_lat - inp.target_lat) ** 2 + (start_lng - inp.target_lng) ** 2
    )
    if dist_to_target < 0.001:
        activity.logger.info(f"{inp.driver_id} already at {leg} destination — skipping navigation")
        return NavigateOutput(
            driver_id=inp.driver_id,
            arrived=True,
            final_lat=inp.target_lat,
            final_lng=inp.target_lng,
        )

    # Build the path to interpolate along
    if inp.waypoints and len(inp.waypoints) >= 2:
        path = [(wp["lat"], wp["lng"]) for wp in inp.waypoints]
    else:
        path = [(start_lat, start_lng), (inp.target_lat, inp.target_lng)]

    # Calculate cumulative distances along the path for proportional interpolation
    segment_dists = []
    for i in range(1, len(path)):
        d = math.sqrt((path[i][0] - path[i - 1][0]) ** 2 + (path[i][1] - path[i - 1][1]) ** 2)
        segment_dists.append(d)
    total_dist = sum(segment_dists) or 1e-9

    # Driver always completes the drive — truck doesn't stop mid-road
    for step in range(1, inp.steps + 1):
        activity.heartbeat(f"step {step}/{inp.steps}")

        fraction = step / inp.steps
        target_dist = fraction * total_dist

        accumulated = 0.0
        new_lat, new_lng = path[-1]
        for i, seg_d in enumerate(segment_dists):
            if accumulated + seg_d >= target_dist:
                remaining = target_dist - accumulated
                seg_frac = remaining / seg_d if seg_d > 0 else 1.0
                new_lat = path[i][0] + (path[i + 1][0] - path[i][0]) * seg_frac
                new_lng = path[i][1] + (path[i + 1][1] - path[i][1]) * seg_frac
                break
            accumulated += seg_d

        await fleet.update_driver_position(inp.driver_id, new_lat, new_lng)
        await asyncio.sleep(0.4)

    # Snap to exact target coords at end of leg. Google polylines end
    # *near* but not exactly at the requested destination (the last
    # waypoint is the closest road point to the target), which could
    # leave the driver 20-50m short of inp.target_lat/lng. Without this
    # snap, a retry after disconnect reads the slightly-off position
    # from FleetState, fails the `dist_to_target < 0.001` skip check,
    # and re-drives the entire polyline from its first waypoint (back
    # at the origin) — the "teleport to origin + redo the drive"
    # symptom.
    await fleet.update_driver_position(inp.driver_id, inp.target_lat, inp.target_lng)

    # Driver arrived — but if disconnected, can't report back.
    # This simulates "delivery complete but comms lost."
    # Temporal sees the failure and retries until reconnected.
    if await fleet.is_driver_disconnected(inp.driver_id):
        activity.logger.warning(
            f"{inp.driver_id} arrived at {leg} but is disconnected — cannot report"
        )
        raise RuntimeError(f"Driver {inp.driver_id} arrived but is disconnected — cannot check in")

    activity.logger.info(
        f"{inp.driver_id} arrived at {leg} ({inp.target_lat:.4f}, {inp.target_lng:.4f})"
    )
    return NavigateOutput(
        driver_id=inp.driver_id,
        arrived=True,
        final_lat=inp.target_lat,
        final_lng=inp.target_lng,
    )


@activity.defn
async def pickup_orders(inp: PickupInput) -> PickupOutput:
    """Simulate picking up ice cream orders at the kitchen.

    Driver physically picks up, then checks connection to report.
    If disconnected, Temporal retries until reconnected.
    """
    await fleet.set_driver_status(inp.driver_id, DriverStatus.PICKING_UP)
    for oid in inp.order_ids:
        await fleet.update_order_status(oid, OrderStatus.PICKED_UP, "Picked up")

    await asyncio.sleep(1.5)

    # Pickup done — but can't report if disconnected
    if await fleet.is_driver_disconnected(inp.driver_id):
        raise RuntimeError(f"Driver {inp.driver_id} picked up but cannot report — disconnected")

    activity.logger.info(f"{inp.driver_id} picked up orders {inp.order_ids}")
    return PickupOutput(driver_id=inp.driver_id, success=True)


@activity.defn
async def deliver_order(inp: DeliverInput) -> DeliverOutput:
    """Simulate delivering an ice cream order at a hotel.

    Driver physically delivers, then checks connection to report.
    If disconnected, Temporal retries until reconnected.

    success meaning:
      - True: this order was actually marked DELIVERED (either by this
        call or by a prior successful run that we're replaying after a
        retry). Workflow signals parent `order_delivered`.
      - False: the order was cancelled before delivery could commit.
        Workflow skips the `order_delivered` signal and moves on.

    CANCELLED and DELIVERED are both terminal; update_order_status and
    complete_order_delivery refuse to overwrite either.
    """
    # Inspect the order's terminal state up-front. Two cases matter:
    #   - DELIVERED: this is a retry of a prior successful run. Skip the
    #     driver-status mutation (it would stomp the IDLE we already set
    #     and leave the driver visibly stuck) and return success=True so
    #     the workflow replays the parent signal.
    #   - CANCELLED: a cancel won the race with our delivery attempt.
    #     Skip the work and return success=False so the workflow doesn't
    #     tell the parent we delivered a cancelled order.
    status = await fleet.get_order_status(inp.order_id)
    if status == OrderStatus.DELIVERED.value:
        if await fleet.is_driver_disconnected(inp.driver_id):
            raise RuntimeError(f"Driver {inp.driver_id} disconnected — cannot report")
        activity.logger.info(
            f"{inp.driver_id} deliver_order retry for {inp.order_id} — already DELIVERED, no-op"
        )
        return DeliverOutput(driver_id=inp.driver_id, order_id=inp.order_id, success=True)
    if status == OrderStatus.CANCELLED.value:
        # Same reasoning as the mid-activity cancel path below: if this
        # cancelled order was the driver's last active order, the driver's
        # FleetState status stays on EN_ROUTE_DELIVERY (set by the nav that
        # just completed) through the return-to-base trip until
        # set_driver_idle finally fires. Transition to IDLE here so the UI
        # updates immediately when nothing else is in the queue.
        remaining = await fleet.get_driver_orders(inp.driver_id)
        if len(remaining) == 0:
            await fleet.set_driver_status(inp.driver_id, DriverStatus.IDLE)
        activity.logger.info(
            f"{inp.driver_id} deliver_order skipped — {inp.order_id} CANCELLED before delivery"
        )
        return DeliverOutput(driver_id=inp.driver_id, order_id=inp.order_id, success=False)

    # Fail-fast if disconnected — don't mutate visible state before checking
    if await fleet.is_driver_disconnected(inp.driver_id):
        raise RuntimeError(f"Driver {inp.driver_id} disconnected — cannot deliver")

    await fleet.set_driver_status(inp.driver_id, DriverStatus.DELIVERING)
    await fleet.update_order_status(inp.order_id, OrderStatus.IN_TRANSIT, "Delivering")

    await asyncio.sleep(1.5)

    # Mark delivered (atomic: won't overwrite CANCELLED).
    # If `delivered` is False, a cancel won the race after our up-front
    # status check — report failure so the workflow skips the parent signal.
    # complete_order_delivery unconditionally removes the driver_orders row
    # regardless of the delivered flag, so remaining_count is accurate for
    # the cancel-race path too. We still need to set IDLE if this was the
    # driver's last order — otherwise the driver's status sticks on
    # DELIVERING (set at line 434) through the return-to-base navigation.
    delivered, remaining_count = await fleet.complete_order_delivery(inp.driver_id, inp.order_id)
    if not delivered:
        if remaining_count == 0:
            await fleet.set_driver_status(inp.driver_id, DriverStatus.IDLE)
        activity.logger.info(
            f"{inp.driver_id} deliver_order — cancel won race mid-activity, "
            f"{inp.order_id} will not be signaled as delivered"
        )
        return DeliverOutput(driver_id=inp.driver_id, order_id=inp.order_id, success=False)

    if remaining_count == 0:
        await fleet.set_driver_status(inp.driver_id, DriverStatus.IDLE)

    # Final disconnect check — delivery committed but can't report
    if await fleet.is_driver_disconnected(inp.driver_id):
        raise RuntimeError(f"Driver {inp.driver_id} delivered but cannot report — disconnected")

    activity.logger.info(f"{inp.driver_id} delivered {inp.order_id}")
    return DeliverOutput(driver_id=inp.driver_id, order_id=inp.order_id, success=True)


# --- Agent tool activities (called by ADK agents via activity_tool) ---


@activity.defn
async def get_fleet_status(inp: GetFleetStatusInput) -> GetFleetStatusOutput:
    """Return fleet status summary for Fleet Agent consumption."""
    summary = await fleet.get_fleet_summary()
    return GetFleetStatusOutput(summary=summary)


@activity.defn
async def get_order_priorities(
    inp: GetOrderPrioritiesInput,
) -> GetOrderPrioritiesOutput:
    """Return order priority details for Customer Agent consumption."""
    summary = await fleet.get_order_priorities_summary()
    return GetOrderPrioritiesOutput(summary=summary)


@activity.defn
async def publish_agent_event(
    inp: PublishAgentEventInput,
) -> PublishAgentEventOutput:
    """Publish an agent reasoning event to the UI panel."""
    await fleet.publish_agent_event(
        inp.agent_name, inp.event_type, inp.content, summary=inp.summary
    )
    return PublishAgentEventOutput(success=True)


@activity.defn
async def publish_agent_events_batch(
    events: list[PublishAgentEventInput],
) -> PublishAgentEventOutput:
    """Publish multiple agent events in a single activity call."""
    for evt in events:
        await fleet.publish_agent_event(
            evt.agent_name, evt.event_type, evt.content, summary=evt.summary
        )
    return PublishAgentEventOutput(success=True)


# --- Customer change activities ---


@activity.defn
async def execute_customer_change(
    inp: ExecuteCustomerChangeInput,
) -> ExecuteCustomerChangeOutput:
    """Execute a customer-initiated change (address update or cancellation)."""
    if inp.change_type == "cancel":
        # cancel_order is atomic — won't overwrite DELIVERED
        await fleet.cancel_order(inp.order_id)
        activity.logger.info(f"Order {inp.order_id} cancelled")
    elif (
        inp.change_type == "address_change" and inp.new_lat is not None and inp.new_lng is not None
    ):
        await fleet.update_order_delivery(inp.order_id, inp.new_lat, inp.new_lng, inp.new_hotel)
        # Status update only — note is empty because update_order_delivery
        # already wrote the reroute log entry
        await fleet.update_order_status(inp.order_id, OrderStatus.REROUTED)
        dest = inp.new_hotel or f"({inp.new_lat:.4f}, {inp.new_lng:.4f})"
        activity.logger.info(f"Order {inp.order_id} rerouted to {dest}")

    return ExecuteCustomerChangeOutput(success=True)


# --- Driver status + position sync activities ---


@activity.defn
async def set_driver_idle(driver_id: str) -> None:
    """Set a driver to idle and clear path history in FleetState."""
    await fleet.set_driver_status(driver_id, DriverStatus.IDLE)
    await fleet.clear_driver_path_history(driver_id)


# --- Warmup visibility activity ---


@activity.defn
async def set_warmup_hidden(driver_ids: list[str], hidden: bool = True) -> None:
    """Hide or show drivers during warmup phase."""
    await fleet.set_drivers_warmup_hidden(driver_ids, hidden)


# --- Position sync activity ---


@activity.defn
async def sync_driver_position(driver_id: str) -> list[float]:
    """Read actual driver position from FleetState — used to sync workflow state after reconnect."""
    lat, lng = await fleet.get_driver_position(driver_id)
    return [lat, lng]
