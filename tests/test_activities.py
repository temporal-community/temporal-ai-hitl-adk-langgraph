"""Unit tests for Temporal activities using ActivityEnvironment."""

import httpx
import pytest
from temporalio.api.failure.v1 import Failure
from temporalio.converter import DataConverter
from temporalio.testing import ActivityEnvironment

from agent_fleet import activities
from agent_fleet.activities import (
    deliver_order,
    generate_order,
    get_route_polyline,
    navigate_to,
    pickup_orders,
    sync_driver_position,
    tool_get_route_info,
)
from agent_fleet.models import (
    DeliverInput,
    DriverStatus,
    GenerateOrderInput,
    NavigateInput,
    NavigateOutput,
    OrderStatus,
    PickupInput,
)
from agent_fleet.simulation import fleet


@pytest.fixture
def env():
    return ActivityEnvironment()


async def test_generate_order(env: ActivityEnvironment):
    result = await env.run(generate_order, GenerateOrderInput(order_number=1))
    assert result.order_id == "order-1"
    assert result.hotel  # should have a hotel name
    assert result.servings > 0
    assert result.deadline_minutes > 0

    # Verify order was registered in fleet state
    order = await fleet.get_order("order-1")
    assert order.status == OrderStatus.PENDING
    assert order.hotel == result.hotel


async def test_navigate_to_interpolates_position(env: ActivityEnvironment):
    # Register an order so navigate_to can update its status
    await env.run(generate_order, GenerateOrderInput(order_number=1))
    inp = NavigateInput(
        driver_id="driver-a",
        order_id="order-1",
        target_lat=36.10,
        target_lng=-115.17,
        leg="pickup",
        steps=4,
        start_lat=36.1040,
        start_lng=-115.1530,
    )
    result = await env.run(navigate_to, inp)
    assert isinstance(result, NavigateOutput)
    assert result.final_lat == pytest.approx(36.10, abs=0.01)
    assert result.final_lng == pytest.approx(-115.17, abs=0.01)

    # Driver should be at target position
    lat, lng = await fleet.get_driver_position("driver-a")
    assert lat == pytest.approx(36.10, abs=0.01)
    assert lng == pytest.approx(-115.17, abs=0.01)


async def test_pickup_orders_sets_status(env: ActivityEnvironment):
    # Generate and register an order first
    await env.run(generate_order, GenerateOrderInput(order_number=1))
    await fleet.assign_order_to_driver("driver-a", "order-1")

    result = await env.run(
        pickup_orders,
        PickupInput(driver_id="driver-a", order_ids=["order-1"]),
    )
    assert result.success is True

    c = await fleet.get_driver("driver-a")
    assert c.status == DriverStatus.PICKING_UP

    o = await fleet.get_order("order-1")
    assert o.status == OrderStatus.PICKED_UP


async def test_deliver_order_sets_status(env: ActivityEnvironment):
    await env.run(generate_order, GenerateOrderInput(order_number=1))
    await fleet.assign_order_to_driver("driver-a", "order-1")
    await env.run(
        pickup_orders,
        PickupInput(driver_id="driver-a", order_ids=["order-1"]),
    )

    result = await env.run(
        deliver_order,
        DeliverInput(driver_id="driver-a", order_id="order-1"),
    )
    assert result.success is True

    o = await fleet.get_order("order-1")
    assert o.status == OrderStatus.DELIVERED

    c = await fleet.get_driver("driver-a")
    assert "order-1" not in c.current_orders


async def test_sync_driver_position(env: ActivityEnvironment):
    """sync_driver_position should return actual driver coords from FleetState."""
    # Move driver to a known position
    await fleet.update_driver_position("driver-a", 36.12, -115.18)

    result = await env.run(sync_driver_position, "driver-a")
    assert isinstance(result, list)
    assert len(result) == 2
    assert result[0] == pytest.approx(36.12, abs=0.001)
    assert result[1] == pytest.approx(-115.18, abs=0.001)


async def test_pickup_orders_batch(env: ActivityEnvironment):
    """Batch pickup should mark all orders as picked up."""
    await env.run(generate_order, GenerateOrderInput(order_number=1))
    await env.run(generate_order, GenerateOrderInput(order_number=2))
    await fleet.assign_order_to_driver("driver-a", "order-1")
    await fleet.assign_order_to_driver("driver-a", "order-2")

    result = await env.run(
        pickup_orders,
        PickupInput(driver_id="driver-a", order_ids=["order-1", "order-2"]),
    )
    assert result.success is True

    o1 = await fleet.get_order("order-1")
    o2 = await fleet.get_order("order-2")
    assert o1.status == OrderStatus.PICKED_UP
    assert o2.status == OrderStatus.PICKED_UP


# --- Maps API key must never reach an activity failure ---

FAKE_MAPS_KEY = "test-maps-key-do-not-leak"
MAPS_ACTIVITIES = [
    (get_route_polyline, (37.78, -122.40, 37.79, -122.41)),
    (tool_get_route_info, (37.78, -122.40, 37.79, -122.41)),
]


def _mock_maps(monkeypatch, handler) -> list[httpx.Request]:
    """Route the activities' httpx calls through a MockTransport (no network)."""
    monkeypatch.setattr(activities, "GOOGLE_MAPS_API_KEY", FAKE_MAPS_KEY)
    sent: list[httpx.Request] = []
    real_client = httpx.AsyncClient

    def recording_handler(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return handler(request)

    monkeypatch.setattr(
        activities.httpx,
        "AsyncClient",
        lambda **kw: real_client(transport=httpx.MockTransport(recording_handler), **kw),
    )
    return sent


def _as_recorded_failure(exc: BaseException) -> str:
    """Serialize an exception the way the worker writes it to Event History."""
    failure = Failure()
    DataConverter.default.failure_converter.to_failure(
        exc, DataConverter.default.payload_converter, failure
    )
    return str(failure)


@pytest.mark.parametrize(("fn", "args"), MAPS_ACTIVITIES)
async def test_maps_http_error_does_not_leak_key(env: ActivityEnvironment, monkeypatch, fn, args):
    """A non-2xx Maps response fails the activity without the key-bearing URL."""
    sent = _mock_maps(monkeypatch, lambda request: httpx.Response(403))

    with pytest.raises(RuntimeError, match="Maps Directions API HTTP 403") as exc_info:
        await env.run(fn, *args)

    assert FAKE_MAPS_KEY in str(sent[0].url)  # the key really was on the request URL
    assert FAKE_MAPS_KEY not in str(exc_info.value)
    assert FAKE_MAPS_KEY not in _as_recorded_failure(exc_info.value)


@pytest.mark.parametrize(("fn", "args"), MAPS_ACTIVITIES)
async def test_maps_transport_error_does_not_leak_key(
    env: ActivityEnvironment, monkeypatch, fn, args
):
    """A timeout/connect error is re-raised without chaining the httpx error (and its URL)."""

    def fail(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout(f"timed out calling {request.url}", request=request)

    _mock_maps(monkeypatch, fail)

    with pytest.raises(RuntimeError, match="request failed: ConnectTimeout") as exc_info:
        await env.run(fn, *args)

    assert exc_info.value.__cause__ is None and exc_info.value.__suppress_context__
    assert FAKE_MAPS_KEY not in _as_recorded_failure(exc_info.value)
