"""Integration tests for Temporal workflows using time-skipping test environment.

DriverRouteWorkflow and OrderGenerationWorkflow are tested with mock
activities — no API keys needed. These cover the core Temporal patterns:
signals, activities, cancellation, child workflows.

MeltdownDemoWorkflow requires the full ADK stack (Gemini + GoogleAdkPlugin)
and is tested manually via ./run.sh.
"""

import asyncio
import logging
import os
import subprocess
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from agent_fleet.activities import (
    deliver_order,
    execute_customer_change,
    generate_order,
    get_fleet_status,
    get_order_priorities,
    navigate_to,
    pickup_orders,
    publish_agent_event,
    set_driver_idle,
    set_warmup_hidden,
    sync_driver_position,
)
from agent_fleet.locations import VENUES, WAREHOUSE
from agent_fleet.models import (
    DriverRouteInput,
    DriverRouteOrder,
    OrderAssignmentResult,
    PublishAgentEventInput,
)
from agent_fleet.queues import DELIVERY_QUEUE, WORKFLOWS_QUEUE
from agent_fleet.simulation import fleet


@activity.defn(name="get_route_polyline")
async def _fake_route_polyline(
    origin_lat: float, origin_lng: float, dest_lat: float, dest_lng: float
) -> list[dict[str, float]]:
    """Test stand-in for the Maps Directions activity (no live API call)."""
    return [
        {"lat": origin_lat, "lng": origin_lng},
        {"lat": dest_lat, "lng": dest_lng},
    ]


@pytest.fixture
async def env():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        yield env


@asynccontextmanager
async def run_delivery_workers(env: WorkflowEnvironment):
    """Start workers for delivery workflow tests (no ADK needed)."""
    from agent_fleet.workflows import DriverRouteWorkflow, OrderGenerationWorkflow

    workflow_worker = Worker(
        env.client,
        task_queue=WORKFLOWS_QUEUE,
        workflows=[DriverRouteWorkflow, OrderGenerationWorkflow],
    )
    delivery_worker = Worker(
        env.client,
        task_queue=DELIVERY_QUEUE,
        activities=[
            generate_order,
            navigate_to,
            pickup_orders,
            deliver_order,
            execute_customer_change,
            _fake_route_polyline,
            get_fleet_status,
            get_order_priorities,
            publish_agent_event,
            set_driver_idle,
            set_warmup_hidden,
            sync_driver_position,
        ],
    )

    async with workflow_worker, delivery_worker:
        yield


async def test_driver_route_completes_with_signal(env: WorkflowEnvironment):
    """DriverRouteWorkflow receives an order via signal, delivers it, then stops."""
    from agent_fleet.workflows import DriverRouteWorkflow

    venue = VENUES[0]

    await fleet.register_order(
        order_id="order-1",
        hotel=venue["hotel"],
        label=f"{venue['hotel']} test delivery",
        priority="standard",
        servings=40,
        delivery_coords=venue["coords"],
        deadline_minutes=30,
    )
    await fleet.assign_order_to_driver("driver-a", "order-1")

    async with run_delivery_workers(env):
        handle = await env.client.start_workflow(
            DriverRouteWorkflow.run,
            DriverRouteInput(driver_id="driver-a"),
            id="test-route-driver-a",
            task_queue=WORKFLOWS_QUEUE,
        )

        await handle.signal(
            DriverRouteWorkflow.add_order,
            DriverRouteOrder(
                order_id="order-1",
                hotel=venue["hotel"],
                delivery_lat=venue["coords"].lat,
                delivery_lng=venue["coords"].lng,
            ),
        )

        await asyncio.sleep(2)
        await handle.signal(DriverRouteWorkflow.stop)

        result = await handle.result()
        assert "driver-a" in result
        assert "1 deliveries" in result or "completed" in result.lower()


async def test_driver_route_handles_multiple_orders(env: WorkflowEnvironment):
    """DriverRouteWorkflow processes multiple orders sequentially."""
    from agent_fleet.workflows import DriverRouteWorkflow

    for i, venue in enumerate(VENUES[:2], 1):
        await fleet.register_order(
            order_id=f"order-{i}",
            hotel=venue["hotel"],
            label=f"{venue['hotel']} test",
            priority="standard",
            servings=40,
            delivery_coords=venue["coords"],
            deadline_minutes=30,
        )
        await fleet.assign_order_to_driver("driver-a", f"order-{i}")

    async with run_delivery_workers(env):
        handle = await env.client.start_workflow(
            DriverRouteWorkflow.run,
            DriverRouteInput(driver_id="driver-a"),
            id="test-route-multi",
            task_queue=WORKFLOWS_QUEUE,
        )

        for i, venue in enumerate(VENUES[:2], 1):
            await handle.signal(
                DriverRouteWorkflow.add_order,
                DriverRouteOrder(
                    order_id=f"order-{i}",
                    hotel=venue["hotel"],
                    delivery_lat=venue["coords"].lat,
                    delivery_lng=venue["coords"].lng,
                ),
            )

        await asyncio.sleep(5)
        await handle.signal(DriverRouteWorkflow.stop)

        result = await handle.result()
        assert "2 deliveries" in result


async def test_driver_route_per_order_hitl_holds(env: WorkflowEnvironment):
    """Two update_pending signals for different orders on the same driver
    must each keep their own hold slot — no overwrite, no cross-contamination
    when resolve_update fills in one decision then the other.

    Regression guard for the single-slot _update_pending_order bug: under
    the old single-slot design, the second update_pending would overwrite
    the first and the first order's resolve_update would be silently dropped.
    """
    from agent_fleet.models import OrderUpdateInput
    from agent_fleet.workflows import DriverRouteWorkflow

    async with run_delivery_workers(env):
        handle = await env.client.start_workflow(
            DriverRouteWorkflow.run,
            DriverRouteInput(driver_id="driver-a"),
            id="test-route-holds",
            task_queue=WORKFLOWS_QUEUE,
        )

        try:
            # Two holds, two different orders — both should coexist.
            await handle.signal(
                DriverRouteWorkflow.update_pending,
                OrderUpdateInput(order_id="order-A", change_type="cancel"),
            )
            await handle.signal(
                DriverRouteWorkflow.update_pending,
                OrderUpdateInput(order_id="order-B", change_type="address_change"),
            )

            status = await handle.query(DriverRouteWorkflow.get_status)
            held = set(status["pending_hold_order_ids"])
            assert held == {"order-A", "order-B"}, f"expected both holds, got {held}"

            # Resolve order-A only. order-B's hold must remain intact.
            await handle.signal(
                DriverRouteWorkflow.resolve_update,
                OrderUpdateInput(order_id="order-A", change_type="cancel"),
            )
            status = await handle.query(DriverRouteWorkflow.get_status)
            held = set(status["pending_hold_order_ids"])
            assert held == {"order-A", "order-B"}, (
                f"resolve_update shouldn't drop holds — got {held}"
            )

            # Stale resolve_update for an unknown order_id must be a no-op
            # (drops without affecting existing holds — replaces the
            # fragile single-slot guard).
            await handle.signal(
                DriverRouteWorkflow.resolve_update,
                OrderUpdateInput(order_id="order-NONEXISTENT", change_type="cancel"),
            )
            status = await handle.query(DriverRouteWorkflow.get_status)
            held = set(status["pending_hold_order_ids"])
            assert held == {"order-A", "order-B"}
        finally:
            await handle.signal(DriverRouteWorkflow.stop)
            await handle.result()


async def test_driver_route_continues_as_new(env: WorkflowEnvironment):
    """DriverRouteWorkflow bounds its own history via continue-as-new (the long-lived
    entity pattern) without losing state.

    With a low history_threshold, delivering an order pushes the run past the threshold, so at
    the next idle loop-top the workflow continues-as-new — same workflow id, fresh history,
    carrying forward only its live state. We assert (a) the run id actually rolled over (a
    continue-as-new happened), (b) the first run ended in a CONTINUED_AS_NEW event, and
    (c) the lifetime delivery count survived into the new generation (queried on the new run,
    which has delivered nothing itself).
    """
    from temporalio.api.enums.v1 import EventType

    from agent_fleet.workflows import DriverRouteWorkflow

    venue = VENUES[0]
    await fleet.register_order(
        order_id="can-order-1",
        hotel=venue["hotel"],
        label=f"{venue['hotel']} c-a-n test",
        priority="standard",
        servings=20,
        delivery_coords=venue["coords"],
        deadline_minutes=30,
    )
    await fleet.assign_order_to_driver("driver-a", "can-order-1")

    async with run_delivery_workers(env):
        # Low threshold: one delivery pushes history past it, so the next idle loop-top
        # continues-as-new. A fresh run's baseline (~5 events) stays under it, so a continued
        # run that delivers nothing won't loop on itself.
        await env.client.start_workflow(
            DriverRouteWorkflow.run,
            DriverRouteInput(driver_id="driver-a", history_threshold=15),
            id="test-route-can",
            task_queue=WORKFLOWS_QUEUE,
        )
        handle = env.client.get_workflow_handle("test-route-can")  # tracks the current run
        first_run_id = (await handle.describe()).run_id

        await handle.signal(
            DriverRouteWorkflow.add_order,
            DriverRouteOrder(
                order_id="can-order-1",
                hotel=venue["hotel"],
                delivery_lat=venue["coords"].lat,
                delivery_lng=venue["coords"].lng,
            ),
        )

        # Poll until the workflow delivers and continues-as-new (run id rolls over). The real
        # navigate activity takes ~20s for a full delivery, so give it a generous window.
        new_run_id = first_run_id
        for _ in range(60):
            new_run_id = (await handle.describe()).run_id
            if new_run_id != first_run_id:
                break
            await asyncio.sleep(1)
        # (a) a continue-as-new actually occurred
        assert new_run_id != first_run_id, "workflow should have continued-as-new after a delivery"

        # (b) the first run ended specifically in CONTINUED_AS_NEW
        first = env.client.get_workflow_handle("test-route-can", run_id=first_run_id)
        first_events = [e async for e in first.fetch_history_events()]
        assert any(
            e.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW
            for e in first_events
        ), "first run should have ended in CONTINUED_AS_NEW"

        # (c) lifetime delivery count carried into the new generation, which delivered nothing
        # itself — so a non-zero count can only have come across the continue-as-new boundary.
        status = await handle.query(DriverRouteWorkflow.get_status)
        assert status["lifetime_deliveries"] == 1, (
            f"lifetime count should survive continue-as-new, got {status['lifetime_deliveries']}"
        )

        await handle.signal(DriverRouteWorkflow.stop)


async def test_parent_continue_as_new_decision():
    """The parent's continue-as-new GUARD (pure logic): only at a quiescent point, and only once
    its own history crosses the threshold. These run without a worker/Gemini — the helpers touch
    no workflow APIs — which is how the orchestrator's c-a-n logic is covered even though the full
    MeltdownDemoWorkflow needs the live ADK stack to run end-to-end."""
    from agent_fleet.workflows import MeltdownDemoWorkflow

    wf = MeltdownDemoWorkflow()
    wf._history_threshold = 100

    # Idle + fresh → quiescent, but only crosses once history reaches the threshold.
    assert wf._parent_quiescent() is True
    assert wf._parent_should_continue_as_new(99) is False
    assert wf._parent_should_continue_as_new(100) is True

    # Any in-flight assignment work blocks continue-as-new, even past the threshold.
    wf._pending_new_orders = [object()]
    assert wf._parent_quiescent() is False
    assert wf._parent_should_continue_as_new(10_000) is False
    wf._pending_new_orders = []

    wf._pending_dispatch = {"order-1": {}}  # a dispatch parked on a human
    assert wf._parent_quiescent() is False
    wf._pending_dispatch = {}

    class _Running:
        def done(self):
            return False

    wf._langgraph_tasks = [_Running()]  # a fire-and-forget assignment still running
    assert wf._parent_quiescent() is False


async def test_parent_continue_as_new_state_round_trips():
    """Carried state survives a parent continue-as-new: build an input from one generation's
    live state, apply it to the next, and confirm the capacity ledger, counters, mode, and
    threshold all come across (and the per-driver positions reset to base)."""
    from agent_fleet.workflows import DRIVER_IDS, MeltdownDemoWorkflow

    gen1 = MeltdownDemoWorkflow()
    gen1._dispatch_mode = "crossframework"
    gen1._history_threshold = 12345
    gen1._driver_orders = {"driver-a": ["o1", "o2"], "driver-b": ["o3"]}
    gen1._orders_generated = 7
    gen1._rereason_count = {"o1": 2}

    carried = gen1._build_continue_as_new_input()
    assert carried.max_orders == 0  # order-gen is a surviving child, not restarted
    assert carried.dispatch_mode == "crossframework"
    assert carried.history_threshold == 12345
    assert carried.driver_orders == {"driver-a": ["o1", "o2"], "driver-b": ["o3"]}
    assert carried.orders_generated == 7
    assert carried.rereason_counts == {"o1": 2}

    gen2 = MeltdownDemoWorkflow()
    gen2._apply_continuation(carried)
    assert gen2._orders_generated == 7
    assert gen2._rereason_count == {"o1": 2}
    assert gen2._driver_orders["driver-a"] == ["o1", "o2"]
    assert gen2._driver_orders["driver-b"] == ["o3"]
    # Every known driver is seeded (capacity ledger intact) and positions reset to base.
    assert set(gen2._driver_orders.keys()) == set(DRIVER_IDS)
    assert all(pos == (WAREHOUSE.lat, WAREHOUSE.lng) for pos in gen2._driver_last_position.values())


async def test_deliver_order_cancel_race():
    """If an order was cancelled before deliver_order fires, the activity
    reports success=False so the workflow skips the parent order_delivered
    signal. Direct FleetState + activity test — no workflow env needed.
    """
    from agent_fleet.activities import deliver_order as deliver_order_activity
    from agent_fleet.models import Coords, DeliverInput

    await fleet.reset()
    await fleet.register_order(
        order_id="order-cancel-race",
        hotel="MGM Grand",
        label="MGM — will be cancelled",
        priority="standard",
        servings=10,
        delivery_coords=Coords(lat=36.1024, lng=-115.1725),
        deadline_minutes=30,
    )
    await fleet.assign_order_to_driver("driver-a", "order-cancel-race")
    await fleet.cancel_order("order-cancel-race")

    # deliver_order is an @activity.defn but it's still a plain async
    # function; invoke directly. The CANCELLED-status short-circuit at the
    # top returns without calling any Temporal activity APIs, so no
    # activity-context is needed for this code path.
    result = await deliver_order_activity(
        DeliverInput(driver_id="driver-a", order_id="order-cancel-race")
    )

    assert result.success is False, (
        "deliver_order must report success=False for a cancelled order — "
        "otherwise the workflow signals the parent order_delivered for a "
        "cancelled order and corrupts bookkeeping"
    )


# --- MeltdownDemoWorkflow logic, offline (no worker, no Gemini) ---
# These call the parent's methods directly. The fixture swaps the few workflow APIs they touch
# for fakes, and records what they would publish to the UI.


@pytest.fixture
def offline_workflow_apis(monkeypatch):
    """Stand-ins for workflow.execute_activity / execute_local_activity / logger / info so
    MeltdownDemoWorkflow methods run outside a worker. Returns the published UI events."""
    from agent_fleet import workflows

    published: list[PublishAgentEventInput] = []

    async def _execute_activity(*args, **kwargs):
        return None

    async def _execute_local_activity(fn, arg, **kwargs):
        published.extend(arg if isinstance(arg, list) else [arg])

    monkeypatch.setattr(workflows.workflow, "execute_activity", _execute_activity)
    monkeypatch.setattr(workflows.workflow, "execute_local_activity", _execute_local_activity)
    monkeypatch.setattr(workflows.workflow, "logger", logging.getLogger("test-workflow"))
    monkeypatch.setattr(
        workflows.workflow, "info", lambda: SimpleNamespace(workflow_id="meltdown-demo")
    )
    return published


def _order(order_id: str, event: str = "", order_value: int = 0) -> OrderAssignmentResult:
    venue = VENUES[0]
    return OrderAssignmentResult(
        order_id=order_id,
        hotel=venue["hotel"],
        delivery_lat=venue["coords"].lat,
        delivery_lng=venue["coords"].lng,
        driver_id="",
        reasoning_summary="",
        event=event,
        order_value=order_value,
    )


@pytest.mark.parametrize("env_value, expected", [(None, 30), ("300", 300)])
async def test_gate_escalation_seconds_comes_from_config(env_value, expected):
    """The approver window is config: 30 s by default, or GATE_ESCALATION_SECONDS from .env.
    A fresh interpreter reads the env var the way a worker process does at startup."""
    from agent_fleet import config, workflows

    assert workflows.GATE_ESCALATION_SECONDS == config.GATE_ESCALATION_SECONDS

    env = {k: v for k, v in os.environ.items() if k != "GATE_ESCALATION_SECONDS"}
    if env_value is not None:
        env["GATE_ESCALATION_SECONDS"] = env_value
    code = "from agent_fleet.config import GATE_ESCALATION_SECONDS as s; print(s)"
    out = subprocess.run(
        [sys.executable, "-c", code],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    assert int(out.stdout) == expected


async def test_rereason_keeps_event_and_order_value(offline_workflow_apis):
    """After an approved address change, both re-reason paths see the order's real event and
    value, not the old fallbacks ("revised order", $0)."""
    from agent_fleet.workflows import MeltdownDemoWorkflow

    wf = MeltdownDemoWorkflow()
    wf._dispatch_mode = "crossframework"

    async def _skip_children(order, driver_id, onum):  # the initial assignment isn't under test
        return None

    wf._run_crossframework_assignment = _skip_children
    await wf._assign_order(_order("order-7", event="Keynote tonight", order_value=5400))
    await asyncio.gather(*wf._langgraph_tasks)

    revised = []

    async def _capture_children(order, driver_id, onum, suffix, apply=True):
        revised.append(order)

    wf._dispatch_via_children = _capture_children
    await wf._rereason_crossframework("order-7", "moved to Oracle Park")
    assert (revised[0].event, revised[0].order_value) == ("Keynote tonight", 5400)

    adk_inputs = []

    async def _capture_adk(inp):
        adk_inputs.append(inp)
        return SimpleNamespace(agent_events=[])

    wf._run_adk_assignment = _capture_adk
    await wf._rereason_order("order-7", "moved to Oracle Park")
    assert adk_inputs[0].event == "Keynote tonight"


class _AsksOnceGraph:
    """Stands in for the compiled LangGraph team: asks a human once, then returns `final`."""

    def __init__(self, final: dict):
        self.final = final
        self.calls = 0

    def compile(self, checkpointer=None):
        return self

    async def ainvoke(self, _input, config=None):
        self.calls += 1
        if self.calls == 1:
            question = {"agent": "Dispatch Agent", "question": "Commit a truck to this order?"}
            return {"__interrupt__": [SimpleNamespace(value=question)]}
        return self.final


@pytest.mark.parametrize(
    "answer, expected",
    [
        ("approve", "Dispatch agent held order order-9 ($5,400) after a human approved it"),
        ("reject", "Supervisor rejected high-value order order-9 ($5,400)"),
    ],
)
async def test_hold_message_says_who_held(offline_workflow_apis, monkeypatch, answer, expected):
    """If a human approves and the Dispatch agent still decides HOLD, the UI must not say
    'Supervisor rejected'. A human reject still does."""
    from agent_fleet import workflows

    team = _AsksOnceGraph({"dispatch_decision": "HOLD", "asked_human": True})
    monkeypatch.setattr(workflows, "graph", lambda name: team)
    wf = workflows.MeltdownDemoWorkflow()

    async def _human_answers(order_id):
        return answer

    wf._await_dispatch_answer = _human_answers
    await wf._run_langgraph_assignment(_order("order-9", order_value=5400), "driver-a", "9")

    gate = [e for e in offline_workflow_apis if e.event_type == "change_rejected"]
    assert len(gate) == 1
    assert gate[0].content.startswith(expected)


@pytest.mark.parametrize(
    "reply, decision",
    [
        ("DISPATCH driver-a — closest with capacity.", "DISPATCH"),
        ("No need to hold — dispatch driver-a.", "DISPATCH"),
        ("Holding is unnecessary — dispatch driver-a.", "DISPATCH"),
        ("HOLD — no driver should take this.", "HOLD"),
        ("**Decision:** hold until a truck frees up.", "HOLD"),
        ("Held: the supervisor rejected it.", "HOLD"),
    ],
)
async def test_text_decision_reads_the_leading_word(reply, decision):
    """Without a submit_dispatch call, only a reply that LEADS with the decision holds."""
    from agent_fleet.langgraph_agents import _text_decision

    assert _text_decision(reply) == decision


async def test_dispatch_reason_does_not_hold_on_a_mention(monkeypatch):
    """The Dispatch node's plain-text fallback: mentioning 'hold' is not a HOLD."""
    from langchain_core.messages import AIMessage

    from agent_fleet import langgraph_agents

    class _FakeChatModel:
        async def ainvoke(self, messages):
            return AIMessage(content="No need to hold this routine order — dispatch driver-a.")

    monkeypatch.setattr(langgraph_agents, "_chat_model", lambda tools=None: _FakeChatModel())
    out = await langgraph_agents.dispatch_reason(
        {
            "venue": "Chinatown",
            "order_value": 900,
            "servings": 40,
            "deadline_minutes": 30,
            "drivers_available": 3,
            "drivers_total": 4,
            "pending_orders": 0,
            "eligible_drivers": ["driver-a", "driver-b"],
            "fleet_assessment": "driver-a — 4min ETA",
            "customer_assessment": "standard",
        }
    )
    assert out["dispatch_decision"] == "DISPATCH"


# --- Temporal owns retries: LLM clients make one attempt (config.LLM_MAX_RETRIES = 0) ---


def _llm_agents(agent) -> list:
    """Every LlmAgent in an ADK agent tree, depth first."""
    from google.adk.agents import LlmAgent

    found = [agent] if isinstance(agent, LlmAgent) else []
    for sub in agent.sub_agents:
        found.extend(_llm_agents(sub))
    return found


async def test_adk_agents_make_one_attempt_per_model_call():
    """Each ADK agent asks google-genai for one attempt per request, so a failed model call
    fails its invoke_model activity and Temporal retries it."""
    from google.genai._api_client import retry_args

    from agent_fleet.agents import create_assessment_team_agent, create_order_assignment_agent

    agents = _llm_agents(create_order_assignment_agent()) + _llm_agents(
        create_assessment_team_agent()
    )
    assert [a.name for a in agents] == [
        "assignment_fleet_agent",
        "assignment_customer_agent",
        "assignment_dispatch_agent",
        "assignment_fleet_agent",
        "assignment_customer_agent",
    ]
    for agent in agents:
        retry = agent.generate_content_config.http_options.retry_options
        assert retry.attempts == 1, agent.name
        assert retry_args(retry)["stop"].max_attempt_number == 1, agent.name


async def test_langgraph_chat_model_is_built_with_max_retries_0(monkeypatch):
    """_chat_model passes max_retries=0 to init_chat_model (the langchain-google-genai
    default is 6 tries inside one activity attempt)."""
    import langchain.chat_models

    from agent_fleet import langgraph_agents
    from agent_fleet.config import DEFAULT_MODEL

    calls = []

    def fake_init_chat_model(model, **kwargs):
        calls.append((model, kwargs))
        return object()

    monkeypatch.setattr(langchain.chat_models, "init_chat_model", fake_init_chat_model)
    monkeypatch.delenv("MODEL_PROVIDER", raising=False)
    langgraph_agents._chat_model()
    assert calls == [(DEFAULT_MODEL, {"model_provider": "google_genai", "max_retries": 0})]


async def test_langgraph_gemini_request_makes_one_attempt(monkeypatch):
    """max_retries=0 reaches google-genai as a single attempt. langchain-google-genai's docs
    warn that 0 means "Google's default" (5 tries); the pinned google-genai reads it as one."""
    from google.genai._api_client import retry_args
    from langchain_core.messages import HumanMessage

    from agent_fleet import langgraph_agents

    monkeypatch.setenv("GOOGLE_API_KEY", "test-key")  # no request is sent
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("MODEL_PROVIDER", raising=False)
    model = langgraph_agents._chat_model()
    assert model.max_retries == 0
    request = model._prepare_request([HumanMessage(content="hi")])
    retry = request["config"].http_options.retry_options
    assert retry_args(retry)["stop"].max_attempt_number == 1
