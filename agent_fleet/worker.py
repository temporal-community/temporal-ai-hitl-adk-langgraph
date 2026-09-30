"""
Temporal worker entry point for the Meltdown demo.

Runs as a separate process from the FastAPI server.
Three workers on three task queues:
  - meltdown-workflows: workflows only (no activities, dedicated to replay)
  - meltdown-delivery: navigation, pickup, delivery, customer changes
  - meltdown-agents: LLM/ADK tool calls (rate-limited, max 5 concurrent)

Run with:
    python -m agent_fleet.worker
"""

from __future__ import annotations

import asyncio
import logging
import signal
import time
from pathlib import Path

from temporalio.client import Client
from temporalio.contrib.google_adk_agents import GoogleAdkPlugin
from temporalio.contrib.langgraph import LangGraphPlugin
from temporalio.contrib.pydantic import PydanticPayloadConverter
from temporalio.converter import DataConverter
from temporalio.worker import Worker

from agent_fleet.activities import (
    deliver_order,
    execute_customer_change,
    generate_order,
    get_fleet_status,
    get_order_priorities,
    get_route_polyline,
    navigate_to,
    pickup_orders,
    publish_agent_event,
    publish_agent_events_batch,
    register_assignment,
    set_driver_idle,
    set_warmup_hidden,
    sync_driver_position,
    tool_get_fleet_status,
    tool_get_order_priorities,
    tool_get_route_info,
    tool_search_venue_events,
)
from agent_fleet.config import FLEET_DB_PATH, TEMPORAL_ADDRESS
from agent_fleet.langgraph_agents import (
    DISPATCH_ONLY_GRAPH_NAME,
    GRAPH_NAME,
    build_dispatch_only_graph,
    build_dispatch_team_graph,
)
from agent_fleet.queues import AGENTS_QUEUE, DELIVERY_QUEUE, WORKFLOWS_QUEUE
from agent_fleet.workflows import (
    AdkAssessmentWorkflow,
    DriverRouteWorkflow,
    LgDispatchWorkflow,
    MeltdownDemoWorkflow,
    OrderGenerationWorkflow,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Quiet Temporal SDK internals — "Timer started", replay chatter, etc.
logging.getLogger("temporalio.worker").setLevel(logging.WARNING)
logging.getLogger("temporalio.activity").setLevel(logging.WARNING)
logging.getLogger("temporalio.workflow").setLevel(logging.WARNING)
# httpx logs every request URL at INFO, and Maps Directions puts the API key in the query string.
logging.getLogger("httpx").setLevel(logging.WARNING)


def create_workflow_worker(client: Client) -> Worker:
    """Workflow worker with local activity support for UI projection.

    GoogleAdkPlugin is needed here for sandbox passthroughs (google.adk,
    google.genai) and deterministic runtime (uuid, time) during replay.
    publish_agent_event registered for local activity execution.
    """
    return Worker(
        client,
        task_queue=WORKFLOWS_QUEUE,
        workflows=[
            MeltdownDemoWorkflow,
            DriverRouteWorkflow,
            OrderGenerationWorkflow,
            # Cross-framework tab: per-order agent child workflows (ADK assessment ∥
            # LangGraph dispatch), joined by MeltdownDemoWorkflow.
            AdkAssessmentWorkflow,
            LgDispatchWorkflow,
        ],
        activities=[publish_agent_event, publish_agent_events_batch],
        # LangGraphPlugin runs the looping multi-agent team (Fleet ∥ Customer reason→act→eval
        # loops → Dispatch), run INLINE in the parent for every langgraph-mode order. Its node
        # activities (the Gemini reason calls + each tool call) execute on this worker. Agents
        # call ask_human mid-loop; the workflow drives the interrupt + answer_dispatch signal
        # resume. See agent_fleet/langgraph_agents.py.
        plugins=[
            GoogleAdkPlugin(),
            LangGraphPlugin(
                graphs={
                    GRAPH_NAME: build_dispatch_team_graph(),
                    # Dispatch-only graph for the cross-framework tab's LangGraph child.
                    DISPATCH_ONLY_GRAPH_NAME: build_dispatch_only_graph(),
                }
            ),
        ],
    )


def create_delivery_worker(client: Client) -> Worker:
    """Navigation, pickup, delivery, order generation, and customer change activities."""
    return Worker(
        client,
        task_queue=DELIVERY_QUEUE,
        activities=[
            generate_order,
            navigate_to,
            pickup_orders,
            deliver_order,
            execute_customer_change,
            get_route_polyline,
            get_fleet_status,
            get_order_priorities,
            publish_agent_event,
            set_driver_idle,
            set_warmup_hidden,
            sync_driver_position,
        ],
        max_concurrent_activities=20,
    )


def create_agents_worker(client: Client) -> Worker:
    """ADK/LLM activities — rate-limited.

    GoogleAdkPlugin registers the invoke_model activity that TemporalModel
    routes LLM calls to.
    """
    return Worker(
        client,
        task_queue=AGENTS_QUEUE,
        activities=[
            register_assignment,
            tool_get_fleet_status,
            tool_get_order_priorities,
            tool_get_route_info,
            tool_search_venue_events,
        ],
        max_concurrent_activities=5,
        plugins=[GoogleAdkPlugin()],
    )


async def create_worker(client: Client) -> list[Worker]:
    """Create all three workers. Returns list for server.py to manage."""
    logger.info("Starting workers (LIVE MODE)")
    return [
        create_workflow_worker(client),
        create_delivery_worker(client),
        create_agents_worker(client),
    ]


# Liveness heartbeat for the UI's Service Online/Offline badge (read by /api/worker-health).
# A pure liveness signal, independent of task-queue activity, so it stays accurate even when
# the fleet is idle (unlike task-queue poller freshness).
_WORKER_HEARTBEAT_PATH = Path(FLEET_DB_PATH).with_name("worker_heartbeat")


async def _heartbeat_loop() -> None:
    """Touch the heartbeat file every ~2s while the worker is alive."""
    while True:
        try:
            _WORKER_HEARTBEAT_PATH.write_text(str(time.time()))
        except Exception:
            logger.debug("heartbeat write failed", exc_info=True)
        await asyncio.sleep(2)


async def run_worker() -> None:
    """Connect to Temporal and run all workers until interrupted."""
    from agent_fleet.config import GOOGLE_API_KEY, GOOGLE_MAPS_API_KEY

    missing_keys = [
        name
        for name, value in (
            ("GOOGLE_API_KEY", GOOGLE_API_KEY),
            ("GOOGLE_MAPS_API_KEY", GOOGLE_MAPS_API_KEY),
        )
        if not value or value.startswith("your-")
    ]
    if missing_keys:
        names = ", ".join(missing_keys)
        raise RuntimeError(
            f"Missing required environment variables: {names}. "
            "Copy .env.example to .env and set both keys."
        )

    maps_key = "SET" if GOOGLE_MAPS_API_KEY else "NOT SET"
    gemini_key = "SET" if GOOGLE_API_KEY else "NOT SET"
    logger.info(f"Worker mode: LIVE (GOOGLE_MAPS_API_KEY={maps_key}, GOOGLE_API_KEY={gemini_key})")

    logger.info(f"Connecting to Temporal at {TEMPORAL_ADDRESS}...")
    client = await Client.connect(
        TEMPORAL_ADDRESS,
        data_converter=DataConverter(
            payload_converter_class=PydanticPayloadConverter,
        ),
    )
    workers = await create_worker(client)
    logger.info(f"Workers started on queues: {WORKFLOWS_QUEUE}, {DELIVERY_QUEUE}, {AGENTS_QUEUE}")

    # Graceful shutdown on SIGINT/SIGTERM
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown_event.set)

    # Heartbeat for the UI's worker online/offline badge (see /api/worker-health).
    heartbeat_task = asyncio.create_task(_heartbeat_loop())

    # Run all workers; cancel on shutdown signal
    tasks = [asyncio.create_task(w.run()) for w in workers]
    shutdown_task = asyncio.create_task(shutdown_event.wait())
    try:
        done, _ = await asyncio.wait([*tasks, shutdown_task], return_when=asyncio.FIRST_COMPLETED)
        if shutdown_event.is_set():
            logger.info("Shutdown signal received, stopping workers...")
        else:
            exited_task = next(task for task in done if task is not shutdown_task)
            await exited_task
            raise RuntimeError("A Temporal worker exited unexpectedly")
    finally:
        for task in [*tasks, shutdown_task, heartbeat_task]:
            task.cancel()
        await asyncio.gather(*tasks, shutdown_task, heartbeat_task, return_exceptions=True)
        # Remove the heartbeat so the UI flips to "offline" immediately on a clean stop
        # (Ctrl-C, or `make stop-worker`, which sends SIGTERM). `make kill-worker` sends
        # SIGKILL, so this never runs and the heartbeat ages out instead.
        _WORKER_HEARTBEAT_PATH.unlink(missing_ok=True)
        logger.info("Workers stopped.")


if __name__ == "__main__":
    asyncio.run(run_worker())
