"""LangGraph multi-agent dispatch team — the LangGraph counterpart to agents.py (ADK).

Same Fleet ∥ Customer → Dispatch service, built on ``temporalio.contrib.langgraph``.
The structural point: this is the *whole* assignment service expressed in LangGraph,
the mirror of the ADK team in ``agents.py`` — not just an escalation gate.

Unlike a single ``ainvoke`` per node, **Fleet and Customer are real reason → act → eval
loops** (ReAct). Each binds the same Temporal-activity tools the ADK team uses, and
**every tool call runs as its own Temporal activity** (mirroring ADK's ``activity_tool``
granularity). Fan out from START → both loops → converge on the Dispatch loop, which makes
the final call (runs once BOTH finish).

**Agent → human HITL, IN the loop (the human is a tool):** Fleet and Dispatch can call
``ask_human`` mid-reasoning when they need outside help / sign-off. That tool's execution
is a durable LangGraph ``interrupt()`` — the graph suspends, the *workflow*
(``_run_langgraph_assignment``) surfaces the question, waits for the human's
``answer_dispatch`` signal, and resumes via ``Command(resume=answer)``. The answer flows
back as the observation the agent reasons on next. So the human is an in-loop async tool,
not a boundary gate.

Per-agent loop (what the graph visualizer shows, per branch)::

    <agent>_reason   ── LLM call, runs as a Temporal ACTIVITY
        │   route: tools? human? done?
        ├── tools ─▶ <agent>_act    ── each tool call = its own ACTIVITY ─▶ back to reason
        ├── human ─▶ <agent>_human  ── interrupt() durable pause ─▶ back to reason
        └── done  ─▶ converge on Dispatch

Determinism note: nodes that run inline in the workflow (``_act`` nodes, the conditional
``route`` fns, and the ``_human`` interrupt nodes) MUST be ``async`` — LangGraph offloads
*sync* callables to a thread executor, which Temporal's deterministic event loop forbids.
"""

from __future__ import annotations

import inspect
import re
from datetime import timedelta
from typing import Annotated, Any, TypedDict

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from langgraph.graph import END, START, StateGraph
    from langgraph.graph.message import add_messages
    from langgraph.types import interrupt

    from agent_fleet.activities import (
        tool_get_fleet_status,
        tool_get_order_priorities,
        tool_get_route_info,
        tool_search_venue_events,
    )
    from agent_fleet.queues import AGENTS_QUEUE

# Graph name registered with LangGraphPlugin (see worker.py).
GRAPH_NAME = "dispatch_team"  # the looping multi-agent team; agents call ask_human in-loop
# Dispatch-ONLY graph for the cross-framework tab (3rd tab): the LangGraph half of a
# split where Fleet+Customer run on ADK (a separate child workflow) and only the
# Dispatch agent runs on LangGraph. Seeded with the ADK-produced assessments; the
# Dispatch agent reasons on them and may call ask_human in-loop. No Fleet/Customer
# nodes, no fan-in barrier.
DISPATCH_ONLY_GRAPH_NAME = "dispatch_only"

# Each tool call is scheduled as its own activity with these knobs. Failures after retry
# surface to the LLM as an error string (see _run_tools) so an agent can reason around a
# down tool — same graceful-degradation behavior as the ADK activity_tool wrapper.
_TOOL_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=10),
    maximum_attempts=3,
)

# name (as the LLM sees it) -> (activity fn, task queue). The act node maps the model's
# tool_calls onto these. No LangChain needed on the workflow side — just execute_activity.
_TOOL_ACTIVITIES: dict[str, Any] = {
    "get_fleet_status": tool_get_fleet_status,
    "get_route_info": tool_get_route_info,
    "get_order_priorities": tool_get_order_priorities,
    "search_venue_events": tool_search_venue_events,
}

ESCALATION_GUIDANCE = (
    "You are the Dispatch agent for an ice cream catering fleet. You have assessments from "
    "the Fleet agent and the Customer agent below.\n\n"
    "Routine catering orders run up to about $2,000, and the fleet absorbs them automatically. "
    "For those, reply DISPATCH and do NOT call any tool — even if drivers are momentarily busy. "
    "Tight capacity by itself is normal operations, NOT a reason to involve a human.\n\n"
    "When you genuinely need a human's judgment — an EXCEPTIONAL order (roughly $3,000+ or a "
    "major VIP commitment) where committing scarce fleet capacity warrants a supervisor's "
    "sign-off — call ask_human with a clear, specific question, then REASON over their answer "
    "together with the Fleet and Customer assessments and the order details. Ask the human at "
    "most once, and only when truly warranted.\n\n"
    "Decide by calling submit_dispatch(driver_id, decision): pick the best driver from the "
    "eligible list (weigh the Fleet agent's ETAs/capacity and the Customer agent's priority) "
    "with decision='dispatch'; or decision='hold' (driver_id='') if a human rejected it or no "
    "driver should take it. If you asked a human, let their answer steer this — approval means "
    "commit the best driver; rejection means hold (or pick a safer driver if they suggested one)."
)

_FLEET_SYS = (
    "You are the Fleet Operations AI for Meltdown Ice Cream Delivery. A new order has arrived "
    "— work out which driver should handle it.\n"
    "Step 1: call get_fleet_status to see each driver's position, capacity, and status.\n"
    "Step 2: call get_route_info for the 1-3 closest drivers with capacity to get real driving "
    "ETAs. Do NOT call it for every driver.\n"
    "Never recommend a DISCONNECTED or at-capacity driver. If you're stuck — e.g. no good "
    "driver is available, or the only option is risky — you may call ask_human ONCE with a "
    "specific question and use the answer. When you have enough to decide, reply with ONLY the "
    "recommended driver id and a one-line reason (e.g. 'driver-b — 4min ETA, closest with "
    "capacity'). No preamble."
)

_CUSTOMER_SYS = (
    "You are the Customer Relations AI for Meltdown Ice Cream Delivery. A new order has arrived "
    "— assess its priority and urgency.\n"
    "Call get_order_priorities for the order details, and search_venue_events to check for "
    "current events at the venue that raise urgency. Then reply with ONLY a one-line priority "
    "read (e.g. 'VIP, tight deadline (25min), 200 servings, conference at venue tonight'). "
    "No preamble."
)


class TeamState(TypedDict, total=False):
    # --- order / fleet context (input) ---
    order_id: str
    venue: str
    order_value: int
    servings: int
    deadline_minutes: int
    proposed_driver_id: str
    drivers_available: int
    drivers_total: int
    pending_orders: int
    eligible_drivers: list  # driver ids the Dispatch agent may choose from
    # --- per-agent ReAct message threads (each branch loops on its own) ---
    fleet_messages: Annotated[list, add_messages]
    customer_messages: Annotated[list, add_messages]
    dispatch_messages: Annotated[list, add_messages]
    # --- produced by the team ---
    fleet_assessment: str
    customer_assessment: str
    fleet_impact: str
    dispatch_decision: str  # "DISPATCH" | "HOLD" — the Dispatch agent's call
    chosen_driver: str  # the driver the Dispatch agent picked (via submit_dispatch)
    asked_human: bool  # did any agent call ask_human this run?
    dispatch_human_answer: str  # the human's answer to Dispatch's ask_human (if any)


# --------------------------------------------------------------------------- #
# Shared helpers
# --------------------------------------------------------------------------- #
def _fleet_impact(state: TeamState) -> str:
    return (
        f"{state['drivers_available']}/{state['drivers_total']} drivers free · "
        f"{state['pending_orders']} pending — commits a slot ahead of other customers."
    )


def _chat_model(tools: list | None = None):
    """Build the (provider-swappable) chat model — runs on the activity side."""
    import os

    from langchain.chat_models import init_chat_model

    from agent_fleet.config import DEFAULT_MODEL

    provider = os.environ.get("MODEL_PROVIDER", "google_genai")
    model = init_chat_model(DEFAULT_MODEL, model_provider=provider)
    return model.bind_tools(tools) if tools else model


def _coerce_text(content: Any) -> str:
    """Flatten a chat message's content to text. Gemini returns the final answer as a
    plain string, but tool-call turns (and some providers) use a list of content parts.
    """
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = [
            p if isinstance(p, str) else p.get("text", "")
            for p in content
            if isinstance(p, (str, dict))
        ]
        return " ".join(t for t in parts if t)
    return str(content)


def _last_text(messages: list | None) -> str:
    """The agent's concluding text: the last AI message that ISN'T a tool-call turn."""
    for msg in reversed(messages or []):
        if getattr(msg, "type", "") != "ai" or getattr(msg, "tool_calls", None):
            continue
        text = _coerce_text(getattr(msg, "content", "")).strip()
        if text:
            return text
    return ""


# A plain-text decision counts only when the reply LEADS with it ("HOLD — ...", "Decision: held").
_LEADING_DECISION = re.compile(r"^\W*(?:decision\W*)?(hold|held|dispatch)\b", re.IGNORECASE)


def _text_decision(text: str) -> str:
    """HOLD or DISPATCH from a reply that didn't call submit_dispatch. A reply that only
    mentions holding ("No need to hold — dispatch driver-a") stays DISPATCH."""
    match = _LEADING_DECISION.match(text)
    return "HOLD" if match and match.group(1).lower() in ("hold", "held") else "DISPATCH"


def _node_summary(agent: str, phase: str) -> str:
    """Temporal-UI label for an agent reason node.

    PLACEHOLDER SEAM: today this returns a static string (the installed LangGraph plugin
    only accepts a static ``summary`` in node metadata). When ``temporalio[langgraph]``
    ships per-call ``summary_fn`` support — the LangGraph analog of ADK's ``TemporalModel``
    ``summary_fn`` (see ``_build_summary`` in agents.py) — swap these for a callable that
    inspects the node's state/messages to produce context-aware labels (e.g.
    "Fleet Agent — ETA for driver-c"). TODO(summary_fn): pass ``summary_fn=...`` in the
    ``add_node`` metadata once the plugin supports it; the static string stays the fallback.
    """
    return f"{agent} — {phase}"


def _tool_summary(agent_label: str, name: str, args: dict[str, Any]) -> str:
    """Human-readable label for the per-call activity in the Temporal UI."""
    dest = args.get("destination_name") or ""
    origin = args.get("origin_name") or ""
    if name == "get_route_info" and (origin or dest):
        return f"{agent_label} — assess ETA — {origin} → {dest}".rstrip(" →")
    if name == "search_venue_events" and args.get("venue"):
        return f"{agent_label} — search events — {args['venue']}"
    pretty = name.replace("_", " ")
    return f"{agent_label} — {pretty}"


def _ordered_args(fn: Any, args: dict[str, Any]) -> list:
    """Map an LLM tool_call's kwargs dict onto the activity's positional args."""
    sig = inspect.signature(fn)
    bound = sig.bind_partial(**args)
    bound.apply_defaults()
    return list(bound.arguments.values())


async def _run_tools(ai_message: Any, agent_label: str) -> list:
    """Execute each tool the model asked for as its OWN Temporal activity (inline in
    the workflow). Returns one ToolMessage per call so the next reason step observes them.
    """
    from langchain_core.messages import ToolMessage

    out: list = []
    for call in getattr(ai_message, "tool_calls", None) or []:
        name = call["name"]
        if name == ASK_HUMAN:
            continue  # handled by the human interrupt node, never as an activity
        args = call.get("args") or {}
        activity_fn = _TOOL_ACTIVITIES.get(name)
        if activity_fn is None:
            out.append(
                ToolMessage(
                    content=f"ERROR: unknown tool {name}", tool_call_id=call["id"], name=name
                )
            )
            continue
        summary = _tool_summary(agent_label, name, args)
        try:
            if args:
                result = await workflow.execute_activity(
                    activity_fn,
                    args=_ordered_args(activity_fn, args),
                    task_queue=AGENTS_QUEUE,
                    summary=summary,
                    start_to_close_timeout=timedelta(seconds=20),
                    retry_policy=_TOOL_RETRY,
                )
            else:
                result = await workflow.execute_activity(
                    activity_fn,
                    task_queue=AGENTS_QUEUE,
                    summary=summary,
                    start_to_close_timeout=timedelta(seconds=20),
                    retry_policy=_TOOL_RETRY,
                )
        except Exception as e:  # retries exhausted — let the LLM reason around it
            result = f"ERROR: {name} failed — {e}"
        out.append(ToolMessage(content=str(result), tool_call_id=call["id"], name=name))
    return out


def _has_ask_human(message: Any) -> bool:
    return any(c["name"] == ASK_HUMAN for c in (getattr(message, "tool_calls", None) or []))


async def _human_node(messages: list, agent_label: str, state: TeamState) -> list:
    """Agent→human HITL, IN the loop: the agent called ask_human, so suspend the graph on
    a durable interrupt() and return the human's answer as the observation. The workflow
    surfaces the question and resumes via Command(resume=answer) once a human signals — so
    the human is an async API the agent calls as a tool, mid-reasoning.
    """
    from langchain_core.messages import ToolMessage

    out: list = []
    for call in getattr(messages[-1], "tool_calls", None) or []:
        if call["name"] != ASK_HUMAN:
            continue
        answer = interrupt(
            {
                "agent": agent_label,
                "order_id": state.get("order_id"),
                "venue": state.get("venue"),
                "order_value": state.get("order_value"),
                "question": (call.get("args") or {}).get("question", ""),
                "fleet_assessment": state.get("fleet_assessment", ""),
                "customer_assessment": state.get("customer_assessment", ""),
            }
        )
        out.append(ToolMessage(content=str(answer), tool_call_id=call["id"], name=ASK_HUMAN))
    return out


# --------------------------------------------------------------------------- #
# Tool schemas (LangChain) — only the model needs these, on the activity side.
# Bodies never run: execution is routed to the Temporal activities in _run_tools.
# --------------------------------------------------------------------------- #
ASK_HUMAN = "ask_human"  # tool name; its execution is a durable interrupt, not an activity


def _ask_human_tool():
    from langchain_core.tools import tool

    @tool
    def ask_human(question: str) -> str:
        """Ask a human dispatcher for help or sign-off when you genuinely can't decide
        alone — e.g. committing scarce fleet capacity for an exceptional/VIP order, or no
        safe driver is available. Returns the human's answer (e.g. 'approve'/'reject' or
        guidance). Use sparingly; ask at most once.

        Args:
            question: a clear, specific question for the human.
        """
        raise NotImplementedError  # executed as a durable interrupt() in the human node

    return ask_human


def _fleet_tools() -> list:
    from langchain_core.tools import tool

    @tool
    def get_fleet_status() -> str:
        """Current fleet state: each driver's position, capacity, and status."""
        raise NotImplementedError  # executed as a Temporal activity in the act node

    @tool
    def get_route_info(
        origin_lat: float,
        origin_lng: float,
        destination_lat: float,
        destination_lng: float,
        destination_name: str = "",
        origin_name: str = "",
    ) -> str:
        """Driving distance and ETA between two points (Google Maps Directions)."""
        raise NotImplementedError

    return [get_fleet_status, get_route_info, _ask_human_tool()]


def _customer_tools() -> list:
    from langchain_core.tools import tool

    @tool
    def get_order_priorities() -> str:
        """Order priority details: VIP vs standard, deadlines, servings."""
        raise NotImplementedError

    @tool
    def search_venue_events(venue: str) -> str:
        """Search current events at the delivery venue that could raise urgency.

        Args:
            venue: the venue / hotel name.
        """
        raise NotImplementedError

    return [get_order_priorities, search_venue_events]


SUBMIT_DISPATCH = "submit_dispatch"  # the Dispatch agent's final decision (driver + dispatch/hold)


def _submit_dispatch_tool():
    from langchain_core.tools import tool

    @tool
    def submit_dispatch(driver_id: str, decision: str) -> str:
        """Commit your dispatch decision. Call this once you've decided.

        Args:
            driver_id: the driver to assign (one of the eligible driver ids), or "" if holding.
            decision: "dispatch" to assign the driver, or "hold" to not commit the order.
        """
        raise NotImplementedError  # the decision is read from the tool call args, not executed

    return submit_dispatch


def _dispatch_tools() -> list:
    # The dispatch agent's toolset. ask_human is the human-in-the-loop, exposed as just
    # another tool: the agent calls it like submit_dispatch, on its own judgment. What
    # differs is execution: its call suspends the graph on a durable interrupt (a Temporal
    # signal + wait_condition), not an activity. That is the whole "human as a tool" pattern.
    return [_ask_human_tool(), _submit_dispatch_tool()]


# --------------------------------------------------------------------------- #
# Agent nodes — reason (activity) / act (workflow, per-call activities) / eval (edge)
# --------------------------------------------------------------------------- #
async def fleet_reason(state: TeamState) -> dict:
    """Fleet agent reasoning turn (Temporal activity): decide on a tool call or conclude."""
    from langchain_core.messages import HumanMessage, SystemMessage

    msgs = list(state.get("fleet_messages") or [])
    seed: list = []
    if not msgs:
        prompt = (
            f"New order at {state['venue']} — ${state['order_value']:,}, "
            f"{state['servings']} servings, due in {state['deadline_minutes']} min.\n"
            f"Fleet: {_fleet_impact(state)}"
        )
        seed = [SystemMessage(content=_FLEET_SYS), HumanMessage(content=prompt)]
        msgs = seed
    resp = await _chat_model(tools=_fleet_tools()).ainvoke(msgs)
    out: dict = {"fleet_messages": seed + [resp]}
    # When the agent concludes (no tool call), stash its assessment NOW as a plain
    # string. `resp` is a fresh object here, so its content is reliable — extracting it
    # later in the dispatch ACTIVITY would read messages post-serialization, where the
    # text isn't reliably reachable.
    if not getattr(resp, "tool_calls", None):
        out["fleet_assessment"] = _coerce_text(resp.content).strip()
    return out


async def fleet_act(state: TeamState) -> dict:
    """Run the Fleet agent's requested tools (each its own activity), inline in workflow."""
    return {"fleet_messages": await _run_tools(state["fleet_messages"][-1], "Fleet Agent")}


async def fleet_human(state: TeamState) -> dict:
    """Fleet called ask_human — suspend on the durable interrupt, observe the answer."""
    return {
        "fleet_messages": await _human_node(state["fleet_messages"], "Fleet Agent", state),
        "asked_human": True,
    }


async def fleet_route(state: TeamState) -> str:
    """Eval/observe: ask the human, run tools, or converge on dispatch."""
    last = state["fleet_messages"][-1]
    if _has_ask_human(last):
        return "fleet_human"
    if getattr(last, "tool_calls", None):
        return "fleet_act"
    return "dispatch_reason"


async def customer_reason(state: TeamState) -> dict:
    """Customer agent reasoning turn (Temporal activity)."""
    from langchain_core.messages import HumanMessage, SystemMessage

    msgs = list(state.get("customer_messages") or [])
    seed: list = []
    if not msgs:
        prompt = (
            f"New order at {state['venue']} — ${state['order_value']:,}, "
            f"{state['servings']} servings, due in {state['deadline_minutes']} min."
        )
        seed = [SystemMessage(content=_CUSTOMER_SYS), HumanMessage(content=prompt)]
        msgs = seed
    resp = await _chat_model(tools=_customer_tools()).ainvoke(msgs)
    out: dict = {"customer_messages": seed + [resp]}
    if not getattr(resp, "tool_calls", None):
        out["customer_assessment"] = _coerce_text(resp.content).strip()
    return out


async def customer_act(state: TeamState) -> dict:
    return {"customer_messages": await _run_tools(state["customer_messages"][-1], "Customer Agent")}


async def customer_eval(state: TeamState) -> str:
    last = state["customer_messages"][-1]
    return "customer_act" if getattr(last, "tool_calls", None) else "dispatch_reason"


async def dispatch_reason(state: TeamState) -> dict:
    """Dispatch agent reasoning turn (Temporal activity): weigh both assessments and decide.
    May call ask_human (mid-loop) for sign-off, then reason again on the answer.
    """
    from langchain_core.messages import HumanMessage, SystemMessage

    msgs = list(state.get("dispatch_messages") or [])
    out: dict = {}
    seed: list = []
    if not msgs:
        fleet_assessment = state.get("fleet_assessment") or _last_text(state.get("fleet_messages"))
        customer_assessment = state.get("customer_assessment") or _last_text(
            state.get("customer_messages")
        )
        eligible = ", ".join(state.get("eligible_drivers") or []) or "(none free)"
        prompt = (
            f"Order: {state['venue']} — ${state['order_value']:,} — {state['servings']} servings, "
            f"due in {state['deadline_minutes']} min.\n"
            f"Fleet agent: {fleet_assessment}\n"
            f"Customer agent: {customer_assessment}\n"
            f"Fleet: {_fleet_impact(state)}\n"
            f"Eligible drivers: {eligible}"
        )
        seed = [SystemMessage(content=ESCALATION_GUIDANCE), HumanMessage(content=prompt)]
        msgs = seed
        # carry assessments forward for publishing / brief
        out["fleet_assessment"] = fleet_assessment
        out["customer_assessment"] = customer_assessment
        out["fleet_impact"] = _fleet_impact(state)
    resp = await _chat_model(tools=_dispatch_tools()).ainvoke(msgs)
    out["dispatch_messages"] = seed + [resp]
    calls = getattr(resp, "tool_calls", None) or []
    sub = next((c for c in calls if c["name"] == SUBMIT_DISPATCH), None)
    if sub:
        # The agent made its call — record the chosen driver + decision (it reasoned over the
        # Fleet/Customer assessments and, if asked, the human's answer to get here).
        args = sub.get("args") or {}
        decision = str(args.get("decision") or "dispatch").strip().lower()
        out["chosen_driver"] = str(args.get("driver_id") or "").strip()
        out["dispatch_decision"] = "HOLD" if decision == "hold" else "DISPATCH"
    elif not calls:
        # Plain-text conclusion (no tool) — fall back; driver left to the parent.
        text = _coerce_text(resp.content).strip()
        if not text:
            ans = (state.get("dispatch_human_answer") or "").strip().lower()
            text = "HOLD" if ans == "reject" else "DISPATCH"
        out["dispatch_decision"] = _text_decision(text)
    # else: ask_human is in the tool calls → dispatch_route sends to dispatch_human
    return out


async def dispatch_human(state: TeamState) -> dict:
    """Dispatch called ask_human — suspend on the durable interrupt, observe the answer."""
    msgs = await _human_node(state["dispatch_messages"], "Dispatch Agent", state)
    # Stash the raw answer as a plain string so the decision survives even if Gemini
    # returns an empty final turn (see dispatch_reason). msgs[-1] is a fresh ToolMessage.
    answer = _coerce_text(msgs[-1].content).strip() if msgs else ""
    return {
        "dispatch_messages": msgs,
        "asked_human": True,
        "dispatch_human_answer": answer,
    }


async def dispatch_route(state: TeamState) -> str:
    last = state["dispatch_messages"][-1]
    return "dispatch_human" if _has_ask_human(last) else "end"


# --------------------------------------------------------------------------- #
# Graph builder
# --------------------------------------------------------------------------- #
def build_dispatch_team_graph() -> StateGraph:
    """Fleet ∥ Customer → Dispatch, all reason→act→eval loops, with the HUMAN AS A TOOL.

    Each agent can call ask_human mid-loop; that suspends the graph on a durable
    interrupt() and the workflow resumes it via Command once a human signals — so the
    human is an in-the-loop tool, not a boundary gate. Fan-out from START, converge on
    Dispatch (runs once both Fleet and Customer finish), which makes the final call.
    """
    activity = {"execute_in": "activity", "start_to_close_timeout": timedelta(seconds=60)}
    workflow_node = {"execute_in": "workflow"}
    g = StateGraph(TeamState)

    g.add_node(
        "fleet_reason",
        fleet_reason,
        metadata={**activity, "summary": _node_summary("Fleet Agent", "reason")},
    )
    g.add_node("fleet_act", fleet_act, metadata=workflow_node)
    g.add_node("fleet_human", fleet_human, metadata=workflow_node)
    g.add_node(
        "customer_reason",
        customer_reason,
        metadata={**activity, "summary": _node_summary("Customer Agent", "reason")},
    )
    g.add_node("customer_act", customer_act, metadata=workflow_node)
    # defer=True makes Dispatch a true fan-in BARRIER: it runs once, only after BOTH the
    # Fleet and Customer branches reach it — never early on partial state and then again
    # (the default LangGraph behavior when one branch loops longer than the other).
    # This does NOT block on agent availability: a degraded agent's branch still completes
    # (its tool call fails, _run_tools catches it, the agent concludes with degraded output),
    # so Dispatch still runs with both assessments — one possibly degraded. The barrier only
    # holds if every branch eventually reaches it; if we ever *skip* a disconnected agent's
    # node entirely, switch to a timeout-based join instead.
    g.add_node(
        "dispatch_reason",
        dispatch_reason,
        defer=True,
        metadata={**activity, "summary": _node_summary("Dispatch Agent", "decide / ask human")},
    )
    g.add_node("dispatch_human", dispatch_human, metadata=workflow_node)

    # Fan out to both agent loops in parallel.
    g.add_edge(START, "fleet_reason")
    g.add_edge(START, "customer_reason")

    # Fleet loop: reason → {ask human | run tools | done → dispatch}
    g.add_conditional_edges(
        "fleet_reason",
        fleet_route,
        {
            "fleet_human": "fleet_human",
            "fleet_act": "fleet_act",
            "dispatch_reason": "dispatch_reason",
        },
    )
    g.add_edge("fleet_act", "fleet_reason")
    g.add_edge("fleet_human", "fleet_reason")

    # Customer loop (no human tool).
    g.add_conditional_edges(
        "customer_reason",
        customer_eval,
        {"customer_act": "customer_act", "dispatch_reason": "dispatch_reason"},
    )
    g.add_edge("customer_act", "customer_reason")

    # Dispatch loop: reason → {ask human → reason | done → END}
    g.add_conditional_edges(
        "dispatch_reason", dispatch_route, {"dispatch_human": "dispatch_human", "end": END}
    )
    g.add_edge("dispatch_human", "dispatch_reason")

    return g


def build_dispatch_only_graph() -> StateGraph:
    """Dispatch agent ALONE — the LangGraph half of the cross-framework tab.

    START → dispatch_reason → {ask human → reason | done → END}. The graph is SEEDED
    with the ADK-produced ``fleet_assessment`` / ``customer_assessment`` (dispatch_reason
    already reads them from state and only falls back to message threads when empty), so
    no Fleet/Customer nodes are needed. The Dispatch agent can call ask_human mid-loop —
    that suspends the graph on a durable interrupt() the child workflow drives via its own
    answer_dispatch signal + Command(resume). There is a single entry node and no parallel
    branches, so no ``defer`` fan-in barrier (unlike the full team graph).
    """
    activity = {"execute_in": "activity", "start_to_close_timeout": timedelta(seconds=60)}
    workflow_node = {"execute_in": "workflow"}
    g = StateGraph(TeamState)

    g.add_node(
        "dispatch_reason",
        dispatch_reason,
        metadata={**activity, "summary": _node_summary("Dispatch Agent", "decide / ask human")},
    )
    g.add_node("dispatch_human", dispatch_human, metadata=workflow_node)

    g.add_edge(START, "dispatch_reason")
    g.add_conditional_edges(
        "dispatch_reason", dispatch_route, {"dispatch_human": "dispatch_human", "end": END}
    )
    g.add_edge("dispatch_human", "dispatch_reason")

    return g
