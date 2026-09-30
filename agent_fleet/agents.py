"""
ADK agent definitions for the Meltdown ice cream delivery demo.

Order assignment pipeline:
- Fleet Agent: assesses driver positions, capacity, and ETAs for new orders
- Customer Agent: evaluates order priority, urgency, and venue context
- Dispatch Agent: synthesizes both and submits a structured driver assignment

Architecture:
- Agent execution happens inline in the workflow via TemporalModel + activity_tool.
  Each LLM call is routed through an invoke_model activity (via TemporalModel),
  and each tool call is its own Temporal activity (via activity_tool wrappers).
- The resolver agent calls tool_submit_assignment to write structured output
  into ADK session state. The workflow reads it back after the runner completes.
"""

from __future__ import annotations

import re
from datetime import timedelta

from google.adk.agents import Agent, ParallelAgent, SequentialAgent
from google.adk.models.llm_request import LlmRequest
from google.adk.tools import ToolContext
from google.adk.tools.google_search_tool import GoogleSearchTool
from google.genai import types
from temporalio.common import RetryPolicy
from temporalio.contrib.google_adk_agents import TemporalModel
from temporalio.workflow import ActivityConfig

from agent_fleet._activity_tool import activity_tool
from agent_fleet.activities import (
    tool_get_fleet_status,
    tool_get_order_priorities,
    tool_get_route_info,
)
from agent_fleet.config import DEFAULT_MODEL, LLM_MAX_RETRIES
from agent_fleet.queues import AGENTS_QUEUE

_TOOL_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=5,
)

# Fleet Agent tools fail fast when disconnected — 2 attempts so the Resolver
# gets an error quickly and can assign based on Customer Agent data alone.
# Temporal UI still shows the retry attempt.
_FLEET_TOOL_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=1.5,
    maximum_interval=timedelta(seconds=5),
    maximum_attempts=2,
)


def _one_attempt_config() -> types.GenerateContentConfig:
    """No client-side retries on model calls: a failure fails the invoke_model activity and
    Temporal retries it. The plugin's invoke_model builds ADK's Gemini from the model name, so
    Gemini(retry_options=...) can't be set; google-genai honors these per-request options.
    (ADK's internal google_search_agent gets no config and keeps Gemini's default,
    retry_options=None, which google-genai also treats as one attempt.)
    """
    return types.GenerateContentConfig(
        http_options=types.HttpOptions(
            retry_options=types.HttpRetryOptions(attempts=LLM_MAX_RETRIES + 1)
        )
    )


# --- Activity-backed tools (each tool call becomes a Temporal activity) ---

_fleet_status_tool = activity_tool(
    tool_get_fleet_status,
    task_queue=AGENTS_QUEUE,
    summary="Fleet Agent — get fleet status",
    start_to_close_timeout=timedelta(seconds=10),
    retry_policy=_FLEET_TOOL_RETRY,
)
_order_priorities_tool = activity_tool(
    tool_get_order_priorities,
    task_queue=AGENTS_QUEUE,
    summary="Customer Agent — get order priorities",
    start_to_close_timeout=timedelta(seconds=10),
    retry_policy=_TOOL_RETRY,
)
_route_info_tool = activity_tool(
    tool_get_route_info,
    task_queue=AGENTS_QUEUE,
    summary="Fleet Agent — assess ETA",
    start_to_close_timeout=timedelta(seconds=15),
    retry_policy=_FLEET_TOOL_RETRY,
)


# --- Per-call summary for invoke_model activities (Temporal UI labels) ---

# Agent display names for Temporal UI summaries
_AGENT_LABELS = {
    "assignment_fleet_agent": "Fleet Agent",
    "assignment_customer_agent": "Customer Agent",
    "assignment_dispatch_agent": "Dispatch Agent",
    "google_search_agent": "Customer Agent",
}

# Human-readable labels for tool responses (success vs error)
_TOOL_LABELS = {
    "tool_get_fleet_status": ("LLM reasoning on fleet data", "fleet status FAILED"),
    "tool_get_route_info": ("LLM reasoning on route ETA", "route ETA FAILED"),
    "tool_get_order_priorities": ("LLM reasoning on order priorities", "order priorities FAILED"),
    "tool_submit_assignment": ("assignment submitted", "assignment FAILED"),
    "google_search_agent": ("LLM reasoning on venue context", "venue search FAILED"),
}


def _extract_order_context(llm_request: LlmRequest) -> tuple[str | None, str | None]:
    """Extract order number and venue from the first user message."""
    if not llm_request.contents:
        return None, None
    first = llm_request.contents[0]
    if first.role == "user" and first.parts:
        text = first.parts[0].text or ""
        num_match = re.search(r"Order ID:\s*order-(\d+)", text)
        hotel_match = re.search(r"Venue:\s*(.+)", text)
        return (
            num_match.group(1) if num_match else None,
            hotel_match.group(1).strip() if hotel_match else None,
        )
    return None, None


def _is_error_response(function_response) -> bool:
    """Check if a tool response indicates an error."""
    if not function_response or not function_response.response:
        return False
    result = function_response.response.get("result", "")
    return isinstance(result, str) and result.startswith("ERROR:")


def _count_tool_responses(contents: list, tool_name: str) -> int:
    """Count how many times a specific tool response appears in the conversation."""
    count = 0
    for content in contents:
        if content.role == "user" and content.parts:
            for part in content.parts:
                if part.function_response and part.function_response.name == tool_name:
                    count += 1
    return count


def _build_summary(llm_request: LlmRequest) -> str:
    """Build a descriptive summary from the llm_request conversation state.

    Wired into `AdkActivityConfig(summary_fn=_build_summary)` so each
    invoke_model activity in the Temporal UI shows what the agent is doing.
    """
    # Agent name from ADK labels
    agent_name = "Agent"
    if llm_request.config and llm_request.config.labels:
        adk_name = llm_request.config.labels.get("adk_agent_name", "")
        agent_name = _AGENT_LABELS.get(adk_name, adk_name)

    # Order context
    order_num, hotel = _extract_order_context(llm_request)
    prefix = f"[#{order_num}] " if order_num else ""

    # Determine phase from conversation contents
    contents = llm_request.contents or []
    if len(contents) <= 1:
        hotel_suffix = f" for {hotel}" if hotel else ""
        return f"{prefix}{agent_name} — evaluating order{hotel_suffix}"

    # Check the last content entry for tool responses
    last = contents[-1]
    if last.role == "user" and last.parts:
        for part in last.parts:
            if part.function_response:
                tool_name = part.function_response.name or "tool"
                is_error = _is_error_response(part.function_response)
                labels = _TOOL_LABELS.get(tool_name)
                if labels:
                    label = labels[1] if is_error else labels[0]
                else:
                    label = f"{tool_name} {'FAILED' if is_error else 'received'}"
                # Add occurrence count to distinguish repeated tool calls
                count = _count_tool_responses(contents, tool_name)
                if count > 1:
                    label = f"{label} ({count})"
                return f"{prefix}{agent_name} — {label}"

    # Dispatch Agent with prior context — synthesizing both agents
    if agent_name == "Dispatch Agent":
        return f"{prefix}{agent_name} — weighing fleet + customer input"

    return f"{prefix}{agent_name} — reasoning"


# --- Order assignment agents ---


async def tool_submit_assignment(
    tool_context: ToolContext,
    driver_id: str,
    reasoning_summary: str,
) -> str:
    """Submit the final order assignment decision. You MUST call this tool with your recommendation.

    Args:
        driver_id: The Driver ID to assign the order to (e.g. "driver-a")
        reasoning_summary: Brief explanation of why this driver was chosen
    """
    tool_context.state["assignment"] = {
        "driver_id": driver_id,
        "reasoning_summary": reasoning_summary,
    }
    return "Assignment submitted successfully."


def create_assignment_fleet_agent() -> Agent:
    """
    Fleet Agent for order assignment — assesses driver positions, capacity,
    and ETAs to recommend the best driver for a new order.
    """
    return Agent(
        name="assignment_fleet_agent",
        model=TemporalModel(
            DEFAULT_MODEL,
            activity_config=ActivityConfig(task_queue=AGENTS_QUEUE),
            summary_fn=_build_summary,
        ),
        generate_content_config=_one_attempt_config(),
        description=(
            "Operational fleet specialist for order assignment. Assesses Driver "
            "positions, capacity, cooler status, and ETAs to recommend the best driver."
        ),
        instruction=(
            "You are the Fleet Operations AI for Meltdown Ice Cream Delivery. "
            "A new order has arrived — assess which Driver should handle it.\n\n"
            "Step 1: Call tool_get_fleet_status — this shows each driver's position, "
            "capacity, and status. Use the coordinates to identify the 1–3 closest "
            "drivers with available capacity (skip DISCONNECTED and full drivers).\n\n"
            "Step 2: Call tool_get_route_info for each of those top candidates to get "
            "actual driving ETAs from Google Maps. Do NOT call it for every driver — "
            "only the closest 1–3.\n\n"
            "Rules:\n"
            "- NEVER recommend a DISCONNECTED driver\n"
            "- Skip drivers at capacity (no free slots)\n"
            "- Prefer the closest driver with capacity\n\n"
            "Respond with ONLY: the recommended driver ID and ETA. "
            "Example: 'driver-b — 4min ETA, closest with capacity.' "
            "No preamble, no comparisons of other drivers."
        ),
        tools=[_fleet_status_tool, _route_info_tool],
        output_key="fleet_assessment",
    )


def create_assignment_customer_agent() -> Agent:
    """
    Customer Agent for order assignment — evaluates priority, urgency,
    venue context, and deadline pressure for a new order.
    """
    return Agent(
        name="assignment_customer_agent",
        model=TemporalModel(
            DEFAULT_MODEL,
            activity_config=ActivityConfig(task_queue=AGENTS_QUEUE),
            summary_fn=_build_summary,
        ),
        generate_content_config=_one_attempt_config(),
        description=(
            "Customer priority specialist for order assignment. Evaluates order "
            "priority, urgency, deadline pressure, and venue context."
        ),
        instruction=(
            "You are the Customer Relations AI for Meltdown Ice Cream Delivery. "
            "A new order has arrived — assess its priority and urgency.\n\n"
            "Call tool_get_order_priorities for order details. "
            "Use Google Search to find current events at the delivery venue.\n\n"
            "Assess: VIP or standard? Deadline tight? Venue events increasing urgency? "
            "How many servings?\n\n"
            "Respond with ONLY: priority level and key urgency factor. "
            "Example: 'VIP, tight deadline (25min), Moscone Center conference tonight.' "
            "No preamble, no full analysis."
        ),
        tools=[
            _order_priorities_tool,
            GoogleSearchTool(bypass_multi_tools_limit=True),
        ],
        output_key="customer_assessment",
    )


def create_assignment_dispatch_agent() -> Agent:
    """
    Dispatch Agent for order assignment — synthesizes fleet and customer assessments,
    picks the best driver, and submits the structured assignment.
    """
    return Agent(
        name="assignment_dispatch_agent",
        model=TemporalModel(
            DEFAULT_MODEL,
            activity_config=ActivityConfig(task_queue=AGENTS_QUEUE),
            summary_fn=_build_summary,
        ),
        generate_content_config=_one_attempt_config(),
        description=(
            "Dispatch Agent. Synthesizes fleet and customer assessments "
            "to pick the best driver for a new order."
        ),
        instruction=(
            "You are the Assignment Coordinator for Meltdown Ice Cream Delivery. "
            "Fleet Agent and Customer Agent have assessed a new order.\n\n"
            "Rules:\n"
            "- NEVER assign to a DISCONNECTED driver\n"
            "- If an agent is offline, compensate with available data\n\n"
            "You MUST call tool_submit_assignment with:\n"
            "- driver_id: the Driver that should get this order\n"
            "- reasoning_summary: one sentence explaining the decision\n\n"
            "Keep reasoning_summary under 20 words."
        ),
        tools=[tool_submit_assignment],
    )


def create_assessment_team_agent() -> ParallelAgent:
    """Fleet ∥ Customer assessment team WITHOUT the Dispatch phase.

    The ADK half of the cross-framework tab (3rd tab): this runs as its own Temporal
    child workflow (AdkAssessmentWorkflow) and produces just the two assessment
    strings (fleet_assessment / customer_assessment via output_key). The Dispatch
    decision is made by a separate LangGraph child — Temporal orchestrates across
    the two frameworks. Reuses the same Fleet/Customer agents as the full pipeline.
    """
    return ParallelAgent(
        name="assessment_parallel",
        sub_agents=[
            create_assignment_fleet_agent(),
            create_assignment_customer_agent(),
        ],
    )


def create_order_assignment_agent() -> SequentialAgent:
    """
    Compose the full order assignment pipeline (workflow context).
    Uses TemporalModel + activity_tool — each LLM and tool call is a Temporal activity.
    1. ParallelAgent: Fleet Agent + Customer Agent assess simultaneously
    2. Dispatch Agent: synthesizes and submits driver assignment
    """
    parallel_assessment = ParallelAgent(
        name="assignment_parallel",
        sub_agents=[
            create_assignment_fleet_agent(),
            create_assignment_customer_agent(),
        ],
    )

    assignment_dispatch_agent = create_assignment_dispatch_agent()

    return SequentialAgent(
        name="order_assignment",
        sub_agents=[parallel_assessment, assignment_dispatch_agent],
    )
