# Meltdown — Ice Cream Delivery Fleet Demo

> Instructions for AI coding agents working in this repo.

Public name: **Ziggy's Durable HITL Agents**, Ziggy's multi-agent HITL demo (the README
title and `pyproject.toml`). The dashboard and workflow ids still say **Meltdown**.

Conference demo for the AI Engineer World's Fair talk **"The Human Is an Async
API: Designing Durable Human-in-the-Loop Agents."** It shows **two** durable
human-in-the-loop patterns across **three use cases** (one per tab) on Temporal,
visualized as an ice cream delivery fleet in **downtown San Francisco**. Use cases 1
and 2 show one pattern each on a single framework; use case 3 combines both (it is
not a third pattern):

- **Pattern A — Human-in-the-loop ("The Human Calls the Agent")** — built on
  **Google ADK** (multi-agent assignment). A customer submits a change mid-delivery
  (address change → a new SF location from a dropdown, or cancel); the driver holds
  at the venue; a human supervisor approves or rejects. Durable primitive: signal →
  `wait_condition` hold → resolve. One gate feeds **both** loops: on an approved
  address change the ADK team RE-REASONS the order for the new location
  (`_rereason_order` → `_run_adk_assignment`: Fleet recomputes ETAs, Dispatch
  reassesses) and then the held driver reroutes. Cancel stays a fixed cancel.
- **Pattern B — Agent-in-the-loop ("The Agent Calls the Human")** — built on
  **LangGraph** via `temporalio.contrib.langgraph`. The framework is chosen by the
  UI **tab** (`set_dispatch_mode` → `"langgraph"`), applying to all orders. The
  multi-agent team (Fleet ∥ Customer → Dispatch) runs INLINE in the parent
  workflow as a **looping ReAct team** — Fleet and Customer are real
  reason→act→eval loops, each Gemini reason call AND each ordinary tool call its own
  Temporal activity in the parent's history. The HITL is **in the loop**:
  mid-reasoning, the Dispatch or Fleet agent calls an `ask_human` tool. LangGraph's
  `interrupt()` suspends the graph, and Temporal makes that suspended gap
  durable. The parent
  (`_run_langgraph_assignment`) surfaces the question, parks on the `answer_dispatch`
  Temporal SIGNAL (`wait_condition`), and resumes the agent via `Command(resume=answer)`
  — the answer flows back as the agent's next observation. **No per-order gate child.**
  The thesis for Pattern B: human judgment is exposed as an async tool the agent can
  call. The tool call raises the interrupt; the human's answer arrives as a Temporal
  signal. Pattern A is the contrast: external human input enters through signals rather
  than a model-visible tool. Both patterns share the same durable wait/signal primitives.
- **Cross-Framework — both patterns, both frameworks (use case 3, the 3rd tab)** — the same
  order assignment, but split across **two frameworks orchestrated as child
  workflows** under one Temporal parent: an **ADK** team does the assessment, then a
  **LangGraph** graph does the dispatch (with its in-loop `ask_human`). Selected by
  the UI **tab** (`set_dispatch_mode` → `"crossframework"`), applying to all orders.
  The point is collaborative, not competitive: Temporal is the durable-execution
  runtime (the substrate) that lets an ADK agent team and a LangGraph dispatch graph
  hand off to each other across process boundaries while staying replay-safe. **Division of labour:** the agent
  children **DECIDE**; the parent **APPLIES**. The ADK child assesses (Fleet ∥
  Customer), the LangGraph child dispatches (and may park on `ask_human`), and only
  the parent ever owns driver state or signals `DriverRouteWorkflow` — the agent
  children never signal drivers directly. Human→agent re-reason works here too: an
  approved address change re-runs both children in reasoning-only mode while the held
  driver reroutes via the existing update/resolve signals.

**Terminology (keep these terms straight in code + docs).** The spine is two parts:
**framework** and **durable-execution runtime / substrate**. *Frameworks own the loop*
(observe → reason → act) and the agent abstractions — **ADK and LangGraph** — and each
framework stops at its own edge. The framework decides what the agent does next; its loop
code runs inside a Temporal workflow, so each model call and tool call is a recorded step.
*Temporal is the **durable-execution runtime** (the substrate)* beneath the frameworks:
persistence, retries, replay, HITL waits (`wait_condition`/`signal`), versioning. It is the
layer everything runs on — woven throughout, not a component beside the others — and in
this demo it is what makes the **handoff between the two frameworks durable**. Canonical
sentence: *"Frameworks own the loop — ADK, LangGraph. Their loop code runs inside Temporal
workflows; Temporal is the durable-execution runtime beneath them, and in this demo it is
what makes the handoff between the two frameworks durable."* Don't claim Temporal is the
only way to connect frameworks (plain code or A2A can); claim what it adds — a handoff that
survives crashes and retries and shows up in history. Always Temporal **with** ADK and
LangGraph, never instead of them. Naming rules: do **not** call Temporal
a "control plane" / "agent control plane" (durable execution is the substrate that control
planes run *on* — the only allowed control-plane phrase is the architect line, *"the same
durable execution Temporal uses to build its own cloud control plane"*); always **qualify
"runtime"** as "durable-execution runtime" (or say "substrate") — never leave it bare or
ambiguous; do **not** add an "execution" bucket/section; prefer **"framework"** over
"harness" (do not build the docs on "harness" — it's a contested term; if it must appear,
use it only in the wide sense, "everything around the model, including the loop"); use
**"framework"** not "scaffolding". **LangGraph note:** in **this repo we use LangGraph as a
*framework*** — its graph/loop abstraction for the dispatch agent's reasoning loop — and let
**Temporal** provide durability and persistence, so the LangGraph checkpointer is just
`InMemorySaver`.

The disconnect/recovery scenarios (agent disconnect, driver disconnect, tool
degradation) are **not** part of the talk's two demos. The underlying signals,
retry logic, and `degraded` flag still exist in the code (dormant; the UI no
longer surfaces disconnect controls), so they're documented below as mechanism,
not as demo use cases.

## How to run

```bash
./run.sh          # starts Temporal dev server + worker process + server process
```

`run.sh` starts three processes: Temporal dev server, worker (`python -m agent_fleet.worker`),
and FastAPI server (`python -m agent_fleet.server`). No manual Temporal setup needed. App is
served at http://localhost:8080; Temporal UI at http://localhost:8233. After startup, the
worker exiting (`make kill-worker` / `make stop-worker`) does **not** end `run.sh`: it prints
a one-line notice and keeps Temporal and the server up so the parked workflows survive for
`make worker`. Ctrl-C, or Temporal or the server exiting, stops everything `run.sh` started
(and the in-memory dev server's history with it). Keep that contract if you edit `run.sh`;
the crash-and-recover demo depends on it.

The worker does **not** load `.env` itself. To run it directly in live mode, pass the env file:

```bash
uv run --env-file .env python -m agent_fleet.worker
```

The server loads `.env` via `load_dotenv()`. Two keys are required for live mode:
`GOOGLE_API_KEY` (Gemini) and `GOOGLE_MAPS_API_KEY` (Directions API).

## Architecture

- **Two separate processes**: FastAPI server (`server.py`) reads FleetState (SQLite) for the
  WebSocket snapshot and sends signals / runs queries only — no workers. Workers run in a
  separate process (`worker.py`). The worker is live-only and requires `GOOGLE_API_KEY` and
  `GOOGLE_MAPS_API_KEY` (no mock mode).
- **Workflows own state** (`workflows.py`): `MeltdownDemoWorkflow` owns driver positions, order
  assignments, and disconnect status. Builds `DriverSnapshot`s and passes to activities as inputs.
  The **Dispatch agent proposes the driver** (`submit_assignment` / `submit_dispatch`, from
  the eligible — connected, under-capacity — set). Capacity guardrail: the parent commits the
  agent's chosen driver if still eligible, else falls back to the least-loaded eligible one
  (`_least_loaded_driver()`, also the default proposal seeded into the agent). On the **adk**
  path only, `_run_adk_assignment` then swaps a valid pick for the least-loaded eligible
  driver when that driver has fewer orders ("Spread load across the fleet"); don't document
  the ADK pick as final. The fleet is a
  deliberately tight **4 drivers (A–D) at capacity 2** (`DRIVER_IDS`, `DRIVER_CAPACITY`), so
  capacity is scarce and the agent must reason about free slots; `WARMUP_HIDDEN` keeps driver-d
  back during the warm-up burst. Orders assigned while Fleet Agent is offline get `degraded=True`.
  `DriverRouteWorkflow` is a per-driver child workflow — batch-picks up to capacity (2) orders at Ziggy's,
  delivers sequentially (venue A → venue B → ...), then returns. Tracks status, is_disconnected,
  is_recovering, path_history, and current_orders. Disconnect uses Temporal-native retry: activities
  check FleetState for disconnect, fail if disconnected, Temporal retries with backoff until
  reconnected. Driver completes delivery, stays at venue, can't report back until reconnected.
  On reconnect, `sync_driver_position` activity reads actual position from FleetState — no
  teleporting. Completed deliveries are not repeated; batch continues from next pending order.
  As a long-lived workflow it bounds its own history with **continue-as-new**: at an idle,
  drained loop-top once history crosses `DRIVER_HISTORY_CONTINUE_AS_NEW` (10K) it hands off to a
  fresh run (same id) carrying only identity, position, and lifetime delivery count. The
  orchestrator `MeltdownDemoWorkflow` is wired the same way (`PARENT_HISTORY_CONTINUE_AS_NEW`;
  children started `ParentClosePolicy.ABANDON`, re-acquired by id on a continued run via the
  `_build_continue_as_new_input` / `_apply_continuation` / `_parent_should_continue_as_new`
  helpers) but is **dormant** — the demo run is bounded by order generation. Driver c-a-n is
  integration-tested; the parent's helpers are unit-tested (see `tests/test_workflows.py`).
  HITL hold pattern: this is **operator-in-the-loop**, not agent-in-the-loop —
  the change is initiated externally (operator submits a customer change via REST)
  and a human supervisor approves it. The ADK agents do not initiate or participate
  in the pending gate; it lives in the workflow, not in any agent tool. On an approved
  address change, they receive the revised order as fresh reasoning input (contrast:
  an `ask_user`-style `@function_tool` where the LLM itself pauses for clarification).
  When the change
  is submitted, parent signals child with `update_pending` — driver navigates to
  the venue but holds before delivering (`awaiting_update` status, `wait_condition`).
  On approval, parent signals `resolve_update` with the decision: cancel → skip
  delivery, address_change → reroute to new destination, release → deliver
  normally. Two `wait_condition` patterns: parent waits for human, child waits for
  parent. For pending/batched orders, changes apply directly without hold.
  Customer changes process serially in the parent (`_drain_pending_signals`) —
  it's simpler and matches the demo flow (changes submitted one at a time).
  The child's HITL state is a **per-order dict** (`_pending_holds: dict[str,
  PendingHold]`): `update_pending` creates an entry, `resolve_update` fills
  in the decision for that specific order, and the delivery loop waits on
  the hold for the order it's currently processing. No single-slot overwrite
  — two changes for different orders on the same driver each get their own
  slot. `deliver_order` now returns `success=False` when a cancel wins the
  race, so the workflow skips the `order_delivered` parent signal for
  cancelled orders. The child's HITL hold also escapes on `self._stop` so
  demo shutdown can't leave a parked child hanging the parent's
  `await handle` join.
  `OrderGenerationWorkflow` is a child workflow that generates orders on a randomized timer and
  signals the parent. Parent handles assignment. Auto-generated orders top out at ~$1,950
  (servings ≤150 × ≤$13) and the prompt steers the agent to escalate only exceptional orders
  (roughly $3,000+), so routine orders usually auto-dispatch — the injected $5,400 premium
  order is the one written to make the agent call `ask_human`. It is the model's call, not a
  code rule; Fleet may ask before Dispatch, so one order can produce two cards.
- **Pattern B — in-loop `ask_human`** (`langgraph_agents.py`): the agent-in-the-loop path, selected
  by the langgraph tab for **all** orders. `_assign_order` runs `_run_langgraph_assignment` INLINE in
  the parent as a concurrent task — the fleet keeps moving while the agents (and possibly a human)
  decide. That assessment is a **looping multi-agent** LangGraph team (`build_dispatch_team_graph`,
  registered as `GRAPH_NAME = "dispatch_team"`) compiled via `temporalio.contrib.langgraph`, the
  mirror of the ADK team: Fleet and Customer are real reason→act→eval **ReAct loops** that fan out
  from `START`, then converge on a Dispatch loop. (The crossframework tab uses a
  dispatch-only sibling graph — `DISPATCH_ONLY_GRAPH_NAME = "dispatch_only"`,
  `build_dispatch_only_graph()`: `START → dispatch_reason → {dispatch_human →
  dispatch_reason | END}`, reusing the existing `dispatch_reason` / `dispatch_human` /
  `dispatch_route` / `ask_human` nodes, seeded with `fleet_assessment` /
  `customer_assessment` in state, with no Fleet/Customer nodes and no defer barrier;
  the full `build_dispatch_team_graph()` / `GRAPH_NAME = "dispatch_team"` is unchanged.)
  Each `*_reason` node is a real Gemini call (through
  `init_chat_model`) executed as a Temporal **activity** recorded in the **parent's** history, and
  **each tool call** (`get_fleet_status`, `get_route_info`, `get_order_priorities`,
  `search_venue_events`) runs as its own
  Temporal activity inline in the workflow (the `*_act` nodes, `execute_in=workflow`), mirroring ADK's
  `activity_tool` granularity. **Tool parity:** Fleet uses `get_fleet_status` + `get_route_info`
  and Customer uses `get_order_priorities` + `search_venue_events` in BOTH frameworks across all
  three tabs — `search_venue_events` (Gemini `GoogleSearch` grounding behind a Temporal activity)
  is the LangGraph analog of ADK's built-in `google_search`, so the agents reason over the same
  inputs regardless of framework.
  **The Dispatch agent decides the driver:** it binds a `submit_dispatch(driver_id, decision)`
  tool (ADK: `tool_submit_assignment`) and picks a driver from the eligible set seeded into
  state (`eligible_drivers`); `_run_langgraph_assignment` / `LgDispatchWorkflow` commit the
  agent's `chosen_driver` (least-loaded fallback if it isn't eligible).
  **HITL is in the reasoning loop, not a boundary gate:** Fleet and Dispatch bind an `ask_human`
  tool and can call it mid-loop when they need outside sign-off (whether to ask is the agent's
  judgment, guided by `ESCALATION_GUIDANCE` / per-agent system prompts — there's no code threshold).
  The approve/reject answer is **not a rubber stamp**: it flows back as the agent's next
  observation, and the agent reasons over it (plus the Fleet/Customer assessments) before calling
  `submit_dispatch` to pick the driver / hold.
  Its execution is NOT an activity; the `*_human` node (`execute_in=workflow`) runs a
  LangGraph `interrupt()` that suspends the graph, while Temporal makes the wait durable.
  The parent (`_run_langgraph_assignment`) loops on
  `result.get("__interrupt__")`: it surfaces the question into `_pending_dispatch`, `wait_condition`s
  on the `answer_dispatch` signal (via `_await_dispatch_answer`), then resumes the agent with
  `Command(resume=answer)` — the answer flows back as the agent's next observation. No per-order
  child workflow. On a `DISPATCH` decision the parent calls `_commit_assignment`; on `HOLD`/reject it
  calls `_reject_order` (cancels the order, preserves fleet capacity). `_reject_order(...,
  agent_hold=...)` publishes "Supervisor rejected …" only for a human reject; when the agent
  held on its own it publishes "Dispatch agent held order … after a human approved it" (or
  "… without asking a human"). These go out as `dispatch_gate` events, which the frontend
  doesn't render (it shows only `fleet_agent` / `customer_agent` / `resolver`), so on screen
  either kind of HOLD is just "… — held" in the Dispatch panel; don't document the
  attribution as visible. `_await_dispatch_answer` waits `GATE_ESCALATION_SECONDS`
  (imported from `config.py`, default 30, env-overridable), then marks the pending entry
  `escalated` / `approver_tier="backup"` and waits with no timeout. It also
  unblocks on `_routes_done` (returns `None`) so demo shutdown can't hang a parked workflow.
  The plain-text fallback in `dispatch_reason` (used only when the model skips
  `submit_dispatch`) goes through `_text_decision`: HOLD only when the reply *leads* with
  "hold"/"held" (optionally after "Decision:"), so "No need to hold — dispatch driver-a" is
  DISPATCH.
  LangGraph callables that run inline in the workflow are `async` because LangGraph offloads sync
  callables to a thread executor, which Temporal's deterministic event loop forbids.
- **Cross-Framework — ADK child → LangGraph child** (`workflows.py`,
  `agents.py`, `langgraph_agents.py`): the crossframework path, selected by the
  crossframework tab for **all** orders. `_assign_order` branches on
  `dispatch_mode == "crossframework"` to `asyncio.create_task(_run_crossframework_assignment(...))`,
  reusing the same `_langgraph_tasks` list, `_on_langgraph_task_done` callback, and
  shutdown drain as Pattern B — the fleet keeps moving while the children decide.
  `_run_crossframework_assignment` calls `_dispatch_via_children(order, driver_id, onum,
  suffix, apply=True)`, which orchestrates two child workflows on the parent's task
  queue (`WORKFLOWS_QUEUE`):
  (1) starts **`AdkAssessmentWorkflow`** (id `assess-<order_id>`) and awaits its
  `fleet_assessment` / `customer_assessment`;
  (2) starts **`LgDispatchWorkflow`** (id `dispatch-<order_id>`), registers it in
  `self._dispatch_children` plus a roll-up entry in `self._pending_dispatch`
  (`{child_id, order_id, venue, order_value, via_child: True}`), and awaits its
  decision (a `finally` pops both registrations). It publishes reasoning via
  `_publish_langgraph_reasoning`; on `HOLD` it calls `_reject_order`, otherwise
  `_commit_assignment`. **Ownership stays with the parent**: the children DECIDE; the
  parent APPLIES (owns driver state, signals `DriverRouteWorkflow` via
  `_commit_assignment` / `_reject_order`); the children never signal drivers.
  Human→agent re-reason: `_rereason_crossframework(order_id, note)` re-runs
  `_dispatch_via_children` with a `-rev<n>` id suffix (`self._rereason_count`) and
  `apply=False` (reasoning-only — the held driver reroutes via the existing
  `update_order` / `resolve_update` signals). The address-change branch in
  `_process_customer_change` dispatches to `_rereason_crossframework` when
  `dispatch_mode == "crossframework"` (else `_rereason_order` for the adk tab).
  On shutdown the parent signals `LgDispatchWorkflow.stop` to each pending child so a
  parked child returns cleanly. New `__init__` state: `self._dispatch_children`
  (`dict[str, ChildWorkflowHandle]`) and `self._rereason_count` (`dict[str, int]`).
  The two children:
  **`AdkAssessmentWorkflow.run(inp: ReasonAboutAssignmentInput) -> AdkAssessmentOutput`**
  runs the ADK `ParallelAgent` from `create_assessment_team_agent()` (Fleet ∥
  Customer, no dispatch phase) via the ADK `Runner` and reads `fleet_assessment` /
  `customer_assessment` from session state (it reuses `ReasonAboutAssignmentInput`).
  **`LgDispatchWorkflow.run(inp: LgDispatchInput) -> LgDispatchOutput`** compiles the
  dispatch-only graph (`DISPATCH_ONLY_GRAPH_NAME`) with `InMemorySaver` and
  `thread_id` = its own workflow id, `ainvoke`s it, and `while result.get("__interrupt__")`
  parks (sets `self._pending_question`), waits on a Temporal signal, and resumes with
  `Command(resume=answer)`. It owns `@workflow.signal answer_dispatch(decision: str)`
  (single arg — the child IS the order), `@workflow.query pending_question() -> dict | None`,
  and `@workflow.signal stop()`. It uses the same `GATE_ESCALATION_SECONDS` window, then
  sets `approver_tier="backup"` / `escalated` on the pending question and keeps waiting.
  Robust decision: `HOLD` if the human rejected, or the agent's `submit_dispatch` decision
  is `hold`, or a plain-text reply leads with HOLD/held (`_text_decision` in
  `langgraph_agents.py`); else `DISPATCH`. `LgDispatchOutput` has no human-rejected flag, so
  on this tab the parent publishes "Supervisor rejected …" for every HOLD, including an
  agent hold after an approve. The `-rev<n>` re-reason children get the order's real `event`
  and `order_value` (stored in `self._orders`), so a high-value `-rev1` Dispatch agent may
  call `ask_human` again, and the held driver waits for that answer.
- **Server reads FleetState** (`server.py`): WebSocket data comes from `fleet.snapshot()` (SQLite).
  Server also writes disconnect/reconnect state directly. Temporal queries used for structural
  state during development — FleetState is the display authority.
- **Activities** (`activities.py`): receive decision data as inputs. Some read FleetState:
  `navigate_to` reads the driver's start position, the agent tools read fleet status and order
  priorities, and the dormant disconnect checks read disconnect flags. Workflows never read
  FleetState. `@activity.defn` with no `name=` override (function names are activity names).
  The two Maps Directions activities (`get_route_polyline`, `tool_get_route_info`) keep the
  API key out of errors: non-2xx → `RuntimeError("Maps Directions API HTTP <code>")`,
  transport errors → `RuntimeError("Maps Directions API request failed: <ErrorType>") from
  None`. Never use `raise_for_status()` or log the request URL there; the key is in the query
  string, and `worker.py` sets the `httpx` logger to WARNING for the same reason.
- **FleetState** (`simulation.py`): SQLite WAL-backed UI projection. Backed by `fleet_state.db`
  for cross-process sharing — activities in the worker write positions/statuses, server reads
  for the frontend WebSocket. In production this would be Redis or Postgres.
- **3-queue workers** (`worker.py`): workflows + local activities, delivery, agents.
  `GoogleAdkPlugin` is on both workflow and agents workers (sandbox + determinism on
  workflow side, `invoke_model` activity on agents side) — and already covers the ADK
  child (`AdkAssessmentWorkflow`). The workflow worker (`WORKFLOWS_QUEUE` /
  `create_workflow_worker`) also registers the two cross-framework child workflows,
  `AdkAssessmentWorkflow` and `LgDispatchWorkflow`. `LangGraphPlugin(graphs={...})` is
  on the **workflow** worker; it stays **one** plugin but now registers **two** graphs:
  `GRAPH_NAME = "dispatch_team"` (the looping multi-agent team — Fleet ∥ Customer
  reason→act→eval loops → Dispatch, run inline in the parent for every langgraph-tab
  order, with the in-loop `ask_human` tool) and
  `DISPATCH_ONLY_GRAPH_NAME: build_dispatch_only_graph()` (the dispatch-only graph the
  `LgDispatchWorkflow` child compiles for the crossframework tab).
  Its node activities (the fleet/customer/dispatch agent Gemini reason calls and each tool call)
  execute on that worker. Agents use the upstream
  `TemporalModel` with `summary_fn=_build_summary` — `_build_summary`
  in `agents.py` generates context-aware summaries (agent name, order, phase) shown
  in the Temporal UI per invoke_model activity. `_activity_tool.py` builds its own
  dynamic summaries for tool-call activities from the bound arguments.
  `publish_agent_event` and `publish_agent_events_batch` are registered on the
  workflow worker for local activity execution (UI projection with minimal history).
- **ADK agents** (`agents.py`): all three kept — Fleet Agent + Customer Agent (parallel) →
  Dispatch Agent (sequential) — this is the multi-agent reasoning used for order assignment when
  the **adk tab** is selected (Pattern A's agent framework). The langgraph tab routes every order to the
  Pattern B LangGraph team instead; the framework is chosen by the tab, not per-order. The ADK path
  runs inline in the workflow via `_run_adk_assignment()`, committing the driver the **Dispatch
  agent picked** (`tool_submit_assignment`; least-loaded fallback) with no dispatch gate involved.
  The same team is re-run on an approved customer **address change**:
  `_process_customer_change` calls `_rereason_order` → `_run_adk_assignment` again so the agents
  re-reason the order for the new location, then the held driver reroutes. The
  crossframework tab reuses a slimmed variant: `create_assessment_team_agent()` builds a
  `ParallelAgent` of just Fleet ∥ Customer (reusing
  `create_assignment_fleet_agent` / `create_assignment_customer_agent`, dropping the
  Dispatch/`SequentialAgent` phase), which the `AdkAssessmentWorkflow` child runs while
  the LangGraph child handles dispatch. The full `create_order_assignment_agent()` is
  unchanged (adk tab). If an activity
  fails, Temporal retries. (Dormant disconnect path: Fleet
  Agent tools fail fast when disconnected (2 attempts), error returned to LLM via
  `_activity_tool.py` catch — Dispatch Agent assigns with available data but orders are flagged
  `degraded`.) Workflow publishes short summary events to FleetState via batched local activity
  after ADK completes (summary from `output_key` fields). Note: the Pattern B agent team is a
  **separate multi-agent LangGraph team** (`langgraph_agents.py`), not part of the ADK pipeline.
- **Server** (`server.py`): signal-only / query-only REST API plus the WebSocket state feed.
  Pattern A endpoints: `POST /api/customer-change` (signals parent `customer_change` + signals
  the child `update_pending` to hold) and `POST /api/approve-change` (signals `change_approved`;
  an approved address change triggers the in-loop re-reason before the held driver reroutes).
  Pattern B endpoints: `POST /api/inject-order` (registers a premium Moscone order in FleetState
  and signals `new_order` — the deliberate trigger for the agent's `ask_human`), `GET /api/pending-dispatch`
  (queries the parent's `get_status` and reads its `pending_dispatch` dict — populated when an agent
  calls `ask_human` mid-loop and the parent surfaces the question), `POST /api/approve-dispatch`
  (signals the parent `MeltdownDemoWorkflow.answer_dispatch` — the durable async endpoint the
  agent's in-loop `ask_human` interrupt is parked on; no gate child involved).
  Cross-Framework wiring: `DispatchMode = Literal["adk", "langgraph", "crossframework"]`
  types `StartRequest.mode` / `DispatchModeRequest.mode`, so an unknown mode is a validation
  error (`tests/test_server.py`). `GET /api/pending-dispatch` now handles the
  roll-up entries the crossframework path puts in `pending_dispatch`: when an entry has
  `via_child` / `child_id`, the server does a **second** query — to that child's
  `LgDispatchWorkflow.pending_question` — and merges the agent/question in (workflows
  can't query each other, so the server bridges). `POST /api/approve-dispatch`
  (`DispatchDecisionRequest`) gains an optional `child_id`: if present it signals the
  child's `answer_dispatch(decision)` directly; otherwise it signals the parent's
  `answer_dispatch(order_id, decision)` (langgraph tab unchanged). Dormant disconnect
  endpoints (`/api/disconnect-crew`, `/api/disconnect-agent`, and reconnect variants) still write
  FleetState and signal workflows but are not wired to UI controls.
- **Frontend** (`frontend/index.html`): single-file SPA with Leaflet map, WebSocket state feed,
  agent reasoning panels. A third **"🔀 Cross-Framework"** tab (`data-tab="cross"`) maps
  to `dispatch_mode "crossframework"`; `tabMode()` is now a 3-way map (adk / langgraph /
  crossframework). The customer-change controls are de-duplicated across the Human +
  Cross tabs via `data-role` attributes plus an active-panel `ccEl()` scope helper. On
  the cross tab the header shows **both** the ADK and LangGraph logos, and a single
  "View the cross-framework graph" combined SVG modal (`#cross-modal` / `openCross` /
  `closeCross`) renders the ADK→LangGraph handoff (children run in sequence, not in
  parallel). The default map style is `DEFAULT_TILE = 'Dark (OpenStreetMap)'` (OSM tiles
  darkened by a CSS filter on the tile pane, with the OSM attribution control); the CARTO
  styles were removed because they now need a key. The `ask_human` card shows the full
  `order_id` and the workflow to signal (`meltdown-demo`, or the `dispatch-…` child id) and
  re-renders only when its HTML changes, so the ids can be copied. It is driven by a Query,
  so it disappears while no worker is running.
- **PydanticPayloadConverter** on `Client.connect` in both server and worker for `LlmResponse`
  serialization.

## Key conventions

- Dataclass models for all Temporal payloads (`models.py`). `dispatch_mode` is a
  3-value field (`"adk"` / `"langgraph"` / `"crossframework"`, set by the active UI tab;
  documented on `MeltdownDemoInput.dispatch_mode`). The cross-framework child contracts
  are dataclasses; add any new field **with a default** for replay-safety:
  `AdkAssessmentOutput(fleet_assessment="", customer_assessment="")`;
  `LgDispatchInput(order_id, venue, order_value, servings, deadline_minutes,
  proposed_driver_id, drivers_available, drivers_total, pending_orders,
  fleet_assessment="", customer_assessment="", eligible_drivers=[])`;
  `LgDispatchOutput(decision="DISPATCH", driver_id="", reasoning="", asked_human=False)`
  (assessments are deliberately not echoed back, to keep child results thin).
- Activities and workflows in separate files
- Worker is live-only and requires both `GOOGLE_API_KEY` and
  `GOOGLE_MAPS_API_KEY` (no mock mode); `run_worker()` raises at startup if either is
  missing or still a `your-...` placeholder
- Two API keys required: `GOOGLE_API_KEY` (Gemini, Generative Language API, paid tier) and
  `GOOGLE_MAPS_API_KEY` (Directions API) — cannot be combined; use a separate key for each,
  restricted to its API
- `DEFAULT_MODEL` defaults to `gemini-3.8-flash` (`config.py`; swappable via env). Google now
  serves `gemini-2.5-flash` only to keys that used it before, and 3.8 Flash has no Grounding
  with Google Search on the free tier, so the Gemini key needs billing. Every agent reads
  `config.DEFAULT_MODEL`; don't hard-code a model name elsewhere. All three tabs were
  measured end to end on `gemini-3.8-flash` on 2026-09-30 (README, Cost to run)
- `GATE_ESCALATION_SECONDS` (default 30) lives in `config.py` and is imported into
  `workflows.py` inside `imports_passed_through()`; it's read once per worker process
- Temporal owns retries: `LLM_MAX_RETRIES = 0` (`config.py`) sets every LLM client to one
  attempt, so a failed model call fails its activity and Temporal's retry policy retries it.
  LangGraph: `init_chat_model(..., max_retries=0)` in `_chat_model` (the langchain-google-genai
  default is 6; the pinned google-genai 1.75.0 reads 0 as one attempt). ADK: the plugin's
  `invoke_model` builds `Gemini` by name (no `retry_options` knob), so each agent passes
  `generate_content_config=_one_attempt_config()` (`HttpRetryOptions(attempts=1)`, applied per
  request); ADK's internal `google_search_agent` keeps the default `retry_options=None` (one
  attempt). `tool_search_venue_events` builds its `genai.Client` with `attempts=1`. Don't add
  client-side retries; `tests/test_workflows.py` and `tests/test_activities.py` pin these.
  Known gap with no public knob: aiohttp is locked (via `google-cloud-aiplatform`), and
  google-genai's async aiohttp path resends once after a connection failure (not an HTTP
  error), so ADK and LangGraph calls can send twice inside one activity attempt
- `DRIVER_CAPACITY = 2` is defined in both `workflows.py` (enforced) and `simulation.py` (what
  the Fleet tool reports); keep them equal
- Cost is always stated as tokens AND dollars (model, settings, calls, tokens, $, measured vs
  estimated, date). `scripts/token_usage.py` measures a pass from local Temporal history
- Geography is **downtown San Francisco** (`locations.py`). Random order generation from 3
  venues: **Moscone Center** (platinum tier — the premium target that makes the agent call `ask_human`),
  **Fisherman's Wharf** (silver), **Chinatown** (gold). The reroute-only destination is
  **Oracle Park** (`COSMOPOLITAN` — historical var name). The per-venue `hotel` key is a
  legacy field name; values are SF venue names.
- Drivers use letter IDs: `driver-a` through `driver-d` (4 drivers), displayed as `Driver-A` etc.
- Ice cream shop is "Ziggy's Ice Cream" = the **Ferry Building** (`WAREHOUSE_LABEL` in `locations.py`)
- Auto-generated orders top out at ~$1,950 and the prompt steers the agent to escalate only
  exceptional orders, so routine orders usually auto-dispatch; the injected $5,400 premium order
  (`order-special-<n>`, a server-process counter that Reset doesn't clear) is the one written to
  make the agent call `ask_human`
- Max 50 orders per demo run; 4 drivers at `DRIVER_CAPACITY = 2`, so each batches up to 2 orders

## Commands

Dependencies are managed with [uv](https://docs.astral.sh/uv/) — `uv sync --all-extras`
creates `.venv/` and installs runtime + dev deps. `uv run <cmd>` runs in that env.

```bash
uv sync --all-extras   # install / refresh deps (creates .venv/)
uv run ruff check .    # lint
uv run ruff format .   # format
uv run pytest          # run tests
make setup             # uv sync --all-extras --frozen (the locked install run.sh does)
make lint              # ruff check + format check (via uv)
make fmt               # ruff fix + format (via uv)
make test              # pytest (via uv)
make run               # start the demo (./run.sh always starts clean)
make reset             # curl -fsS -X POST http://localhost:8080/api/reset (= Reset button; app must be running)
make kill-worker       # SIGKILL the worker (a real crash); badge goes offline within ~9 s
make failure           # same as make kill-worker
make stop-worker       # SIGTERM the worker (graceful); badge goes offline within ~3 s
make worker            # start a worker with --env-file .env; it replays open workflows
make recovery          # same as make worker
uv run python scripts/token_usage.py   # tokens + $ per pass from local Temporal history
```

Tests: `uv run pytest -k "not driver_route"` runs offline; the four `driver_route` tests
download Temporal's time-skipping test server on first run. Cross-framework verification:
gitignored manual spikes under `spikes/langgraph_hitl/` —
`crossharness_smoke.py` (ADK child → LangGraph child, low-value → `DISPATCH`, no gate)
and `crossharness_hitl_smoke.py` (high-value → child parks on `ask_human` →
reject = `HOLD` / approve = `DISPATCH`). `uv run pytest` is green. No automated test kills
a worker mid-`ask_human`.
