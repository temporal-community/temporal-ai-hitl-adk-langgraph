# How It Works — Under the Hood

Presenter-facing deep dive for **"The Human Is an Async API: Designing Durable
Human-in-the-Loop Agents."** This is the architectural grounding to answer
"how does that actually work?" questions confidently. Not for reading aloud —
see [DEMO_GUIDE.md](DEMO_GUIDE.md) for the talk track.

The demo is Ziggy's Ice Cream catering fleet running on **downtown San
Francisco** (the Ferry Building is the shop; orders come from Moscone Center,
Fisherman's Wharf, and Chinatown). It shows **two durable human-in-the-loop
patterns** across **three use cases** (one per tab): **Pattern A** (the human calls
the agent) on a **Google ADK–only** tab, **Pattern B** (the agent calls the human)
on a **LangGraph–only** tab — two patterns on different frameworks, sharing the same
Temporal wait/signal primitives — and a **Cross-Framework** tab that
runs **both patterns across both frameworks** in one system (Fleet + Customer on ADK,
Dispatch on LangGraph). The third tab isn't a third pattern; it combines both. In this
demo, **Temporal**, the durable-execution runtime (the substrate), is what makes the
handoff between the two frameworks durable.

---

## The spine: frameworks own the loop, Temporal is the runtime beneath

Two parts, kept distinct throughout these docs:

- **Frameworks own the loop.** The agent loop — observe → reason → act — and the
  agent abstractions belong to the **agent framework** (ADK, LangGraph). The
  framework decides what the agent does next. Its loop code runs inside a Temporal
  workflow, so each model call and tool call is a recorded step.
- **Temporal is the durable-execution runtime (the substrate)** beneath the
  frameworks: persistence, retries, replay, HITL waits (`wait_condition` / signal),
  versioning. It's the layer everything runs on — woven throughout, not one
  component beside the others. On the Cross-Framework tab each framework runs in its
  own child workflow and the parent applies the result. Plain code or A2A could
  connect the two frameworks; what Temporal adds is a handoff that survives crashes,
  retries, and shows up in history.

Canonical framing: *Frameworks own the loop — ADK, LangGraph. Their loop code runs
inside Temporal workflows; Temporal is the durable-execution runtime beneath them, and
in this demo it is what makes the handoff between the two frameworks durable.*

For an infra or architect audience, the credible framing is: *the same durable
execution Temporal uses to build its own cloud control plane.* (Note: we do **not**
call Temporal "the agent control plane" — durable execution is the substrate that
control planes run *on*.)

## Two patterns, three use cases

| | Pattern A — Human-in-the-loop | Pattern B — Agent-in-the-loop |
|---|---|---|
| "The Human…" | …calls the agent | …gets called by the agent |
| Framework | **Google ADK** (`temporalio[google-adk]`) | **LangGraph** (`temporalio.contrib.langgraph`) |
| Who initiates | An **operator**, externally, mid-delivery | The **agent**, when it hits a decision it shouldn't make alone |
| Triggers on | Every order, while the **ADK tab** is active | Every order, while the **LangGraph tab** is active; the prompt steers the agent to escalate only exceptional ones |
| Agents involved | Fleet + Customer (parallel) → Dispatch (sequential) | Fleet ∥ Customer → Dispatch — a separate LangGraph team, each agent a real reason→act→eval ReAct loop |
| Where the human enters | the **workflow** (a boundary hold), not any agent tool | **inside the reasoning loop** — the agent calls an `ask_human` tool |
| Durable primitive | signal → `wait_condition` hold → resolve | `interrupt()` in the loop → `wait_condition` on the `answer_dispatch` signal → `Command(resume=answer)` |

**The key routing fact:** the **active UI tab** picks the dispatch framework for
*all* orders — `set_dispatch_mode("adk" | "langgraph" | "crossframework")` sets
`_dispatch_mode`, and `_assign_order` routes on it.

- **ADK tab → ADK.** Every order runs `_run_adk_assignment()` inline in the parent
  workflow; the ADK multi-agent pipeline reasons about it and the Dispatch agent
  proposes a driver (`submit_assignment`, from the eligible set). The parent then
  rebalances to the least-loaded eligible driver when that driver has fewer orders.
  No gate.
- **LangGraph tab → inline LangGraph team.** Every order runs
  `_run_langgraph_assignment(order, driver_id, onum)` — the looping multi-agent
  LangGraph team runs *inline in the parent workflow*. There is **no per-order gate
  child**: when an agent decides it needs a human, it calls the `ask_human` tool
  mid-loop. LangGraph's `interrupt()` suspends the graph; the parent surfaces the
  question and waits durably in Temporal for the `answer_dispatch` signal.
- **Cross-Framework tab → child workflows.** Every order runs
  `_run_crossframework_assignment(...)`, which runs **two Temporal child workflows** one
  after the other — `AdkAssessmentWorkflow` (`assess-<order_id>`, the ADK Fleet ∥ Customer
  team), then `LgDispatchWorkflow` (`dispatch-<order_id>`, the LangGraph Dispatch agent,
  seeded with the ADK assessments). The agents run *inside the children*; the parent
  starts each child, awaits it, and applies the result.

This split is deliberate: each framework dispatches all orders while its tab is
active, on the same durable-execution runtime (Temporal), with the same durable-signal
HITL primitive underneath both.

**Pattern B exposes human judgment as a tool, not an out-of-band gate.** In Pattern B
(including the cross-framework use case), `ask_human` is bound into the agent's toolset
alongside `get_fleet_status`, `get_route_info`, and `submit_dispatch` (see
`_dispatch_tools()` / `_fleet_tools()` in `langgraph_agents.py`). The agent chooses when to
call it. Its execution is intentionally different from an ordinary tool: data tools run as
activities and return promptly, while `ask_human` routes to a LangGraph `interrupt()`;
Temporal preserves the resulting wait until the answer arrives through `answer_dispatch`.
Pattern A is external workflow input, not a model-visible tool call, but it uses the same
Temporal wait/signal primitives.

### The third tab — cross-framework (ADK + LangGraph in one system)

The first two tabs each run *one* framework inline in the parent. The **Cross-Framework
tab** (`_dispatch_mode == "crossframework"`) shows the case the other two can't: a single
order handled by **two different frameworks** — Fleet + Customer on ADK, then Dispatch
on LangGraph. ADK orchestrates ADK agents and LangGraph orchestrates LangGraph nodes; in
this demo, **Temporal** makes the handoff between them durable: the parent
`MeltdownDemoWorkflow` runs one child workflow per framework, in sequence, and applies
the result.

- **`AdkAssessmentWorkflow`** (`assess-<order_id>`) runs the ADK `ParallelAgent`
  (`create_assessment_team_agent()`) and returns the two assessment strings.
- **`LgDispatchWorkflow`** (`dispatch-<order_id>`) runs the dispatch-only LangGraph graph
  (`build_dispatch_only_graph()`), seeded with those assessments, and decides — calling
  `ask_human` mid-loop when warranted.

The core of `_dispatch_via_children` (`workflows.py`), shared by the first assignment
(`suffix=""`) and by a re-reason after an approved address change (`suffix="-rev<n>"`,
`apply=False`):

```python
# 1. ADK child: Fleet ∥ Customer assessment only
adk_handle = await workflow.start_child_workflow(
    AdkAssessmentWorkflow.run,
    ReasonAboutAssignmentInput(order_id=order.order_id, event=order.event, ...),
    id=f"assess-{order.order_id}{suffix}",
)
assessment = await adk_handle

# 2. LangGraph child: dispatch decision, with its own ask_human wait
lg_handle = await workflow.start_child_workflow(
    LgDispatchWorkflow.run,
    LgDispatchInput(
        order_id=order.order_id, order_value=order.order_value, ...,
        fleet_assessment=assessment.fleet_assessment,
        customer_assessment=assessment.customer_assessment,
        eligible_drivers=self._eligible_drivers(),
    ),
    id=f"dispatch-{order.order_id}{suffix}",
)
result = await lg_handle   # LgDispatchOutput(decision, driver_id, reasoning, asked_human)

# 3. The parent applies: _commit_assignment, or _reject_order on HOLD
```

Ownership is layered: the **agent children decide**, the **parent applies** (it owns
driver state and signals `DriverRouteWorkflow` via `_commit_assignment` / `_reject_order`),
and the **driver workflows execute**. The children never signal drivers directly. Because
each child is its own execution, the ADK `invoke_model`/tool activities live in the
`assess-*` history and the LangGraph `dispatch_reason` activity lives in the `dispatch-*`
history — the cross-framework boundary is literally visible in the Temporal UI.

**Why per-order (request/response), not one long-lived agent workflow:** each assignment is
a *bounded* computation — the agent's reason→act→eval loop terminates when it concludes, the
workflow completes, and its history is sealed. The agents here are **stateless between
orders** (fresh ADK session + LangGraph thread each time), so there's no state to keep alive
between decisions; a customer change is simply a new decision over the order's new state
(`_rereason_crossframework`, with `-rev<n>` child ids). The durable, *stateful* per-entity
workflows are the **drivers** (`DriverRouteWorkflow`), which genuinely carry evolving state
(position, route, cargo) across orders. Rule of thumb: **long-lived workflow for a stateful
entity; per-job workflow for a stateless request/response reasoner.** (In cross-framework mode
the demo generates fewer, slower orders — `CROSSFRAMEWORK_MAX_ORDERS` / interval — so the ~2
child workflows per order stay legible in the Temporal UI.)

**Keeping a long-lived workflow's history bounded — continue-as-new.** A per-job workflow
seals its (small) history when it completes, so it never grows. A *long-lived* one is the
opposite: every signal, activity, and child it touches appends to its event history, which
would eventually hit Temporal's hard limit (it warns at 10,240 events or 10 MB and terminates
the workflow at 51,200 events or 50 MB). The fix is **continue-as-new**:
at a quiescent point the workflow atomically completes the current run and starts a fresh one
with the **same workflow ID**, carrying forward only its small live state — history resets to
~0, signals keep flowing to the new run, and it's logically "one workflow that runs forever."
`DriverRouteWorkflow` does this: when its history crosses `DRIVER_HISTORY_CONTINUE_AS_NEW`
(10K) and it's idle with nothing pending, it continues-as-new carrying just identity, position,
and lifetime delivery count. The orchestrator (`MeltdownDemoWorkflow`) is wired the same way
(`PARENT_HISTORY_CONTINUE_AS_NEW`; children are started `ParentClosePolicy.ABANDON` so they
survive the hand-off, and a continued run re-acquires them by ID) — though it's **dormant in
the demo**, which is bounded by order generation and finishes well under the threshold. The
discipline that makes this work: keep the carried state small, and only continue-as-new at a
point where that state fully captures the workflow (no in-flight activity to lose). Both
thresholds count **events**, not bytes. Model inputs and outputs make the parent's events
large: in the measured 2026-09-30 passes the inline ADK tab's `meltdown-demo` ended at 4,947
events but about 9.4 MB, just under the 10 MB warning (the Cross-Framework parent, whose model
calls run in children, stayed at about 0.4 MB). On a longer run, size warns first.

**The agent loops are real, but deliberately shallow.** Each agent is a genuine
reason→act→observe ReAct loop, and every reason call and ordinary tool call is its own
Temporal activity; `ask_human` routes to a workflow interrupt instead. The loops are only
a few hops deep (Dispatch is `reason → maybe ask_human →
reason → decide`), because picking a driver is a *bounded* task. Loop **depth is orthogonal to
the durable-HITL thesis**: a shallow loop is enough to fire `ask_human` mid-reasoning and prove
the pause is durable, and a 10×-deeper loop would exercise the *exact same* contract — just with
more steps to crash-and-replay through. Shallow loops are also *why* the per-order children stay
bounded; deepen them enough and the children would start to need continue-as-new too.

**"Long-lived" means history, not wall-clock.** A per-order child parked on `ask_human` can be
*long in wall-clock* — it may wait minutes or hours for a human — yet it stays near-free and
never needs continue-as-new, because a `wait_condition` adds almost nothing to history. That's
the durability flex: a workflow can be long-lived *purely by waiting*, cheaply, surviving
crashes. Only workflows that accumulate **history** (the drivers, delivery after delivery) are
"long-lived" in the sense that needs continue-as-new.

**Both HITL directions run on this tab.** Agent→human: the dispatch child owns its own
`answer_dispatch` signal + `pending_question` query, so the human signals the *agent's own
workflow*. Temporal preserves the wait with `wait_condition` + signal; LangGraph's
`Command(resume)` returns the answer to the loop (`interrupt()` only suspends the graph).
Human→agent: an approved address change re-runs the whole cross-framework flow via
`_rereason_crossframework` (ADK reassesses, LangGraph re-decides) and the held driver
reroutes. The `-rev<n>` children get the order's real `event` and `order_value`, so on a
high-value order the `-rev1` Dispatch agent may call `ask_human` again. The parent awaits
the re-reason before it signals the held driver, so the truck waits for that second
answer (which escalates after `GATE_ESCALATION_SECONDS`).

The disconnect/recovery scenarios (agent disconnect, driver disconnect, tool
degradation) are **dormant code**, not demo features — the UI no longer surfaces
disconnect controls. The signals, retry logic, and `degraded` flag still exist
and are documented below as mechanism, not as something you'd show on stage.

---

## Terminology

This demo has two distinct actor types:

- **AI Agents** — these **reason**. They call LLMs, use tools, and make
  decisions. Pattern A's agents (Fleet, Customer, Dispatch) run inline in the
  workflow via ADK. Pattern B's agents (Fleet, Customer, Dispatch) are looping
  LangGraph ReAct nodes — each reason call and each ordinary data-tool call runs as its
  own Temporal activity; `ask_human` instead routes to a workflow interrupt.
- **Delivery actors** (Driver-A through Driver-D) — these **execute**. They
  receive orders via signals, batch-pickup at Ziggy's (the Ferry Building), then
  deliver sequentially to multiple venues before returning. Each runs in its own
  child workflow (`DriverRouteWorkflow`). They don't reason.

In Temporal terms, the delivery actors are **child workflows**. They are not
Temporal workers (infrastructure) and not AI agents (reasoning).

---

## What is Temporal?

**The 30-second version:**

> "Temporal is a durable execution platform. You write your business logic as
> code — workflows and activities — and Temporal keeps it going when the
> service crashes, times out, or gets disconnected. Every step is recorded in an
> event log. If the worker dies mid-execution, Temporal replays the history, and
> your code resumes from the last recorded step: completed calls return their
> recorded results, and a call that was in flight runs again."

**Key points to land:**
- Workflows are durable — crashes don't lose state.
- Activities are retryable by default — transient failures self-heal.
- Signals let you inject events into a running workflow (new order, delivery
  complete, human approval).
- The Temporal UI shows the full event history for every workflow run — nothing
  is a black box. This is what makes the **worker-kill durability** demo land:
  kill the worker mid-approval, restart it, and the pending question is still in
  the workflow's history.

---

## How Temporal replay works

Temporal records every activity result in an append-only event log on the
Temporal server. When the worker crashes and restarts, it re-executes your
workflow function from the top — but when it hits an `execute_activity()` call
that already completed, Temporal intercepts it and returns the cached result from
the log instead of running the activity again. The workflow code runs again; the
side effects don't.

This means workflow code has one strict rule: **it must be deterministic**. No
real I/O, no `random`, no `datetime.now()`, no `asyncio.sleep()` directly.
Temporal provides sandboxed equivalents:

| Don't use in a workflow | Use instead |
|------------------------|-------------|
| `logging.info()` | `workflow.logger.info()` — suppressed during replay |
| `asyncio.sleep()` | `workflow.sleep()` — returns immediately if already completed |
| `datetime.now()` | `workflow.now()` — deterministic time from the event log |
| Any real I/O | `workflow.execute_activity()` — cached result during replay |

If you break determinism, Temporal raises a non-determinism error on replay.
This is a feature — it catches bugs that would otherwise silently corrupt state.

> **Why LangGraph nodes are `async`:** any callable that runs *inline in the
> workflow* (the interrupt node, conditional edges) is `async`, because LangGraph
> offloads *sync* callables to a thread executor, which Temporal's deterministic
> event loop forbids.

---

## The workflow classes

**`MeltdownDemoWorkflow`** is the brain. It owns the fleet state — driver
positions, order assignments, disconnect/reconnect status. It routes each new
order by the active tab's `_dispatch_mode`: **ADK tab → ADK inline**
(`_run_adk_assignment()`), or **LangGraph tab → inline LangGraph team**
(`_run_langgraph_assignment()`, which runs the looping team inline and drives any
in-loop `ask_human` interrupt with the `answer_dispatch` signal — no gate child), or
**Cross-Framework tab → two child workflows** (`_run_crossframework_assignment()`). It builds
`DriverSnapshot`s from its own state, applies the capacity guardrail over the
Dispatch agent's chosen driver (least-loaded fallback), and handles customer
changes — including the human→agent
re-reason path (`_process_customer_change` → `_rereason_order` on an approved
address change). It never does delivery work directly — it delegates to child
workflows.

### Order lifecycles

- **Routine (Human → Agent tab, ADK):** the order generator signals `new_order` → the ADK
  agents reason (Fleet ∥ Customer → Dispatch) → Dispatch proposes a driver
  (`submit_assignment`) → the parent checks capacity and rebalances to the least-loaded
  eligible driver when that driver has fewer orders → the driver batch-picks up at Ziggy's
  → delivers in sequence → signals the parent on each delivery → returns to base.
- **High-value (Agent → Human tab, LangGraph):** the injected order runs the looping
  LangGraph team inline in the parent (Fleet ∥ Customer → Dispatch) → mid-reasoning an
  agent (usually Dispatch, sometimes Fleet) calls `ask_human` → `interrupt()` suspends the
  graph and the parent waits durably for the `answer_dispatch` signal, with a
  `GATE_ESCALATION_SECONDS` timer that adds the backup-approver label → the human answers
  → `Command(resume=answer)` feeds it back → Dispatch reasons over the answer and the
  assessments and calls `submit_dispatch` → the parent commits that driver (if still
  eligible), or on HOLD cancels the order and keeps the capacity free.
- **Cross-Framework tab:** the parent runs `assess-<order_id>` (ADK Fleet + Customer
  return two assessment strings) → then `dispatch-<order_id>` seeded with them → the
  LangGraph Dispatch agent decides and may call `ask_human`, which parks the dispatch
  child on its own `answer_dispatch` signal (`pending_question` query) → the decision
  returns to the parent, which applies it (it owns driver state and signals the driver
  workflows). An approved address change re-runs both children (`-rev<n>`) and the driver
  reroutes.

**`DriverRouteWorkflow`** is the legs. One instance per driver, it batches
pending orders: navigate to Ziggy's → batch-pickup all orders → deliver
sequentially (venue A → venue B → …) → signal parent after each delivery →
return to base → loop. It owns its own state (status, is_disconnected,
is_recovering, path_history, current_orders). The Pattern A HITL hold lives here:
on `update_pending` the driver navigates to the venue but holds before delivering
(`awaiting_update`, `wait_condition`); on `resolve_update` it cancels, reroutes
(to the human's chosen `REROUTE_OPTIONS` location), or releases. Its HITL state is a **per-order dict**
(`_pending_holds`), so two changes on the same driver for different orders each
get their own slot.

**`OrderGenerationWorkflow`** is a child workflow that generates orders on a
timer and signals the parent with each new order. The first
`WARMUP_BURST_ORDERS` = 5 orders fire in a quick burst (`WARMUP_BURST_SECONDS` =
2s apart) to get multiple drivers on the road, then it settles into a normal
cadence (±30% jitter around a 12s base — `ORDER_INTERVAL_SECONDS`, min 5s).
Auto-generated orders top out around $1,950 (servings ≤150 × ≤$13), below the
prompt's roughly $3,000 escalation guidance, so the agents usually dispatch them
without asking. The deliberately injected $5,400 premium order is the one written to
make an agent ask. It's the model's call either way.

The workflows connect through signals in both directions:
- **Parent → child:** `add_order`, `update_pending` (HITL hold), `resolve_update`
  (HITL decision), `cancel_order`, plus dormant `driver_disconnected` /
  `driver_reconnected`.
- **Child → parent:** `order_delivered` (driver state), `new_order` (from the
  order generator).
- **External → parent (Pattern B):** `answer_dispatch(order_id, decision)` — a human's
  answer to an agent's in-loop `ask_human`, which resumes the suspended LangGraph
  team. (No child workflow involved — the parent runs the team inline.)
- **External → parent (Pattern A):** `customer_change` — a customer's order change;
  the parent holds the driver, waits for human approval, and on an approved address
  change re-reasons the assignment via the ADK team (`_rereason_order`).

Both child → parent signals are **guarded with try/except** so a terminated
parent (e.g. during demo reset) can't crash the child mid-delivery.

---

## Pattern A — Human-in-the-loop (Google ADK)

**The human calls the agent.** A customer submits an order change (address
change → pick a new SF location from a dropdown, or cancel) mid-delivery; the
driver holds at the venue; a human supervisor approves or rejects. This is **ONE
human gate that feeds both loops** — the agent reasoning loop (re-reason) and the
driver delivery loop (hold → reroute).

This is **customer-in-the-loop**, not agent-in-the-loop. The change is initiated
*externally* (submitted via REST) and the gate lives in the **workflow**,
not in any agent tool. The agents don't decide *whether* to act on the change —
but on an approved address change they **re-reason the assignment** for the new
location (see below). (Contrast an `ask_user`-style `@function_tool` where the
LLM itself pauses for clarification — that's Pattern B's shape, not this one.)

The flow:
1. `POST /api/customer-change` → signals the parent `customer_change` *and*
   signals the child `update_pending` to hold.
2. The driver navigates to the venue but holds before delivering
   (`awaiting_update`, `wait_condition`). The parent waits for the human; the
   child waits for the parent — **two `wait_condition` pauses, both durable**. The
   dashboard doesn't show `awaiting_update`; read it with
   `temporal workflow query --workflow-id route-driver-X --type get_status`.
3. `POST /api/approve-change` → signals `change_approved` → `execute_customer_change`
   activity → for an **address change**, the parent updates the order to the human's
   chosen location and the **ADK assignment team re-reasons** it (`_rereason_order`,
   below) → parent signals `resolve_update` to the child with the decision:
   cancel → skip delivery; address_change → reroute to the chosen location; release →
   deliver to the original destination.

The same approval drives the **agent's reasoning loop** as well as the driver's.
On an approved address change, `_process_customer_change` updates the order record
to the human's chosen location and calls `_rereason_order`, which **re-runs the
full ADK assignment team** (`_run_adk_assignment` — Fleet ∥ Customer → Dispatch)
over the revised order (with its real `event`): Fleet recomputes ETAs to the new spot,
Customer re-reads priority, Dispatch reassesses — and the reassessment is published to the
agent panels. So the human's edit is the new input the agents reason over. The re-reason
result is published only; the held driver then reroutes to the human's chosen destination
either way. **Cancel** is a
fixed cancel (no re-reason); **reject** releases the driver to deliver to the
original destination. It's the same durable primitive throughout (signal +
`wait_condition`), feeding both loops off one approval.

The reroute destinations come from a curated `REROUTE_OPTIONS` list in
`locations.py` (Oracle Park + Salesforce Tower, Union Square, Coit Tower, Palace
of Fine Arts), exposed via `/api/locations` as `reroute_options` and shown in the
customer-change dropdown.

`deliver_order` returns `success=False` when a cancel wins the race, so the
workflow skips the `order_delivered` signal for cancelled orders. The child's
HITL hold also escapes on `self._stop` so demo shutdown can't leave a parked
child hanging the parent's `await handle` join.

### Where the ADK agents fit

The ADK agents run **inline in the workflow** via
`_run_adk_assignment()` in `MeltdownDemoWorkflow`. The workflow builds
`DriverSnapshot`s from its own state and passes them to the ADK pipeline. Each
LLM call becomes an `invoke_model` Temporal activity via `TemporalModel`; each
data-tool call becomes a Temporal activity via `activity_tool`. If an activity fails,
Temporal retries.

Two pieces of code make that work. First, the ADK `Runner` executes inside the workflow's
execution context, not as an external call:

```python
# workflows.py → _run_adk_assignment()
runner = Runner(agent=agent, app_name="meltdown_demo", session_service=session_service)
async for event in runner.run_async(
    user_id="workflow", session_id=session.id,
    new_message=Content(parts=[Part(text=prompt)]),
):
    events_count += 1
```

Second, `GoogleAdkPlugin` is registered on the workers (`worker.py`): on the workflow
worker it adds sandbox passthroughs and a deterministic runtime for replay, and on the
agents worker it registers the `invoke_model` activity that `TemporalModel` routes each
Gemini call to:

```python
Worker(
    client, task_queue=AGENTS_QUEUE,
    activities=[register_assignment, tool_get_fleet_status, ...],
    plugins=[GoogleAdkPlugin()],
)
```

If the worker crashes mid-reasoning, the workflow replays from the event log: completed
model and tool calls return their recorded results, and a call that was in flight runs
again.

| ADK concept | Temporal concept |
|-------------|-----------------|
| **LLM Agent** (`Agent` + `TemporalModel`) | Each Gemini call → an `invoke_model` activity, recorded in the event log |
| **Orchestrator Agent** (`SequentialAgent`, `ParallelAgent`) | Plain Python coordination inside the workflow — no activity, no LLM |
| **Data-tool call** (via `activity_tool`) | Each call → a named Temporal activity, retryable and replayable |
| **Entire agent pipeline** | Runs inline in the workflow via `_run_adk_assignment()` |

Temporal doesn't see the orchestration logic as separate steps; it records the individual
LLM calls and tool calls as activities.

The pipeline is composed in `agent_fleet/agents.py` with ADK's `ParallelAgent`
and `SequentialAgent`:

```python
def create_order_assignment_agent() -> SequentialAgent:
    parallel_assessment = ParallelAgent(
        name="assignment_parallel",
        sub_agents=[
            create_assignment_fleet_agent(),    # positions, capacity, ETAs
            create_assignment_customer_agent(), # priority, deadline, venue context
        ],
    )
    dispatch_agent = create_assignment_dispatch_agent()  # synthesizes → tool_submit_assignment
    return SequentialAgent(
        name="order_assignment",
        sub_agents=[parallel_assessment, dispatch_agent],
    )
```

**What each agent reasons about:**

| Agent | What it evaluates | Tools |
|-------|-------------------|-------|
| **Fleet Agent** | Delivery actor positions, free capacity slots, driving ETAs to destination | `tool_get_fleet_status`, `tool_get_route_info` (Google Maps Directions) |
| **Customer Agent** | VIP vs standard tier, deadline tightness, venue events (conference catering, receptions, festivals), servings/guest count | `tool_get_order_priorities`, `google_search` (Gemini grounding) — LangGraph uses `tool_search_venue_events` for the same web-grounded venue check |
| **Dispatch Agent** | Synthesizes both assessments, proposes the final delivery actor, submits a structured assignment | `tool_submit_assignment` |

Fleet Agent and Customer Agent run in parallel; the Dispatch Agent runs
sequentially after both complete. Fleet, Customer, and Dispatch are all **LLM
Agents** (`Agent` + `TemporalModel(...)`). `create_order_assignment_agent()`
returns an **Orchestrator Agent** (`SequentialAgent`) — no model, no LLM call, no
Temporal activity. It purely sequences the sub-agents.

Not every tool is an activity. `tool_get_fleet_status`, `tool_get_route_info` and
`tool_get_order_priorities` are wrapped with `activity_tool()`. `tool_submit_assignment`
is plain in-workflow code that records the decision (`agents.py`). `google_search` is
Gemini grounding inside the model call itself, so it shows up as part of an
`invoke_model` activity. Gemini's built-in search normally can't share a request with
custom function tools; ADK's `GoogleSearchTool(bypass_multi_tools_limit=True)` gets
around that by running the search through an internal sub-agent built on the same
`TemporalModel`, so it is an `invoke_model` activity too.

**Tool parity across frameworks.** Fleet and Customer reason over the same inputs in ADK
and LangGraph, across all three tabs. Fleet uses `tool_get_fleet_status` +
`tool_get_route_info` in both. Customer uses `tool_get_order_priorities` plus a
venue-events web search in both: ADK through `google_search` grounding, LangGraph through
`tool_search_venue_events` (an activity that calls Gemini with `GoogleSearch` grounding).
Switching tabs changes which framework orchestrates, not what the agents can see.

**Driver choice.** The Dispatch agent proposes a driver — it calls `submit_assignment`
(ADK) / `submit_dispatch` (LangGraph) with a driver from the eligible set
(connected + under capacity). The parent then applies a **capacity guardrail**:
it commits the agent's chosen driver if that driver is still eligible, otherwise
falls back to the least-loaded eligible one (`_least_loaded_driver()`, also the
default proposal seeded into the agent). On the ADK tab the parent goes one step
further: it swaps a valid pick for the least-loaded eligible driver when that driver has
fewer orders, to keep the whole fleet moving (`workflows.py`, "Spread load across the
fleet"). The LangGraph and Cross-Framework paths keep the agent's pick when it's eligible.
The fleet runs **4 drivers (A–D) at capacity 2**, so capacity is genuinely scarce — the
agent has to reason about who has a free slot. The Fleet tool reports capacity as `x/2`,
matching what the workflow enforces.

---

## Pattern B — Agent-in-the-loop (LangGraph, human judgment is a tool)

**The agent calls the human.** While the LangGraph tab is active, every order runs
a **looping multi-agent LangGraph team** that mirrors the ADK side — *inline in the
parent workflow*, not in a child. The HITL is **inside the reasoning loop**: an agent
that hits a decision it shouldn't make alone calls the `ask_human` tool mid-reasoning.
LangGraph's `interrupt()` suspends the graph, and Temporal durably preserves the wait for
the human's answer signal. There is **no per-order gate child**.

### Routing

The tab selects the framework with a Temporal signal; it does not start a workflow:

```js
// frontend/index.html → switchTab(); tabMode() maps agent → langgraph, cross → crossframework, else adk
api('dispatch-mode', 'POST', { mode: tabMode(tabName) });
```
```python
# server.py: the endpoint signals the already-running parent
await handle.signal(MeltdownDemoWorkflow.set_dispatch_mode, body.mode)
# workflows.py: the signal handler sets a flag that _assign_order routes on
self._dispatch_mode = mode
```

In `MeltdownDemoWorkflow._assign_order`, the LangGraph branch runs the team as a
concurrent asyncio task (appended to `self._langgraph_tasks`)
so the order loop and fleet keep moving while the agents — and possibly a human —
deliberate:

```python
if self._dispatch_mode == "langgraph":
    self._langgraph_tasks.append(
        asyncio.create_task(
            self._run_langgraph_assignment(order, self._least_loaded_driver(), onum)
        )
    )
    return
```

`_run_langgraph_assignment` compiles `graph(GRAPH_NAME)` (`GRAPH_NAME = "dispatch_team"`)
and `ainvoke`s it in-workflow — the Fleet ∥ Customer → Dispatch reason calls and ordinary
data-tool calls execute as Temporal activities recorded in the **parent's** history. Whether
to ask a human is the **agent's** judgment (guided by `ESCALATION_GUIDANCE` and the
per-agent system prompts in `langgraph_agents.py`), not a code threshold. Auto-generated
orders top out around $1,950, so the agents usually dispatch them directly; the
deliberately injected $5,400 Moscone order (`POST /api/inject-order`) is written to be
exceptional enough that an agent usually calls `ask_human`. The model may ask zero, one,
or two times (Fleet, then Dispatch).

Each node carries `metadata={"execute_in": "activity"}` (the reason nodes) or
`"workflow"` (the `*_act`, `*_human` and routing nodes), so the Gemini reason calls run as
**Temporal activities recorded in the parent's event history**, not in a separate child
workflow. The team runs as a concurrent task so the fleet keeps moving while the agents
deliberate.

### The looping multi-agent team graph

`build_dispatch_team_graph` (in `langgraph_agents.py`) compiles a LangGraph graph via
`temporalio.contrib.langgraph`. It mirrors the ADK team — Fleet and Customer fan out from
START in parallel, then converge on Dispatch — but each agent is a **real reason → act →
eval ReAct loop**, not a single `ainvoke`:

```
START → fleet_reason    ──┐  (loops: reason → {ask_human | run tools | done})
START → customer_reason ──┴→ dispatch_reason → (END | ask_human → reason)
```

Each `*_reason` node is a **real Gemini call** (through `init_chat_model`,
provider-swappable via `MODEL_PROVIDER`) executed as a **Temporal activity**. Each ordinary
data-tool call runs in the `*_act` node as **its own Temporal activity** (via
`workflow.execute_activity`, on the agents queue with its own retry policy) — mirroring
ADK's `activity_tool` granularity. The data tools are the same ones the ADK team uses:
`get_fleet_status`, `get_route_info`, `get_order_priorities`. Fleet and Dispatch also bind
the `ask_human` tool; Customer does not.

**Fan-in barrier (`defer=True`).** `dispatch_reason` is registered with `defer=True` so it
is a true convergence point: it runs **once, only after both** the Fleet and Customer
branches reach it. Without `defer`, LangGraph would run the downstream node *early* on
partial state (when one branch loops longer than the other) and then *again* when the
slower branch finishes — Dispatch would decide on half-arrived input. Note `defer` waits
for both *branches to complete*, not for an agent to be "available": a degraded agent's
branch still completes (its tool call fails, `_run_tools` catches it, the agent concludes
with degraded output), so Dispatch still runs with both assessments — one possibly
degraded. (If we ever *skipped* a disconnected agent's node entirely, `defer` would block;
we'd switch to a timeout-based join then.)

> **Temporal-UI labels (`summary`) — placeholder.** Each reason node gets a static
> `summary` string via `_node_summary(...)`. That helper is a seam: when the LangGraph
> plugin ships per-call `summary_fn` support (the analog of ADK's `TemporalModel`
> `summary_fn` / `_build_summary`), swap the static string for a callable that builds
> context-aware labels (e.g. "Fleet Agent — ETA for driver-c"). Inert until that lands.

### Human judgment is a tool — `ask_human`, interrupt, and durable wait

When an agent decides it needs a human, it calls the `ask_human(question)` tool. The
tool body never runs (`raise NotImplementedError`); the graph's `route` function sees the
`ask_human` tool call and routes to a `*_human` node (`fleet_human` / `dispatch_human`)
whose body is the tool's real "execution":

```python
async def _human_node(messages, agent_label, state):
    answer = interrupt({"question": ..., "order_id": state["order_id"], ...})  # ⏸ suspend
    return [ToolMessage(content=str(answer), ...)]   # answer flows back as the observation
```

`interrupt()` suspends the graph; Temporal makes the gap durable. The answer becomes the
`ToolMessage` the agent observes on its **next reason turn**. So the human's answer is an
in-loop observation, not a boundary decision the system applies.

**Why `interrupt()` and not just a signal?** In this in-loop design, the human's answer
has to flow back into the running graph as the agent's next observation. `interrupt()` is
LangGraph's way to pause inside a node and resume it with a value
(`Command(resume=answer)`). The Temporal `answer_dispatch` signal + `wait_condition` is
the durable *wait*; `interrupt()` is the graph plumbing that lets the answer rejoin the
loop.

### The parent preserves the wait; the answer signal resumes the graph

`_run_langgraph_assignment` loops on the graph's `__interrupt__` marker. On each interrupt
it surfaces the question into `self._pending_dispatch[order_id]`, marks the order
`awaiting_dispatch_approval`, publishes an agent event, then parks on
`_await_dispatch_answer` (a `wait_condition` on the `answer_dispatch` signal). The human's
answer resumes the graph via `Command(resume=answer)`:

```python
result = await compiled.ainvoke(state, config=config)
while result.get("__interrupt__"):
    self._pending_dispatch[order.order_id] = result["__interrupt__"][0].value
    answer = await self._await_dispatch_answer(order.order_id)   # ⏸ wait_condition on answer_dispatch
    if answer is None:                                           # demo shutting down — exit cleanly
        return
    result = await compiled.ainvoke(Command(resume=answer), config=config)  # resume the agent
```

```python
@workflow.signal
async def answer_dispatch(self, order_id: str, decision: str):  # the human responds → resolves it
    self._dispatch_answers[order_id] = decision
```

`_await_dispatch_answer` first waits `GATE_ESCALATION_SECONDS` (default 30, set in
`agent_fleet/config.py` and overridable from `.env`). If no answer arrives, it marks the
pending entry `escalated` / `approver_tier: "backup"` — the card then shows "Escalated to
backup approver (primary window timed out)" — and keeps waiting with no timeout. The
timer is a durable Temporal timer, so it fires even while no worker is running. Nobody
else is contacted; "backup approver" is a label. `_await_dispatch_answer` returns `None`
if the demo shuts down (`self._routes_done`) while parked, so the team task exits cleanly
instead of hanging the parent's teardown.

Once the team finishes, the workflow trusts the human's answer (a `rejected` flag) over
the graph's text, because Gemini sometimes returns an empty final turn. The decision is
**HOLD** if the human rejected, or the agent's `submit_dispatch` decision is `hold`, or a
plain-text reply *leads* with "HOLD" or "held" (`_text_decision` in
`langgraph_agents.py`; a reply that only mentions holding stays DISPATCH). HOLD →
`_reject_order` (cancel, preserve fleet capacity); otherwise → `_commit_assignment` to the
agent's chosen driver if still eligible, else the proposed one. On this tab the
`dispatch_gate` event that `_reject_order` publishes tells the two kinds of HOLD apart:
"Supervisor rejected high-value order …" after a human reject, "Dispatch agent held
order … after a human approved it" (or "… without asking a human") when the agent held on
its own. The Cross-Framework tab still says "Supervisor rejected" for every HOLD. The
dashboard doesn't render `dispatch_gate` events, so on screen both kinds show only as
"… — held" in the Dispatch panel.

### How the UI sees the pending approval

When an agent calls `ask_human`, the parent stores the interrupt payload (the question +
order context) in its `pending_dispatch` dict (keyed by order_id).
`GET /api/pending-dispatch` queries the parent's `get_status` and reads that
`pending_dispatch` dict. `POST /api/approve-dispatch` signals the parent's
`answer_dispatch(order_id, decision)` directly — no per-order gate child is involved.
The card shows the full `order_id` and the workflow to signal (`meltdown-demo`, or
`dispatch-<order_id>` on the Cross-Framework tab), so the same answer can be sent from
the CLI; it re-renders only when its content changes, so the ids can be selected and
copied.

Be precise about the three roles: **`wait_condition` is the pause**, the **`signal` is
the resume** (it delivers the human's answer *and* unblocks the wait), and the **`query`
is the read** that surfaces the pending question to the UI. The query unblocks nothing —
it's required for a human to *see and answer*, but the workflow's pause and resume depend
only on `wait_condition` + `signal`. The UI is never part of the durability path.

### The durability moment

Kill the worker while the approval card is up (`make kill-worker`, which sends SIGKILL).
The fleet freezes — but the pending-approval state lives in **Temporal's event log, not
the worker's memory**. The card itself can disappear while the worker is down, because
`/api/pending-dispatch` reads it through a Query and Queries need a live worker; the
Temporal UI still shows `meltdown-demo` as Running. The human can answer anyway:
`temporal workflow signal --workflow-id meltdown-demo --name answer_dispatch --input
'"<order_id>"' --input '"approve"'` is recorded by the Temporal server with no worker
present. Restart the worker (`make worker`): the workflow replays from history, the
graph is suspended on its `interrupt()` again, the parent is parked on the
`answer_dispatch` `wait_condition` (or picks up the recorded signal), and the card comes
back. If the escalation timer expired during the outage, the card now carries the
backup-approver label.

Completed model and tool calls are not re-run on replay; a call that was in flight at the
kill runs again (and is billed again). No automated test covers the kill-and-resume path
yet: `tests/test_workflows.py` covers the driver hold, continue-as-new, the escalation
setting and the decision rules, and the gitignored `spikes/langgraph_hitl/` scripts
exercise the interrupt path without an LLM. Note that LangGraph's `InMemorySaver` (the
checkpointer the graph compiles with) is **non-durable on its own** — it's Temporal's event
log that makes the in-loop wait survive the crash.

> **A note on LangGraph.** This demo uses LangGraph as a *framework* — for its
> graph/loop abstraction (the Dispatch agent's reason→act→eval loop) — and lets
> **Temporal** provide durability and persistence, so the LangGraph checkpointer here
> is just `InMemorySaver`.

---

## How `TemporalModel` and `activity_tool` work (Pattern A's ADK side)

A common question: "where are the Temporal activities defined for each agent's
LLM call and tool call?" They aren't — they're injected automatically by two
wrappers:

- **`TemporalModel(DEFAULT_MODEL, activity_config=...)`** — set as an agent's
  model, every LLM call that agent makes runs as a Temporal `invoke_model`
  activity routed to the agents queue. You don't write the activity. ADK supports
  other providers too — swap `DEFAULT_MODEL`.
- **`activity_tool(tool_get_fleet_status, ...)`** — wrap a tool function this way
  and every call runs as a Temporal activity. Our local `_activity_tool.py` adds
  two fixes over upstream: correct multi-arg handling, and **graceful failure** —
  when an activity exhausts its retry policy, the error is returned as a string to
  the LLM instead of crashing the pipeline. (This is also how the dormant
  disconnect path degrades: Fleet Agent tools fail fast, the LLM sees the error,
  the Dispatch Agent assigns with available data, and the order is flagged
  `degraded`. The same path handles real Maps API failures gracefully.)

```python
fleet_agent = Agent(
    name="assignment_fleet_agent",
    model=TemporalModel(
        DEFAULT_MODEL,
        activity_config=ActivityConfig(task_queue=AGENTS_QUEUE),  # route to agents worker
    ),
    tools=[_fleet_status_tool, _route_info_tool],
    ...
)

_fleet_status_tool = activity_tool(
    tool_get_fleet_status,
    task_queue=AGENTS_QUEUE,
    start_to_close_timeout=timedelta(seconds=10),
    retry_policy=_TOOL_RETRY,
)
```

No `@activity.defn` decorator, no explicit registration. **ADK composes and
sequences agents; Temporal makes every external call durable.** This is the
recommended pattern for `temporalio[google-adk]`.

**Model calls retry in one place: Temporal.** Every LLM client makes a single attempt
(`LLM_MAX_RETRIES = 0` in `config.py`), so a failed call fails its activity and Temporal's
retry policy retries it. You see each attempt in the event history, and
`scripts/token_usage.py` counts it. The plugin's `invoke_model` builds ADK's `Gemini` from
the model name, so there is no `Gemini(retry_options=...)` to set. Instead each agent passes
`generate_content_config=_one_attempt_config()` (`HttpRetryOptions(attempts=1)`), which
google-genai applies per request. The LangGraph side passes `max_retries=0` to
`init_chat_model` (the langchain-google-genai default is 6 tries inside one activity attempt),
and the venue-search activity builds its `genai.Client` with `attempts=1`. One resend
has no knob: with aiohttp installed (it is, through `google-cloud-aiplatform`), google-genai's
async path resends a request once after a connection failure (not after an HTTP error such as
a 429). ADK and LangGraph model calls are async, so that resend happens inside the activity,
out of Temporal's sight.

**Maps errors behave differently in the two places they happen.** `tool_get_route_info`
(the agents' ETA tool) calls the Directions API; when it fails after its retries, the
error goes back to the model as text (`ERROR: Tool tool_get_route_info failed: ...` in ADK,
`ERROR: get_route_info failed — ...` in LangGraph), the Fleet agent notes the missing ETA,
and Dispatch assigns with the data it has. `get_route_polyline` (the truck's route) is
different: it retries for up to 5 minutes while the truck waits, and then the
`DriverRouteWorkflow` fails. Both report errors without the request URL, because the Maps
key rides in the query string: a non-2xx reply raises `Maps Directions API HTTP <code>`, a
timeout or connection error raises `Maps Directions API request failed: <ErrorType>`, and
an HTTP 200 with a bad status (`REQUEST_DENIED`, `OVER_QUERY_LIMIT`) raises
`Maps Directions API returned status: <status>`. The worker also sets the `httpx` logger to
WARNING so request URLs never reach the log.

### Why LangGraph and ADK look so different — you own the loop vs. batteries-included

The two framework files diverge on purpose. **In LangGraph, you own the loop**, so
`langgraph_agents.py` carries the helpers that hand-build it: the reason↔act loop and its
routing, per-tool-call activities (`_run_tools`), message parsing (`_coerce_text` /
`_last_text`), the `interrupt()` human node (`_human_node`), and model + tool binding
(`_chat_model`). **ADK doesn't need any of that** — its `Runner` runs the loop. In both
cases the framework decides the loop, and its code runs inside a workflow. `TemporalModel`
+ `activity_tool` make each model call and each data-tool call a durable Temporal
activity, and structured output comes back through ADK session state. So: **LangGraph =
assemble the loop from primitives; ADK = the loop is batteries-included** — same durable
substrate underneath, different amount of plumbing on top.

---

## Communication patterns — what goes where, and why

The demo routes different kinds of data through different mechanisms. This is the
part new Temporal users most often get wrong — they put everything in signals or
workflow state and the event log blows up.

| Data flow | Mechanism | Why |
|-----------|-----------|-----|
| Driver position (updates every ~400ms during navigation) | Shared state (FleetState / SQLite) | High-frequency telemetry. No workflow decision depends on sub-second position. Routing it through signals would bloat the event log ~100× for no benefit. |
| Delivery completed | Child → parent signal (`order_delivered`) | Milestone event. Parent needs it for bookkeeping and as input to the next ADK assignment. |
| New order generated | Child → parent signal (`new_order`) | Milestone, low frequency. The assignment loop waits on it. |
| Order assignment | Parent → child signal (`add_order`) | Parent decides, child executes. The signal is the durable handoff. |
| Customer change (Pattern A) | External → parent → child signal chain | Preserves replay + audit. Every approval/rejection is in the event log. |
| Agent asks a human (Pattern B) | In-loop LangGraph `interrupt()` + Temporal wait + human → parent signal (`answer_dispatch`) | The agent calls `ask_human` mid-loop; the interrupt suspends the graph, Temporal preserves the wait, the question flows into the parent's `pending_dispatch` for the UI, and the human's answer signal returns as the next agent observation. |
| Driver state for reasoning | The agents call `tool_get_fleet_status`, which reads FleetState (SQLite); the parent enforces capacity from its own workflow state | The agents need live positions; the capacity rule has to be replay-safe, so it lives in the workflow. Both use 2 orders per driver. |

**Temporal event log vs shared state — two different questions:**

| Temporal event log | Shared state (SQLite / FleetState) |
|---|---|
| Durable, append-only, replayable | Mutable, last-writer-wins, disposable |
| *"How did we get here?"* | *"Where are we now?"* |
| Source of truth for workflow replay | Source of truth for the UI's live view |

A production system pairs Temporal with Redis or Postgres for exactly this split;
in the demo, SQLite (`fleet_state.db`, WAL-backed, shared across processes) is the
toy stand-in.

**Driver position updates don't go through the event log at all.**
`navigate_to` heartbeats position to FleetState every ~400ms during a drive —
none of those are Temporal events. At production scale (GPS pings per second
across a 15-minute delivery) that's where the volume lives, against ~100 durable
orchestration events per order. Temporal carries the decisions; shared state
carries the telemetry.

---

## The 3-queue worker architecture

The demo runs three Temporal workers in a **separate worker process**
(`python -m agent_fleet.worker`), each on a dedicated task queue. The FastAPI
server runs in its own process.

| Queue | Worker | What it runs |
|---|---|---|
| `meltdown-workflows` | Workflows + minimal local activities | `MeltdownDemoWorkflow`, `DriverRouteWorkflow`, `OrderGenerationWorkflow`, plus the cross-framework child workflows `AdkAssessmentWorkflow` + `LgDispatchWorkflow`; `publish_agent_event` / `publish_agent_events_batch` (local activities); the LangGraph reason-node activities (Fleet/Customer/Dispatch Gemini calls, via `LangGraphPlugin`, 60 s timeout, no heartbeat). `LangGraphPlugin` registers **two** graphs: `GRAPH_NAME = "dispatch_team"` (the looping multi-agent team with the in-loop `ask_human` tool, run inline in `MeltdownDemoWorkflow` for the LangGraph tab) and `DISPATCH_ONLY_GRAPH_NAME = "dispatch_only"` (the Dispatch-only graph run inside `LgDispatchWorkflow` for the cross-framework tab). |
| `meltdown-delivery` | Delivery | `generate_order`, `navigate_to`, `pickup_orders`, `deliver_order`, `execute_customer_change`, `get_route_polyline`, `get_fleet_status`, `get_order_priorities`, `set_driver_idle`, `set_warmup_hidden`, `sync_driver_position` (max 20 concurrent) |
| `meltdown-agents` | ADK/LLM activities | `register_assignment`, `tool_get_fleet_status`, `tool_get_order_priorities`, `tool_get_route_info`, `tool_search_venue_events` (LangGraph Customer's venue-events search; the LangGraph `*_act` nodes call these same tool activities) plus the ADK `invoke_model` activity, which includes `google_search` grounding (max 5 concurrent) |

**Why a workflows-only-ish worker?** Workflows must be deterministic and
replayable. Keeping the heavy activities on other queues keeps workflow code away
from `FleetState` and I/O; the workflow sandbox enforces the rest.

**Why separate activity queues?** LLM calls are slow (3–5s each). Without queue
separation, a flood of assignment requests could starve navigation activities and
cause drivers to miss heartbeat timeouts. The agents queue caps at 5 concurrent;
delivery at 20. The LangGraph reason calls run on the workflows queue, so the agents
cap doesn't apply to them; the LangGraph data-tool calls run on the agents queue.

**Plugin placement** (in `worker.py`):
- `GoogleAdkPlugin` is on **both** the workflow worker (sandbox passthroughs for
  `google.adk` / `google.genai`, deterministic runtime for replay) and the agents
  worker (hosts the `invoke_model` activity that calls Gemini for Pattern A).
- `LangGraphPlugin(graphs={...})` is on the **workflow** worker, registering two graphs:
  `GRAPH_NAME: build_dispatch_team_graph()` for the inline Pattern B team and
  `DISPATCH_ONLY_GRAPH_NAME: build_dispatch_only_graph()` for the cross-framework
  LangGraph child. Their reason-call activities execute there; the `*_act` nodes send
  data-tool calls to the agents queue; `ask_human` routes to workflow code instead.

`TemporalModel` uses `ActivityConfig(task_queue=AGENTS_QUEUE)` to route Pattern
A's LLM calls from the workflow to the agents queue.

---

## Two processes

`run.sh` starts a worker process and a server process (plus the Temporal dev
server).

- **The worker process** owns all activities and workflow execution. It does
  **not** load `.env` itself — to start it by hand, pass the env
  file: `uv run --env-file .env python -m agent_fleet.worker` (`make worker`). The worker
  is live-only: it refuses to start unless `GOOGLE_API_KEY` and `GOOGLE_MAPS_API_KEY` are
  both set; there is no mock mode. If it exits after startup (`make kill-worker`), `run.sh`
  keeps Temporal and the server running so the parked workflows wait for `make worker`.
- **The server process** runs no workers. Its WebSocket snapshot is built from
  **FleetState (SQLite)** — `_build_snapshot()` → `fleet.snapshot()` — not from
  Temporal queries. Activities (in the worker) write positions, statuses, and
  agent events to FleetState; the server reads them for the frontend. Otherwise the
  server starts workflows, sends **signals**, runs **queries** (e.g. `get_status` for
  `/api/pending-dispatch`), terminates the demo's workflows on Reset, and writes a few
  FleetState rows itself (the injected order, reset, the dormant disconnect controls).
  It has no GoogleAdkPlugin and no activity registration.

---

## When does an agent become its own Temporal workflow?

On the single-framework tabs, the whole selected team runs **inline in the parent
workflow** (`MeltdownDemoWorkflow`): ADK via the `Runner`, LangGraph via the graph, with
each LLM call and ordinary tool call as an activity. The Cross-Framework tab deliberately
splits the ADK assessment and LangGraph dispatch phases into child workflows. Use this test
when deciding whether an agent or framework phase deserves its own child workflow:

| Make it a **child workflow** when it… | Keep it an **activity / graph node** when it… |
|---|---|
| has an independent lifecycle / failure-and-retry domain | is a short, stateless step in a tightly-coupled flow |
| needs its own **signals or queries** (e.g. a HITL pause) | needs no independent interaction |
| should **scale on its own** task queue | scales fine with the parent |
| is **reused** by other workflows | is only ever used here |
| must be **independently observable** in the Temporal UI | doesn't warrant its own history |

Applied here: **Dispatch** is the clearest candidate — `ask_human` can leave it parked while
its workflow waits for a human signal, which is exactly "a workflow as a durable async
endpoint." That is what the cross-framework `LgDispatchWorkflow` demonstrates. On the
LangGraph-only tab Fleet can also call `ask_human`, but both Fleet and Dispatch remain graph
nodes because the whole team intentionally runs inline in the parent. So the rule of thumb
is: **HITL pauses and long-lived/independent units may justify a workflow; quick, tightly
coupled reasoning/tool steps usually stay activities or graph nodes.** Split for lifecycle
and ownership boundaries, not tidiness alone.

### Splitting moves orchestration from the framework to Temporal

A key consequence: today the **framework** (LangGraph graph / ADK `Runner`) owns
*inter-agent* orchestration (fan-out, converge, sequence). If each agent becomes a child
workflow, **Temporal** owns inter-agent orchestration (parent spawns children, `gather`-
joins, sequences) and the framework keeps only the *intra-agent* loop. ADK is the
opinionated one here — its `SequentialAgent`/`ParallelAgent` + shared session assume one
`Runner`, so splitting fights its design; LangGraph decomposes more naturally because its
nodes already run as separate activities. Neither is impossible; both hand the team layer
to Temporal.

### Multi-framework (cross-framework) orchestration — built (the Cross-Framework tab)

The compelling reason to split is **heterogeneous agents**: one agent built on ADK, another
on LangGraph, in one system. ADK orchestrates ADK agents, LangGraph orchestrates LangGraph
nodes, and each one's durability stops at its own edge. The moment you mix
them, something has to carry the handoff between them. Plain code or a protocol like A2A
can do that; in this demo **Temporal — the durable-execution runtime (the substrate) —
makes the handoff durable** (each framework phase is a workflow, and Temporal runs the
sequence and the HITL waits between them, regardless of what framework runs inside each).

**This is now built — it's the Cross-Framework tab** (see "The third tab" above). Per order,
the parent runs an `AdkAssessmentWorkflow` (Fleet ∥ Customer on ADK), then an
`LgDispatchWorkflow` (Dispatch on LangGraph), as child workflows and applies the result; the
dispatch child owns its own `ask_human` HITL. Each framework keeps only the *intra-agent* loop inside
its child, and **Temporal runs the handoff between the agents** — the split this
section argues for. ADK's `ParallelAgent` still runs Fleet ∥ Customer inside the one ADK
child (its parallelism stays the framework's job); Temporal owns the single ADK→LangGraph
cross-framework boundary. The full-team graph's `defer` fan-in barrier isn't needed here — the parent's
`await child` sequence is the join.

---

## Key files

| File | What it does |
|------|-------------|
| `agent_fleet/models.py` | Dataclass models for all Temporal payloads (incl. `DriverSnapshot`, `LgDispatchInput` / `LgDispatchOutput`) |
| `agent_fleet/simulation.py` | FleetState — the SQLite (WAL) projection in `fleet_state.db`, shared across processes. Activities write it and some read it (positions, fleet status, disconnect flags); the server reads it for every dashboard snapshot. `DRIVER_CAPACITY = 2` mirrors the workflow's value |
| `agent_fleet/activities.py` | Temporal activities — navigation, delivery, Maps Directions (key-safe errors), agent tools, the LangGraph venue search |
| `agent_fleet/workflows.py` | Temporal workflows — owns driver state, signals, queries. Drives both HITL flows: `_run_langgraph_assignment` / `_await_dispatch_answer` (Agent → Human: surfaces the in-loop `ask_human`, waits on `answer_dispatch` with the escalation timer, resumes via `Command`) and `_process_customer_change` / `_rereason_order` (Human → Agent: holds the driver and, on an address change, re-runs the ADK team). The Cross-Framework tab uses `AdkAssessmentWorkflow` (`assess-<order_id>`) then `LgDispatchWorkflow` (`dispatch-<order_id>`, owns its `answer_dispatch` signal and `pending_question` query); the parent applies the decision and signals the drivers. Also `DriverRouteWorkflow` and `OrderGenerationWorkflow` |
| `agent_fleet/agents.py` | ADK agent composition — Fleet, Customer, Dispatch (an approved address change re-runs this team via `_rereason_order` → `_run_adk_assignment`) and the Fleet ∥ Customer assessment team for the Cross-Framework tab |
| `agent_fleet/langgraph_agents.py` | The looping LangGraph team (Fleet ∥ Customer reason→act→eval loops → Dispatch), the dispatch-only graph, `ask_human`, `ESCALATION_GUIDANCE`, and `_text_decision` |
| `agent_fleet/_activity_tool.py` | Local `activity_tool` wrapper: multi-arg handling and errors returned to the LLM as text |
| `agent_fleet/config.py` | Env config — `GOOGLE_API_KEY`, `GOOGLE_MAPS_API_KEY`, `DEFAULT_MODEL` (default `gemini-3.8-flash`), `TEMPORAL_ADDRESS`, `FLEET_DB_PATH`, `GATE_ESCALATION_SECONDS` |
| `agent_fleet/queues.py` | Task queue names (workflows / delivery / agents) |
| `agent_fleet/worker.py` | Three Temporal workers — workflows, delivery, agents — plus the heartbeat file for the Service Online badge. Live-only; requires `GOOGLE_API_KEY`. Quiets `httpx` logging so Maps URLs (and the key) stay out of the log |
| `agent_fleet/server.py` | FastAPI server — start / signal / query / reset API for both patterns, WebSocket, frontend |
| `agent_fleet/locations.py` | Downtown SF venue pool (Moscone, Fisherman's Wharf, Chinatown; Ferry Building shop), reroute options and random order generation |
| `frontend/index.html` | Single-file SPA — Leaflet map (default style "Dark (OpenStreetMap)"), agent panels, the `ask_human` card, overlays |
| `scripts/token_usage.py` | Tokens and dollars per pass from local Temporal history (no API calls) |

---

## What this would look like without Temporal

Without Temporal, the same orchestration would require:
- A state machine in a database (enum column per driver tracking route phase).
- Manual retry loops with custom backoff for every activity.
- A polling loop to implement "wait for human approval"
  (`while not db.get("approved"): sleep(1)`) — for *both* patterns.
- Defensive DB writes before every step so a crash doesn't lose position.
- Manual reconstruction of in-flight state on worker restart.
- A shared state store for cross-service coordination.
- Custom cancellation logic for mid-activity interruption.

Temporal collapses all of that into the workflow execution model. The event log
*is* the state persistence. `execute_activity` *is* the retry logic. Signals
*are* the message passing. `wait_condition` *is* the durable human pause. The
workflow code reads like a straightforward sequential program because Temporal
handles everything else — and the worker-kill demo proves it: kill the process
mid-approval, restart, and the pending state replays from history intact.
