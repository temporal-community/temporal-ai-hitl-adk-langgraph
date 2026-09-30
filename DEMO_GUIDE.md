# Meltdown Demo Delivery Guide

Talk track and presenter notes for the AI Engineer World's Fair session
**"The Human Is an Async API: Designing Durable Human-in-the-Loop Agents"**
(Moscone West, San Francisco). The demo shows **two** human-in-the-loop patterns
on Temporal durable execution, visualized as Ziggy's Ice Cream catering fleet in
downtown San Francisco.

---

## Before You Start
(See [Quickstart](README.md#quickstart) for full setup instructions.)

**Requirements:**
- `.env` with two API keys, one per API, each restricted to its own API:
  - `GOOGLE_API_KEY` — Gemini key, restricted to the Generative Language API, on a **paid-tier** (billing-enabled) project. Required; the worker is live-only. The default model is `gemini-3.8-flash`, which has no Grounding with Google Search on the free tier. Override with `DEFAULT_MODEL` in `.env` (e.g. `gemini-2.5-flash`, the old default, if your key already has access to it). Rehearse once on the model you'll present with.
  - `GOOGLE_MAPS_API_KEY` — Maps key, restricted to the Directions API. Required: every truck leg calls it.
- `./run.sh` (or `make run`) started — this starts the Temporal dev server, worker process, and server process automatically.
  - **Note:** the worker does **not** load `.env` itself. If you ever start it by hand, use `make worker` (`uv run --env-file .env python -m agent_fleet.worker`). The worker refuses to start unless both keys are set (no mock fallback).
- **Recording hygiene:** record from a fresh terminal after restarting `./run.sh`. The worker no longer logs Maps request URLs (the key is in the query string), but older scrollback may still contain them; rotate the Maps key if any was shared or recorded.
- **Approval window:** `GATE_ESCALATION_SECONDS` (default 30) sets how long an unanswered `ask_human` waits before the "Escalated to backup approver" label. Set `GATE_ESCALATION_SECONDS=300` in `.env` for a clean card during the kill beat, or keep 30 and narrate the durable timer firing during the outage.
- Browser open at http://localhost:8080 for the web app
- Temporal UI open at http://localhost:8233 (optional but great for showing workflow history and the worker-kill recovery)

## How it works
See [How It Works](HOW_IT_WORKS.md) for more detailed "under the hood" information.

## Pre-flight check
- Map shows **downtown San Francisco** with three delivery venues — **Moscone Center**, **Fisherman's Wharf**, **Chinatown** — and Ziggy's Ice Cream at the **Ferry Building**
- All 4 drivers (A–D) are parked at Ziggy's, status idle (capacity 2 each — a deliberately tight fleet so capacity pressure shows; driver-d stays hidden during the warm-up burst). The Fleet agent's tool reports capacity as `x/2`
- The map loads the "Dark (OpenStreetMap)" style, with OpenStreetMap credit in the bottom-left corner. Other styles are in **Map Style**; the choice resets on every reload
- **Two HITL patterns, three use cases (one per tab):** **🧑 Human → Agent** (use case 1 — Google ADK only, Pattern A), **🤖 Agent → Human** (use case 2 — LangGraph only, Pattern B), and **🔀 Cross-Framework · ADK + LangGraph** (use case 3 — both patterns, both frameworks). The third isn't a third pattern — it combines both.
- "Start Deliveries" button is active on all tabs
- If you see a stale state from a prior run, click **Reset** first

**Tip:** Do a dry run of each pattern before presenting to get familiar with the agent reasoning panel timing and the approval-card flow.

---

## The Thesis (say this up front)

> "We keep designing human-in-the-loop as a special case — a pause, a webhook, a polling loop someone has to babysit. But there are really only two shapes. Sometimes a **human calls into the system** and changes work already in flight. Sometimes the **agent calls the human** because it hits a decision it should not make alone. Both use the same durable wait-and-signal primitives. The key agent insight is in the second pattern: human judgment is a model-visible `ask_human` tool. LangGraph interrupts the loop, Temporal preserves the wait, and the human's answer arrives by signal as the agent's next observation. Let me show you both on an ice cream fleet here in downtown San Francisco."

---

## Two frameworks, one durable-execution runtime

The demo deliberately uses **two different agent frameworks** on the **same** durable-execution runtime (Temporal), to make the point that the durable-HITL pattern is framework-agnostic. **Frameworks own the loop — ADK, LangGraph. Their loop code runs inside Temporal workflows; Temporal is the durable-execution runtime beneath them, and in this demo it is what makes the handoff between the two frameworks durable.** The framework decides what the agent does next; because its code runs inside a workflow, each model call and tool call is a recorded step.

- **Pattern A (Human-in-the-loop)** is built on **Google ADK** — a multi-agent assignment pipeline (Fleet + Customer Agents in parallel → Dispatch Agent). It starts outside the agent: a customer change enters the workflow by signal and makes the driver hold at the venue. On an approved address change, the new location becomes fresh input to the ADK team, which **re-reasons** before the held driver reroutes.
- **Pattern B (Agent-in-the-loop)** is built on **LangGraph** via Temporal's `temporalio.contrib.langgraph` integration — a looping multi-agent team (Fleet + Customer → Dispatch) where, mid-reasoning, the Dispatch agent calls the model-visible `ask_human` tool.
- **The active tab picks the framework for *all* orders** — the dashboard signals `set_dispatch_mode` (`adk`, `langgraph`, or `crossframework`). There's no value threshold steering orders between them; the active mode handles every order.

### What is Google ADK? (30 seconds)

> "Google ADK is an open-source framework for composing multi-agent systems. You wire agents — each with their own tools and model — into pipelines that run sequentially or in parallel. In this demo a Fleet Agent assesses driver positions and capacity, a Customer Agent evaluates order priority and venue context, and a Dispatch Agent synthesizes both into an assignment. Each Gemini call and each tool call becomes its own Temporal activity — individually durable and replayable."

### What is the LangGraph integration? (30 seconds)

> "Pattern B uses LangGraph — a graph of nodes — running *inside* the parent Temporal workflow via `temporalio.contrib.langgraph`. It's a looping multi-agent team that mirrors the ADK side: Fleet and Customer assess in parallel, then Dispatch decides, with each Gemini reason call recorded as a Temporal activity in the parent's history. Here's the headline: human judgment is **inside the reasoning loop as a tool**. Mid-reasoning, Dispatch calls `ask_human`; LangGraph's `interrupt()` suspends the graph, the parent waits durably in Temporal on `wait_condition`, and the human's answer arrives through the `answer_dispatch` signal. `Command(resume=answer)` returns it as the agent's next observation. There's no per-order gate child. Pattern A and B share the same Temporal wait/signal primitives, but only Pattern B exposes the human interaction as a model-visible tool."

**"How does this demo use LangGraph?"**
> "We use LangGraph as a *framework* here — its graph/loop abstraction for the Dispatch agent's reasoning loop — and let **Temporal** provide durability and persistence, so the LangGraph checkpointer here is just `InMemorySaver`."

---

## Architecture Talking Points

Optional drop-ins for mid-demo — when the conversation turns to scale or to what "production Temporal" actually looks like. Open the Temporal UI alongside the dashboard.

- **"Open the event history."** Open `meltdown-demo` to show the inline LangGraph team's Gemini and ordinary tool-call activities in the parent's history; open a `route-driver-*` child to show navigation and delivery activities. Each activity is an independent retry unit. Crash the worker mid-Dispatch-Agent and the Fleet Agent's completed assessment replays from history instead of being called again.
- **"Where are driver positions in the event log?"** They're not. `navigate_to` heartbeats position to shared state (SQLite here, Redis or Postgres in prod) every ~400ms. None of those writes hit Temporal. The pattern: signals for milestones (delivery complete, new order, human approval), shared state for continuous telemetry.
- **"Where does the agent's question to the human live?"** There's **no per-order gate child** — open the `meltdown-demo` parent workflow while the approval card is up. The looping LangGraph team ran inline in the parent; mid-reasoning the Dispatch agent called `ask_human`, and LangGraph's `interrupt()` suspended the graph. The parent surfaces that question into its `pending_dispatch` dict and waits durably in Temporal on the `answer_dispatch` signal + `wait_condition`. The brief the human sees is surfaced via `/api/pending-dispatch`, which reads the parent's `pending_dispatch` dict (via the `get_status` query) — not from any database the UI polls blindly.

Full breakdown lives in [HOW_IT_WORKS.md](HOW_IT_WORKS.md).

---

## Opening: Ziggy's Opens for Business
**Time: 1–2 min | Run this first on either tab**

**Setup:** Click **Start Deliveries**. Ziggy's kitchen starts taking orders. Venues around downtown place orders every few seconds — Moscone Center, Fisherman's Wharf, Chinatown.

**What happens automatically:**
1. Each order triggers multi-agent reasoning — watch the ADK Agent Team panel.
2. Fleet Agent calls `tool_get_fleet_status` for driver positions and capacity, then `tool_get_route_info` for the closest drivers to get driving ETAs from Google Maps. Each ETA call is a separate Temporal activity.
3. Customer Agent calls `tool_get_order_priorities` and a venue-events web search (ADK: `google_search` Gemini grounding; LangGraph: `tool_search_venue_events`, the same grounding behind a tool) — evaluates VIP tier, deadline pressure, venue events, and guest count.
4. Dispatch Agent synthesizes both assessments and calls `tool_submit_assignment` — **proposes the driver** (from the eligible, under-capacity set) and explains why.
5. The parent applies a **capacity guardrail** over the agent's pick (least-loaded eligible driver as the fallback if the pick is full/disconnected). On the Human → Agent (ADK) tab it also swaps a valid pick for the least-loaded eligible driver when that driver has fewer orders, so don't narrate "the agent chose this truck" as final there. With only 4 drivers at capacity 2, slots are genuinely scarce.
6. Drivers batch-pickup at Ziggy's (up to capacity, 2 orders per trip) and deliver sequentially to the venues.

**What to say:**
> "This is Ziggy's delivery system running live. Orders keep flooding in from downtown, and three AI agents reason about every single one. Fleet Agent checks who's closest — those are real Google Maps calls, each its own Temporal activity. Customer Agent evaluates priority. Dispatch Agent weighs both and assigns. Everything you see in the Temporal UI is individually durable and replayable."

**Temporal concept to highlight:** Child workflow isolation, continuous workflows with signals, per-call visibility in the event log.

---

## Pattern A — Human-in-the-Loop: "The Human Calls the Agent"
**Time: 2–3 min | Tab: 🧑 Human → Agent | Best for: signals, `wait_condition`, cross-workflow coordination**

This is **customer-initiated**: the change is submitted externally, and a human supervisor approves it. The gate lives in the workflow, not in any agent tool — but it's **one human gate that feeds both loops**: the driver holds, you approve, and on an address change the ADK agents **re-reason** the new location before the driver reroutes. Contrast that with Pattern B, where the *agent* initiates the escalation.

**Setup:** On the **🧑 Human → Agent** tab, click **Start Deliveries** and wait for a driver to be en route to a venue.

**Steps:**
1. In the order dropdown, pick an active order being delivered.
2. Select **Address Change** (then pick a new SF location from the dropdown) or **Cancel Order**, and click **Submit Change**.
3. Watch the driver: it **arrives at the venue but holds before delivering**. The dashboard doesn't show a status for this; to prove it, run `temporal workflow query --workflow-id route-driver-X --type get_status` and point at `"status": "awaiting_update"` and `pending_hold_order_ids`, or show the gap in the `route-driver-X` history. The parent workflow is waiting for your approval; the child workflow is waiting for the parent's decision. Two `wait_condition` pauses, both durable.
4. Meanwhile, everything else keeps running — other orders still come in, other drivers still deliver.
5. Click **Approve** (or **Reject**):
   - **Cancel:** the driver skips delivery entirely and moves to its next order (or returns to Ziggy's). (Fixed cancel — no re-reason.)
   - **Address change:** the **ADK assignment team re-reasons** the order for the new location — Fleet recomputes ETAs, Customer re-reads priority, Dispatch reassesses (watch the agent panels update) — then the held driver reroutes from the venue to the chosen location; a new marker appears on the map, the order card updates.
   - **Reject:** the driver delivers normally to the original venue.

**What to say:**
> "A customer just changed this order, and look — the driver arrived but it's holding. It won't deliver until we decide. That's two `wait_condition` pauses working together: the parent waits for the human, the child waits for the parent. Now watch — I approve the new address, and the agents don't apply a script: they *re-reason* it. Fleet re-checks ETAs to the new spot, Customer re-weighs priority, Dispatch re-decides — and *then* the held driver reroutes. One approval feeds both loops: the agents re-reason, and the driver reroutes. Meanwhile the rest of the fleet keeps running, unaffected. Temporal held both workflows in that waiting state, fully durable. No polling, no timeout hacks."

**What you'll see in the Temporal UI:**
- `meltdown-demo`: `WorkflowExecutionSignaled` (`customer_change`) → `update_pending` to child → `WorkflowExecutionSignaled` (`change_approved`) → `execute_customer_change` activity → (address change) `_rereason_order` re-runs the ADK team → `resolve_update` to child
- `route-driver-X`: `WorkflowExecutionSignaled` (`update_pending`) → driver holds `awaiting_update` → `WorkflowExecutionSignaled` (`resolve_update`) → cancel skips `deliver_order` / reroute triggers a new `navigate_to`

**Temporal concept to highlight:** One human gate feeding both loops (agent re-reason + driver reroute), dual `wait_condition` (parent + child), cross-workflow signals, durable pause without polling.

The reroute choices come from a curated `REROUTE_OPTIONS` list (Oracle Park + Salesforce Tower, Union Square, Coit Tower, Palace of Fine Arts), served via `/api/locations` and shown in the **Address Change** dropdown.

---

## Pattern B — Agent-in-the-Loop: "The Agent Calls the Human"
**Time: 3–4 min | Tab: 🤖 Agent → Human | Best for: the headline — human judgment as an async agent tool whose wait survives worker death**

On the **🤖 Agent → Human** tab, **every order** runs a **looping multi-agent LangGraph team** inline in the parent workflow — Fleet and Customer nodes assess in parallel, then a Dispatch node decides — each Gemini reason call runs as a Temporal activity recorded in the parent's own history. The HITL is **inside the reasoning loop**: **mid-reasoning**, the Dispatch agent **decides for itself** to escalate by calling the model-visible `ask_human` tool. LangGraph's `interrupt()` suspends the graph. The parent workflow (`_run_langgraph_assignment`) surfaces the question, waits durably in Temporal on the `answer_dispatch` signal + `wait_condition`, and resumes the agent with `Command(resume=answer)` — the human's answer flows back as the agent's *next observation*. There is **no per-order gate child**. If the agent doesn't escalate, the order commits directly. The prompt steers the agent to escalate only exceptional orders (roughly $3,000+), and auto-generated orders top out around ~$1,950 (servings ≤150 × ≤$13), so routine orders usually auto-dispatch — the approval card normally appears only when you drop the premium order. It's the model's call, so rehearse it.

**Setup:** On the **🤖 Agent → Human** tab, click **Start Deliveries** so the fleet is moving.

**Steps:**
1. Click **Drop high-value order**. This injects a premium **Moscone Center** catering order ($5,400, well above the routine ~$1,950 cap) via `POST /api/inject-order`. Drop it when at least one truck has a free slot, so an approval can actually dispatch.
2. The looping LangGraph multi-agent team (Fleet + Customer → Dispatch) — running **inline in the parent workflow** — assesses the value and fleet impact; **mid-reasoning** an agent, usually Dispatch, **calls the `ask_human` tool**. LangGraph interrupts the graph, and the parent waits durably in Temporal for the `answer_dispatch` signal — no child workflow spawned. The Fleet agent sometimes asks first, so be ready for a second card.
3. An **approval card appears over the map** — "Agent called `ask_human`" — with the agent's question, order value, a line like `order_id order-special-1 · workflow meltdown-demo`, and the reminder that your answer returns as its next observation. The ids are selectable, for the CLI fallback below. The brief is surfaced via `GET /api/pending-dispatch`, which reads the parent workflow's `pending_dispatch` dict (populated when the agent's `ask_human` interrupt fires) through a Query.
4. **The durability moment — kill the worker now.** While the card is up, from a **second terminal** run **`make kill-worker`** (alias `make failure`; leave `make run` going in the first — that keeps Temporal + the web server alive). It sends SIGKILL, a real crash: the header badge flips to **Service Offline** within about 9 s (6 s heartbeat staleness + 3 s poll). The fleet freezes — but the *pending question is in Temporal, not in the worker's memory.* Show `meltdown-demo` still **Running** in the Temporal UI. **The card can disappear now:** it's read through a Query, and Queries need a live worker. That's expected; the Temporal UI is your proof. (Don't Ctrl-C `make run`; that tears down Temporal too and wipes the in-memory dev-server state. `make stop-worker` is the graceful SIGTERM alternative; its badge flips within about 3 s.)
5. **For the strongest beat, answer while the worker is down** — from the CLI, because the card may be gone. Copy the order_id from the card before the kill (the counter doesn't reset on Reset, so it may be `order-special-2` or higher):
   ```bash
   temporal workflow signal --workflow-id meltdown-demo --name answer_dispatch \
     --input '"order-special-1"' --input '"approve"'
   ```
   Temporal records the signal with no worker present.
6. **Restart the worker** with **`make worker`** (alias `make recovery`). It replays from Temporal's history — the fleet resumes, and either your recorded answer is applied or the card comes back, waiting. If more than `GATE_ESCALATION_SECONDS` passed, the card now reads "Escalated to backup approver (primary window timed out)": the durable timer fired while no worker was running, and the wait continued. (Optionally show the parked `meltdown-demo` parent workflow in the Temporal UI before and after — same `wait_condition` on `answer_dispatch`, resumed from history. No `gate-*` child to look for.) Calls that finished before the kill are not re-run; a call that was in flight runs again.
7. If you didn't answer from the CLI, click **Approve dispatch** or **Reject** (`POST /api/approve-dispatch` signals `MeltdownDemoWorkflow.answer_dispatch`):
   - **Approve:** the answer flows back as the agent's next observation; the agent **reasons over that approval plus the Fleet/Customer assessments and picks the driver** (`submit_dispatch`) — it's not a rubber stamp. Usually the fleet then delivers it. The agent can still hold (for example, no free slot); the Dispatch panel then shows the order as "held", the same as after a reject.
   - **Reject:** the answer flows back as a reject; the order is held — fleet capacity is preserved, the order shows as cancelled, and the Dispatch panel shows it as "held".

**What to say:**
> "Routine orders, the agents just dispatch. But this one's a big-ticket Moscone catering order, and committing scarce capacity deserves human judgment. So the agent does what agents do mid-reasoning: it calls a tool, `ask_human`. LangGraph interrupts the loop. Temporal parks the parent on a durable `wait_condition` until the human's answer arrives by signal. Watch: I kill the worker. The tool call is still outstanding, but the wait lives in Temporal's event history, not in the process that just died. I can even answer now, with no worker running. I restart the worker, and the question, or my answer, is right where Temporal recorded it. Human judgment is an async tool the agent can call. Now I approve; that answer returns as the agent's next observation, and it reasons before committing the fleet."

**Temporal concept to highlight:** Agent-initiated escalation **inside the reasoning loop**: `ask_human` → LangGraph `interrupt()` → Temporal `wait_condition` → human answer via `answer_dispatch` signal → `Command(resume=answer)` → next agent observation. Query-backed brief, **no per-order child**, **survives worker death**.

**Why `interrupt()` and not just the signal?** Because this HITL lives *inside* the loop, the human's answer has to flow **back into the running graph** as the agent's next observation — and `interrupt()` is the only LangGraph primitive that can suspend and resume a graph **mid-node** and inject that answer via `Command(resume=answer)`. There's **no "signal-only, no interrupt" option** for the in-loop pattern: the `answer_dispatch` signal + `wait_condition` is the durable *wait*, but `interrupt()` is the graph plumbing that lets the answer rejoin the loop.

---

## Cross-Framework — Temporal WITH ADK and LangGraph: "One Runtime Across Two Frameworks"
**Time: 3–4 min | Tab: 🔀 Cross-Framework · ADK + LangGraph | Best for: the cross-framework point — a durable handoff from one agent framework to another**

This tab runs both frameworks **on the same delivery**, one after the other. Fleet and Customer assessment runs on **Google ADK**; Dispatch runs on **LangGraph** — each its own Temporal **child workflow**, run in sequence by the Temporal parent, which applies the result. The header shows **both the Google ADK and LangGraph logos** to make the point visible. ADK orchestrates ADK agents and LangGraph orchestrates LangGraph nodes; in this demo **Temporal makes the handoff between them durable**. Plain code or A2A could connect them; Temporal adds a handoff that survives crashes and retries and shows up in history. Both HITL directions appear here — the agent-initiated `ask_human` tool and interrupt (LangGraph), plus the externally initiated address-change flow that re-runs ADK and LangGraph after approval — across the cross-framework boundary.

**Setup:** On the **🔀 Cross-Framework** tab, click **Start Deliveries** so the fleet is moving.

**Steps:**
1. Click the **🔀 Cross-Framework** tab, then **Start Deliveries**. Orders flow as before (slower: about one every 22 s), but each one hands off from an ADK child workflow (assessment) to a LangGraph child workflow (dispatch).
2. **(Agent → human direction.)** Click **Drop high-value order**. The **LangGraph Dispatch agent usually calls `ask_human` mid-reasoning** and an **approval card appears over the map**, showing `workflow dispatch-order-special-N`. The answer signals the **dispatch agent's OWN child workflow**: **Approve** → usually dispatched (the agent can still hold, which looks the same as a reject on screen); **Reject** → held (not dispatched). CLI fallback, which takes only the decision: `temporal workflow signal --workflow-id dispatch-order-special-N --name answer_dispatch --input '"approve"'`. For the kill beat on this tab, kill the worker while `dispatch-order-special-N` is parked on `ask_human` — the gap between the two children is only a few seconds, too short to hit live.
3. **(Human → agent direction.)** Use the customer-change controls: pick a **routine** order, select **Address Change** → a new location, click **Submit Change**, then **Approve**. The driver holds, the **cross-framework team re-reasons** — **ADK reassesses, LangGraph re-decides** (`assess-order-N-rev1`, `dispatch-order-N-rev1`) — and the driver reroutes. Avoid doing this on the high-value order: the `-rev1` Dispatch agent sees its real value, may call `ask_human` again, and the truck waits for that second answer.
4. Click **View the cross-framework graph** to show the combined diagram: Temporal parent → ADK child [Fleet ∥ Customer] + LangGraph child [Dispatch + `ask_human`] → driver loop.
5. Open the **Temporal UI** (localhost:8233). Per cross-framework order there are **separate child workflow histories** — `assess-<order>` (ADK) and `dispatch-<order>` (LangGraph) — under `meltdown-demo`. That split is the **visible cross-framework boundary**.

**What to say:**
> "So far each tab used one framework. This one uses both — on the same order. Fleet and Customer assess on Google ADK; Dispatch decides on LangGraph; each is its own Temporal child workflow, and the Temporal parent hands the result from one to the next. Here's the point: you could wire two frameworks together with plain code, but then the handoff dies with the process. Here the handoff is durable — it survives a crash and it's in the history. Watch both patterns we just saw, now spanning the boundary. The LangGraph dispatch agent calls `ask_human` and escalates — I approve, and it signals dispatch's own child workflow. And when a customer changes an address, the whole cross-framework team re-reasons — ADK reassesses the new location, LangGraph re-decides — then the driver reroutes. Temporal WITH ADK and LangGraph: one durable-execution runtime, two frameworks, joined durably."

**What you'll see in the Temporal UI:**
- Under `meltdown-demo`, per cross-framework order: a child workflow `assess-<order>` (the ADK Fleet + Customer assessment) and a child workflow `dispatch-<order>` (the LangGraph Dispatch decision, including its in-loop `ask_human` interrupt). The two separate histories are the cross-framework boundary made visible.

**Temporal concept to highlight:** A durable cross-framework handoff via separate Temporal child workflows (`assess-<order>` ADK, then `dispatch-<order>` LangGraph) run in sequence by the parent; both HITL directions (agent→human `ask_human`, human→agent re-reason) spanning the cross-framework boundary.

**Operational note:** After any code change, **terminate the `meltdown-demo` workflow (Reset) and restart the worker** — otherwise stale child histories fail to replay.

### Cross-Framework code tour (for "show me the code")

In the order the flow happens (search for these symbol names; line numbers drift):

1. **Where the graph is defined** — `build_dispatch_only_graph()` in `agent_fleet/langgraph_agents.py`: `START → dispatch_reason → {dispatch_human → dispatch_reason | END}`. Name: `DISPATCH_ONLY_GRAPH_NAME = "dispatch_only"`.
2. **The prompt that makes the agent escalate** — `ESCALATION_GUIDANCE` in `langgraph_agents.py`; the "call ask_human with a clear, specific question" instruction is in its middle paragraph.
3. **Where the agent asks the human** — the tool `_ask_human_tool()` / `ask_human(question)` (body is `raise NotImplementedError`), and `interrupt(...)` inside `_human_node` — that payload is the question. LangGraph suspends here; the Temporal wait in step 5 supplies durability.
4. **The query that loads the human's question box** — `LgDispatchWorkflow.pending_question` query in `agent_fleet/workflows.py`; the interrupt payload is captured into `self._pending_question` in `LgDispatchWorkflow.run`. (`/api/pending-dispatch` reads this child query to render the card.)
5. **The durable wait** — `await workflow.wait_condition(..., timeout=timedelta(seconds=GATE_ESCALATION_SECONDS))` in `LgDispatchWorkflow.run`, then an untimed wait after the escalation label is set.
6. **The signal that resumes (cross-framework)** — `LgDispatchWorkflow.answer_dispatch`; the human signals the dispatch **child** directly (no order_id — the child *is* the order), which unblocks the wait and the graph resumes via `Command(resume=answer)`.

**How the pause/resume works.** `interrupt(payload)` checkpoints the graph and returns from `ainvoke()` with the payload in `result["__interrupt__"]` — the graph is suspended at that node. Calling the graph again with `Command(resume=answer)` restores it and the `interrupt()` call *returns* `answer`, so the node finishes and the edge `dispatch_human → dispatch_reason` makes the Dispatch agent **re-reason over the human's answer — on approve *and* reject**. A reject still re-reasons, but the workflow's `rejected` flag forces a final HOLD regardless of what the agent concludes. After an approve, the final decision is HOLD only if the agent's `submit_dispatch` says hold or its plain-text reply leads with "HOLD"/"held". LangGraph gives the pause/resume; **Temporal makes the gap durable** — the checkpointer is `InMemorySaver` (scratch); the signal + `wait_condition` + re-invoke live in Temporal's event history, so a worker kill mid-pause loses nothing.

---

## Handling Questions

**"How is this different from just using a queue?"**
> "A queue gives you one retry per message. Temporal gives you a full execution model — retries, timeouts, backoff, heartbeating, child workflows, signals, queries — all in code, not config. The human pause in both patterns is just a `wait_condition` on a signal, and the durable-execution runtime holds it across crashes. In this demo the agent's wait also has a 30-second durable timer that escalates to a backup-approver label, then keeps waiting."

**"Why two frameworks?"**
> "To show the durable human-interaction primitives aren't tied to one framework. Pattern A starts outside the agent: human input enters the workflow by signal and, for an approved address change, becomes fresh context for ADK re-reasoning. Pattern B starts inside the LangGraph loop: the agent calls the model-visible `ask_human` tool and receives the answer as its next observation. Same Temporal wait/signal primitives; different initiator and continuation."

**"Aren't these agent loops pretty shallow?"**
> "Yes — deliberately. They're real reason→act→observe loops, and every reason call and ordinary tool call is its own Temporal activity; `ask_human` is the workflow-interrupt exception. The loops are only a few hops deep because picking a driver is a bounded task. Loop depth is orthogonal to the point: a shallow loop still fires `ask_human` mid-reasoning and proves the pause is durable, and a 10×-deeper loop would run the exact same contract with more steps to replay. I kept them shallow so the demo stays legible; depth is a knob, not a missing piece."

**"Why does the LangGraph code look so much heavier than the ADK code?"**
> "Because in LangGraph **you own the loop**. `langgraph_agents.py` hand-builds it from primitives — the reason↔act loop and routing, per-tool-call activities (`_run_tools`), message parsing (`_coerce_text` / `_last_text`), the `interrupt()` human node (`_human_node`), and model + tool binding (`_chat_model`). **ADK doesn't need any of that**: its `Runner` runs the loop. (In both cases the framework decides the loop, and its code runs inside a Temporal workflow.) `TemporalModel` + `activity_tool` make each model call and each tool call a durable Temporal activity, and structured output comes back through session state. So it's the same durable-execution runtime underneath — LangGraph just exposes more of the plumbing. **LangGraph = assemble the loop from primitives; ADK = batteries-included.**"

**"What if Gemini returns something unexpected?"**
> "The ADK agents submit output via structured tool calls — `tool_submit_assignment` writes a typed object the workflow reads. The LangGraph agent's escalation is a tool call too (`ask_human`). If a step produces garbage or fails, it's a Temporal activity, so it retries with backoff. There's a clear contract."

**"What happens if nobody answers the agent?"**
> "Nothing is lost. The agent's `ask_human` is parked on a durable `wait_condition` in the parent workflow, surviving worker restarts. After 30 seconds (`GATE_ESCALATION_SECONDS`) a durable timer fires and the card is labeled 'Escalated to backup approver'; the wait then continues with no timeout. In this demo that's only a label, nobody else is paged; in production you'd notify someone and set the window to minutes or hours. If the run ends first (50 orders), the open question exits without a decision. Meanwhile the rest of the fleet keeps delivering, because the agent's reasoning task runs concurrently — it doesn't block the parent."

**"What does a run cost?"**
> "Measured on September 30, 2026, one full pass per tab on the default `gemini-3.8-flash` with default thinking, at list price: Human → Agent was 459 Gemini calls, 1.2 million tokens, $1.44; Agent → Human 322 calls plus 51 venue searches whose tokens aren't recorded, 730K tokens, $1.49; Cross-Framework 400 calls, 840K tokens, $1.55. That's about 3 cents and 14K to 25K tokens per order. Search fees past the free allowance and Maps are extra. Waiting on a human costs zero tokens: a parked `ask_human` makes no model calls, and the Temporal dev server is local." (Numbers and how to measure your own run: README, *Cost to run*.)

**"Is this production-ready?"**
> "The pattern is; this demo isn't. The integrations shown — `temporalio[google-adk]` and `temporalio.contrib.langgraph` — are Temporal's own, and both are marked experimental in the SDK. The demo's approve endpoints have no authentication, approvals are signals rather than validated updates, and escalation is a prompt, not a rule in code. The README's 'What this is not' section lists the gaps."

---

## Reset Between Demos

1. Click **Reset** on the dashboard, or run `make reset` from a second terminal (`curl -fsS -X POST http://localhost:8080/api/reset`; the app must be running). Never press **Start Deliveries** without it: starting while `meltdown-demo` is open returns HTTP 500.
2. Wait about 15 seconds. Verify all delivery actors return to idle at Ziggy's Ice Cream (Ferry Building). The badge reads **Service Online** right after Reset even with no worker; trust it after the next 3 s poll.
3. If any workflows are stuck, run `temporal workflow list` and cancel manually (`meltdown-demo`, `order-generation`, `route-driver-*`).
4. Refresh the browser before the next run.
5. For a clean take (fresh history, injected-order counter back to `order-special-1`): Ctrl-C `./run.sh`, run `make stop-worker` if you started a worker by hand, then `./run.sh` again.
