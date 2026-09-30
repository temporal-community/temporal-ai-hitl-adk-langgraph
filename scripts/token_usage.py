"""Measure the Gemini tokens and dollars a fleet demo pass used, from local Temporal history.

Reads Event History with the Temporal CLI. Makes no model or API calls. Stdlib only.
Run it BEFORE stopping ./run.sh: the dev server keeps history in memory.

    python scripts/token_usage.py                  # every matching run on localhost:7233
    python scripts/token_usage.py --since 15:30    # runs started after 15:30 local time today
    python scripts/token_usage.py --run-id <meltdown-demo run ID> --json   # one pass + children

Where usage is recorded (activity results, base64 JSON in history):
  ADK        invoke_model -> LlmResponse.usageMetadata. Human -> Agent tab: inline in
             meltdown-demo. Cross-Framework tab: in the assess-* children.
  LangGraph  <graph>.<agent>_reason -> AIMessage.usage_metadata. Agent -> Human tab: inline in
             meltdown-demo. Cross-Framework tab: in the dispatch-* children.
Counted but not priced, because history keeps no tokens for them: retried model attempts (each
may have been billed; only the last attempt's result is kept), LangGraph venue searches (a
grounded Gemini call that returns text only), Maps requests and Google Search fees.
`calls` counts model responses with recorded usage only, so it leaves out the venue searches:
total Gemini calls = calls + venue searches (both are printed).
Orders = new_order signals (generated + injected).
"""

from __future__ import annotations

import argparse
import base64
import json
import re
import subprocess
import sys
from collections import Counter
from datetime import UTC, date, datetime
from pathlib import Path

# USD per 1M tokens: (input, cached input, output incl. thinking). Paid tier, standard.
# Source: https://ai.google.dev/gemini-api/docs/pricing, checked 2026-09-29.
PRICES = {
    "gemini-2.5-flash": (0.30, 0.03, 2.50),
    "gemini-2.5-flash-lite": (0.10, 0.01, 0.40),
    "gemini-3.5-flash-lite": (0.30, 0.03, 2.50),
    "gemini-3.8-flash": (0.75, 0.075, 3.75),  # through 2026-12-31; doubles on 2027-01-01
}
# List prices that change on a date: model -> (first day, new prices). The cached-input rate
# is assumed to double along with input and output.
PRICE_CHANGES = {"gemini-3.8-flash": (date(2027, 1, 1), (1.50, 0.15, 7.50))}
UNMETERED = {
    "tool_search_venue_events": "LangGraph venue searches (grounded Gemini, tokens not kept)",
    "tool_get_route_info": "Maps ETA requests",
    "get_route_polyline": "Maps route requests",
}
FIELDS = ("calls", "input", "cached", "output", "thinking")


def default_model() -> str:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from agent_fleet.config import DEFAULT_MODEL  # env or repo default; .env is not loaded

    return DEFAULT_MODEL


def money(x: float) -> str:
    return f"${x:.2f}" if round(x, 2) == x else f"${x}"


def price(model: str, args: argparse.Namespace) -> tuple[float, float, float]:
    if args.input_price is not None:
        cached = args.input_price if args.cached_price is None else args.cached_price
        return args.input_price, cached, args.output_price
    for name in sorted(PRICES, key=len, reverse=True):  # longest prefix, so -lite wins
        if model.startswith(name):
            first_day, later = PRICE_CHANGES.get(name, (date.max, None))
            return later if date.today() >= first_day else PRICES[name]
    sys.exit(f"No list price for {model!r}: pass --input-price and --output-price.")


def temporal(args: argparse.Namespace, *cmd: str):
    conn = ["--address", args.address, "--namespace", args.namespace, "-o", "json"]
    conn += ["--no-json-shorthand-payloads"]  # raw payloads: usage_records() decodes `data`
    out = subprocess.run(["temporal", "workflow", *cmd, *conn], capture_output=True, text=True)
    if out.returncode:
        sys.exit(f"temporal workflow {cmd[0]} failed: {out.stderr.strip()}")
    return json.loads(out.stdout)


def dicts_with(obj, key: str):
    """Yield every dict nested in obj that has `key` set."""
    if isinstance(obj, dict):
        if obj.get(key) is not None:
            yield obj
        obj = list(obj.values())
    for item in obj if isinstance(obj, list) else []:
        yield from dicts_with(item, key)


def snake(d: dict) -> dict:
    return {re.sub(r"(?<!^)(?=[A-Z])", "_", k).lower(): v for k, v in d.items()}


def usage_records(activity: str, result: dict | None):
    """Yield (framework, model, token Counter) for each model response in an activity result."""
    payloads = (result or {}).get("payloads", [])
    decoded = [json.loads(base64.b64decode(p["data"])) for p in payloads if p.get("data")]
    if activity == "invoke_model":  # ADK: a list of LlmResponse, camelCase in history
        found = [*dicts_with(decoded, "usageMetadata"), *dicts_with(decoded, "usage_metadata")]
        for r in (snake(x) for x in found if not x.get("partial")):
            u = Counter({k: v for k, v in snake(r["usage_metadata"]).items() if type(v) is int})
            yield (
                "ADK",
                r.get("model_version"),
                Counter(
                    input=u["prompt_token_count"] + u["tool_use_prompt_token_count"],  # grounding
                    cached=u["cached_content_token_count"],
                    output=u["candidates_token_count"],
                    thinking=u["thoughts_token_count"],
                    grounded=int(r.get("grounding_metadata") is not None),  # search fee unpriced
                ),
            )
    elif activity.endswith("_reason"):  # LangGraph: the node's new AIMessage comes last
        if msgs := [m for m in dicts_with(decoded, "usage_metadata") if m.get("type") == "ai"]:
            u, meta = msgs[-1]["usage_metadata"], msgs[-1].get("response_metadata") or {}
            think = (u.get("output_token_details") or {}).get("reasoning") or 0
            yield (
                "LangGraph",
                meta.get("model_name"),
                Counter(
                    input=u.get("input_tokens") or 0,
                    cached=(u.get("input_token_details") or {}).get("cache_read") or 0,
                    output=(u.get("output_tokens") or 0) - think,  # LangChain folds thinking in
                    thinking=think,
                ),
            )


def measure(history: dict, args: argparse.Namespace) -> dict:
    n, frameworks, models, kind = Counter(), set(), set(), {}
    for e in history["events"]:
        if a := e.get("activityTaskScheduledEventAttributes"):
            kind[e["eventId"]] = name = a["activityType"]["name"]
            if name in UNMETERED:
                n[name] += 1
        elif a := e.get("activityTaskStartedEventAttributes"):
            t = kind.get(a["scheduledEventId"], "")
            if t == "invoke_model" or t.endswith("_reason"):  # one event, final attempt number
                n["retries"] += (a.get("attempt") or 1) - 1
        elif a := e.get("activityTaskCompletedEventAttributes"):
            t = kind.get(a["scheduledEventId"], "")
            for fw, model, tok in usage_records(t, a.get("result")):
                frameworks.add(fw)
                models.add(model := model or args.model)
                p_in, p_cached, p_out = price(model, args)
                n.update(tok, calls=1)
                fresh, out = tok["input"] - tok["cached"], tok["output"] + tok["thinking"]
                n["usd"] += (fresh * p_in + tok["cached"] * p_cached + out * p_out) / 1e6
        elif e.get("workflowExecutionSignaledEventAttributes", {}).get("signalName") == "new_order":
            n["orders"] += 1
    return {"frameworks": sorted(frameworks), "models": sorted(models), "usage": n}


def row(name: str, run: str, label: str, u: dict | None = None, digits: int = 0) -> str:
    cells = [*FIELDS, "$"]
    if u is not None:
        cells = [f"{u.get(k, 0):,.{digits}f}" for k in FIELDS] + [f"{u.get('usd', 0):.4f}"]
    return f"{name:<24}{run:<10}{label:<15}" + "".join(f"{c:>10}" for c in cells)


def main() -> None:
    fmt = argparse.RawDescriptionHelpFormatter
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=fmt)
    ap.add_argument("--address", default="localhost:7233")
    ap.add_argument("--namespace", default="default")
    # route-driver-* runs hold no model calls, only the Maps route requests (get_route_polyline).
    match = r"^(meltdown-demo|assess-|dispatch-|route-driver-)"
    ap.add_argument("--match", default=match, help="ID regex")
    ap.add_argument("--since", help="ISO start time, or HH:MM today (no zone = local time)")
    ap.add_argument("--run-id", action="append", default=[], help="a run and its children")
    ap.add_argument("--model", default=default_model(), help="if history has none: %(default)s")
    ap.add_argument("--input-price", type=float, help="USD per 1M input tokens")
    ap.add_argument("--cached-price", type=float, help="USD per 1M cached (default: input)")
    ap.add_argument("--output-price", type=float, help="USD per 1M output tokens incl. thinking")
    ap.add_argument("--json", action="store_true", help="print JSON instead of a table")
    ap.epilog = "Default: list price for the recorded model, per 1M tokens (checked 2026-09-29)\n"
    ap.epilog += "\n".join(f"  {m}: {' / '.join(map(money, p))}" for m, p in PRICES.items())
    ap.epilog += "\n  (input / cached input / output incl. thinking; 3.8 Flash doubles 2027-01-01)"
    args = ap.parse_args()
    if (args.input_price is None) != (args.output_price is None):
        ap.error("pass --input-price and --output-price together")
    since = args.since
    if since and re.fullmatch(r"\d{1,2}:\d{2}(:\d{2})?", since):  # HH:MM means today
        since = f"{date.today()}T{since}"
    since = since and datetime.fromisoformat(since).astimezone(UTC)

    rows = []
    for w in temporal(args, "list"):
        wid, run = w["execution"]["workflowId"], w["execution"]["runId"]
        root = (w.get("rootExecution") or {}).get("runId")
        if not re.search(args.match, wid) or (args.run_id and not {run, root} & set(args.run_id)):
            continue
        if since and datetime.fromisoformat(w["startTime"]) < since:
            continue
        info = {"workflow": wid, "run_id": run, "started": w["startTime"]}
        rows.append(info | measure(temporal(args, "show", "-w", wid, "-r", run), args))
    if not rows:
        sys.exit("No matching workflow runs. Check --match, --since and --run-id.")
    total = sum((r["usage"] for r in rows), Counter())
    orders = total["orders"]
    per_order = {k: total[k] / orders for k in (*FIELDS, "usd")} if orders else None
    models = sorted({m for r in rows for m in r["models"]})
    prices = {m: dict(zip(("input", "cached", "output"), price(m, args))) for m in models}

    if args.json:
        report = {"measured": str(date.today()), "prices_per_1m": prices, "workflows": rows}
        print(json.dumps(report | {"total": total, "per_order": per_order}, indent=2))
        return
    print(f"MEASURED {date.today()} from Temporal history at {args.address}: tokens and dollars")
    print(row("workflow", "run", "framework"))
    for r in rows:
        print(row(r["workflow"], r["run_id"][:8], "+".join(r["frameworks"]) or "-", r["usage"]))
    print(row("TOTAL", "", f"{orders} orders", total))
    if per_order:
        print(row("PER ORDER", "", "", per_order, digits=1))
    for m, p in prices.items():
        print(f"\n{m}: {' / '.join(map(money, p.values()))} per 1M input / cached / output")
    print("Input includes ADK grounding tool-use prompt tokens. Cached is part of input.")
    extra = [f"{total[k]} {label}" for k, label in UNMETERED.items() if total[k]]
    extra += [f"{total['grounded']} ADK grounded prompts (tokens priced, search fee not)"]
    print(f"Counted, not priced: {total['retries']} retried model attempts", *extra, sep=", ")


if __name__ == "__main__":
    main()
