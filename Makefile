.PHONY: install setup lint fmt test run reset worker kill-worker stop-worker failure recovery

install:
	uv sync --all-extras

# The locked install run.sh does: installs exactly what uv.lock pins, without updating it.
setup:
	uv sync --all-extras --frozen

lint:
	uv run ruff check . && uv run ruff format --check .

fmt:
	uv run ruff check --fix . && uv run ruff format .

test:
	uv run pytest

run:
	./run.sh

# Same as the dashboard's Reset button: terminates the demo workflows and clears FleetState.
# The app must be running (`make run` / ./run.sh in another terminal). You only need this
# between runs; ./run.sh itself always starts clean (new in-memory dev server, fresh FleetState).
reset:
	curl -fsS -X POST http://localhost:8080/api/reset
	@echo; echo "Wait about 15 s, reload the dashboard, then Start Deliveries."

# --- Durability demo: kill the worker mid-run, then bring it back ---
# Temporal + the web server keep running, so the paused workflow survives in Temporal's
# event history. `make worker` replays it straight back to the pause. Run these from a
# SECOND terminal (leave `make run` / ./run.sh running in the first).
#   kill-worker: SIGKILL, a real crash. No cleanup runs; the UI badge flips to offline once
#                the heartbeat goes stale (6 s window + 3 s UI poll, so up to ~9 s).
#   stop-worker: SIGTERM, a graceful stop. The worker cancels its tasks and removes the
#                heartbeat, so the badge flips on the next UI poll (up to 3 s).
# The [a] in the pattern keeps pkill from matching the shell running this recipe.
kill-worker:
	@pkill -9 -f "[a]gent_fleet.worker" && echo "worker killed (SIGKILL) — workflow is parked in Temporal" || echo "no worker running"

stop-worker:
	@pkill -f "[a]gent_fleet.worker" && echo "worker stopped (SIGTERM, graceful) — workflow is parked in Temporal" || echo "no worker running"

worker:
	uv run --env-file .env python -m agent_fleet.worker

# Framework names for the same beat: failure = kill-worker (SIGKILL), recovery = worker.
failure: kill-worker

recovery: worker
