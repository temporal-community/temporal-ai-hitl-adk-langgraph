#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Load .env if present
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

for command_name in uv temporal; do
    if ! command -v "$command_name" >/dev/null 2>&1; then
        echo "Error: $command_name is required." >&2
        exit 1
    fi
done

echo "Syncing dependencies with uv..."
uv sync --all-extras --frozen

TEMPORAL_PID=""
WORKER_PID=""
SERVER_PID=""

cleanup() {
    exit_code=$?
    trap - EXIT
    echo
    echo "Shutting down..."
    for pid in "$SERVER_PID" "$WORKER_PID" "$TEMPORAL_PID"; do
        if [[ -n "$pid" ]]; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    for pid in "$SERVER_PID" "$WORKER_PID" "$TEMPORAL_PID"; do
        if [[ -n "$pid" ]]; then
            wait "$pid" 2>/dev/null || true
        fi
    done
    echo "Done."
    exit "$exit_code"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "Cleaning up state..."
DEMO_DB_PATH="${FLEET_DB_PATH:-$SCRIPT_DIR/fleet_state.db}"
DEMO_HEARTBEAT_PATH="$(dirname "$DEMO_DB_PATH")/worker_heartbeat"
rm -f "$DEMO_DB_PATH" "$DEMO_DB_PATH-wal" "$DEMO_DB_PATH-shm" "$DEMO_HEARTBEAT_PATH"

echo "Starting Temporal dev server..."
temporal server start-dev &
TEMPORAL_PID=$!

echo "Waiting for Temporal to be ready..."
temporal_ready=false
for _ in {1..60}; do
    if temporal operator cluster health 2>/dev/null | grep -q "SERVING"; then
        temporal_ready=true
        break
    fi
    if ! kill -0 "$TEMPORAL_PID" 2>/dev/null; then
        echo "Error: Temporal dev server exited before becoming ready." >&2
        exit 1
    fi
    sleep 0.5
done
if [[ "$temporal_ready" != true ]]; then
    echo "Error: Temporal dev server was not ready after 30 seconds." >&2
    exit 1
fi

echo "Starting workers..."
uv run python -m agent_fleet.worker &
WORKER_PID=$!

echo "Waiting for workers to be ready..."
worker_ready=false
for _ in {1..40}; do
    if [[ -f "$DEMO_HEARTBEAT_PATH" ]]; then
        worker_ready=true
        break
    fi
    if ! kill -0 "$WORKER_PID" 2>/dev/null; then
        echo "Error: worker process exited during startup." >&2
        exit 1
    fi
    sleep 0.25
done
if [[ "$worker_ready" != true ]]; then
    echo "Error: workers were not ready after 10 seconds." >&2
    exit 1
fi

echo "Starting server..."
uv run python -m agent_fleet.server &
SERVER_PID=$!

echo ""
echo "  App:      http://localhost:8080"
echo "  Temporal: http://localhost:8233"
echo ""
echo "Press Ctrl+C to stop."

# The worker is meant to die mid-demo (`make kill-worker` / `make stop-worker`), so its exit
# must not end this script: the dev server keeps workflow history in memory, and the EXIT trap
# would stop it and erase the parked workflows. Keep going while Temporal and the server are up.
while kill -0 "$TEMPORAL_PID" 2>/dev/null && kill -0 "$SERVER_PID" 2>/dev/null; do
    if [[ -n "$WORKER_PID" ]] && ! kill -0 "$WORKER_PID" 2>/dev/null; then
        # Forget the PID so cleanup never signals a reused one. A worker started later with
        # `make worker` belongs to that terminal; stop it there (or with `make stop-worker`).
        WORKER_PID=""
        echo "Worker stopped — workflows are parked in Temporal. Bring it back with: make worker"
    fi
    sleep 1
done

if ! kill -0 "$TEMPORAL_PID" 2>/dev/null; then
    echo "Temporal dev server exited unexpectedly; stopping the demo." >&2
else
    echo "Server exited unexpectedly; stopping the demo." >&2
fi
exit 1
