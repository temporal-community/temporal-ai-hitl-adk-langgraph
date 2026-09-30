"""Centralized configuration for the Meltdown demo."""

from __future__ import annotations

import os
from pathlib import Path

GOOGLE_API_KEY: str | None = os.environ.get("GOOGLE_API_KEY")
GOOGLE_MAPS_API_KEY: str | None = os.environ.get("GOOGLE_MAPS_API_KEY")
# Google recommends 3.8 Flash for new projects; 2.5 models only serve keys that used them before.
DEFAULT_MODEL: str = os.environ.get("DEFAULT_MODEL", "gemini-3.8-flash")
TEMPORAL_ADDRESS: str = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
FLEET_DB_PATH: str = os.environ.get(
    "FLEET_DB_PATH", str(Path(__file__).parent.parent / "fleet_state.db")
)
# Seconds an unanswered ask_human waits before escalating to a backup approver (short for the demo).
GATE_ESCALATION_SECONDS: int = int(os.environ.get("GATE_ESCALATION_SECONDS", "30"))
# Temporal owns retries: every LLM client makes one attempt, so a failed model call fails its
# Activity and the Activity retry policy retries it, where each attempt shows up in history.
# google-genai counts the first request in `attempts`, so there it becomes attempts=1.
# Not covered: google-genai's async aiohttp path resends once after a connection failure.
LLM_MAX_RETRIES: int = 0
