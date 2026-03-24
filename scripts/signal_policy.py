from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
SIGNAL_POLICY_PATH = REPO_ROOT / "config" / "signal-policy.json"


@lru_cache(maxsize=1)
def load_signal_policy() -> dict[str, Any]:
    if not SIGNAL_POLICY_PATH.exists():
        return {}
    try:
        with SIGNAL_POLICY_PATH.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception as exc:
        print(f"[signal_policy] Warning: failed to load signal policy ({exc})")
        return {}
    return data if isinstance(data, dict) else {}


def signal_policy_path() -> Path:
    return SIGNAL_POLICY_PATH
