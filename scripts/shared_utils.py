"""Shared utility functions used across multiple pipeline scripts.

This module centralizes functions that were previously duplicated in
emit_signal_events.py, notification_compiler.py, tweet_candidate_compiler.py,
insider_holdings.py, and sec_form4_support.py.
"""

import hashlib
import os
import re


def extract_sec_accession(value: str | None) -> str | None:
    """Extract an SEC accession number from a URL or raw string.

    Supports both dashed (0001234567-23-012345) and compact (/0001234567012345/)
    accession formats.
    """
    raw = str(value or "").strip()
    if not raw:
        return None
    dash_match = re.search(r"(\d{10}-\d{2}-\d{6})", raw)
    if dash_match:
        return dash_match.group(1)
    compact_match = re.search(r"/(\d{18})/", raw)
    if compact_match:
        compact = compact_match.group(1)
        return f"{compact[:10]}-{compact[10:12]}-{compact[12:]}"
    return None


def stable_id(parts: list[str]) -> str:
    """Produce a deterministic SHA-1 hex digest from a list of strings."""
    raw = "|".join(parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def positive_int_env(name: str, default: int) -> int:
    """Read a positive integer setting without crashing an entrypoint."""
    try:
        value = int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default
