from __future__ import annotations

import re
from datetime import datetime


OPTION_SIDE_RE = re.compile(r"\b(call|put)\s+options?\b", flags=re.IGNORECASE)
OPTION_STRIKE_RE = re.compile(
    r"\bstrike(?:\s+price)?(?:\s+of)?\s*[:;]?\s*\$?\s*([0-9][0-9,]*(?:\.\d+)?)",
    flags=re.IGNORECASE,
)
OPTION_EXPIRATION_RE = re.compile(
    r"\bexp(?:ires?|iration(?:\s+date)?)(?:\s+of)?\s*[:;]?\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4}|[0-9]{4}-[0-9]{2}-[0-9]{2})",
    flags=re.IGNORECASE,
)
OPTION_MARKER_RE = re.compile(r"\[OP\]|\b(option|strike|expir(?:e|ation))\b", flags=re.IGNORECASE)


def normalize_option_date(raw_value: str | None) -> str | None:
    raw = str(raw_value or "").strip()
    if not raw:
        return None

    for pattern in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, pattern).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def extract_politician_option_metadata(*texts: str | None, asset_type: str | None = None) -> dict[str, str | None] | None:
    combined = " ".join(str(text or "").strip() for text in texts if str(text or "").strip())
    normalized = re.sub(r"\s+", " ", combined).strip()
    if not normalized:
        return None

    is_option = str(asset_type or "").strip().upper() == "OP" or bool(OPTION_MARKER_RE.search(normalized))
    if not is_option:
        return None

    side_match = OPTION_SIDE_RE.search(normalized)
    strike_match = OPTION_STRIKE_RE.search(normalized)
    expiration_match = OPTION_EXPIRATION_RE.search(normalized)

    side = side_match.group(1).lower() if side_match else None
    strike_price = strike_match.group(1).replace(",", "") if strike_match else None
    expiration_date = normalize_option_date(expiration_match.group(1) if expiration_match else None)

    if not side and not strike_price and not expiration_date and str(asset_type or "").strip().upper() != "OP":
        return None

    return {
        "side": side,
        "strike_price": strike_price,
        "expiration_date": expiration_date,
    }


def normalize_politician_asset_type(
    asset_type: str | None,
    *texts: str | None,
    option_metadata: dict[str, str | None] | None = None,
) -> str:
    normalized_asset_type = str(asset_type or "").strip().upper()
    if normalized_asset_type == "OP":
        return "OP"

    if option_metadata and any(option_metadata.get(key) for key in ("side", "strike_price", "expiration_date")):
        return "OP"

    combined = " ".join(str(text or "").strip() for text in texts if str(text or "").strip())
    if combined and OPTION_MARKER_RE.search(combined):
        return "OP"

    return normalized_asset_type or "Stock"


def format_politician_asset_name(
    asset_name: str | None,
    *,
    asset_type: str | None = None,
    option_metadata: dict[str, str | None] | None = None,
) -> str:
    base_name = re.sub(r"\s+", " ", str(asset_name or "").strip())
    normalized_asset_type = normalize_politician_asset_type(asset_type, base_name, option_metadata=option_metadata)
    base_name = re.sub(r"\s*\[OP\]\s*$", "", base_name, flags=re.IGNORECASE)
    if not base_name:
        return ""

    parts = [base_name]
    if normalized_asset_type == "OP":
        parts[0] = f"{parts[0]} [OP]"

    if option_metadata:
        if option_metadata.get("side"):
            parts.append(f"{str(option_metadata['side']).title()} option")
        if option_metadata.get("strike_price"):
            parts.append(f"Strike ${option_metadata['strike_price']}")
        if option_metadata.get("expiration_date"):
            parts.append(f"Expires {option_metadata['expiration_date']}")

    return " | ".join(parts)
