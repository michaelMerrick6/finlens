from __future__ import annotations

from supabase import Client

_POLITICIAN_TRADES_HAS_ASSET_NAME: bool | None = None


def politician_trades_has_asset_name_column(supabase: Client) -> bool:
    global _POLITICIAN_TRADES_HAS_ASSET_NAME

    if _POLITICIAN_TRADES_HAS_ASSET_NAME is not None:
        return _POLITICIAN_TRADES_HAS_ASSET_NAME

    try:
        supabase.table("politician_trades").select("id,asset_name").limit(1).execute()
        _POLITICIAN_TRADES_HAS_ASSET_NAME = True
    except Exception as exc:
        message = str(exc).lower()
        if "asset_name" in message and (
            "does not exist" in message
            or "could not find" in message
            or "schema cache" in message
        ):
            _POLITICIAN_TRADES_HAS_ASSET_NAME = False
        else:
            print(f"[politician_schema_support] asset_name column probe failed: {exc}")
            _POLITICIAN_TRADES_HAS_ASSET_NAME = False

    return _POLITICIAN_TRADES_HAS_ASSET_NAME
