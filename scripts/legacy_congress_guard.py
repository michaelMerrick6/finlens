import os


def require_legacy_write_opt_in(script_name: str) -> None:
    if os.environ.get("VAIL_ALLOW_LEGACY_CONGRESS_WRITERS") == "1":
        return

    raise SystemExit(
        f"{script_name} is disabled by default because it writes non-authoritative Congress data. "
        "Set VAIL_ALLOW_LEGACY_CONGRESS_WRITERS=1 only for an intentional repair or one-off backfill."
    )


def require_repair_write_opt_in(script_name: str) -> None:
    if os.environ.get("VAIL_ALLOW_CONGRESS_REPAIR_WRITES") == "1":
        return

    raise SystemExit(
        f"{script_name} is disabled by default because it rewrites authoritative Congress rows in bulk. "
        "Set VAIL_ALLOW_CONGRESS_REPAIR_WRITES=1 only for an intentional supervised repair run."
    )
