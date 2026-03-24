import json
import os
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv
from supabase import Client, create_client

load_dotenv(dotenv_path=".env.local")

SCRAPER_RUN_STAT_COLUMNS = {
    "records_seen",
    "records_inserted",
    "records_updated",
    "records_skipped",
    "signal_events_created",
    "error_count",
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def truncate_text(value: str | None, limit: int = 4000) -> str | None:
    if value is None:
        return None
    if len(value) <= limit:
        return value
    return f"{value[: limit - 3]}..."


def _get_env(*names: str) -> str:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return ""


def get_supabase_client() -> Client:
    url = _get_env("SUPABASE_URL", "NEXT_PUBLIC_SUPABASE_URL")
    key = _get_env("SUPABASE_SERVICE_KEY", "SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        raise RuntimeError("Missing Supabase credentials for pipeline support.")
    return create_client(url, key)


def emit_summary(summary: dict[str, Any]) -> None:
    print(f"SUMMARY_JSON:{json.dumps(summary, sort_keys=True)}")


def start_scraper_run(
    supabase: Client | None,
    *,
    scraper_name: str,
    source_name: str,
    mode: str,
    parent_run_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> str | None:
    if supabase is None:
        return None

    payload = {
        "scraper_name": scraper_name,
        "source_name": source_name,
        "mode": mode,
        "status": "running",
        "started_at": utc_now_iso(),
        "parent_run_id": parent_run_id,
        "run_metadata": metadata or {},
    }

    try:
        response = supabase.table("scraper_runs").insert(payload).execute()
        if response.data:
            return response.data[0]["id"]
    except Exception as exc:
        print(f"[pipeline_support] Failed to start scraper run: {exc}")
    return None


def finish_scraper_run(
    supabase: Client | None,
    run_id: str | None,
    *,
    status: str,
    started_at: datetime,
    stats: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    stdout_excerpt: str | None = None,
    stderr_excerpt: str | None = None,
) -> None:
    if supabase is None or run_id is None:
        return

    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=timezone.utc)

    finished_at = utc_now()
    run_metadata = metadata.copy() if metadata else {}
    payload: dict[str, Any] = {
        "status": status,
        "finished_at": finished_at.isoformat(),
        "duration_ms": int((finished_at - started_at).total_seconds() * 1000),
        "stdout_excerpt": truncate_text(stdout_excerpt),
        "stderr_excerpt": truncate_text(stderr_excerpt),
    }

    if stats:
        for key, value in stats.items():
            if key in SCRAPER_RUN_STAT_COLUMNS:
                payload[key] = value
            else:
                run_metadata[key] = value
    if run_metadata:
        payload["run_metadata"] = run_metadata

    try:
        supabase.table("scraper_runs").update(payload).eq("id", run_id).execute()
    except Exception as exc:
        print(f"[pipeline_support] Failed to finish scraper run: {exc}")


def log_scraper_error(
    supabase: Client | None,
    run_id: str | None,
    *,
    stage: str,
    message: str,
    details: dict[str, Any] | None = None,
    severity: str = "error",
) -> None:
    if supabase is None or run_id is None:
        return

    try:
        supabase.table("scraper_errors").insert(
            {
                "run_id": run_id,
                "stage": stage,
                "severity": severity,
                "message": truncate_text(message, limit=2000) or "Unknown error",
                "details": details or {},
            }
        ).execute()
        supabase.rpc("increment_scraper_run_error_count", {"target_run_id": run_id}).execute()
    except Exception as exc:
        try:
            existing = supabase.table("scraper_runs").select("error_count").eq("id", run_id).limit(1).execute()
            current = 0
            if existing.data:
                current = int(existing.data[0].get("error_count") or 0)
            supabase.table("scraper_runs").update({"error_count": current + 1}).eq("id", run_id).execute()
        except Exception as inner_exc:
            print(f"[pipeline_support] Failed to log scraper error: {exc}; counter update also failed: {inner_exc}")


def merge_metadata(*values: dict[str, Any] | None) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for value in values:
        if value:
            merged.update(value)
    return merged
