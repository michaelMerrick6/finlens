import os
import sys
from datetime import datetime, timezone

from daily_scraper import log, run_script
from pipeline_support import finish_scraper_run, get_supabase_client, start_scraper_run


CORE_PIPELINE_STEPS = [
    ("House PDFs", "ingest_house_official.py"),
    ("House Recent Sync", "sync_recent_house_filings.py"),
    ("Senate", "ingest_senate_official.py"),
    ("Senate Recent Sync", "sync_recent_senate_filings.py"),
    ("SEC Edgar", "ingest_sec_daily.py"),
    ("SEC Recent Sync", "sync_recent_sec_filings.py"),
    ("Signal Events", "emit_signal_events.py"),
    ("13F Deadline Reminders", "emit_13f_deadline_events.py"),
    ("Derived Signals", "compile_derived_signal_events.py"),
    ("Alert Queue", "queue_alert_deliveries.py"),
]

OPTIONAL_13F_STEPS = [
    ("13F", "ingest_sec_13f.py"),
    ("Signal Events (13F Refresh)", "emit_signal_events.py"),
]

REQUIRED_CORE_STEPS = {
    "House PDFs",
    "House Recent Sync",
    "Senate",
    "Senate Recent Sync",
    "SEC Edgar",
    "SEC Recent Sync",
    "Signal Events",
    "13F Deadline Reminders",
    "Derived Signals",
    "Alert Queue",
}


def core_steps() -> list[tuple[str, str]]:
    steps = list(CORE_PIPELINE_STEPS)
    if os.environ.get("FINLENS_RUN_13F_DAILY", "0") == "1":
        steps[7:7] = OPTIONAL_13F_STEPS
    return steps


def required_steps() -> set[str]:
    required = set(REQUIRED_CORE_STEPS)
    if os.environ.get("FINLENS_RUN_13F_DAILY", "0") == "1":
        required.update({"13F", "Signal Events (13F Refresh)"})
    return required


def main() -> None:
    log("Started Vail core product pipeline")
    start = datetime.now()
    mode = "core"
    base_dir = os.path.dirname(os.path.abspath(__file__))

    os.environ["FINLENS_DAILY_MODE"] = "1"

    try:
        supabase = get_supabase_client()
    except Exception as exc:
        supabase = None
        log(f"Pipeline tracking disabled: {exc}")

    pipeline_run_started_at = datetime.now(timezone.utc)
    pipeline_run_id = start_scraper_run(
        supabase,
        scraper_name="core_product_pipeline",
        source_name="orchestrator",
        mode=mode,
        metadata={"base_dir": base_dir},
    )

    results: dict[str, bool] = {}
    for label, script_name in core_steps():
        results[label] = run_script(
            os.path.join(base_dir, script_name),
            supabase=supabase,
            parent_run_id=pipeline_run_id,
            mode=mode,
            critical=False,
        )

    elapsed = (datetime.now() - start).total_seconds()
    log(f"\n{'=' * 50}")
    log(f"CORE PRODUCT PIPELINE COMPLETE ({elapsed:.0f}s)")
    for name, success in results.items():
        status = "OK" if success else "FAILED"
        log(f"  [{status}] {name}")
    log(f"{'=' * 50}")

    failed_required_steps = sorted(name for name in required_steps() if not results.get(name, False))
    if failed_required_steps or not any(results.values()):
        if failed_required_steps:
            log(f"Required core stages failed: {', '.join(failed_required_steps)}")
        else:
            log("All core pipeline stages failed.")
        finish_scraper_run(
            supabase,
            pipeline_run_id,
            status="failed",
            started_at=pipeline_run_started_at,
            metadata={"results": results, "failed_required_steps": failed_required_steps},
        )
        sys.exit(1)

    finish_scraper_run(
        supabase,
        pipeline_run_id,
        status="success",
        started_at=pipeline_run_started_at,
        metadata={"results": results},
    )


if __name__ == "__main__":
    main()
