import os
import sys
import subprocess
import json
from datetime import datetime, timezone

from pipeline_support import (
    finish_scraper_run,
    get_supabase_client,
    log_scraper_error,
    merge_metadata,
    start_scraper_run,
)
from shared_utils import positive_int_env

SCRAPER_TIMEOUT_SECONDS = positive_int_env("SCRAPER_TIMEOUT_SECONDS", 900)

# Unified logging output
def log(msg: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

SCRIPT_CONFIG = {
    "ingest_house_official.py": {
        "scraper_name": "house_official_daily",
        "source_name": "congress_house",
        "fatal_parse_failures": False,
    },
    "sync_recent_house_filings.py": {
        "scraper_name": "house_recent_sync",
        "source_name": "congress_house",
        "fatal_parse_failures": False,
    },
    "ingest_senate_official.py": {
        "scraper_name": "senate_official_daily",
        "source_name": "congress_senate",
        "fatal_parse_failures": True,
    },
    "sync_recent_senate_filings.py": {
        "scraper_name": "senate_recent_sync",
        "source_name": "congress_senate",
        "fatal_parse_failures": True,
    },
    "ingest_sec_daily.py": {"scraper_name": "sec_form4_daily", "source_name": "insider_form4"},
    "sync_recent_sec_filings.py": {
        "scraper_name": "sec_form4_recent_sync",
        "source_name": "insider_form4",
        "fatal_parse_failures": True,
    },
    "audit_recent_insider_coverage.py": {
        "scraper_name": "sec_form4_recent_audit",
        "source_name": "insider_form4",
        "fatal_parse_failures": True,
    },
    "validate_recent_trade_capture.py": {
        "scraper_name": "trade_capture_freshness_guard",
        "source_name": "trade_capture",
        "fatal_parse_failures": True,
    },
    "ingest_sec_13f.py": {
        "scraper_name": "sec_13f_refresh",
        "source_name": "hedge_fund_13f",
        "fatal_parse_failures": True,
    },
    "audit_recent_13f_coverage.py": {
        "scraper_name": "sec_13f_audit",
        "source_name": "hedge_fund_13f",
        "fatal_parse_failures": True,
    },
    "backfill_recent_politician_member_ids.py": {
        "scraper_name": "politician_member_backfill",
        "source_name": "congress_members",
    },
    "audit_recent_congress_coverage.py": {
        "scraper_name": "congress_recent_audit",
        "source_name": "congress_audit",
        "fatal_parse_failures": False,
    },
    "capture_capitol_trades_leads.py": {
        "scraper_name": "capitol_trades_capture",
        "source_name": "capitol_trades",
        "fatal_parse_failures": False,
    },
    "audit_capitol_trades_gaps.py": {
        "scraper_name": "capitol_trades_gap_audit",
        "source_name": "capitol_trades",
        "fatal_parse_failures": True,
    },
    "emit_signal_events.py": {"scraper_name": "signal_event_emitter", "source_name": "signal_events"},
    "compile_derived_signal_events.py": {"scraper_name": "derived_signal_compiler", "source_name": "signal_events"},
    "queue_alert_deliveries.py": {"scraper_name": "alert_delivery_queue", "source_name": "notifications"},
    "queue_tweet_candidates.py": {"scraper_name": "tweet_candidate_queue", "source_name": "social_posting"},
    "dispatch_email_alerts.py": {
        "scraper_name": "email_dispatch",
        "source_name": "notifications",
        "blocked_config_is_skipped": True,
    },
    "dispatch_sms_alerts.py": {
        "scraper_name": "sms_dispatch",
        "source_name": "notifications",
        "blocked_config_is_skipped": True,
    },
    "dispatch_discord_alerts.py": {"scraper_name": "discord_dispatch", "source_name": "notifications"},
    "dispatch_discord_broadcasts.py": {"scraper_name": "discord_broadcast_dispatch", "source_name": "social_posting"},
    "dispatch_twitter_posts.py": {"scraper_name": "twitter_dispatch", "source_name": "social_posting"},
}


def parse_summary(stdout: str) -> dict:
    for line in reversed(stdout.splitlines()):
        if line.startswith("SUMMARY_JSON:"):
            try:
                return json.loads(line.split("SUMMARY_JSON:", 1)[1].strip())
            except json.JSONDecodeError:
                return {}
    return {}


def run_script(
    script_path: str,
    *,
    supabase=None,
    parent_run_id: str | None = None,
    mode: str = "daily",
    critical: bool = True,
):
    log(f"--- Executing: {script_path} ---")
    script_name = os.path.basename(script_path)
    config = SCRIPT_CONFIG.get(
        script_name,
        {"scraper_name": script_name.replace(".py", ""), "source_name": "unknown"},
    )
    started_at = datetime.now(timezone.utc)
    run_id = start_scraper_run(
        supabase,
        scraper_name=config["scraper_name"],
        source_name=config["source_name"],
        mode=mode,
        parent_run_id=parent_run_id,
        metadata={"script_path": script_path},
    )
    try:
        python_exe = os.environ.get("PYTHON_EXE", "python") if sys.platform == "linux" else sys.executable
        result = subprocess.run([python_exe, script_path], check=True, text=True, capture_output=True, timeout=SCRAPER_TIMEOUT_SECONDS)
        print(result.stdout)
        summary = parse_summary(result.stdout)
        if config.get("fatal_parse_failures") and int(summary.get("parse_failures") or 0) > 0:
            log(f"!!! Parse failures reported by {script_name} !!!")
            log_scraper_error(
                supabase,
                run_id,
                stage="parse",
                message=f"{script_name} reported {summary.get('parse_failures')} parse failures",
                details={"summary": summary},
            )
            finish_scraper_run(
                supabase,
                run_id,
                status="failed",
                started_at=started_at,
                stats=summary,
                metadata=merge_metadata({"script_path": script_path}, {"return_code": result.returncode}),
                stdout_excerpt=result.stdout,
                stderr_excerpt=result.stderr,
            )
            return False
        if int(summary.get("parse_failures") or 0) > 0:
            log(f"!!! Parse warnings reported by {script_name}; continuing pipeline. !!!")
            finish_scraper_run(
                supabase,
                run_id,
                status="warning",
                started_at=started_at,
                stats=summary,
                metadata=merge_metadata({"script_path": script_path}, {"return_code": result.returncode}),
                stdout_excerpt=result.stdout,
                stderr_excerpt=result.stderr,
            )
            return True
        if config.get("blocked_config_is_skipped") and int(summary.get("deliveries_blocked_config") or 0) > 0:
            log(f"--- Skipped: {script_path} blocked by missing optional delivery config ---")
            finish_scraper_run(
                supabase,
                run_id,
                status="skipped",
                started_at=started_at,
                stats=summary,
                metadata=merge_metadata({"script_path": script_path}, {"return_code": result.returncode}),
                stdout_excerpt=result.stdout,
                stderr_excerpt=result.stderr,
            )
            return True
        finish_scraper_run(
            supabase,
            run_id,
            status="success",
            started_at=started_at,
            stats=summary,
            metadata=merge_metadata({"script_path": script_path}, {"return_code": result.returncode}),
            stdout_excerpt=result.stdout,
            stderr_excerpt=result.stderr,
        )
        log(f"--- Completed: {script_path} ---")
        return True
    except subprocess.CalledProcessError as e:
        log(f"!!! Error executing {script_path} !!!")
        print(e.stdout)
        print(e.stderr)
        summary = parse_summary(e.stdout or "")
        if config.get("blocked_config_is_skipped") and int(summary.get("deliveries_blocked_config") or 0) > 0:
            log(f"--- Skipped: {script_path} blocked by missing optional delivery config ---")
            finish_scraper_run(
                supabase,
                run_id,
                status="skipped",
                started_at=started_at,
                stats=summary,
                metadata=merge_metadata({"script_path": script_path}, {"return_code": e.returncode}),
                stdout_excerpt=e.stdout,
                stderr_excerpt=e.stderr,
            )
            return True
        if not config.get("fatal_parse_failures") and int(summary.get("parse_failures") or 0) > 0:
            log(f"!!! Parse warnings reported by {script_name}; continuing pipeline. !!!")
            finish_scraper_run(
                supabase,
                run_id,
                status="warning",
                started_at=started_at,
                stats=summary,
                metadata=merge_metadata({"script_path": script_path}, {"return_code": e.returncode}),
                stdout_excerpt=e.stdout,
                stderr_excerpt=e.stderr,
            )
            return True
        log_scraper_error(
            supabase,
            run_id,
            stage="subprocess",
            message=f"{script_name} exited with code {e.returncode}",
            details={"stdout": e.stdout, "stderr": e.stderr},
        )
        finish_scraper_run(
            supabase,
            run_id,
            status="failed",
            started_at=started_at,
            metadata=merge_metadata({"script_path": script_path}, {"return_code": e.returncode}),
            stdout_excerpt=e.stdout,
            stderr_excerpt=e.stderr,
        )
        if critical:
            sys.exit(1)
        return False
    except subprocess.TimeoutExpired as exc:
        log(f"!!! Timeout executing {script_path} ({SCRAPER_TIMEOUT_SECONDS}s limit) !!!")
        log_scraper_error(
            supabase,
            run_id,
            stage="timeout",
            message=f"{script_name} exceeded the {SCRAPER_TIMEOUT_SECONDS}s timeout window",
        )
        finish_scraper_run(
            supabase,
            run_id,
            status="timed_out",
            started_at=started_at,
            metadata={"script_path": script_path},
            stdout_excerpt=exc.stdout,
            stderr_excerpt=exc.stderr,
        )
        if critical:
            sys.exit(1)
        return False

def main():
    log("Started Vail full operations pipeline")
    start = datetime.now()
    mode = "daily"
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    try:
        supabase = get_supabase_client()
    except Exception as exc:
        supabase = None
        log(f"Pipeline tracking disabled: {exc}")

    pipeline_run_started_at = datetime.now(timezone.utc)
    pipeline_run_id = start_scraper_run(
        supabase,
        scraper_name="daily_pipeline",
        source_name="orchestrator",
        mode=mode,
        metadata={"base_dir": base_dir},
    )
    
    # Daily mode: only check current year + last year for PDF scrapers
    os.environ["FINLENS_DAILY_MODE"] = "1"
    # Keep scheduled SEC scans bounded; freshness validation below catches lag
    # against official feeds without letting broad replay jobs time out.
    os.environ.setdefault("SEC_DAILY_MAX_FILINGS", "120")
    os.environ.setdefault("SEC_DAILY_MAX_PAGES", "3")
    os.environ.setdefault("SEC_DAILY_RECENT_SCAN_DAYS", "10")
    os.environ.setdefault("SEC_RECENT_SYNC_LIMIT", "120")
    os.environ.setdefault("SEC_RECENT_SYNC_PAGES", "3")
    os.environ.setdefault("SEC_RECENT_AUDIT_LIMIT", "120")
    os.environ.setdefault("SEC_RECENT_AUDIT_PAGES", "3")
    os.environ.setdefault("CAPTURE_FRESHNESS_SOURCE_LIMIT", "120")
    os.environ.setdefault("CAPTURE_FRESHNESS_SEC_PAGES", "3")
    os.environ.setdefault("CAPTURE_FRESHNESS_PARSE_LIMIT", "50")
    
    results = {}
    
    # 1) Official House PDF scraper (electronic filings)
    results["House PDFs"] = run_script(
        os.path.join(base_dir, "ingest_house_official.py"),
        supabase=supabase,
        parent_run_id=pipeline_run_id,
        mode=mode,
        critical=False,
    )
    results["House Recent Sync"] = run_script(
        os.path.join(base_dir, "sync_recent_house_filings.py"),
        supabase=supabase,
        parent_run_id=pipeline_run_id,
        mode=mode,
        critical=False,
    )
    
    # 2) Official Senate scraper
    results["Senate"] = run_script(
        os.path.join(base_dir, "ingest_senate_official.py"),
        supabase=supabase,
        parent_run_id=pipeline_run_id,
        mode=mode,
        critical=False,
    )
    results["Senate Recent Sync"] = run_script(
        os.path.join(base_dir, "sync_recent_senate_filings.py"),
        supabase=supabase,
        parent_run_id=pipeline_run_id,
        mode=mode,
        critical=False,
    )
    results["Member Backfill"] = run_script(
        os.path.join(base_dir, "..", "ops", "backfill_recent_politician_member_ids.py"),
        supabase=supabase,
        parent_run_id=pipeline_run_id,
        mode=mode,
        critical=False,
    )
    results["Congress Audit"] = run_script(
        os.path.join(base_dir, "..", "ops", "audit_recent_congress_coverage.py"),
        supabase=supabase,
        parent_run_id=pipeline_run_id,
        mode=mode,
        critical=False,
    )
    results["Capitol Lead Capture"] = run_script(
        os.path.join(base_dir, "capture_capitol_trades_leads.py"),
        supabase=supabase,
        parent_run_id=pipeline_run_id,
        mode=mode,
        critical=False,
    )
    results["Capitol Gap Audit"] = run_script(
        os.path.join(base_dir, "..", "ops", "audit_capitol_trades_gaps.py"),
        supabase=supabase,
        parent_run_id=pipeline_run_id,
        mode=mode,
        critical=False,
    )
    
    # 4) SEC Edgar (Form 4 insider trades - Daily Lightweight)
    results["SEC Edgar"] = run_script(
        os.path.join(base_dir, "ingest_sec_daily.py"),
        supabase=supabase,
        parent_run_id=pipeline_run_id,
        mode=mode,
        critical=False,
    )
    results["SEC Recent Sync"] = run_script(
        os.path.join(base_dir, "sync_recent_sec_filings.py"),
        supabase=supabase,
        parent_run_id=pipeline_run_id,
        mode=mode,
        critical=False,
    )
    results["SEC Audit"] = run_script(
        os.path.join(base_dir, "..", "ops", "audit_recent_insider_coverage.py"),
        supabase=supabase,
        parent_run_id=pipeline_run_id,
        mode=mode,
        critical=False,
    )
    results["Trade Freshness Guard"] = run_script(
        os.path.join(base_dir, "validate_recent_trade_capture.py"),
        supabase=supabase,
        parent_run_id=pipeline_run_id,
        mode=mode,
        # Missing official-source rows mean the product missed trades; fail loud.
        critical=True,
    )

    # 5) Canonical signal event emitter
    results["Signal Events"] = run_script(
        os.path.join(base_dir, "emit_signal_events.py"),
        supabase=supabase,
        parent_run_id=pipeline_run_id,
        mode=mode,
        critical=False,
    )

    if os.environ.get("FINLENS_RUN_13F_DAILY", "1") == "1":
        results["13F"] = run_script(
            os.path.join(base_dir, "ingest_sec_13f.py"),
            supabase=supabase,
            parent_run_id=pipeline_run_id,
            mode=mode,
            critical=False,
        )
        results["13F Audit"] = run_script(
            os.path.join(base_dir, "..", "ops", "audit_recent_13f_coverage.py"),
            supabase=supabase,
            parent_run_id=pipeline_run_id,
            mode=mode,
            critical=False,
        )
        results["Signal Events (13F Refresh)"] = run_script(
            os.path.join(base_dir, "emit_signal_events.py"),
            supabase=supabase,
            parent_run_id=pipeline_run_id,
            mode=mode,
            critical=False,
        )

    results["Derived Signals"] = run_script(
        os.path.join(base_dir, "compile_derived_signal_events.py"),
        supabase=supabase,
        parent_run_id=pipeline_run_id,
        mode=mode,
        critical=False,
    )

    results["Alert Queue"] = run_script(
        os.path.join(base_dir, "queue_alert_deliveries.py"),
        supabase=supabase,
        parent_run_id=pipeline_run_id,
        mode=mode,
        critical=False,
    )
    results["Tweet Queue"] = run_script(
        os.path.join(base_dir, "queue_tweet_candidates.py"),
        supabase=supabase,
        parent_run_id=pipeline_run_id,
        mode=mode,
        critical=False,
    )
    results["Email Dispatch"] = run_script(
        os.path.join(base_dir, "dispatch_email_alerts.py"),
        supabase=supabase,
        parent_run_id=pipeline_run_id,
        mode=mode,
        critical=False,
    )
    results["Text Dispatch"] = run_script(
        os.path.join(base_dir, "dispatch_sms_alerts.py"),
        supabase=supabase,
        parent_run_id=pipeline_run_id,
        mode=mode,
        critical=False,
    )
    results["Discord Dispatch"] = run_script(
        os.path.join(base_dir, "dispatch_discord_alerts.py"),
        supabase=supabase,
        parent_run_id=pipeline_run_id,
        mode=mode,
        critical=False,
    )
    results["Discord Broadcast Dispatch"] = run_script(
        os.path.join(base_dir, "dispatch_discord_broadcasts.py"),
        supabase=supabase,
        parent_run_id=pipeline_run_id,
        mode=mode,
        critical=False,
    )
    results["X Dispatch"] = run_script(
        os.path.join(base_dir, "dispatch_twitter_posts.py"),
        supabase=supabase,
        parent_run_id=pipeline_run_id,
        mode=mode,
        critical=False,
    )
    
    elapsed = (datetime.now() - start).total_seconds()
    log(f"\n{'='*50}")
    log(f"DAILY PIPELINE COMPLETE ({elapsed:.0f}s)")
    for name, success in results.items():
        status = "✅" if success else "❌"
        log(f"  {status} {name}")
    log(f"{'='*50}")
    
    house_core_ok = results.get("House Recent Sync", False) or results.get("House PDFs", False)
    senate_core_ok = results.get("Senate Recent Sync", False) or results.get("Senate", False)
    congress_core_ok = house_core_ok and senate_core_ok
    insider_core_ok = results.get("SEC Recent Sync", False) or results.get("SEC Edgar", False)
    freshness_ok = results.get("Trade Freshness Guard", False)
    fund_core_ok = os.environ.get("FINLENS_RUN_13F_DAILY", "1") != "1" or results.get("13F", False)
    signal_core_ok = (
        results.get("Signal Events", False)
        or results.get("Signal Events (13F Refresh)", False)
        or results.get("Derived Signals", False)
    )
    alert_core_ok = results.get("Alert Queue", False)

    core_failures = []
    if not congress_core_ok:
        core_failures.append("congress")
    if not insider_core_ok:
        core_failures.append("insider")
    if not freshness_ok:
        core_failures.append("freshness")
    if not fund_core_ok:
        core_failures.append("13f")
    if not signal_core_ok:
        core_failures.append("signals")
    if not alert_core_ok:
        core_failures.append("alerts")

    # Keep the nightly fallback quiet unless the core data/alert path is broken.
    # Audit and optional delivery-channel failures are visible in mission control,
    # but should not generate GitHub failure emails when the pipeline is usable.
    if not any(results.values()) or core_failures:
        if core_failures:
            log(f"Core pipeline failures: {', '.join(core_failures)}; marking pipeline run failed.")
        else:
            log("All scrapers failed!")
        finish_scraper_run(
            supabase,
            pipeline_run_id,
            status="failed",
            started_at=pipeline_run_started_at,
            metadata={"results": results, "core_failures": core_failures},
        )
        sys.exit(1)

    finish_scraper_run(
        supabase,
        pipeline_run_id,
        status="success",
        started_at=pipeline_run_started_at,
        metadata={"results": results, "core_failures": core_failures},
    )

if __name__ == "__main__":
    main()
