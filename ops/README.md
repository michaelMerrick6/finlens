# Ops Workspace

This directory holds maintenance entrypoints that are intentionally outside the core product path.

Use this area for:

- audits
- backfills
- repair and recovery jobs
- seed scripts
- targeted historical remediation
- one-off SQL repair files in `ops/sql/`

The production product path stays in `scripts/` and is driven by:

- `scripts/run_core_pipeline.py`
- `scripts/emit_signal_events.py`
- `scripts/compile_derived_signal_events.py`
- `scripts/queue_alert_deliveries.py`

Run ops scripts from the repo root with commands like:

```bash
python3 ops/audit_recent_congress_coverage.py
python3 ops/backfill_politician_asset_names.py --limit 250
python3 ops/repair_house_filings.py
```

`ops/sitecustomize.py` adds the main `scripts/` directory to `sys.path` so these moved entrypoints can still import shared pipeline modules cleanly.
