# SEC recovery deployment and behavior

Apply `supabase_vail_sec_recovery.sql` before deploying the workers. It adds a
service-only filing queue and atomic publication functions. Applied to production
September 19, 2026, after rollback and idempotency tests against isolated PostgreSQL.

## Prevented failure modes

- Form 4 discoveries are stored before parsing; pending/failed accessions survive
  process exits, rate limits, and disappearance from the bounded RSS feed.
- Each parsed filing commits separately. A later timeout cannot discard completed
  earlier filings. Known zero-purchase/sale filings are also marked complete.
- Replays compare full row multisets before writing, retaining IDs for unchanged
  filings and preserving legitimate repeated source rows.
- Insider corrections and 13F period replacements are atomic: failed inserts roll
  back deletion. A 13F replacement targets one fund/quarter, never other quarters
  sharing its publication date.
- Scrapers no longer overwrite existing company sector/industry classifications
  with generic placeholders.
- HTTP 403/429 stops the capture loop after bounded retries. Failed accessions are
  retained with an hour cooldown. Budget exhaustion leaves unprocessed work queued.
- Ingest and repair scripts exit nonzero for unresolved errors; audits already fail
  on mismatches. Capture reports incomplete queue state rather than false success.
- Scheduled jobs queue behind existing runs rather than canceling them. Dependency
  installation explicitly runs `pip check`.

The hourly workflow uses the durable capture worker, then a bounded independent
source audit and freshness check. It no longer destructively replays hundreds of
already captured filings every hour. `sync_recent_sec_filings.py` remains available
for explicit correction work and uses the same atomic publisher.

## Remaining limits

The discovery feed is bounded. The queue protects discovered documents, not filings
that aged out before they were discovered. Multi-day outages can still require an
SEC index backfill. Provider downtime/rate limits and unreadable source documents
cannot be prevented; they must remain visible and retryable. Historical Congress
review failures and 13F ticker-resolution discrepancies require separate source
review; these operational protections do not certify that backlog as complete.

Tests: `test_sec_recovery_worker.py`, `test_sec_recovery_postgres.py`, and the existing
SEC/parser/audit tests. Set `VAIL_TEST_POSTGRES_DSN` only to an isolated test database.
