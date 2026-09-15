# Congressional capture reliability

## Deployment order

1. Apply `supabase_vail_phase14_congress_capture.sql` with the database owner before deploying these workers. The production schema installer includes it. It creates private queue/history tables, service-only RPCs, correction guards, and a unique transaction-key index. If duplicate document IDs exist, migration fails without deleting them.
2. Deploy the code and the updated Vercel cron configuration. Congress capture runs hourly through GitHub Actions at minute 20. Vercel dispatch remains a daily fallback because the current plan rejected hourly crons; nightly fallback retains its own trigger. Congress capture, nightly fallback, and signal processing share a non-canceling workflow concurrency group.
3. Inspect `congress_filings` and the capture summary. A successful bounded run is not a completeness claim: `coverage_complete` and `filings_unresolved` report the inventory backlog separately.

Production rollout verification is recorded in the deployment task and the local `artifacts/congress-rollout/` reports.

## Capture behavior

- The hourly workflow captures and audits official sources. Secondary Capitol Trades lead capture remains in nightly maintenance; its rate limits do not block official capture.
- `python scripts/capture_congress.py` handles both chambers, even if one fails. The former official-ingest and recent-sync commands delegate to it. Core and nightly pipelines invoke it once.
- Discovery inventories all House PTR indexes from 2012 and exhausts the Senate feed from the same date. Set `--start-year` to change this explicit boundary. Missing/malformed indexes, duplicate keys, changing Senate totals, and truncated pagination are errors.
- Discovery registers every filing before processing limits apply. Existing transaction `-0` rows do not establish completion. Rediscovery preserves retry history; changed metadata invalidates an old worker's claim.
- Workers claim filings with expiring tokens. Every fourth work slot selects the oldest due filing so retries and history continue to advance. Failed documents remain queued with bounded exponential backoff, without a date cutoff or truncated failure list.
- Default work budgets are 40 documents and 240 processing seconds per chamber, with 45 seconds per document. Timed-out workers and their OCR subprocesses are stopped. Unattempted documents remain queued. Recent successful documents are checked again daily; older ones every 30 days.
- All parsed rows must resolve to a known member and have transaction dates no later than the official filing date. Unsupported scans, incomplete text/HTML rows, and ambiguous identities remain unresolved. Non-public assets are not equivalent to no transactions.
- Publication atomically replaces a filing's transactions, raw records, and base signals. A failed write restores the previous state automatically. Empty replacement requires explicit no-transaction verification. Unchanged document keys retain their database IDs.
- Corrections archive old records and delivery references, invalidate dependent derived signals, and cancel obsolete pending payloads. The normal compiler rebuilds derived signals. Delivery tombstones prevent already-delivered keys being queued again. Guards stop an older emitter snapshot from resurrecting removed or changed trades.

## Validation

`scripts/run_tests.py` includes parser, pagination, failure propagation, and queue tests. CI also runs `test_congress_postgres.py` against PostgreSQL 17: rollback after a foreign-key failure, stale claims, correction cleanup, notification deduplication, prefix isolation, unchanged IDs, empty-extraction rejection, and service-only permissions.

To run the database tests locally, supply `VAIL_TEST_POSTGRES_DSN` pointing to an **isolated test database** and install `psycopg[binary]`. The tests install their schema there. Never point this test variable at production.

Recent source audits are bounded samples (five documents per chamber in scheduled workflows). They now compare identity, asset name/type, dates, direction, ticker, amount, and duplicate multiplicity. They share parser code and are not independent verification of every source transaction. Historical unmatched filings and roster discrepancies still require classification/source review; this change does not certify complete history, annual disclosures, or holdings.
