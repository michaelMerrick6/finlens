# Bounded congressional filing audit

Run from the repository root with the configured Python environment:

```bash
python ops/audit_recent_congress_coverage.py \
  --house-days 30 --house-limit 5 \
  --senate-days 30 --senate-limit 5 \
  --document-timeout 120 \
  --progress-file data/audits/congress-review.json
```

Each document's parsing and database comparison runs in an isolated POSIX process. The timeout defaults to 120 seconds. Exceeding it kills the process group, including OCR descendants, records a source-parse timeout, and continues to the next filing. The main audit stays read-only for parser metadata writes. macOS and Linux are supported; this process runner does not support Windows.

The report is replaced atomically after each finished document. Existing progress survives a subsequent worker failure; setup exceptions and manual interruption are recorded at the run level. Reusing an explicit output path starts a new report; it does not resume or skip earlier documents. Without an output path, the audit creates a timestamped JSON file under data/audits. Source discovery/reference-data loading occurs before per-document processing and uses its existing request timeouts; the document budget is not a total audit deadline.

A document outcome of `completed` means its comparison returned, not that the filing is clean. Inspect mismatch, unmapped-paper, and source-parse-failure lists. Timeouts and unmapped paper filings count as failed checks and cause exit code 1. A bounded sample is not proof of historical coverage.

## Verification

A live run using deliberately short five-second document budgets processed three House filings and two Senate filings. Two House comparisons and one Senate comparison completed without detected count/date/identity discrepancies. The remaining House and Senate documents timed out. All five outcomes were saved in `ops/congress-bounded-audit.json`; processing continued into the Senate after the House timeout. Exit code 1 correctly reports unresolved checks, not a crashed audit.

Seven runner regression tests cover timeout continuation, inherited read-only policy, worker exceptions/crashes, large results, atomic progress replacement, invalid budgets, and terminating OCR-style descendants. All 26 Python test files passed after the change.

No production database changes, notifications, or deployment occurred. The seven missing members remain an accepted gap. The timed-out documents still need separate investigation or another audit with a longer budget; this fix makes those failures bounded and visible, not successfully parsed.
