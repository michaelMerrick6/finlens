# Operations audit — September 19, 2026

## Confirmed repairs

- Dependency resolution: Pillow 12.1.1 conflicted with pdfplumber 0.11.10. Pillow 12.2.0 resolves it; GitHub frontend and backend checks passed on 98987a2.
- Recent Congress recovery: 14 House trade rows filed September 17, four Alan Armstrong Senate rows filed September 17, and five Diana Harshbarger bond rows filed September 11 were recovered. The bond filing was visually reviewed on both original PDF pages and transcribed with a source SHA-256 lock. Repeated physical rows remain separate; the printed example is excluded.
- Repeated recent Congress source/database comparison passed for 17 House and six Senate filings. This is a bounded two-week audit, not a historical completeness certification. See the adjacent JSON report.
- Applied existing `supabase_vail_pelosi_holdings_sync.sql` to production: the missing table had caused daily updates to fail. Local updater returned no issues; GitHub run 35428554696 succeeded.
- Committee refresh run 35428284778 and classification refresh run 35428285677 succeeded.
- Recovered 382 insider rows from 166 SEC filings using downloaded official documents. Source/database comparison passed for 408 downloaded recent feed filings plus 80 existing accessions. See the adjacent verified JSON. Documents with no supported purchases/sales were explicitly counted.
- Widened insider ingest/replay/audit from 120 to 600 filings and 3 to 10 pages; disabled the consecutive-existing early stop, which can skip older gaps. Added per-process SEC request pacing and unbuffered insider job output.
- Fixed the 13F coverage audit to exit nonzero for parse failures or mismatches, rather than printing problems but letting the workflow report success. Added regression tests.
- Live trade API, politicians page, screener page and Pelosi holdings page returned HTTP 200. This is availability checking, not a complete interactive UI test.

## Still open

- Congress ledger at audit time: 2,778 House and 602 Senate filings marked failed/review-required. This includes older parser, identity and date problems; it is not a count of confirmed missing trades. Recent source coverage passed despite these historical failures keeping Capture Congress red.
- SEC HTTP 429 interrupted the wider insider recovery. The remaining 41 feed filings are preserved in `2026-09-19-insider-pending.json`. They have not been source-parsed, so neither their trade count nor database completeness is certified. Wider scheduled scans can revisit them; the feed is still bounded and is not a durable historical inventory.
- The local 13F audit examined 547 filings across 59 tracked funds: 385 periods matched, with 148 combined parse/coverage failures reported. Examples include absent Berkshire 2025-Q1 and D. E. Shaw 2026-Q2 rows, and holdings discrepancies for Hudson Bay, Armistice and Alyeska. A coverage mismatch can reflect parser/ticker resolution differences and needs source review before destructive correction. The separate GitHub recovery run 35428283072 was still running at the final status check.
- Alert delivery was not manually dispatched. A post-fix scheduled delivery success has not been verified.

## Verification

- Recent Congress audit: no source parse failures, row-count mismatches, transaction mismatches, publication-date mismatches or recent unknown-member rows in the checked window.
- Reviewed filing tests: 11 passed.
- SEC parser/pacing tests: 18 passed.
- 13F audit exit regression tests: 3 passed.

Unrelated local strategy/UI edits were not included in the operational commits.
