# Congressional reconciliation: dry-run review

## Results

The read-only proposal in `congress-roster-proposal.json` contains seven canonical additions, 56 existing-member metadata differences, and 88 active-status exceptions. It contains no executable mutations, automatic merges, or deletions.

The metadata differences comprise 54 first-name changes, three last-name changes, and one party change across 56 records. These are source-versus-database differences, not automatically approved corrections. Preserve aliases when canonical display names change; review the party discrepancy against term/source dates.

Three proposed additions have no same-surname candidates; four have candidates requiring review. Surname alone is not identity evidence. Alan Armstrong has both `A000377` and `unknown-alan-armstrong` as surname candidates. In particular, the fallback record requires filing-level identity verification before its trades or follows can be relinked. Never merge all surname candidates.

## Storage baseline

`congress-storage-baseline.json` contains exact database transaction-row counts by chamber and publication year, 2015–2026 inclusive:

- House: 80,398 stored rows.
- Senate: 17,170 stored rows.
- Every chamber/year bucket has rows.

These totals include all stored asset types and sources, may include duplicate transactions, and do not establish complete filing coverage. Official filing denominators remain unmeasured. The current year is partial.

## Checks and constraints

Seven new regression tests exercise official House/Senate parsing, empty/duplicate rosters, named members lacking IDs, vacancies, surname collisions, before/after diffs, nonmutation, and member inventory pagination beyond 1,000 rows. They are automatically discovered by `scripts/run_tests.py`.

Existing recent-filing audit tools compare a bounded sample of indexed filings and parsed trades. They are not a historical completeness census. Some supporting ingestion modules also expose database-writing paths, so the next filing audit should use explicitly isolated read-only code paths and preserve source outcomes.

## Next actions

1. Verify each proposed canonical addition and fallback alias against filings, prioritizing Alan Armstrong.
2. Review the 88 active flags against term history; preserve former members and their records.
3. Prepare a transactional repair with before-values, alias mappings, and reference counts. Do not execute the JSON proposal as a blind upsert.
4. Establish official filing inventories by chamber/year and reconcile captured/parsed/failed/no-trade outcomes.

No production records, notifications, schedules, or deployments were changed.

## Follow-up: accepted gap and audit safety

The user accepted the seven missing canonical members as a known gap. Their addition/repair is deferred and does not block the remaining backend work. The measured roster coverage remains unchanged.

The 88 active-status exceptions break down into 82 fallback IDs and six canonical IDs. They must not be treated as 88 confirmed former members or mass-deactivated.

Code inspection found metadata writes in shared House/Senate parsing helpers: unknown member resolution could insert placeholders, and company resolution could upsert company records. Added a context-local read-only parsing policy and applied it to both recent-congress audit functions. Ingestion keeps its existing write behavior outside this scope. Three regression tests verify that audit metadata paths make no database calls and that nested scopes/exceptions restore the policy. All 25 Python test files passed.

A five-filing-per-chamber, 30-day full parser audit was attempted, then interrupted after more than two minutes in House OCR without a completed summary. It is not counted as a successful coverage check. A lighter reference-only check found stored rows under each of the latest five House PTR filing prefixes (9, 66, 40, 1, and 2 rows). Evidence is in `recent-house-reference-check.json`; transaction correctness, completeness, and Senate coverage remain unverified.

Next: give each audited document a total time budget and save per-document results as they finish; classify timeouts explicitly. Then compare official filing inventories against stored/parsed outcomes, without requiring the accepted missing-member gap to be repaired first.

## Identity matching correction

Removed the House/Senate importer fallback that assigned a filing to the sole matching surname despite a conflicting or missing first name. Compatible name matches must now resolve to one distinct ID; multiple compatible IDs remain unresolved instead of using database ordering or current active status. Existing name-normalization/alias rules remain in use and require further source-level validation; this is not a complete identity-resolution redesign.

Added four regression tests covering conflicting first names, ambiguous initials in both row orders, matching former members versus active namesakes, and missing first names. All 27 Python test files passed. Existing production identities/trades were not rewritten; future unresolved matches can increase until reviewed aliases are supplied.

A new latest-filings sample completed all five comparisons with a 30-second document limit. One Senate filing dated September 9, 2026 (`e5144197-e49c-4f74-bec4-19f1f3124668`) parsed seven transactions but had no stored rows at audit time. This is a current capture discrepancy; check pipeline timing before classifying it as a persistent ingestion failure. Evidence: `congress-followup-audit.json`. New filings changed the sample, so this run alone does not resolve the two previously timed-out documents.

The exact previously timed-out filings were also retried independently of the rolling sample. House filing `house-2026-9116328` still exceeded 30 seconds. Senate filing `929216d5-5dbd-429c-858c-1e9332924627` completed and returned 23 parsed rows versus 19 stored rows, with no detected filing-date or unknown-ID discrepancy. This row-count mismatch requires row-level comparison and the same deduplication/normalization rules used by ingestion before concluding that four transactions are missing. Evidence: `congress-targeted-followup.json`. No repair was applied.
