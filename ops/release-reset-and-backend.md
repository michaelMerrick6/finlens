# UI reset and backend hardening release

This batch removes the previous UI and leaves a rebuild notice, preserves the 39 API route URLs, removes unused modules and UI dependencies, updates dependencies, and fixes pagination, member matching, and audit execution. Publishing main may deploy the rebuild notice; customer UI workflows remain unavailable until rebuilt.

## Additional fixes in the final pass

- Senate paper parsing strips noisy leading ownership markers before ticker detection. `(S)` can no longer become stock S solely because OCR inserted `=`, `»`, or a single-letter artifact before a spouse marker. Real company labels ending in `(S)` are retained.
- Congressional filing audits compare transaction field multisets (ticker, dates, direction, amount), not only row counts. They paginate stored rows with a stable order and a delimited filing prefix.
- Slow documents are isolated with configurable deadlines, subprocess cleanup, saved progress, and explicit failed outcomes.

## Unresolved production data: do not overwrite from OCR output

A longer read-only House parse of `house-2026-9116328` completed within a three-minute limit and returned 40 rows versus 40 stored. Eight signatures differ in dates, direction, or amounts. Some stored dates are later than the filing date. Matching row counts are insufficient to approve these records. Source-document verification is required before repair; the new audit exposes these discrepancies.

The Senate paper filing `929216d5-5dbd-429c-858c-1e9332924627` returned 23 rows versus 19 stored before the final ownership-marker fix. Differences include additional Teknova dates and overlapping TIC entries; neither count alone establishes the correct transactions. The ownership-marker bug also affects existing records, which have not been automatically rewritten.

The new Senate filing dated September 9, 2026 with seven parsed rows and none stored remains a captured audit observation, not a confirmed persistent pipeline failure. Check subsequent ingestion before backfilling.

The seven missing members remain an accepted gap. Active-status/fallback records have not been mass-edited. No production database writes or notification dispatches were performed in this batch. This release is a code and audit improvement, not a declaration that historical data is fully correct.
