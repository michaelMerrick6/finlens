# Congress-wide holdings rollout

## Current state

Not Congress-wide yet. Pelosi retains her separately reviewed estimates. The read-only coverage audit found 491 stored House holdings rows for one other member (B001325), including candidate/new-filer reports. These are not 491 validated current positions. Most stored annual rows do not have a source-established valuation date.

The database marks 628 member records active (513 House, 115 Senate). This is a database hygiene finding, not the size of Congress. Reconcile IDs and current terms before calculating population coverage. Exact-name index matching found annual document candidates for 277 House records; candidates are not verified baseline attribution. Senate annual holdings require a separate adapter. See `congress-holdings-coverage.json` for per-record evidence.

## Implemented groundwork

- Shared quantity roll-forward engine for either chamber, keeping owner and asset identity separate; explicit share additions/disposals and confirmed splits; duplicate and negative-balance rejection; ambiguous events held for review.
- Fixed general annual-reader selection: candidate/new-filer reports are not annual baselines; newer None rows cannot resurrect older positive holdings; owners/accounts are no longer merged by ticker; competing same-date documents/amendments require review.
- General estimator uses valuation date rather than filing date. Missing annual baselines no longer fall back to constructing a portfolio from transactions alone.
- Repeatable, read-only coverage inventory across stored member records and 2024-onward House indexes.

## Remaining work before user-facing rollout

1. Resolve current member roster and exact filer identity; import and validate latest House annuals, with document-level completeness and zero-asset records.
2. Build Senate annual source adapter, preserving owners, reporting dates, amendments and source documents.
3. Preserve explicit owner, share/contract counts and original descriptions in transaction ingestion across both chambers; existing dollar-only trade rows cannot safely supply these fields.
4. Feed validated baselines and subsequent transactions into the shared engine; add historical-price/corporate-action adapters for bounded models, keeping assumptions visible. Do not activate the older general midpoint/cost-basis portfolio implementation as if it were equivalent to the Pelosi reviewed ledger.
5. Reuse the holdings UI only for members with usable snapshots, with source dates, ranges, coverage warnings and unavailable states.
6. Generalize the daily updater and apply the pending database migration/deployment. Keep the once-daily cadence requested by the user.

No Congress-wide snapshots were fabricated or deployed in this pass. Existing Pelosi estimates are unchanged.

## House document staging pass

`python scripts/prepare_congress_holdings_baselines.py` downloads/caches the newest candidate annual for each exact-name matched House record, preserving document hashes and raw ownership/asset entries. Pelosi is excluded so the manually reviewed ledger remains authoritative. Source PDFs live under ignored `artifacts/congress-holdings/sources`; the staging manifest is `congress-house-baselines.json`.

The checks compare document name, Member status, Annual Report type, reporting year, filing date and filing ID with the candidate index. Ordinary calendar-year annuals use December 31 of the explicitly printed Filing Year, under the House annual instructions: https://ethics.house.gov/wp-content/uploads/2026/04/2025-Final-Instruction-Guide.pdf . Explicit alternative reporting periods, amendments/competing filings, scanned pages and incomplete asset blocks are held for review. Blank ownership fields remain unspecified. Duplicate tickers across accounts/owners remain separate. No count of parsed rows is treated as a count of confirmed stocks.

During staging, found that missing PDF font glyphs reduce Schedule B headings to `S B: T`; the existing parser had continued reading those transaction rows as assets. Added a section-boundary check and regression test, then reran the batch. Every staged document remains `current_holdings_eligible: false`. Machine checks are not a substitute for source review or post-baseline transaction reconciliation, and no new rows were written to production.

## Cross-page account continuation repair

Some annual asset rows start with an account heading and value at the bottom of a page, with the typed asset name on the next page. The sequential parser had attached that value to the previous asset and left the next asset unparsed. Added a narrow repair for an explicit account arrow plus complete value immediately followed by a typed asset with no trailing fields. Preserves any explicit owner, leaves blank owners unspecified, and refuses to reassign a value when the next asset has its own fields. Also removes standalone transaction-threshold header fragments (`$1,000?`, `$200?`) from section lines.

Verified against Alford annual 10076207: the First Trust Managed Municipal ETF (FMB) entry now retains its Edward Jones IRA account and $1,001–$15,000 bracket. All 54 asset blocks in this document parse after the repair. This is source-text verification, not confirmation of current ownership or manual approval of the full document. Seven parser validation tests cover both repairs and rejection paths.

## Split value-column repair

Added recovery for an account heading containing a partial dollar range whose upper bound appears after the typed asset name, including wrapped asset names. Requires the account arrow, no intervening account or typed asset, and an exact standard House dollar bracket. Income-column amounts are never selected as the upper bound unless they exactly occupy the expected first value continuation; conflicting patterns remain unparsed. Source checks include filing 10078132 (VB Pointe West: $5,000,001–$25,000,000), filing 10074745 (EFV: $100,001–$250,000, joint ownership), and filing 10075256 (BOND: $50,001–$100,000). These are historical disclosed brackets, not current values.

Damaged-font `D:` and `L:` metadata labels no longer become prefixes on the next asset's name. Eleven tests cover identities, annual dates, schedule boundaries, owners, account continuations, split ranges, income separation and conflicting upper bounds. Staging remains ineligible for current-holdings publication until source review and roll-forward reconciliation.

## Wrapped names, account identity and undetermined values

Extended the complete-value continuation repair to wrapped asset names. It now anchors the value directly after the account marker/optional owner, so a later complete income bracket cannot stand in for a missing asset-value bracket. `Undetermined` is retained literally (e.g. joint-owned State of Arizona Pension in filing 10076619), never converted to zero. Standalone account headings are preserved when the following asset name wraps onto multiple lines, keeping IRA and non-IRA entries distinguishable in addition to their separate row IDs.

Source checks: FNODX in 10077486 retains its OKD 529 account and $1,001–$15,000 bracket; BPRIX entries in 10075256 retain their $15,001–$50,000 brackets; pension ownership and unknown value are preserved. Fifteen tests cover the supported cases and income/asset-range ambiguity rejection. Staged rows remain unavailable as current estimates until reconciliation and review.

## Filing identity suffixes and explicit empty annuals

Preserve the printed member name and record whether identity matched exactly or after normalizing only a terminal generational suffix (Jr., Sr., II, III, IV). Different explicit suffixes, first names, middle names, or surnames still fail. Five source PDFs add a suffix absent from the matched index name; four now pass all machine checks, while the fifth retains an independent asset parsing issue.

Filing 10082756 explicitly states “None disclosed.” in Schedule A before Schedule B. This now becomes an explicit empty annual baseline rather than a missing-assets parser failure. The check requires the bounded section to contain only that declaration; absent sections and conflicting asset text remain unresolved. An empty annual is not evidence of no current holdings.

Full cached rerun: 276 candidate documents, 178 passing machine checks pending review, 98 requiring review, and 11,859 parsed asset entries. All remain current_holdings_eligible=false. Eighteen regression tests pass. No production data or public holdings estimates were changed.

## Asset-value column validation and rejected-row evidence

Value parsing now requires the value at the start of the column after an optional owner code. It no longer searches past missing or broken asset values into income columns. Explicit “Spouse/DC Over $1,000,000” categories are retained verbatim alongside the separate owner field. This preserves open-ended source values without inventing an upper bound.

Dollar-only continuation lines cannot become the next asset name. A typed asset with a split bracket can be rejoined with the immediately following line only when the first amount completes the exact standard disclosure bracket. In filing 10074497, Vanguard Long Term Treasury and Vanguard Target 2015 both retain $100,001–$250,000 asset values; the former's $5,001–$15,000 income does not replace its holding value.

Filings 10074497 and 10078274 now pass machine checks. Filing 10076386 now requires review: its damaged VB column extraction includes an income amount where the old parser could select a value; unresolved text is retained rather than accepted. Specifically the VB block contains “Spouse/DC OverDividends” with the $1,000,000 continuation out of order.

Each rejected block now includes its extracted text and original block index in the staging manifest. Parsed row indices also retain their original positions when an earlier block is rejected, preserving traceability. Full rerun: 179 machine-checked pending review, 97 requiring review, 11,840 parsed entries across 276 candidate documents. Fewer accepted entries reflect stricter validation, not removal of source evidence. All remain ineligible for current-holdings publication. Twenty-four regression tests and Python compilation pass; no deployment or production writes.

## Second reviewed pilot after Crockett

Thomas Massie (M001184) now has a local holdings page, supported by visual review of both annual pages and current index checks. One public-stock estimate (TSLA) and four historical non-stock entries are shown separately. See massie-holdings-review.md for the evidence. The reviewed-pilot registry now drives shared source checks, valuation and UI for Crockett and Massie. Pelosi retains its separate transaction ledger. These three local pages are not Congress-wide completion; all unreviewed staging documents remain ineligible for publication.
