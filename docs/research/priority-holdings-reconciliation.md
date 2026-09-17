# Priority holdings reconciliation — September 17, 2026

## Status

**The 12-person holdings rollout is incomplete. No additional public holdings pages or production estimates were enabled by this batch.**

The table below separates extracted evidence, account matching, and candidate calculations. A matching row count does not verify a current balance. Candidate calculations remain blocked from publication until source-row review, security mapping, amendments and corporate actions are resolved.

| Politician | Annual asset rows staged | PTR rows staged | Post-baseline account matches | Conditional account-level models | Remaining review |
|---|---:|---:|---:|---:|---|
| Nancy Pelosi | — | — | — | — | Existing Pelosi ledger retained; not re-verified by this batch. |
| Michael McCaul | — | — | — | — | Scanned/rotated annual and PTR matrix review. Latest annual found covers 2024. |
| Ro Khanna | — | — | — | — | Scanned annual and PTR matrix review. Structured notes must not be mapped to issuer common stock. |
| Alan Armstrong | — | — | — | — | New-filer valuation date and document-level joint ownership override. |
| Josh Gottheimer | 395 | 122 | 74 | 291 | New/changed securities, source rows and corporate actions. |
| April McClain Delaney | 250 | 339 | — | 11 | Repeated positions across children’s trusts; PTRs do not identify the individual trust. |
| Gilbert Cisneros | 643 | 883 | 590 | 515 | New/changed securities, account matches, source rows and corporate actions. |
| Cleo Fields | 68 | 31 | 22 | 43 | Four new/unmatched account positions; source rows and corporate actions. |
| Markwayne Mullin | 213 | 320 | 72 | 52 | 2024 baseline, ambiguous accounts and subsequent security changes. |
| Kevin Hern | 335 | 73 | 37 | 101 | Foundation exclusion, corrected Organon sale and deleted Versant sale, corporate actions. |
| Tim Moore | 72 | 35 | 19 | 48 | New/unmatched securities, source rows and corporate actions. |
| Dave McCormick | 351 | 413 | 1 | 12 | Repeated accounts, charitable assets and structured notes. |

## Completed in this pass

- OCR completed for 807 scanned pages across 30 documents. OCR completion is not row verification; rotated forms and checkbox meanings still need review.
- Re-extracted six readable House annuals: **1,763 asset rows**, including account-specific repeats and non-stock assets.
- Re-extracted 57 readable House PTR PDFs: **1,483 transaction rows**. Independent typed-row counts match; no remaining structural extraction issues in those files.
- Fixed numeric asset codes such as `[5F]`, page-spanning rows, borderless transaction tables, and descriptions that spill across value/income columns.
- Preserved source URLs, page references, original metadata and PDF hashes. Senate account containers are not added to asset values.
- Quarantined amended/deleted transactions and possible originals. Missing starting holdings are never changed to zero. An explicit House **Value of Asset: None** is treated separately, using the House instruction guide’s definition of a disposed asset. Senate **None (or less than $1,001)** remains uncertain.
- Added a tested conditional range calculator using baseline dollar brackets and daily trading-price ranges on one split basis. It rejects missing prices, unresolved events, impossible sales and unmatched accounts.
- Retrieved 725 security price series for candidate calculations. These are internal review artifacts, not verified holdings or exact share counts.

## Source findings that affect balances

- [Hern filing 20035196](https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20035196.pdf), page 1: Organon is **Amended**, allocated to Hern Family Revocable Trust; Versant is **Deleted**. Treating both as new sales would corrupt the balance. Possible originals are in [20035134](https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20035134.pdf). They remain quarantined pending explicit original-row linkage.
- Delaney’s annual has separate dependent-child trust holdings in the same stocks. A PTR owner of `DC` alone does not identify which trust sold.
- McCormick’s charitable gift fund and Hern’s foundation assets are excluded from personal-portfolio candidate calculations.
- [Senate disclosure guidance](https://www.ethics.senate.gov/public/index.cfm/financialdisclosure) distinguishes annual year-end values from new-filer valuations. Armstrong’s report title/filing date alone is not used as an exact asset valuation date.

## Review artifacts

- `priority-holdings-columns.json`: actual House financial columns and metadata.
- `priority-holdings-ledger.json`: account-aware positions, events, matches and correction flags.
- `priority-holdings-models.json`: conditional account-level ranges, price dates and blocking reasons.
- `priority-holdings-inventory.json` / `priority-holdings-transactions.json`: source inventory and original extracted evidence.

## Reproduce locally

Install `scripts/requirements.txt`; Poppler and Tesseract are needed for scanned sources. Run from the repository root:

```sh
python3 scripts/stage_priority_columns.py
python3 scripts/reconcile_priority_holdings.py
python3 scripts/stage_priority_models.py
python3 scripts/test_holdings_columns.py
python3 scripts/test_holdings_ranges.py
```

These jobs are read-only with respect to the production database. They do not activate a recurring job, deploy pages, or promote candidates to verified holdings.

## Additional source review and tests

- Visually reviewed Cleo Fields’ entire Schedule A (PDF pages 1–7), including the transition to Schedule B. Confirmed the 2025 annual identity, separate brokerage accounts, value versus income columns, and the explicit None entries. This verifies the annual source reading; it does not verify all later transactions or today’s balances.
- The [House instruction guide, printed page 51 / PDF page 57](https://wordpress-1275988-6249768.cloudwaysapps.com/wp-content/uploads/2026/07/7-8-2026-2025-Published-Instruction-Guide.pdf#page=57) defines Value of Asset “None” for assets no longer held at year end. This must not be confused with missing rows, income “None,” or the Senate under-$1,001 category.
- 47 Python pipeline test files and 71 backend tests passed. After the explicit-zero change, all 15 focused column/account/range tests passed again.
- Source hashes are checked against the staged inventory before ledger construction. A changed annual/PTR stops reconstruction.
