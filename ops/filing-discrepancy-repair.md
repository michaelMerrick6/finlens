# Filing discrepancy repair — September 10, 2026

## Verified paper filings

The originals contained substantially more transactions than either the old OCR
output or the stored data. Every attachment page was reviewed; identical trades
in separate accounts were retained.

| Filing | Verified rows | Initially stored | Stored immediately before repair |
| --- | ---: | ---: | ---: |
| [Rohit Khanna, House 9116328](https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/9116328.pdf) | 244 | 40 | 40 |
| [Richard Blumenthal, Senate 929216d5-5dbd-429c-858c-1e9332924627](https://efdsearch.senate.gov/search/view/paper/929216d5-5dbd-429c-858c-1e9332924627/) | 111 | 19 | 15 |

The scheduled importer changed stored rows between the initial audit and repair.
The concurrency check rejected the stale plan; a new snapshot and plan were
prepared before applying corrections.

The transaction committed 355 verified trades, corresponding raw filing records,
and corresponding signal events. It replaced 16 stale derived records and
upserted 198 corrected derived records, including updates to 60 existing clusters.
Two sent alert records and their signal UUIDs were preserved. Six pending,
unpublished social drafts based on stale records were removed after backup. No
delivery or publishing workers were run.

A full dry run rolled back successfully before the repair. The applied transaction
verified all trade, raw-filing, and signal fields against the plan and checked that
sent alert records remained unchanged. Durable before-value backups and the exact
plan/results are in ignored `artifacts/filing-repair/`.

## Code and database corrections

- Added compact source-verified transcriptions with page, row, account, owner,
  source URL, and source hash. They apply only to the exact reviewed PDF/images;
  changed sources fail and require review. Normal parsers handle other filings.
- Preserved transaction dates separately from notification, maturity, and option
  expiration dates; corrected disclosure buckets and purchase/sale direction.
- Retained reviewed issuer ticker mappings. Private assets, structured notes,
  hybrid perpetual securities, municipal bonds, and unresolved issuers do not
  inherit an unrelated common-stock ticker.
- Fixed the summary compiler so grouped tickers retain their original filing key,
  producing one filing summary instead of a separate summary per ticker. Summary
  counts still follow the existing publishable-signal filtering policy; they are
  not the total count of all disclosed private/non-stock transactions.
- Fixed Senate HTML parsing to preserve the source asset-type column, including
  Non-Public Stock.
- Added the missing `signal_events(raw_filing_id)` index concurrently in production
  and included it in schema/migration files. The missing FK index caused repeated
  full-table scans and a safely rolled-back repair timeout. The new index was
  confirmed ready and valid.

## Validation and remaining scope

All 30 Python test files pass, including source-change rejection, source row
counts, distinct repeated transactions, corrected amounts/dates, filing summary
identity, and non-public asset types. The real downloaded PDF and Senate images
also pass the source-hash checks.

These are targeted repairs, not proof of complete historical coverage. Broader
OCR template support and filing inventory reconciliation remain necessary.
BlackRock Funding and FedEx Freight common-stock ticker mappings remain unresolved
in this filing. The seven missing current members remain the explicitly accepted
roster gap.

## Latest Senate filing

The previously missing [Cory A Booker filing of September 9](https://efdsearch.senate.gov/search/view/ptr/e5144197-e49c-4f74-bec4-19f1f3124668/)
was captured by the scheduled importer during this work. All seven rows were
compared with the official HTML table. One non-public stock classification was
corrected in the trade, raw-filing payload, and signal payload, preserving their
IDs. Dates, directions, amount ranges, and tickers matched the source.

The final bounded audit covered the latest five House and five Senate filings.
All ten completed with zero row-count, transaction, publication-date, parse,
or unknown-member discrepancies. Results are saved in
[congress-post-repair-audit.json](congress-post-repair-audit.json). The three
reviewed filings were included in that sample.
