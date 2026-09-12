# Historical coverage baseline — September 11, 2026 (Pacific)

The baseline defines what is stored and what remains unclassified. It does not
claim complete history or verified OCR. The accepted seven-member roster gap is a
separate measure and remains accepted.

## Official congressional filing inventory, 2015–2026

| Chamber | Official PTR filings | Have recognizable stored transaction rows | No matching stored rows |
| --- | ---: | ---: | ---: |
| House | 7,655 | 6,580 | 1,075 |
| Senate | 2,064 | 1,915 | 149 |
| Total | 9,719 | 8,495 | 1,224 |

All annual House indexes and all paginated Senate PTR listings in that period were
compared with recognizable stored document keys. Senate counts are checked against
the feed's reported total; duplicate keys, changing totals and out-of-year results
abort the comparison. Source hashes and the full unmatched-key lists are retained
in historical-coverage-baseline.json.

These inventories include the filers returned by the official sources. They are
not restricted to the current congressional roster. An unmatched filing may be a
no-transaction report, an out-of-scope filer, an unsupported document, or a capture
gap. Conversely, a filing with stored rows may still be incomplete or inaccurate.
Legacy IDs and wrong stored filing years can also affect matching. No transaction
correctness percentage is inferred from these counts.

## Stored history at the database snapshot

| Dataset | Stored rows |
| --- | ---: |
| House transactions, all stored years | 86,386 |
| Senate transactions, all stored years | 18,092 |
| Insider transactions, 2015–2026 | 803,959 |
| Institutional holding rows, 2013–2026 | 470,286 |

SEC datasets have per-year stored counts and source-URL counts only. They were not
compared against the complete SEC filing universe. Holding rows are not filings.
The official congressional inventory boundary starts in 2015; earlier stored rows
are counted but are not covered by that inventory comparison.

The snapshot also records these review indicators:

| Indicator | House | Senate |
| --- | ---: | ---: |
| Rows without recognizable official document keys | 786 | 5 |
| Rows with unresolved member IDs | 3,721 | 710 |
| Transaction date later than stored filing date | 891 | 77 |
| Extra rows sharing an identical non-null document ID | 0 | 0 |

These are indicators for source review, not approval to rewrite dates, merge
people or delete transactions. Zero duplicate IDs does not rule out economic
transaction duplicates under different IDs.

## Coverage policy for the rebuilt frontend

- Describe the dataset as available disclosed records. Do not claim every trade,
  every historical filing, or complete portfolios.
- Keep filing presence, transaction verification and roster coverage separate.
  An empty feed does not establish that a politician made no trades.
- Unsupported or ambiguous OCR stops the entire document for review. The House
  scan path now also rejects unreadable PDFs and pages with no recognized layout,
  including possible cover pages. This intentionally favors review over partial
  publication. Do not resume broad OCR backfills until that work is explicitly resumed.
- A future historical reconciliation should classify each unmatched key as
  verified no-transactions, explicitly excluded, review-required, or capture-needed.
  Verify source rows before any repair and remeasure after applying it.

## Reproduce

The audit is read-only and never fetches or parses filing PDFs. Install the normal
pipeline dependencies plus `psycopg[binary]` in the audit environment. Supply
DATABASE_URL through the local environment; optional connection overrides belong
in an ignored file.

```sh
python ops/audit_historical_coverage_baseline.py \
  --start-year 2015 --output ops/historical-coverage-baseline.json
```

Use `--connection-options /path/to/ignored-options.json` only if the environment
requires a session pooler or host-address override. The database snapshot is
repeatable-read; official inventories are fetched afterward and may change later.
