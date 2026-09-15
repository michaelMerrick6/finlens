# Reviewed paper filings

These compact transcriptions correct scanned filings that the generic OCR
parsers cannot read reliably. They are data exceptions, not a replacement for the
normal ingestion pipeline. Each physical transaction row is retained, including
identical trades in different accounts. `page` is the PDF page or Senate image
position (including the cover); `row` is the source table row. House rows are
numbered from the first transaction on each attachment page.

| Filing | Source rows | Initially observed database rows |
| --- | ---: | ---: |
| House 2026 / 9116328, Rohit Khanna | 244 | 40 |
| Senate 929216d5-5dbd-429c-858c-1e9332924627, Richard Blumenthal | 111 | 19 |
| House 2026 / 9116267, Rohit Khanna | 274 | queued for review |
| House 2026 / 9116290, Charles Fleischmann | 11 | queued for review |
| House 2026 / 9116292, Tracey Mann | 10 | queued for review |
| House 2026 / 9116308, Tony Wied | 7 | queued for review |
| House 2026 / 9116311, Harold Rogers | 0 (explicit declaration) | queued for review |
| House 2026 / 9116326, Tony Wied | 8 | queued for review |
| Senate 3a4c5095-028a-4614-a692-836719da4e63, Richard Blumenthal | 46 | queued for review |
| Senate ec20cd93-6702-4a29-b3a6-983f4b17f365, Richard Blumenthal | 32 | queued for review |

House source verification uses SHA-256 of the original PDF bytes. Senate
verification uses an ordered list of SHA-256 hashes of decoded RGB image bytes;
every source image, including the cover, must match. Changed sources fail
closed and require a fresh review. Unknown filings still use the normal parser.

The source URLs, filing dates, page/row positions, owner markers, and account
headings are included in each manifest. Names are normalized transcriptions;
transaction dates come from the transaction column, not notification dates or
option/bond maturities. All amounts are the selected disclosure buckets, never
estimates of exact trade values. Purchase/sale columns are separate from the
capital-gains and partial-transaction checkboxes.

Stock tickers were matched to the existing company reference by reviewed issuer
identity. Structured notes, hybrid perpetual securities, private entities, and
municipal bonds remain N/A rather than inheriting an issuer's common-stock ticker.
BlackRock Funding and FedEx Freight common-stock rows remain unresolved because
the reference did not establish their exact ticker. Option rows retain OP asset
type; printed underlying tickers are retained where established. XSP index options
remain N/A. Senate spouse markers are never interpreted as ticker S.

Original downloads, rendered pages, extraction scratch files, database snapshots,
and transactional repair plans are kept in ignored `artifacts/filing-repair/`.
Do not commit production alert payloads or destinations. The repair command is
`ops/apply_reviewed_filing_repair.py`; it defaults to a transaction that rolls back,
requires the existing Congress repair opt-in, validates current records against
the reviewed plan, writes a durable backup, and verifies readback before commit.

The September 15 review preserves Mann's printed 2024 transaction dates despite
his 2026 filing date. Rogers's single-page declaration explicitly says nothing
to report; an empty manifest requires both `verified_no_trades` and a review note.
The normal House parser verifies the PDF hash and filing date before accepting it.
Changed sources or unreviewed empty extractions still fail closed.

The new Khanna and Senate transcriptions were checked against the full source
pages and a separate pixel-based check of transaction and amount columns.
Khanna's Cigna row has extraneous ink in a different amount column; its selected
first amount column was confirmed visually. Review downloads, enlarged crops,
checks, and production readback results are in ignored `artifacts/congress-review/`.
The new filing rows use the atomic phase14 publisher through the normal parser.

Issuer checks for the smaller House ETF rows include
[Schwab SCHO](https://www.schwabassetmanagement.com/products/scho),
[iShares IEUR](https://www.ishares.com/us/products/264617/ishares-core-msci-europe-etf),
and [ProShares REGL](https://www.proshares.com/our-etfs/strategic/regl).
Unverified OTC and renamed issuer tickers remain N/A rather than being guessed.
