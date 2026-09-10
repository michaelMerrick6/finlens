# Reviewed paper filings

These compact transcriptions correct two scanned filings that the generic OCR
parsers read incorrectly. They are data exceptions, not a replacement for the
normal ingestion pipeline. Each physical transaction row is retained, including
identical trades in different accounts. `page` is the PDF page or Senate image
position (including the cover); `row` is the source table row. House rows are
numbered from the first transaction on each attachment page.

| Filing | Source rows | Initially observed database rows |
| --- | ---: | ---: |
| House 2026 / 9116328, Rohit Khanna | 244 | 40 |
| Senate 929216d5-5dbd-429c-858c-1e9332924627, Richard Blumenthal | 111 | 19 |

House source verification uses SHA-256 of the original PDF bytes. Senate
verification uses an ordered list of SHA-256 hashes of decoded RGB image bytes;
all nine source images, including the cover, must match. Changed sources fail
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
