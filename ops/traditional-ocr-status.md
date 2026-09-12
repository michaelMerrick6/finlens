# Traditional OCR extraction

The Senate ruled-table scan path now uses OpenCV to align pages and locate grid
boundaries, Tesseract on individual text cells, and ink measurements inside checkbox
cells. No LLM or model API is involved. HTML/PDF text paths remain available.

Supported layout: the Senate paper form with a row-number column, asset column,
three transaction types, date column, and eleven amount columns. Every page's
transaction and amount headings are read and checked before using their positions.
The broad legacy 'Over $1,000,000' column remains distinct from the detailed ranges.

The extractor preserves physical rows, including equal transactions across accounts.
It handles the introductory enclosure letter, example rows above the numbered grid,
account headings, blank spacers and footnotes below the amount-column dividers.
Unknown layouts, missing/broken grids, conflicting or missing marks and unreadable
dates stop the document before publishing partial rows. The importer records these
as failures requiring review. Reviewed, source-hash-verified filings retain their
existing overrides. Company writes occur only after document extraction succeeds.

Validation:

- All 33 Python pipeline test files passed.
- The original nine-image Blumenthal filing is used for an offline regression:
  111 expected transactions across eight table pages; all dates, directions, amount
  ranges and normalized asset names matched. No reviewed-row override is used in
  this extraction. The manifest is used only after extraction for scoring and
  before extraction for original-image hash verification.
- The first transaction page also retained all ten correct financial records at
  half resolution and with a one-degree rotation.
- Synthetic tests cover grid borders versus marks, conflicting selections, all
  eleven amount columns, blank scans, rotation, and refusing partial documents.

Run the source regression (original cached images are intentionally untracked):

```sh
python ops/verify_senate_table_ocr.py \
  --dataset artifacts/ocr-benchmark/dataset-v1 \
  --output artifacts/ocr-benchmark/table-verification.json
```

Limits: this is one development filing, not a held-out or historical completeness
claim. Other form layouts and degraded handwriting can still require review. House
scan parsing remains separate. Its safety checks now reject missing or conflicting
checkboxes, unreadable transaction-date cells and future transaction years. The
first-date-anywhere fallback was removed because it could select an option expiry.
Equal transactions on separate physical rows are retained. These House changes
are regression-tested safeguards, not a demonstrated recovery of all House layouts. No OpenAI requests were made. Production capture and identity repairs are
documented in backend-release.md.
