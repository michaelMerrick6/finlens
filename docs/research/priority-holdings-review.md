# Priority holdings review — screenshot list

Latest extraction and reconciliation results: [September 17 continuation](priority-holdings-reconciliation.md). The sections below record the initial source-inventory pass.

These are the 12 fully visible names requested by the user. Partial cards at the bottom were not identifiable and are not silently added. No new current-holdings estimate was published from this batch.

| Politician | Starting source | Current work remaining | PTR documents staged |
| --- | --- | --- | --- |
| Nancy Pelosi | Existing reviewed Pelosi ledger | Keep existing daily updater and reconciliation; no duplicate reconstruction | Existing ledger |
| Michael McCaul | [2024 annual · 55 pages](https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2024/9115586.pdf) | Scanned annual/PTRs: OCR and source review, then reconciliation | 19 |
| Ro Khanna | [2025 annual · 353 pages](https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2025/9116272.pdf) | Scanned annual/PTRs: OCR and source review, then reconciliation | 9 |
| Alan Armstrong | [New Filer Report for 03/24/2026](https://efdsearch.senate.gov/search/view/annual/37264946-5291-4f15-aea8-c23453c027c3/) | New-filer valuation date and assets + PTR reconciliation | 1 |
| Josh Gottheimer | [2025 annual · 71 pages](https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2025/10074410.pdf) | Automated annual checks passed; visual review and PTR reconciliation remain | 9 |
| April McClain Delaney | [2025 annual · 37 pages](https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2025/10074769.pdf) | Annual extraction issues, visual review and PTR reconciliation | 9 |
| Gilbert Cisneros | [2025 annual · 121 pages](https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2025/10074952.pdf) | Annual extraction issues, visual review and PTR reconciliation | 9 |
| Cleo Fields | [2025 annual · 23 pages](https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2025/10074378.pdf) | Automated annual checks passed; visual review and PTR reconciliation remain | 11 |
| Markwayne Mullin | [Annual Report for CY 2024](https://efdsearch.senate.gov/search/view/annual/1536b22a-9260-4cc9-9d88-736a81dfd3e7/) | Annual asset review + PTR/amendment reconciliation | 26 |
| Kevin Hern | [2025 annual · 32 pages](https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2025/10075834.pdf) | Annual extraction issues, visual review and PTR reconciliation | 10 |
| Tim Moore | [2025 annual · 24 pages](https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2025/10075481.pdf) | Annual extraction issues, visual review and PTR reconciliation | 9 |
| Dave McCormick | [Annual Report for CY 2025](https://efdsearch.senate.gov/search/view/annual/3e2b0cca-3ab7-428e-b2ce-f08691f27dd1/) | Annual asset review + PTR/amendment reconciliation | 24 |

## Changes made

- Added explicit source-name and district mappings to canonical member IDs for this list. This avoids routing McCaul, Khanna and McClain Delaney evidence into placeholder member IDs. No production member records were merged or changed.
- Annual selection now prioritizes reporting year before filing date. Same-year conflicting filings and amendments remain flagged. An older-year filing submitted later no longer displaces the newer-year annual. Older documents remain in the inventory for historical reconciliation.
- Added Senate annual staging with separate owner/value/income columns and parent-account paths. Account containers are not added to asset values; low/unknown brackets remain literal. Exemption flags use the checkbox state, not the presence of its label.
- Added local evidence caching for 136 PTR documents across the 11 new targets: 85 House PDFs and 51 Senate reports. All downloaded successfully. These are source documents, not 136 reconciled transactions.
- Preserved all raw extracted PDF pages, Senate tables, source URLs, filing dates and hashes. Scanned documents stay explicitly marked as requiring OCR.

## Important findings

- Khanna: visually checked the annual cover against Rohit Khanna, CA17, 2025 annual and August 10, 2026 receipt. The document has 353 scanned pages. The cover check is not verification of its holdings rows.
- McCaul: visually checked the 2024 annual cover against Michael T. McCaul, TX10. The latest annual found in the 2024–2026 indexes is 2024; the 2025 index contains an extension. The document is scanned/rotated and contains 55 PDF pages.
- Armstrong: new-filer report is not automatically treated as a December 31 annual snapshot. Its valuation date must be established before roll-forward calculations.
- Mullin: latest annual found in this search covers 2024; older amended annuals and subsequent PTRs are retained. An extension is not a new baseline.
- All three Senate sources have the omitted-assets checkbox unchecked. Merely extracting the checkbox label would incorrectly flag exemptions; a regression test covers this.

## Validation and next work

33 Python tests pass (26 House baseline tests and 7 priority/Senate tests). Rerunning the entire 276-document House batch after the year-selection fix yields 194 passing machine checks and 82 requiring review; those are not verified portfolios. All staged records remain current_holdings_eligible=false.

Next steps are OCR and row verification for scanned sources; annual asset review for text/HTML sources; then matching actual transaction dates, owners, assets, amendments and corporate actions to each starting balance. Do not use the unchanged-position Crockett/Massie model for these active traders.
