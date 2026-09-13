# Politician ranking audit — 2026-09-13 UTC

## Subsequent Pelosi source review

The snapshot below is superseded for Pelosi. Reviewing all four source filings represented in her rolling-year records identified three charitable contributions classified as sales, five stock acquisitions from exercised options (four wrongly labeled OP), and a public AllianceBernstein partnership purchase excluded by the asset-code filter. Source-reviewed presentation corrections now restore truncated ranges, distinguish contributions, identify stock exercises, and include that verified public partnership. Original database records are preserved; these corrections apply to directory ranking and the rebuilt disclosure/profile APIs. Other downstream pipelines have not been repaired by this frontend correction.

Pelosi's corrected ranking is **$16,600,023 minimum across 23 transactions**, versus $21,700,025 / 25 before this review. Three contributions totaling $6,100,003 minimum are excluded; the $1,000,001 public partnership purchase is included. Live assertions checked the total, all three contribution labels, and all five exercise descriptions. Source filings identify the owner as spouse. These records cannot establish current holdings or exact portfolio value.

Sources: https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2025/20033337.pdf ; https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20033725.pdf ; https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20034836.pdf ; https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20035143.pdf

## Earlier aggregate snapshot

Independently recomputed aggregates from 9,484 purchase/sale records dated 2025-09-13 through 2026-09-13, inclusive. Compared all 538 active directory members with the API: zero amount/count discrepancies and zero duplicate member IDs across pages. No repeated source-URL/document-row keys were found in the scanned records. This does not rule out duplicate transactions across separate filings.

Found and corrected a scope error: Jefferson Shreve's former $28,000,008 / eight-row ranking consisted of annuities labeled RILA, not stock ticker activity. House filings 20034084, 20033705 and 20033699 establish the annuity classification. Their amounts support the stored lower bounds, but do not justify inclusion in a stock ranking.

The ranking now includes identified stock, ETF and option records only (ST, STOCK, ETF, ET, OP). Unidentified tickers and other asset classes are excluded. Amounts sum reported lower bounds; purchases and sales both count, including household disclosures. Dollar values display without compact rounding. Missing activity is not evidence of no trading.

Verified leading totals in this snapshot:

| Member | Reported minimum | Rows |
| --- | ---: | ---: |
| Nancy Pelosi | $21,700,025 | 25 |
| Michael McCaul | $14,552,166 | 166 |
| Ro Khanna | $9,382,668 | 668 |
| Tony Wied | $6,402,028 | 28 |
| Kevin Hern | $3,883,084 | 84 |

These are internally reconciled available-record totals, not certified comprehensive trading volumes or portfolio returns. Every filing was not individually source-audited. Legacy amount truncation, missing tickers, classification errors, and incomplete history remain limits. Evidence scripts and snapshots are ignored under artifacts/ranking-audit.
