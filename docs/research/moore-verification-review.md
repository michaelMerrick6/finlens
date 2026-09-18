# Tim Moore — verification review, September 17, 2026

**Decision: source rows reviewed; current portfolio not fully verified. A dated partial-estimate page is available; unresolved positions remain excluded from its subtotal and ranking.**

## Completed

- Reviewed all 72 annual Schedule A rows against pages 1–6 of the 2025 annual report.
- Reviewed all 35 transaction rows in nine 2026 PTRs, including continuation pages.
- Checked the ten source PDF hashes against the reviewed inventory. Fixtures preserve the reviewed values.
- Reviewed annual Schedule B page 19 for the HY conflict.
- Isolated accounts: duplicate tickers across accounts are separate positions. Blank PTR account labels are not direct account evidence.
- Explicit year-end `None` with no later changes produces a conditional zero without requiring an unavailable quote. SMCYY is not silently mapped to SMCY.
- Recorded provider split checks and primary-source confirmation for GNPX and TZA. This is not comprehensive clearance of all corporate actions.
- Reconstructed the HON/HONA issuer conversion using the $195.09 unadjusted December 31, 2025 close and the legal 1-for-2 distribution and reverse-split ratios. See `moore-honeywell-reconciliation.json`. No adjusted-history ratio is reused as a share multiplier.

## Candidate inventory

The general model contains 33 positive account candidates (32 distinct tickers), 16 conditional zero accounts, two blocked annual positions, and four additional tickers without established starting balances. These counts are **not verified holdings counts**.

Honeywell is a separate reviewed scenario: approximately 38–128.146 shares each of HON and HONA, conditional on retaining the original position and subsequent entitlements. The lower endpoint allows fractional cash-out; cash proceeds are excluded. Neither entitlement nor current ownership is confirmed by Moore's filings. The general model retains its corporate-action block.

## Evidence needed to close the remaining gaps

| Issue | Evidence found | What would resolve it |
|---|---|---|
| HY opening balance | Annual Schedule A says None; Schedule B records a December 31 purchase; January PTR records a January 5 sale. | Corrected annual filing or dated account-level quantity evidence resolving which account held the shares at year-end. |
| T, SMPL, IHG, RYCEY | Subsequent trades exist; no reviewed starting positions. Older candidate/new-filer report text supplies no missing balances. | Dated starting quantities, explicit zero-position evidence, or complete account history establishing entry from zero. |
| HON/HONA ownership | Issuer ratios and price basis sourced; no subsequent HON/HONA transactions in reviewed PTR inventory. | Account-level entitlement/retention evidence, or acceptance of a clearly conditional estimate. |
| Other corporate actions | Provider split checks plus selected issuer confirmations. | Complete applicable merger, spinoff, distribution, split and fractional-settlement reconciliation. |
| Exact balances generally | Annual values and most transaction amounts are brackets, with unknown execution prices; PTR accounts are blank. | Share quantities and account identifiers, or acceptance of ranges and explicit assumptions. |

Matching purchase/sale dollar brackets do not establish equal share counts. Plain `S` does not establish a complete exit. Absence from the annual report does not establish a zero starting balance. No arithmetic change can turn those gaps into verified quantities.

## Primary sources

- [2025 annual report](https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2025/10075481.pdf), Schedule A pages 1–6; Schedule B page 19.
- [January PTR](https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20033856.pdf), HY purchase and sale.
- [Honeywell proxy](https://www.sec.gov/Archives/edgar/data/773840/000077384026000023/hon-20260305.htm), Outstanding Equity Awards footnote 2, year-end closing price.
- [Honeywell Aerospace distribution](https://www.honeywellaerospace.com/us/en/company/newsroom/2026/06/honeywell-aerospace-spin-off-begins-trading-nasdaq).
- [Honeywell reverse split](https://investor.honeywell.com/reverse-stock-split).

Machine-readable evidence: `moore-source-review.json`, `moore-split-evidence.json`, `moore-honeywell-reconciliation.json`, and `moore-verification-status.json`.

## Follow-up: preserve incomplete position evidence

The candidate generator now retains a `flow_model` for Moore's unanchored positions after checking transaction source hashes against the reviewed inventory. It reports share changes from the disclosed transactions, with current share totals explicitly unknown. These rows cannot enter portfolio totals or rankings. Negative net flow remains negative; matching dollar brackets never imply a full exit.

The separate `moore-missing-balance-evidence.json` records the calculation inputs and HY conflict. The fresh 2025/2026 House index check in `moore-latest-index-check.json` found no later annual amendment or additional 2026 PTR beyond the reviewed inventory. This improves the treatment of incomplete evidence; it does not supply the missing starting balances or resolve the contradictory HY filing.

## Account audit and fractional-share follow-up

The account-evidence audit is complete (`moore-account-audit.json`): all 35 PTR rows omit an account. Of these, 19 post-baseline events were linked by a unique annual-position inference, 12 lack an established baseline account, and four belong to the baseline period. Candidate models now expose the inferred attribution explicitly. Further processing of these same rows cannot confirm an undisclosed account label.

Two split settlement policies are now reflected as conservative allowances in Moore's modeled positions, without applying split ratios twice:

- **GNPX:** [2026 second-quarter SEC report](https://www.sec.gov/Archives/edgar/data/1595248/000143774926027846/gnpx20260630_10q.htm) specifies rounding up fractions. The upper bound is widened to the next whole share, while the lower bound allows broker fractional custody.
- **TZA:** [issuer split notice](https://www.direxion.com/press-release/direxion-to-split-nine-etfs) specifies redemption of fractions. The lower bound is widened down to the whole-share floor, while the upper bound allows fractional custody. Cash proceeds are not included.

These narrow adjustments require current reviewed split evidence and no subsequent transactions. Changed evidence or a later transaction returns the affected position to review.

Additional primary-source checks:

- [IHG currency-change FAQ](https://www.ihgplc.com/en/investors/shareholder-centre/faqs-share-price-currency-change): the January 2026 London quotation-currency change does not affect the New York ADR listing. It must not create an ADR share conversion. [ADR holder information](https://www.ihgplc.com/investors/shareholder-centre/adr-holders) identifies one ADS per ordinary share.
- [Rolls-Royce payments](https://www.rolls-royce.com/investors/shareholder-information/payments-to-shareholders.aspx): ordinary cash distributions do not establish that Moore reinvested in more ADRs. No reinvestment is invented.
- [Verizon acquisition terms](https://www.verizon.com/about/sites/default/files/Frontier-Press-Release.pdf): cash paid for Frontier shares does not convert Moore's existing VZ share count.
- [DoorDash acquisition information](https://ir.doordash.com/resources/Acquisition-and-Tax-Information/default.aspx): the Deliveroo acquisition occurred before Moore's year-end baseline; it is not a new 2026 share entitlement inferred for this portfolio.

These targeted checks do not constitute complete issuer-by-issuer corporate-action coverage. The existing HY conflict, missing baseline quantities and absent account identifiers remain external evidence requirements. Full current-portfolio verification remains blocked; the review does not authorize publishing a verified portfolio.

## SEC filing inventory review — September 17 follow-up

Screened **57 selected SEC primary filings across 32 mapped issuers**, covering the latest 10-Q/20-F and 2026 primary documents for 8-K items 2.01, 3.03 and 5.03, Forms 25 and S-4. All 32 recent-submission inventories extend back beyond January 1, 2026. No selected document fetch failed. The scope contains 38 symbols; 6 lack a mapping in the SEC company ticker file (AMZY, JPST, MSTY, RYCEY, SMCY, TZA). Exact symbols and source hashes are in `moore-sec-corporate-action-screen.json`.

Findings from the identified notices:

- **AMZN:** the Globalstar exchange terms apply to Globalstar holders. They do not multiply Moore's existing Amazon shares.
- **BKR:** the July acquisition notice cashes out Chart common stock. It does not convert existing Baker Hughes shares.
- **V:** the exchange offer addresses Visa Class B holdings, not the publicly traded Class A security represented by ticker V in this candidate model.
- **HON, MS, T, V and WMT:** the Forms 25 identified in this screen describe debt securities. They must not remove the candidate common-stock positions.
- **CBRL:** diluted earnings-per-share conversion language is not a new conversion of Moore's common shares.
- **GNPX and HON:** the primary filings agree with the previously reviewed reverse split and Honeywell separation.
- **IHG:** historical consolidation references and standard depositary-fee provisions do not establish a new 2026 ADR conversion.

No additional applicable share conversion was identified in these selected documents. This is a completed, bounded filing screen—not proof that every corporate action has been captured. Primary-document screening excludes exhibits, most 8-K items, foreign 6-K reports and fund-specific filings. Fund/ADR issuer checks and the provider split evidence supplement it; actual reinvestment, fractional custody and account transfers remain unknown.

**Verification disposition:** HY, missing starting balances and blank account labels cannot be resolved by this issuer review. An amended disclosure or dated account/position evidence is still required. Do not promote the portfolio to fully verified, retain the unresolved evidence requirements explicitly.

## Expanded current-report screen completed

The follow-up collected all **339 captured 2026 Form 8-K/6-K current reports** for the SEC-mapped issuers and explicitly linked EX-99 documents: **393 documents**, with no fetch failures. Flagged passages were reviewed and classified in `moore-final-evidence-disposition.json`. The flags concern the known GNPX/Honeywell actions, IHG historical/depositary provisions, and Visa's Class B exchange offer. This is keyword-based screening with review of flagged passages, not exhaustive assurance over every possible corporate event.

The YieldMax closure notice concerns ABNY, DISO, FEAT and FIVY, not the modeled AMZY/MSTY/SMCY holdings. Moore's May 2026 amendment to his older new-filer report supplies no missing 2025 year-end quantities for the unresolved symbols.

**Decision needed:** a usable, clearly labeled partial estimate can retain the source-backed calculations and visibly exclude unresolved positions from totals. A fully verified portfolio still requires additional position/account evidence and resolution of HY. Neither repeated calculations nor additional issuer filings can establish Moore's undisclosed opening quantities. Publication remains disabled pending the completion standard; no verified status has been granted.

## Local presentation completed

Moore now has a local, dated partial-estimate page at `/politicians/M001236/holdings`, linked from his profile. It combines the 33 positive account scenarios into 32 stock/fund rows, ranks by estimated midpoint value, and charts only the modeled portion. Six unresolved source positions remain visible and excluded from values/weights. Sixteen conditional-zero account scenarios remain inspectable.

The presentation snapshot replaces any same-day price with a completed prior close and rejects stale, invalid or missing replacement prices. It does not claim full verification or automatic incorporation of later filings. The research pipeline's full-verification gates remain in place. No production deployment was performed.

Validation: snapshot regression tests, TypeScript typecheck, focused ESLint and existing reviewed-holdings tests passed; local route opened successfully and its ranked rows, partial subtotal, exclusions and source links were inspected in the browser.

## Six-position recheck — September 17, 2026

Freshly fetched the official 2024, 2025 and 2026 indexes. The 2025/2026 inventories are unchanged. Fresh downloads of the annual and all nine reviewed 2026 PTRs match the ten reviewed PDF hashes. Re-extracted the four older candidate/new-filer reports and amendments (10059443, 10071812, 10067159, 10076901); their text supplies no starting balance for HY, T, SMPL, IHG or RYCEY. Reconfirmed the HON reverse split and HONA distribution from issuer sources.

No position was promoted to verified. Honeywell's conditional entitlement calculation is already complete; confirmation of ownership is a separate evidence requirement. HY needs the source contradiction resolved. The other four need an opening balance. There is no newly found filing that supplies those facts. The page and its exclusions remain unchanged. Evidence and fresh source hashes are recorded in `moore-six-position-recheck.json`.

## Full first-term transaction reconstruction

Fetched and parsed all **21 PTRs indexed in 2025–2026**, containing **237 transaction rows** across all assets. The column parser reports no structural extraction issues. This is not a claim of visual verification of every one of the 237 rows. The 15 additional 2025 HY rows were visually checked on six PDF pages, with an independently transcribed test fixture; the two later HY rows were part of the earlier source review. Across the five target symbols there are no repeated date/action/amount/owner fingerprints and no amended/deleted rows.

| Target | First reported transaction | Target rows | Result of rolling forward with an explicitly assumed zero opening balance |
|---|---|---:|---|
| HY | August 7, 2025 purchase | 17 | Broad range includes zero today; modeled year-end minimum is about 500 shares, contradicting annual Schedule A None. |
| T | January 9, 2026 purchase | 6 | Approximately 0–5,496 shares; matching purchase/sale dollar brackets do not establish a full exit. |
| SMPL | February 3, 2026 purchase | 4 | Approximately 1,869–8,174 shares acquired, with no reported sales in the indexed history. |
| IHG | May 7, 2026 purchase | 1 | Approximately 6.7–102.5 shares acquired. |
| RYCEY | May 7, 2026 purchase | 1 | Approximately 57–889 shares acquired. |

These are **counterfactual zero-start scenarios**, not newly verified current balances. They assume no earlier ownership, complete disclosed activity, a long-only portfolio, and no unmodeled transfers or reinvestments. Price conversions use daily trading ranges on a consistent provider split basis. Corporate actions other than splits still require independent review. The three buy-only scenarios reproduce disclosed acquisition estimates; going farther back did not establish their opening positions. Neither scenarios nor annual transaction duplicates are added to the published holdings subtotal.

HY annual Schedule B includes the same 16 dated 2025 transactions found in the PTR chronology, including the December 31 purchase; these are corroborating reports of the same trades, not extra trades. Its Schedule A None remains inconsistent with the long-only roll-forward. Six older HY source pages: 20031020 p2; 20032256 p1; 20033413 p1; 20033564 pp1–2; 20033670 p1.

Evidence: `moore-full-transaction-history.json`. Reproduce with `scripts/reconstruct_moore_history.py`; test the independent HY transcription and scenario safeguards with `scripts/test_moore_full_history.py`. This research does not promote any position to confirmed ownership or change the live page.
