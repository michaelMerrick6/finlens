# Priority holdings reconciliation — September 17, 2026

## Status

**The 12-person holdings rollout is incomplete. Pelosi has an existing estimate page; Moore now has a dated partial-estimate page. The other ten priority members still await holdings pages. These estimates do not establish fully verified current ownership.**

The table below separates extracted evidence, account matching, and candidate calculations. A matching row count does not verify a current balance. General candidate calculations remain blocked from publication until source-row review, security mapping, amendments and corporate actions are resolved. Moore’s separate snapshot exposes only explicitly labeled conditional estimates, with six unresolved positions excluded from the subtotal and ranking.

| Politician | Annual asset rows staged | PTR rows staged | Post-baseline account matches | Conditional account-level models | Remaining review |
|---|---:|---:|---:|---:|---|
| Nancy Pelosi | — | — | — | — | Existing Pelosi ledger retained; not re-verified by this batch. |
| Michael McCaul | — | — | — | — | Scanned/rotated annual and PTR matrix review. Latest annual found covers 2024. |
| Ro Khanna | — | — | — | — | Scanned annual and PTR matrix review. Structured notes must not be mapped to issuer common stock. |
| Alan Armstrong | — | — | — | — | New-filer valuation date and document-level joint ownership override. |
| Josh Gottheimer | 395 | 122 | 74 | 301 | New/changed securities, source rows and corporate actions. |
| April McClain Delaney | 250 | 339 | — | 13 | Repeated positions across children’s trusts; PTRs do not identify the individual trust. |
| Gilbert Cisneros | 643 | 883 | 590 | 524 | New/changed securities, account matches, source rows and corporate actions. |
| Cleo Fields | 68 | 31 | 22 | 44 | Annual and 31 PTR rows visually checked; four unmatched account positions and corporate actions remain. |
| Markwayne Mullin | 213 | 320 | 72 | 52 | 2024 baseline, ambiguous accounts and subsequent security changes. |
| Kevin Hern | 335 | 73 | 37 | 111 | Correction links reviewed; Organon trust opening balance and corporate actions remain. |
| Tim Moore | 72 | 35 | 19 | 49 | Dated partial page: 32 ranked stocks/funds; six unresolved positions excluded. HY conflict, missing baselines, conditional Honeywell treatment and inferred PTR accounts prevent full verification. |
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

- [Hern filing 20035196](https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20035196.pdf), page 1: Organon is **Amended**, allocated to Hern Family Revocable Trust; Versant is **Deleted**. Treating both as new sales would corrupt the balance. Possible originals are in [20035134](https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20035134.pdf). They were subsequently linked by source-locked visual review; see the correction pass below.
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

## Fields transaction review — continuation

- Refreshed the 2025 and 2026 House filing indexes: no new Fields filings relative to the inventory. Fresh downloads of the annual and all 11 staged PTRs matched their saved SHA-256 hashes; see `fields-source-check.json`.
- Visually checked all **31 transaction rows** on page 1 of those 11 PTRs against ticker, action, transaction date, amount bracket and account. Five December 2025 transactions belong to the annual baseline period; the other **26 are 2026 purchases**. This review does not confirm current ownership or exact share balances.
- Saved an independently transcribed regression fixture in `scripts/test_fields_reviewed_transactions.py`. Same-day Microsoft and NVIDIA purchases in different accounts remain separate transactions.
- Fixed account isolation: a GOOG purchase in an IRA with an unknown starting balance no longer blocks the separately identified GOOG brokerage holding. Missing or unresolved account identities and amendments still block affected estimates.
- All **44 stock/ETF annual account rows** now produce conditional calculations, including zero balances. This is not 44 distinct current holdings. Four additional account positions remain separate with no invented opening quantity: GOOG in the IRA, and LRCX, MU and Quantinuum (filed as QNT) in brokerage #2. QNT was subsequently confirmed as Quantinuum Class A stock from issuer IPO releases; see the purchase audit below.
- Hardened account matching against brokerage #1 versus #10 and a parent trust versus its child account. Full-sale amounts must be consistent with the modeled balance before it is set to zero.
- No additional public holdings page was enabled. Corporate-action/security review and the four unmatched starting balances remain open for Fields.

Validation after this continuation: all **49 Python pipeline test files passed**, including the independent Fields source-row fixture, account isolation and full-sale consistency tests. `git diff --check` passed.

## Fields purchase-size and split audit

The four unmatched positions now have **conditional estimates of shares acquired in the specific disclosed purchase**. They are not total holdings and are not added to portfolio totals. The annual does not establish an exact opening balance for these account/security combinations.

| Security / account | Transaction date | Disclosed purchase | Estimated acquired shares |
|---|---|---|---|
| GOOG / IRA | 2026-01-12 | $1,001–$15,000 | 2.99–46.08 |
| LRCX / brokerage #2 | 2026-02-03 | $15,001–$50,000 | 60.83–220.38 |
| MU / brokerage #2 | 2026-02-03 | $100,001–$250,000 | 226.09–613.45 |
| QNT / brokerage #2 | 2026-06-04 | $1,001–$15,000 | 14.03–250.46 |

These ranges divide the disclosed amount bounds by the execution-day high/low price envelope. They assume execution within that envelope; fractional results express uncertainty, not reported fractional share counts. QNT additionally includes the documented $60 IPO price in the envelope, since the filing does not distinguish an allocation from an open-market purchase. No zero starting balance is inferred from the IPO date: private or converted interests have not been ruled out.

- Quantinuum’s [June 3 pricing release](https://ir.quantinuum.com/news-releases/news-release-details/quantinuum-announces-pricing-upsized-initial-public-offering-0) identifies QNT as Class A common stock, the $60 offering price and June 4 expected trading date. Its [June 5 closing release](https://ir.quantinuum.com/news-releases/news-release-details/quantinuum-announces-closing-upsized-initial-public-offering) confirms the completed offering and Nasdaq listing. This resolves the issuer identity without substituting a similarly named asset.
- Lam’s [investor FAQ](https://investor.lamresearch.com/faqs) identifies its most recent split as October 2024, before this annual baseline and the February 2026 purchase. No extra tenfold multiplier is applied to the 2026 transaction.
- Requested explicit split-event data for all **34 source stock/ETF tickers** from December 20, 2025 through the audit date. The provider returned no split events. This is a provider check, not proof that there were no mergers, spinoffs, transfers or reinvestments.
- `fields-purchase-audit.json` stores source URLs, request windows, response hashes, account identities, purchase assumptions and remaining gaps. `python3 scripts/audit_fields_holdings.py` reproduces the audit; changed disclosure hashes stop it for re-review.
- Public portfolio eligibility remains false. The four opening balances and the broader corporate-action review remain unresolved.

Validation after the purchase audit: all **50 Python pipeline test files passed**; `git diff --check` passed.

## Earlier Fields records and Hern corrections

- Downloaded all ten Fields 2025 PTRs in the checked index and searched their extracted text for the four unresolved securities. The earlier Alphabet purchases do not establish the IRA opening balance; no exact opening quantity was recovered. This was a candidate search, not a visual verification of every historical row. Source hashes and search hits are saved in `fields-earlier-filings-check.json`.
- Visually compared Hern’s original filing 20035134 page 2 with correction filing 20035196 page 1. Organon’s account changes from Hern Family Foundation to Hern Family Revocable Trust. Versant’s sale is deleted. Ticker, date, amount bracket, action and owner match uniquely among the staged transactions.
- The originals do not print transaction IDs. The links are therefore **reviewed inference from matching details**, not a direct ID join. The correction’s printed IDs (2000166537 and 2000166538), both PDF hashes, exact accounts and expected row details are saved in `reviewed-holdings-corrections.json`.
- The ledger now preserves both original rows as superseded, gives the deleted Versant row no balance effect, and retains Organon as the sole replacement sale in the trust account. A changed source hash, missing row or ambiguous original stops the job for review. Original filing statuses remain intact.
- Organon still lacks a matched trust opening position. Resolving its amendment does not create an opening balance or make the portfolio publishable. No new public holdings page was enabled.

Validation: both live Hern PDFs still match the visually reviewed hashes. All **51 Python pipeline test files passed**, including correction replacement/deletion, changed-source rejection and ambiguous-original rejection. `git diff --check` passed.

## Tim Moore source review

- Visually checked all **72 annual Schedule A rows** on pages 1–6 of [10075481](https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2025/10075481.pdf), and **35 transaction rows across nine staged 2026 PTRs**. Continuation pages were checked, including the Verizon amount split across pages and Simply Good Foods’ split asset name. Fresh downloads of all ten PDFs matched staged hashes.
- Saved independently transcribed annual value brackets and PTR ticker/date/action/amount expectations in `test_moore_reviewed_sources.py`. Owner and account fields are blank in the PTRs; a unique annual position is an inferred account match, not an explicitly disclosed PTR account.
- **Hyster-Yale conflict:** annual page 4 says Value of Asset None. [20033856](https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20033856.pdf) reports a December 31, 2025 purchase of $15,001–$50,000 and a January 5, 2026 sale in the same bracket. Under the assumed annual-account match, the sale cannot be reconciled to zero opening shares. The model stays blocked; the December purchase is not added again after the year-end baseline to hide the conflict.
- All AT&T sales are marked plain S, not S (full). Matching purchase/sale dollar brackets do not establish an exited position. T, SMPL, IHG and RYCEY still lack reviewed opening balances.
- SMCY is held in two separately named accounts, Robinhood and Schwab. SMCYY is filed as SIMCORP, with annual None, and must not be silently merged into SMCY. Apple is None in Schwab but has a positive value bracket in TradeWinds. Non-stock cash/property values remain separate from stock totals.
- Remaining model blocks: HY source/account inconsistency, SMCYY missing market series, BRK.B provider-symbol mapping. Corporate actions still need review before publication. No additional public holdings page was enabled.
- Evidence and live-source hashes: `moore-source-review.json`.

Moore review validation: all **52 Python pipeline test files passed**; `git diff --check` passed.

## Moore market identity and Honeywell action review

- Resolved the filed **BRK.B** symbol to the provider’s **BRK-B**, preserving the original symbol and Class B identity. Berkshire’s [2025 Form 10-K](https://www.sec.gov/Archives/edgar/data/1067983/000119312526083899/brka-20251231.htm) identifies BRK.B as Class B; the provider response must match the explicit BRK-B symbol, Berkshire issuer, USD and equity instrument type. No generic punctuation replacement or Class A substitution is allowed. This restores candidate prices for Moore, Gottheimer and Cisneros.
- Honeywell’s [completed aerospace separation](https://www.honeywellaerospace.com/us/en/company/newsroom/2026/06/honeywell-aerospace-spin-off-begins-trading-nasdaq) on June 29, 2026 distributed one HONA share per two pre-separation HON shares, with fractional shares paid in cash. The remaining company also implemented a 1-for-2 reverse split; see its [completion release](https://investor.honeywell.com/news-releases/news-release-details/separation-8-k-supplemental-financial-information-newsletter).
- Added an explicit corporate-action gate to all candidate HON positions crossing that date. It affects Moore, Cisneros, Mullin and Hern. Their prior annual HON brackets cannot be divided by an automatically adjusted historical series and treated as a complete current holding without reconciling the new HONA interest and the pricing basis. No HONA ownership is invented while entitlement remains unreviewed.
- This is a registry of a **known** action, not a complete corporate-action service or a claim that unlisted tickers are clear. Future actions still need ingestion and review.
- HY remains blocked by the source/account conflict, and SMCYY remains unpriced. These are not resolved by the BRK.B alias fix. No additional portfolios were published.

Refreshed Moore’s 2025–2026 House indexes: the same annual and nine 2026 PTRs are listed, with no newer amendment resolving HY. Index rows are preserved in `moore-source-review.json`. All **53 Python pipeline test files passed**; `git diff --check` passed.

## One-member focus: Tim Moore

The current active review stays with Moore until the remaining evidence gaps are classified or resolved. `moore-verification-status.json` is the per-position checkpoint, generated by `scripts/summarize_moore_review.py`; it is explicitly not a public verified portfolio.

- **33 positive account-level candidates / 32 distinct tickers**, **16 conditional zero positions**, **2 blocked baseline positions (HY and HON)**, and **4 additional tickers with unknown starting balances (T, SMPL, IHG, RYCEY)**. Counts describe stock/ETF positions; cash, mutual funds, property and private assets are outside this candidate model.
- Resolved SMCYY's price dependency: an explicitly closed annual position with no subsequent supplied changes carries zero without requiring a quote. A later purchase still requires prices and reconciliation. This does not reinterpret SMCYY as SMCY.
- Visually reviewed annual Schedule B page 19: it independently repeats HY's December 31 purchase, strengthening the finding of an unresolved source/account conflict rather than an extraction omission. Schedule A page 4 still says None; no opening quantity is fabricated.
- Collected explicit split-event responses for Moore's source stock/ETF tickers. Crosschecked GNPX's July 16 **1:22** reverse split against [Nasdaq](https://www.nasdaqtrader.com/TraderNews.aspx?id=ECA2026-495), and TZA's July 15 **1:10** reverse split against [Direxion](https://www.direxion.com/press-release/direxion-to-split-nine-etfs). The provider agrees on those events. Candidate calculations continue to use one adjusted-price basis; no second split multiplier is added. Fractional-share cash/rounding is not reconstructed.
- HON exposes why provider events alone are insufficient: its supplied adjustment factor is 1907:2000, not the legal reverse-split/distribution ratio. The HON calculation remains blocked; this factor must not be interpreted as shares received.
- This pass does **not** complete issuer-level non-split corporate-action coverage. Moore is not marked fully verified or enabled for publication. The remaining steps are listed explicitly in the one-member checkpoint.
