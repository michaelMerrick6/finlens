# Pelosi quantity-baseline search

Reviewed September 17, 2026 UTC. This continues the prior source inventory; it does not replace existing conditional estimates.

## Result

No additional complete, dated share-balance snapshot was established in this pass. Found a useful Visa acquisition anchor, but it must not be represented as a confirmed balance or current holding.

An archived [Pelosi office statement](https://www.legistorm.com/stormfeed/view_rss/382260/office/1978/title/pelosi-spokesman-statement-on-60-minutes-report.html) gives three purchases: 5,000 shares March 18, 2008; 10,000 March 25; 5,000 June 4. These total 20,000 shares acquired. The [2008 annual filing](https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2009/8140536.pdf#page=17) lists those dates together and a November 24 partial sale of $15,001–$50,000, without a share quantity. The purchase anchor does not resolve that sale, the amended annual, or the later transaction chain. Do not carry 20,000 forward unchanged.

## Why the other searches did not establish balances

- Apple, Salesforce, Comcast, Morningstar and Qualcomm appear in older annual inventories as dollar ranges. Their positive inventory predates the modern share-level transaction chain. Search hits are recorded in the accompanying JSON; OCR hits are leads, not visual verification.
- Apple has multiple sale dates grouped under a single dollar bracket in an older filing. Do not assign that entire bracket to every date.
- Amazon's opening inference is already documented; the outstanding problem is the later June 2022 option outcome. Another copy of the opening annual cannot resolve it.
- Alphabet requires share-class continuity, not just a total company exposure estimate.
- Existing Comcast/Versant and AT&T/WBD spinoff leads remain conditional: reverse-calculating parent shares requires accounting for fractional treatment and account aggregation. No new baseline promoted from those ratios.

## Outside portfolio sources checked

- [Trade With Congress](https://www.tradewithcongress.org/politician/nancy-pelosi) describes portfolio shares as proportions of disclosed bracket midpoints. These are not actual share quantities.
- [Equibles](https://equibles.com/congress/members/ae827700-6aeb-4a4b-b831-5930f5c3fcfa/nancy-pelosi) labels its quantities as estimates from disclosed ranges and omits some split-crossing positions. Not an independent verified opening balance.
- Other retrieved trade pages repeat purchases/exercises or transaction ranges, rather than a complete account snapshot. No third-party estimate was imported as fact.

## Next evidence needed

For each unresolved holding, accept either an explicit dated total quantity, a demonstrated zero/reset followed by complete share-level changes, or a bounded valuation-derived estimate with its price date and assumptions. Preserve these as different evidence classes. A total of known acquisitions is not automatically an opening balance. A missing annual entry is still an assumption, not a confirmed zero.

Current result remains sixteen conditional quantity estimates and twelve unresolved positions. No current market-value total or weights certified.

## Visa range follow-through

The amended 2008 annual is 8142737, PDF page 18 (printed page 17). Its verified schedule retains the November 24 sale bracket. Original 2019 PTR 20012288 contains two 1,000-share sale rows; amendment 20012343 identifies one amended event but does not by itself prove whether one or two economic sales occurred. The annual also contains two rows. Consequently calculate both scenarios, do not silently deduplicate.

A research calculation using the November 24, 2008 Yahoo daily high/low, restored to pre-split units, bounds the dollar-range sale at 301–1,092 whole shares. Roll-forward with the 2014 sales, the issuer-confirmed 2015 four-for-one split and the listed later disposals produces 14,632–18,796 shares across the two 2019 interpretations. This is conditional on the acquisition anchor exhausting the starting balance, regular-session execution without fees, complete later activity, and treating both 2020 sale rows as distinct. It is not a statistical confidence interval or a verified current holding.

Reproducible script and source links: `pelosi-visa-range.py` / `.json`. Retained as a research scenario; not fed to the public holdings ledger while early completeness and amended identity remain unresolved. This narrows the unknown but does not establish an exact quantity.

## Annual-value constraints: Apple and Salesforce

Added a separate modeled-range class to the local holdings page. These are not extra disclosed-quantity reconstructions. Apple: 26,251–91,959 modeled whole shares; Salesforce: 19,002–37,710. Display ranges round outward. Each 2022–2025 annual value bracket is divided by that year's final trading-day close, then rolled forward by disclosed share changes. Intersect compatible intervals; never average contradictory results. Source prices, split-event response and dates are preserved in pelosi-baseline-prices.json; executable arithmetic is pelosi-annual-ranges.py.

Apple uses +12,100 net shares in 2023, −31,600 in 2024, −73,582 in 2025. Salesforce uses no common-share change in the reviewed 2023–2026 chain. The price response contains no intervening split events. The model assumes closing-price valuation and complete share changes; House guidance permits other valuation methods, so the interval is explicitly conditional and not a statistical confidence interval. The local display preserves a null exact share count and distinguishes modeled ranges from the sixteen explicit-event reconstructions. A different baseline bracket or new share delta disables these saved ranges pending recomputation. No portfolio weights or live-value total are inferred.

Visa original and amended 2019 filings still do not resolve the identity of the duplicate-looking rows. The public Visa estimate remains withheld; prior research scenarios are retained.

## Additional 2025 snapshot models

Added conditional whole-share intervals for T (4,026–10,064), CLNE (477–7,142), IBKR (7,775–15,549), MORN (461–1,150), and QCOM (88–292). These use only the reviewed 2025 annual bracket and the last trading-day closing price, with no subsequent share changes in the reviewed ledger. They are not multi-year reconciliations or disclosed quantities. Reproduce with `pelosi-annual-ranges.py`; source prices are saved in `pelosi-baseline-prices.json`. The UI checks owner, stock type, unchanged baseline bracket, and zero subsequent net changes before displaying the model.

Coverage now: 16 conditional transaction-quantity estimates, 7 value-based modeled intervals, and 5 unresolved stock/unit quantities. The five unresolved are AMZN, GOOGL, V, CMCSA and WBD. Current market-value ranking remains unavailable.

CMCSA is deliberately excluded: Yahoo's January 2026 1067:1000 event reflects the Versant spinoff price adjustment, not multiplication of Comcast shares. The baseline quote must be reconciled to an unadjusted historical price before applying a valuation model. Comcast reports the separation in https://www.sec.gov/Archives/edgar/data/1166691/000162828026004994/R25.htm and provides allocation guidance at https://www.cmcsa.com/node/26781 . IBKR's June 2025 four-for-one split predates the year-end snapshot and must not be applied again (https://www.sec.gov/Archives/edgar/data/1381197/000119312526016540/ibkr-ex99_1.htm).

Validation: 57 backend tests pass, TypeScript and targeted ESLint pass, and local HTTP rendering contains all five added ranges. Tests cover changed-bracket rejection, non-spouse rejection, new share-change rejection, small whole-share intervals and Comcast remaining unresolved.

## Comcast price resolved; Amazon uses newer baseline

Comcast's 2026 proxy statement explicitly gives its December 31, 2025 closing price as $29.89 (https://www.sec.gov/Archives/edgar/data/1166691/000119312526177138/cmcsa-20260424.htm, printed page 51, footnote 1). This resolves the prior price-feed adjustment blocker. The $500,001–$1,000,000 annual bracket / $29.89 yields 16,729–33,456 whole shares under the same closing-price valuation assumption. No 1067:1000 multiplication is applied. Retained the adjusted feed quote alongside the issuer price for auditability. The 776 Versant receipt is not reverse-converted into an exact Comcast balance because account/fractional treatment remains unknown.

Amazon's 2025 annual stock entry gives a newer baseline independent of the unresolved 2022 option outcome. Divide its $5,000,001–$25,000,000 bracket by the December 31 close of $230.82000732421875, then add the 5,000 shares exercised January 16, 2026 (PTR 20033725, page 2, reviewed event house-2026-20033725-6). Result: 26,662–113,309 modeled shares. This broad range is not the unsupported 45,000-share historical scenario and does not claim to resolve the old option event. The range is withheld if the subsequent net change differs from the reviewed +5,000.

Latest coverage: 16 conditional transaction-quantity estimates, 9 modeled intervals, 3 unresolved quantities (GOOGL, V, WBD). These counts indicate available estimation methods, not confirmed ownership. Tests compare every published modeled interval with the reproducible calculation artifact and protect owner, bracket and subsequent-change guards.

## Remaining 2025 snapshot models

Added GOOGL Class A (20,975–84,872 shares), V (14,257–71,283), and WBD (1,735–3,469), using saved December 31, 2025 closing prices of $313, $350.7099914550781 and $28.81999969482422. No post-baseline split events were returned by the price feed. Alphabet adds the reviewed 5,000-share January 16, 2026 Class A exercise (2026 PTR 20033725, page 1). Visa and WBD have no net share changes in the reviewed subsequent ledger.

Alphabet is strictly the Class A position as filed; the older Class C reconciliation remains open. Visa's older duplicate-looking sale rows and opening quantity are not resolved by this valuation model. WBD's 2022 2,419-share spinoff receipt is already before the snapshot: adding it again would double-count. Paramount's September 8, 2026 issuer update says closing is paused during litigation: https://paramount.gcs-web.com/news-releases/news-release-details/paramount-skydance-moves-protect-against-costs-delay-wbd-merger . Therefore no merger cash conversion is applied. The WBD model expressly assumes no subsequent closing and needs review when closing occurs; no automated merger-status feed was added.

All 28 listed stock/unit positions now have a conditional estimate: 16 transaction-quantity reconstructions and 12 valuation-based ranges. This is coverage of the listed positions, not proof of complete current ownership, accurate exact quantities, or resolved historical discrepancies. The broad value brackets remain broad. Current-dollar values/weights are still not calculated. Local rendering, TypeScript, targeted lint and 58 backend tests pass, including Class A/Class C separation and no duplicate WBD receipt.

## Largest-position consistency check and allocation display

Compared the conditional 2025 year-end quantities for MSFT (25,000), NVDA (40,000 before its 2026 +5,000 exercise), and AVGO (20,000) with December 31 daily prices. Implied values were approximately $12.09M, $7.46M and $6.92M respectively, all inside the filed $5M–$25M brackets. No contradiction found; this does not independently prove the assumed opening balances. Inputs, prices and source URLs are in `pelosi-large-position-crosscheck.json`.

The UI now uses recent daily quote prices to value estimated share quantities and ranges. Ranking and illustrative percentages use value-range midpoints; this is a display scenario, not a newly inferred share balance or a verified allocation. The graphic shows the ten largest priced positions with logos plus a combined remainder; every stock/unit is individually numbered below. Options and non-public assets are outside the denominator. Missing, nonpositive, future-dated or more-than-five-day-old quotes are excluded from valuation and weights. Block is quoted as XYZ while its as-filed SQ identity is preserved. Quote dates, coverage and price-times-quantity calculations are visible. Quotes cache for 15 minutes; no production deployment performed.
