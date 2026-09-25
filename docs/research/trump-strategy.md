# Trump strategy source audit

Status: disclosure strategy dashboard; not a copy-ready portfolio. On September 23 the user requested a visual holdings overview, sector breakdown and recent trades. The page now adds those views to the compact holdings table. DJT remains included; funds, direct bonds, cash and private businesses are excluded. Historical research and calculation code are retained without publishing unverified performance. Chart percentages describe the displayed annual estimates or ticker counts, not the complete portfolio's allocation. Current-balance claims remain unavailable while reconciliation is incomplete. Work has not been deployed.

## Source material checked on 2026-09-22 UTC

- [Official White House disclosure index](https://www.whitehouse.gov/disclosures/). The latest transaction report listed there was signed 08.12.26. Later activity must be applied to the annual baseline before labeling anything current. The OGE individual-disclosure API could not be reached from this environment; a successful White House index check does not establish complete OGE coverage.
- [Certified annual report published June 30, 2026](https://oge.box.com/shared/static/zycb5i2ny8kssm51uzqm8ygyq2zkpkqq.pdf), reporting calendar year 2025. Government original; the official Box-hosted copy has selectable text. The White House copy is a larger scan.
- Part 6, PDF pages 7–158: 6,181 asset entries, of which 4,686 report a lower bound above zero. These include bonds, cash, funds, equities and separate accounts. They are **not** 4,686 distinct stock holdings.
- Schedule 1, PDF page 865, line 349: 114,750,000 DJT common shares, subject to restrictions. Page 4 describes the revocable trust ownership. The strategy page shows this reported share count with its corroborating SEC evidence and date. An unlimited value band is not assigned an invented midpoint.
- [2015 candidate report](https://s3.documentcloud.org/documents/3870993/Trump-Donald-J-2015-Financial-Disclosure-Report.pdf): earliest source located. The cover confirms filing July 15, 2015; [contemporaneous reporting](https://abcnews.com/Politics/donald-trump-487-job-titles-learned-finances-today/story?id=32622993) establishes public release July 22. The filing date must not be used as the model's start date. It includes investments since merged or delisted, so current ticker prices cannot simply be backfilled.
- Additional 2016, 2017, 2018, 2019, 2020, 2021, 2023 and 2024 government documents were located and downloaded for research. Newly located: [2019 annual filing](https://s3.documentcloud.org/documents/21154683/trump-oge-2019.pdf), 88 PDF pages, and [2021 termination filing](https://www.citizensforethics.org/wp-content/uploads/2021/01/Trump-Donald-J.-2021Termination-278.pdf), 79 PDF pages. Their hashes are pinned in the source inventory; holdings are not yet reconciled. The 2023 and 2024 downloads are partial documents, not complete annual baselines. Missing years are not assumed to have no holdings or no filing requirement.

### First disclosure: complete Part 6 transcription

`trump-2015.json` preserves all **326 Part 6 entries** on PDF pages 35–45. Every name, value range, income type, account boundary and EIF field was checked visually against the original pages; OCR was only a review aid. The source SHA-256 is `682d0e0ca9d6ca0da9eac33b8d574dbfaed0952c335ae8c70935b017d35a83de`.

The six sequences are other assets (26), Barclays (33), Oppenheimer (32), Deutsche A/C 1 (116), Deutsche A/C 2 (59), and JP Morgan Clearing (60). All sequences and page/row identities are retained. Part 7 on page 46 says N/A; this is not evidence of no actual trading. No precise valuation date is supplied by this review, and the candidate filing must not be labeled a December 31 annual snapshot.

There are **121 stock candidate rows**, **11 unresolved instrument rows**, 111 rows below the reporting threshold, 45 interest-bearing rows, 27 fund rows, 10 cash/deposit rows, and one gold entry. Candidates still need historical security/share-class mapping and are not distinct tickers or a complete portfolio. Three positive Apple stock candidates are separate from two positive interest-bearing Apple entries and a none/under-threshold entry. The Energy Transfer partnership is left unresolved rather than classifying it as a fund solely because its EIF field is Y. Bare issuer names among debt holdings are not automatically converted to stocks.

The retained holdings API supports separate 2015 and 2026 source downloads. The 2015 download includes no modern DJT stake, inferred weights, or current-holdings claims. The main strategy page always shows the latest reviewed holdings, even when reached through an old historical-filing URL. There are **zero verified full-portfolio return years**; historical returns are currently outside the page's scope.

## Extraction and verification

`src/lib/strategies/trump-annual.json` preserves every Part 6 row, value range, PDF page and printed line, plus the separately verified DJT stake. Two independent extractors (Poppler and pdfplumber) must agree on every row identifier and value range. Table-cell extraction retains wrapped descriptions. A pinned SHA-256, expected page range and expected row count prevent a changed document or a partial parse from silently replacing the evidence.

Some pages restart line numbering when a new investment account begins. IDs therefore use page and row ordinal. Repeated asset names remain separate report rows. “None (or less than $1,001)” is retained as a threshold range, not treated as a proven complete sale. No company-name-only conversion to equity tickers is performed: e.g. Apple's 4.3% bond is not AAPL stock.

The extraction now also preserves the source account, reported income type and EIF field. All 6,181 rows retain account context across page breaks and changes within a page. Page 157 explicitly describes the family trusts as income-only interests with JPMorgan as sole trustee; those underlying fund positions are not treated as directly owned shares.

### Identified stock evidence

`trump-stocks.json` indexes 44 tickers against 345 exact annual row IDs and descriptions. `scripts/strategies/trump_stock_identities.json` contains the reviewed aliases; `build_trump_stocks.py` validates them against Nasdaq's [traded-symbol directory](https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt). Common stocks, share classes, preferred securities, debt and funds are not interchangeable. Bare `ALPHABET INC` and unspecified Berkshire share classes remain unresolved. Apple's bond and Apple Hospitality REIT do not enter AAPL's evidence. A partial index is explicitly labeled partial, and its values are never normalized into a whole-portfolio allocation.

```sh
python scripts/strategies/build_trump_stocks.py NASDAQ_TRADED.txt
```

The source listing was checked September 22, 2026. Ticker lookup is a reviewed research index, not an automatic classifier for every company-name occurrence or future filing.

### Primary-source recheck and corrections, September 22, 2026

A fresh download of the official OGE annual report matched the stored SHA-256, `84b5987e4c8a418188600bea1e1ba6b44e0d5735cdd757cee5aba551977ca402` (8,319,883 bytes). Re-extraction with Poppler and pdfplumber reproduced all 6,181 stored Part 6 rows. The raw extraction was intact, but exact-name matching omitted 11 stock entries across eight existing tickers. All 11 omitted entries were visually checked on the original PDF pages, their reviewed aliases were added, and the index was rebuilt against the Nasdaq directory. No new tickers or inferred share counts were introduced.

The corrections below are changes to displayed midpoint estimates, rounded to the nearest dollar. They are not exact balances or current market values. Page and line refer to the original PDF and printed table line.

| Stock | Previous estimate | Corrected estimate | Added source evidence |
| --- | ---: | ---: | --- |
| GE | $3,690,003 | $6,765,004 | Page 19, line 74: `GENERALELECTRICCO`; page 72, line 135: `GENERAL ELECTRIC CO` |
| IBM | $1,747,504 | $4,822,505 | Page 27, line 218: `INTL BUSINESS MACH`; page 77, line 357: `INTERNATIONAL BUSINESS MACHINE` |
| JNJ | $7,915,005 | $8,090,005 | Page 19, line 92: `JOHNSONANDJOHNSONCO` |
| KO | $6,647,504 | $6,897,505 | Page 10, line 131: `COCA COLA COMPANY`; page 24, line 126: `COCA COLA CO COM` |
| LLY | $6,700,003 | $6,875,004 | Page 18, line 64: `ELILILLYANDCO` |
| META | $19,925,004 | $20,300,005 | Page 19, line 70: `FACEBOOKINC` |
| RTX | $4,373,004 | $4,405,504 | Page 20, line 130: `RAYTHEONTECHNOLOGIES` |
| WFC | $3,623,003 | $3,698,004 | Page 21, line 174: `WELLSFARGOANDCONEW` |

Former-name mappings were corroborated with [Meta's ticker-change announcement](https://investor.atmeta.com/investor-news/press-release-details/2022/Meta-Platforms-Inc.-to-Change-Ticker-Symbol-to-META-on-June-9/default.aspx), [GE Aerospace's separation announcement](https://www.geaerospace.com/news/press-releases/ge-aerospace-launches-independent-investment-grade-public-company-following-0), and [RTX's 2023 Form 10-K](https://www.sec.gov/Archives/edgar/data/101829/000010182924000008/rtx-20231231.htm). GE HealthCare, GE Vernova, Johnson Controls, issuer bonds and preferred shares remain separate. Bare Alphabet entries still lack a verified share class and were not assigned arbitrarily to GOOG or GOOGL.

All 44 API holdings were independently recomputed from the freshly extracted rows: lower bounds, upper bounds and unrounded midpoints match. The API includes the same 6,181 raw rows and preserves null current values and weights, incomplete stock coverage and `currentHoldingsVerified: false`. This does not constitute a new visual review of all 345 mapped entries or reconciliation of subsequent trades. The SEC Form 4 and White House disclosure index were rechecked; the DJT count remains corroborated as of June 19, 2026, and the index still lists later transaction reports through August 12 that have not been fully reconciled.

### Newer transaction review

`extract_trump_ptr.py` removes long table rules before local Tesseract OCR, producing much clearer review text for the August filing. The original rendered pages remain beside each draft. All 34 pages were extracted, but OCR alone does not certify their content. The pinned source checksum is `e1ec58ef09d3046f6e2741533df80fe88a1210b3b5c2eaa7a7c74c850bda58ed`.

`trump-reviewed-activity.json` contains **56 selected stock transactions** visually checked on original PDF pages 2 and 33. These are a subset of the report's 1,051 rows. Page 2 lines 1–15 and 17–30, and page 33 lines 1025–1051 are included. Page 2 line 16's instrument identity remains unreviewed; lines 31–33 are bonds/funds. The report was signed August 12, received August 13 and certified August 20. Its exact public availability date is not verified and remains null. No share quantity, per-account match, full-sale flag or historical rebalance date is inferred from these selected dollar-range transactions.

```sh
python scripts/strategies/extract_trump_ptr.py INPUT.pdf REVIEW_DIRECTORY
```

### DJT corroborating ownership evidence

The trustee's [June 2026 SEC Form 4](https://www.sec.gov/Archives/edgar/data/1849635/000143774926021434/xslF345X06/rdgdoc.xml), Table I indirect holding and footnote 4, reports the same 114,750,000 trust shares as of June 19, signed June 23. The trustee's 61,098 direct shares/RSUs are not President Trump's holding. The [Schedule 13D amendment](https://www.sec.gov/Archives/edgar/data/947033/000114036125046424/xslSCHEDULE_13D_X01/primary_doc.xml) identifies President Trump as settlor and sole beneficiary, and his son as trustee. The repeated trust position across reporting persons is one economic stake. `trump-djt.json` preserves these references separately from the annual evidence. This corroboration does not prove no subsequent changes.

Rebuild with Poppler installed and dependencies from `scripts/requirements.txt`:

```sh
python scripts/strategies/import_trump_annual.py INPUT.pdf src/lib/strategies/trump-annual.json
```

The large source PDFs and OCR scratch files stay in ignored `tmp/pdfs/`. Do not commit those scans. The source URLs and hashes make the committed artifact reproducible.

## Current data blockers

1. The newer PTR scans (May, June and August 2026) have OCR errors in descriptions, dates and value bands. The improved extraction and selected August-page review do not resolve the remaining ledger. Review account/instrument identities and reconcile transactions chronologically, accounting for amendments and duplicates. PTRs may omit account labels; do not invent an account-level match from a company name.
2. The full historical source chain is not reviewed, including reporting/valuation dates and public availability dates. A model must begin on the first trading session *after* information became public, not on the historical transaction date.
3. Yahoo chart requests for the 2015–2016 period returned HTTP 404 and no prices for AGU (Agrium), MJN (Mead Johnson), MDVN (Medivation), ESRX (Express Scripts), WWAV (WhiteWave) and HOT (Starwood). These issuers appear in the first disclosure. An archive with delisted securities and corporate-action history is needed to avoid dropping historical losers/acquired companies and introducing survivorship bias. Check recorded in `tmp/pdfs/delisted-price-check.json`.
4. Multi-class shares, open-ended ranges, restricted stock and trust interests need explicit modeling decisions. Do not renormalize an arbitrary priced subset and call it the whole portfolio. The user's stocks-only scope removes fund share-class work but does not remove historical listed companies that were subsequently acquired/delisted.

Stooq requests for all six delisted names also returned HTTP 404 on September 22; they did not provide an alternate historical series. EODHD documents a [delisted-security archive](https://eodhd.com/financial-apis/delisted-stock-companies-data-2), but coverage of the exact symbols, appropriate access and a usable dataset have not been verified. No provider account or subscription was created and no unprovided API credential is assumed.

Legacy Nasdaq Data Link WIKI requests for AGU and ESRX also returned HTTP 403 on September 22; no data was obtained or substituted. This does not establish that an authorized archive export is unavailable.

A read-only check through the connected Robinhood historical-price tool also failed for the six known gaps: AGU, MJN, ESRX and WWAV were classified as inactive instruments; MDVN and HOT were missing. No account balances or private portfolio information were requested. This did not supply the missing historical series.

### Portfolio calculation implementation

`disclosure-backtest.ts` implements a pure, provider-independent calculator. It requires a reviewed disclosure chain, complete snapshots with verified publication dates, a verified market-session calendar, resolved security identities, USD daily raw/adjusted closes, and reviewed corporate actions. It is **not yet supplied with a complete Trump dataset**, and none of its test examples are published as Trump returns.

Rebalances occur at the close of the first verified session after public release. Bounded reported ranges use their midpoint; exact share counts use raw market prices for allocation values. Adjusted closes measure investment growth with reinvested distributions. Weights drift between disclosures, gains on a rebalance day are preserved, and the latest disclosure takes precedence when multiple publication dates map to the same session. Full calendar years start from the prior year-end model value; inception and YTD are partial. A reviewed snapshot with no eligible stocks holds model cash at 0% until another eligible snapshot; this is a model convention, never a claim about personal cash balances. Fees and taxes are excluded.

Missing interior prices, missing delisted securities, unreviewed corporate actions, duplicate identities/amendments, open-ended values and unverified dates block the calculation. The calculator returns reasons with empty result arrays; it does not normalize a priced subset into a full portfolio. Regression tests use hand-checkable hypothetical inputs for allocation and year-boundary calculations.

Verification after this continuation: 32 JavaScript strategy tests and 6 Python importer/identity tests pass, as do typecheck, lint and the production build. The browser's filing selector, historical Apple evidence (stock versus interest-bearing entries), source downloads and history section were checked. Both download variants were also verified through the running local API. No deployment was performed.

## Implemented behavior

- Dedicated Trump route linked from Strategies. Following the user's request for a simpler Quiver-style presentation, the page now uses a compact title, annual snapshot date and a single holdings table. The large summary blocks, introduction and sidebar are removed. All 44 reviewed annual tickers plus DJT appear in one searchable list, with no historical-performance sections or filing selector.
- DJT appears first with its full reported share count, followed by annual holdings ordered by the midpoint of their reported range. Each annual stock now shows one full-dollar estimated value, rounded to the nearest dollar, under “Estimated holding.” For example, AAPL's $14,000,009–$64,000,000 range produces $39,000,005 on screen. The original range remains in hover titles and expanded evidence; no weights are inferred from the selection.
- `reportedValueMidpoint` returns null for open-ended or invalid ranges. The index and download expose the unrounded midpoint as `estimatedReportedValue`, separately from `currentValue: null`, with the calculation method in the API. This is an annual disclosed-value estimate, not an exact account balance, present-day valuation or inferred share count. [OGE's reporting guidance](https://www.oge.gov/web/278eGuide.nsf/For_Ethics_Officials) confirms asset values are reported by category.
- Partial coverage remains visible in one small line. Dates, scope, reconciliation status and source-data links are available under the collapsed “Sources & details” control. Visual reference: [Quiver's Pelosi strategy page](https://www.quiverquant.com/strategies/s/Nancy%20Pelosi/); no paywalled portfolio data was accessed or reused.
- Selecting a company expands its original source rows, account ranges and filing-page links. DJT expands to its ownership evidence; its reported 114,750,000 shares are dated June 19, 2026.
- Partial coverage and outstanding transaction reconciliation remain explicit. Annual ranges are dated December 31, 2025, with publication June 30, 2026. No live-value marks, normalized weights or claim of complete current holdings.
- The source-data download retains annual entries, partial-coverage flags, reviewed activity and DJT references. Historical data, source-discovery helpers and the return calculator remain available for later work without being rendered or fetched by this page.

Holdings-only verification: typecheck, lint, all 32 strategy tests and the production build pass. Browser checks covered the company list, Apple source expansion, DJT search and ownership sources, empty search results, and old historical URLs still showing the latest reviewed holdings. The simplified page is available in the local preview; no deployment was performed.

Compact-layout verification: typecheck, lint, all 32 strategy tests and the production build pass again. The browser confirms the shorter page header, multiple holdings visible immediately, collapsed source notes, ticker search and exact Apple ranges behind the rounded display. This is a presentation change; disclosure coverage has not advanced.

Single-number verification: all 34 strategy tests, typecheck, lint and the production build pass. New cases verify the AAPL midpoint and downloadable estimate without promoting it to a verified current balance, and reject open-ended, nonfinite or invalid ranges. Browser inspection confirms full-dollar estimates, the full DJT reported share count and the original AAPL range beside its calculation. The changes remain local.

Primary-source correction verification: all 36 JavaScript strategy tests and 6 Python importer tests pass, along with typecheck, lint, the production build and the whitespace check. The two added regression cases retain the 11 reviewed aliases and prevent issuer notes, preferred shares and separate companies from entering those stock totals. Browser inspection confirms the corrected IBM and Meta values, their expanded original ranges, and links for the restored filing entries. All 44 live API estimates match an independent aggregation of the fresh source extraction. These corrections remain local and have not been deployed.

## Strategy dashboard, September 23, 2026

Design references: [Quiver's strategy portfolio insights](https://www.quiverquant.com/strategies/s/Nancy%20Pelosi/) separates holdings from recent activity; [iShares IVV](https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf-ivv) puts key facts, holdings and sector exposure into distinct views. The dashboard adopts that information hierarchy while retaining Vail's existing colors, type and source evidence. No external performance figures or allocations are copied.

- The holdings donut shows the five largest displayed annual estimates plus the other 39 stocks. Its denominator is only the 44 reviewed annual dollar estimates. This is explicitly labeled as a partial view, not a complete portfolio allocation. The original `weight: null` and `currentValue: null` fields remain intact.
- DJT has a prominent separate share-count panel and stays in the holdings list and sector count. Its shares are not added to dollars to create a misleading chart weight.
- Sector classifications for all 45 ticker positions are sourced from the iShares IVV (September 22) and IWM (September 21) holdings downloads, fetched September 23. `trump-sectors.json` records each source URL, checksum, date and ticker mapping. These downloads supply classifications only. `BRK B` maps to `BRK.B`, and the display expands `Communication` to `Communication services`. Unknown future tickers remain unclassified. The sector bars count positions, including separate Alphabet share classes, rather than dollar exposure.
- Clicking a chart legend item, sector bar or DJT panel filters the holdings table. Company-name/ticker search, sector selection and original filing expansions work together. The activity tab supports search, buys/sells, pagination and source links.
- Three latest **reviewed** trade entries lead into the 56-row activity sample. Trade dates remain June 23–24, while the report's signing date is August 12 and receipt date is August 13. This sample is not presented as the complete latest trading history; the remaining report entries may include newer trades. Nothing is inferred about full position closures.
- `Follow strategy` uses Vail's existing account sign-in, atomic follow writer, tracking quota, delivery preferences and daily email roundup. The saved item links back to this strategy from Tracking. The RSS modal, feed endpoint and feed metadata have been removed; no separate subscription system or database migration is needed.
- `emit_strategy_updates.py` runs in the existing hourly signal workflow before alert queuing. Newly detected official Trump PDFs become `strategy_filing` events, matched only to the `strategy:trump` follow. Existing source rows marked `notificationBaseline` prevent historical alerts; keep those markers when reviewing or refreshing the inventory and leave them off new filings. Stable source URLs and insert-once writes preserve event IDs and first-detection dates. The script fails if the index is unavailable or unrecognized. Email notifications respect the existing user opt-in and daily cadence. A filing notice says the holdings and trades still need review; detection dates never stand in for unknown publication or trade dates.

Historical research and data reconciliation remain incomplete. The UI does not add a return chart, live market values, exact balances, executed trades or copy-trading claims.

Dashboard verification: 41 JavaScript strategy tests cover chart denominators, the DJT unit boundary, sector counts, trade-date ordering, filtering and source coverage. Earlier browser checks covered stock and sector selection, the other 39 stocks, company search, empty results, original filing expansion, buy/sell filters, pagination and keyboard tabs. At a 390px mobile viewport, holdings and trades fit without page overflow; activity uses compact rows with the date, direction, range and source visible. The client receives only the displayed holdings and reviewed activity rather than importing the full annual report. The initial SVG-title hydration issue was fixed by rendering its title as one string.

Notification integration verification: the account follow tests cover authenticated strategy requests, registered target identity, removal, and existing atomic quota failures. Eight new Python tests cover source filtering, the historical baseline, repeat polling, follower matching, disabled subscriptions and inclusion in the existing daily roundup. The existing queue, alert-rule and digest tests also pass. A read-only `--dry-run` against the White House index found no new documents beyond the 18 baseline filings. No database events or actual notifications were sent during validation. Source notices will begin processing once this code is deployed to the existing workflow; reviewed portfolio changes still require the data reconciliation described below.

Safari hydration correction: compact USD formatting now explicitly sets both fraction-digit bounds. Safari previously rendered `$39.0M` where Node rendered `$39M`, which caused a recoverable hydration error. Reloading the affected Safari tab confirms the overlay is gone and both holdings and recent trade amounts use matching compact labels. A regression test models the differing currency defaults; all 42 strategy tests, typecheck and targeted lint pass.

## Remaining implementation before a copy-ready strategy

Review and map the entire eligible holdings history, reconcile newer filings, obtain missing historical price/corporate-action data, then compute publication-date model rebalances, allocation weights, benchmark comparisons and annual returns. Persist reviewed snapshots and their effective dates. A scheduled ingestion job should publish only validated updates; a source-format change must retain the previous reviewed snapshot and show stale coverage.
