# Pelosi historical audit — first reconciliation

Audit date: September 13, 2026. Read-only; no production rows changed.

## Established
- Checked official House annual indexes for 2008–2026.
- Downloaded 85 indexed filings: 63 periodic transaction reports, 17 annual reports (O), 3 amendments (A), 2 extension requests (X).
- Stored database contains 227 Pelosi transactions.
- Existing text parser extracts 212 transactions from the 63 indexed PTRs. Multiset reconciliation found no differences in transaction date, direction, or amount range against the database rows for those reports.
- Two additional 2014 PTRs already in the database account for the other 15 rows: 20001991 (10), 20002236 (5). Both PDFs downloaded successfully. The 2014 official bulk index contains no Pelosi entry. The index is not a sufficient completeness oracle.

## Not yet verified
- This is parser-to-database reconciliation, not independent visual certification of every row.
- 13 asset/ticker/type discrepancies across eight indexed PTRs require source review; some may reflect existing reviewed corrections. Do not overwrite automatically.
- Older annual documents (2008–2013) are scanned with no usable text layer. They and amendments need visual/OCR reconciliation.
- Annual transaction schedules must be compared against PTRs without double counting. Annual holdings, ownership, gifts, exercises, and transfers need a separate ledger.
- The missing 2014 index entry remains unresolved. The report indexed as 2025 was signed in May 2026 and covers 2025; index year and filing date must not be treated as equivalent.
- No market prices added. Actual execution prices and exact portfolio performance are not established.

## Artifacts
- indexes.json: per-year index outcomes and original Pelosi entries.
- manifest.json: indexed source URLs, hashes, download outcomes, stored counts.
- stored-transactions.json: read-only snapshot.
- ptr-reconciliation.json: parsed PTR rows and field comparisons.
- sources/: PDFs and extracted text, including the two unindexed 2014 PTRs.

## Next pass
Independently review the 13 asset discrepancies and annual transaction schedules, then build a proposed reconciliation ledger with source page/row references. Keep missing/uncertain records explicit; no guessed quantities or execution prices.

## Second pass: September 13
- Twelve of the original 13 asset flags are casing-only (`Stock` vs `STOCK`); the remaining flag is the stored TEM ticker versus parser N/A. These were not 13 independently verified asset errors.
- Source text confirms upper range bounds absorbed into names, e.g. Visa July 1, 2024 is $500,001–$1,000,000, stored as $500,001 with the upper bound in the asset name.
- Filing 20006683 explicitly describes two real-estate sales, currently labeled Stock (unidentified ticker). These should be classified separately.
- Extracted all 19 Schedule B rows in the 2025 annual report (10075701, signed May 15, 2026) into annual-2025-ledger.json: nine option purchases, six sales, three charitable contributions, one option exercise. Source descriptions retained. This is text extraction, pending visual page certification.
- Eighteen rows matched the basic date/direction/ticker/lower-bound lookup. The nineteenth (Matthews International Mutual Fund, June 20, 2025) is present but has ticker T in the database. Do not interpret that as AT&T; source identifies a mutual fund and sale of 2,822 units.
- Broadcom June 20, 2025 is described in the annual report as exercise of 200 calls into 20,000 shares at $80. The raw stored row is labeled OP / Call option. A holdings ledger must record conversion into shares, not a new call purchase. Existing display overrides must be checked before any write.
- No production writes performed. All proposed corrections require source-page references and amendment checks before application.

## Third pass: annual cross-check and older scans
- Database transaction dates span September 22, 2014–July 28, 2026. Earlier disclosed transactions are demonstrably missing; historical completeness is NOT established.
- Extracted 176 Schedule B rows across 12 text-readable annual/amendment documents (2015–2025). This includes both versions of the 2015 report, so it is not 176 distinct economic transactions. All have a candidate date/direction/minimum-amount database match. This is NOT an asset-identity certification.
- Visually checked all 19 latest annual Schedule B rows against PDF pages 7–9. Confirmed Matthews mutual fund identity, Broadcom exercise, contributions, share quantities and option terms. Stored corrections remain pending.
- OCR completed for all 159 pages of eight scanned annual/amendment documents. Located 26 candidate Schedule IV pages. OCR is an aid, not certification; other pages and attachments may contain relevant information.
- Visually transcribed the 12 source rows on 2008 filing 8135973, page 15 (2007 transactions), into verified-2007-page15.json. All precede database coverage. Two source rows list multiple dates against one reported range: preserve the grouped range, do not duplicate its full value onto each date.
- Next required work: transcribe remaining scanned transaction pages; reconcile amendments before backfill; resolve 2013/2014 coverage and index gaps; verify modern asset identity and corrections; reconcile annual holdings separately. No database mutations performed.

New reproducible artifacts: annual-reconcile.py / annual-reconciliation.json; ocr-annuals.py / ocr/; scanned-schedule-pages.json; verified-2007-page15.json.

## Fourth pass: visually transcribed 2007–2009 schedules
- 2007: all five Schedule IV pages (15–19), 49 source rows / 58 listed dates, transcribed.
- 2008: amended Schedule IV pages 16–18 (printed 15–17), 33 source rows / 39 listed dates, transcribed. The June 11, 2009 amendment letter explicitly corrects the AIG sale to a complete sale with no remaining interest. Applied in the audit ledger.
- 2009: amended Schedule IV pages 2–3 (printed 14–15), 19 source rows / 29 listed dates, transcribed. The June 8, 2010 cover letter corrects capital-gain flags only. Two Yes / eleven No sale rows agree with the letter.
- Total staged historical source rows: 101, representing 126 listed dates; no invented allocation of grouped amounts. Includes 12 rows previously transcribed, so this pass adds 89 source rows.
- Each yearly ledger retains source hash, page, source row, owner, dates, direction, and range. Historical ticker mappings and full-filing attachment review remain pending. These are staged audit records, NOT live website data.
- Seven 2009 sales have source ranges $1–$1,000. They must not be dropped merely because a modern ingestion threshold expects $1,001+.
- Scripts complete-2007.py, complete-2008.py, complete-2009.py preserve the manual transcriptions. No production writes. Remaining scanned reporting years 2010–2012, missing 2013/2014 coverage, and modern corrections remain open.

## Fifth pass: September 14 — 2010–2012 scans
- Visually transcribed 2010 Schedule IV pages 13–18: 46 source rows. OCR heading lookup had missed pages 13–15 entirely.
- Visually transcribed 2011 Schedule IV pages 13–17: 41 source rows. Linked the two legs of each Active/Active Network and Granite/Entropic exchange; these are not independent cash purchases. OCR lookup missed pages 16–17.
- Visually transcribed 2012 Schedule IV pages 12–16: 36 source rows. OCR lookup missed page 16.
- 2012 has a possible duplicated Sacramento investment on October 2 ($100,001–$250,000), at the end of page 14 and beginning of page 15. Both source rows are preserved with aggregation_hold; no automatic deduplication or summation.
- 2012 J. Crew and Oakwood entries are explicitly residual distributions from prior-year sales; economic_type retained accordingly.
- Total 2007–2012 staged source rows: 224. This is NOT a count of distinct stock trades, and not a completeness certification. Includes private investments, exchanges, grouped dates, and unresolved source duplication.
- Validated row counts, unique page/row references, date years, and amount bounds across all six ledgers. No production changes.
- Remaining: locate reporting-year 2013/2014 sources and index gaps; review full-filing attachments/holdings and amendments; resolve duplicate; historical ticker/entity mapping; modern corrections; controlled backfill.

## Sixth pass: recovered 2013/2014 official sources
- Located and downloaded annual 10000512 (reporting 2013), annual 10005768 (reporting 2014), and amendments 10008188 (2013), 10008154/10008189 (2014), directly from official House URLs discovered through a secondary inventory. Source URLs and hashes retained in supplemental-manifest.json.
- Also recovered scanned PTR 8214491 (April 2014) and scanned document 9106227; both require visual review. Previously stored unindexed PTRs 20001991/20002236 retained in supplemental manifest.
- Annual Schedule B text extraction yields 12 rows for 2013 and 18 for 2014 in each version. Do not add versions together: amendment reconciliation is pending.
- supplemental-annual-reconciliation.json retains headers, full Schedule B text and parsed date/direction/ranges. supplemental-amendment-comparisons.json records comparison results. Finding sources closes the document-location gap, not row-level completeness. No production writes.

## Seventh pass: source verification of recovered years
- Visually checked all 12 2013 and 18 2014 Schedule B rows against amended PDF pages 7–8; saved verified-2013-schedule.json and verified-2014-schedule.json with source hashes and classification evidence.
- 2013 supporting letter 9106227, visually reviewed: all ten Sacramento team purchase entries were operating costs, explicitly NOT additional equity purchases. Excluded from equity purchase totals in staged ledger. Letter also discusses similar 2012 reporting; do not blanket-reclassify other years without reviewing scope.
- Visually checked PTR 8214491, both pages: Active Network November 26, 2013 sale $50,001–$100,000 corroborates annual; note explains acquisition by Vista and cash in lieu of shares. One economic event, not two.
- 2014 amendment adds call-option strike and expiration details to Hertz and Disney rows. Captured contracts, strikes $22/$90, expiration January 15, 2016.
- 2014 MGRC and RHI entries are charitable contributions, not ordinary market sales. Visa share counts preserved. Blank owner cells remain null rather than assumed SP.
- 2014: all 18 rows have date/direction/minimum-amount DB candidates; asset-level identity matching still required. 2013: all 12 source rows predate the DB, including the ten non-equity operating-cost entries.
- 254 staged source rows across 2007–2014; not distinct stock trades. Completeness NOT certified. Pending full-filing/holdings reconciliation, modern field corrections, source-duplicate resolution, and historical entity mapping. No production writes.

## Eighth pass: 2014 identity and amendment reconciliation
- Visually verified official PTR 20003320 page 1: all six Hertz/Disney rows explicitly Amended. Matched each to its original by asset, date, direction and amount.
- 24 stored 2014 records represent 18 annual source events; six original records are superseded by amended call-option records. Saved exact canonical/superseded IDs in 2014-identity-reconciliation.json, reproducible with reconcile-2014-identities.py. No production mutation.
- Historical BRCM is stored as AVGO; retained explicit entity-mapping review flag. Two charitable contributions remain distinct from market sales.
- Scanned all stored records for exact ticker/date/direction/range duplicate candidates in duplicate-candidates.json; matches alone do not establish duplication.
- Full completeness remains unverified: staged backfill, modern field corrections, full-filing holdings/attachments and independent source inventory reconciliation remain outstanding.

## Ninth pass: modern amendment and duplicate triage
- Visually verified 20020561 page 1 and 20018539 page 1, compared original text: eight additional superseded records (five January 2022 transactions, three AllianceBernstein purchases). Saved reproducible modern-amendment-reconciliation.json. Four January 2022 entries are exercises into shares, not new option purchases.
- Source text distinguishes six other candidate groups by strike, quantity, or contribution recipient; preserve separate events. Two 2021 Apple entries are charitable contributions to different institutions.
- Unresolved source repetitions: Visa May 2020 and PayPal June 2020 identical entries; Visa August 2019 has an amendment whose original identity remains ambiguous. No automatic deletion. Amazon January 2020 contains two separate strikes, each amended.
- Total confirmed superseded records staged: 14 including prior six. No production writes; completeness remains unverified.

## Tenth pass: annual corroboration of repeated entries
- Visually checked annual 10039988 pages 9 and 10: two PayPal 5,000-share purchases and two Visa 3,000-share sales appear separately, matching PTR multiplicity. Preserve both source rows; cannot independently prove brokerage executions.
- Visually checked annual 10035243 page 7: two Visa 1,000-share sales on August 7, 2019. Third DB row is amendment; underlying row link remains ambiguous, so do not arbitrarily delete a particular original.
- Visually checked 20016961 page 1: Amazon $1,700/$1,600 call sales explicitly amended. Added two exact supersession pairs; confirmed staged superseded records total 16.
- No production writes. Confidence applies to these source comparisons, not full history or exact portfolio.

## Eleventh pass: holdings baseline
- Visually verified all 32 ticker-bearing stock, option and publicly traded partnership Schedule A entries in annual 10075701, pages 1–7. Saved 2025-holdings-baseline.json with value ranges, SP ownership and option quantities/strikes/expirations.
- This is a reporting-year 2025 baseline filed May 15, 2026, not current holdings. Stock share counts remain unknown; no midpoint-derived counts.
- AVGO options, PYPL stock and DIS stock have reported asset value None; retained verbatim, not treated as current active positions. AVGO description retains historical contracts exercised in 2025.
- Private assets, cash and untickered assets excluded from this scoped baseline. Full history still requires remaining reconciliation and backfill.

## Twelfth pass: 2026 position changes
- Visually reviewed all pages of three cached 2026 PTRs: 20033725, 20034836, 20035143. Staged 16 transaction-year 2026 events with source hashes and page references in 2026-position-changes.json; reproducible via roll-forward-2026.py.
- Five 50-contract January 2026 series reconcile to exercises adding 5,000 shares each (GOOGL, AMZN, NVDA, TEM, VST). Do not count both options and exercised shares as held.
- Excluded December 2025 activity filed in January from baseline changes to avoid double counting.
- Added source-disclosed Bloom share/call purchases, Intel shares and two distinct expirations, Uber calls, AllianceBernstein units and Versant spinoff. Private REOF quantity remains unknown.
- VSNT: 776 shares and cash in lieu, no Comcast shares surrendered; $15.00 is not treated as stock purchase price.
- Eight positive option series in this conditional roll-forward, not certified current holdings. Fresh inventory, production corrections/backfill and unknown starting share quantities remain unresolved. No production writes.

## Thirteenth pass: fresh inventory and application corrections
- Re-fetched all 19 official annual bulk indexes 2008–2026 successfully; 85 Pelosi records and no new IDs compared with previous inventory. Hashes and retrieval time in fresh-inventory.json. Does not certify known bulk-index omissions or unpublished activity.
- Applied 16 verified original/amendment supersession pairs to shared TypeScript review layer via source/member-guarded mapping. Original records retained and excluded from totals; reconciled amendments counted once. Four 2022 exercises classified as shares, Amazon sales as options, public partnership units recognized for ranking.
- Regression tests cover all pairs, source/member guards and exercise classification. Typecheck passed. Local changes only; no DB mutation or deployment.
- Still unresolved: staged historical backfill, other identified field corrections, ambiguous 2019 Visa original identity, full historical coverage and unavailable exact starting share counts. Do not certify current holdings or returns.

## Fourteenth pass: 2025 annual identity completion
- Matched all 19 visually reviewed annual transaction rows to 19 distinct stored records. Matthews required name-based identity because stored ticker T was wrong. Saved exact mapping in 2025-row-identity-reconciliation.json.
- Added source/member-guarded local presentation corrections for five January option purchase amount bounds, Broadcom exercise into stock, and Matthews fund sale with null ticker. Raw DB records preserved.
- Four regression tests pass, including all 16 supersession pairs and 2025 fund/exercise corrections; typecheck passes. Not deployed.
- This closes annual-to-stored identity matching for reviewed 2025 Schedule B, not independent assurance that every reportable event was disclosed. Historical backfill and older unresolved issues remain.

## Fifteenth pass: 2024 annual transactions
- Visually verified all 14 Schedule B rows in 10066169 pages 6–8 and matched 14 distinct stored identities. Saved source/page/hash mapping.
- Applied five local source/member-guarded corrections: Forge and REOF private investment classification, Visa full amount range, PANW and NVDA exercises into shares. Raw records untouched.
- PANW original purchase descriptions expire January 17, 2025; exercise description says December 20, 2024. Preserved explicit discrepancy. Split-related quantity and strike changes require corporate-action evidence before portfolio linking.
- Five regression tests and typecheck pass. No deployment. Annual-row coverage verified within this source; historical completeness and holdings not certified.

## Sixteenth pass: option corporate actions
- Read OCC memos 55607 (PANW), 54623 (NVDA, exchange-hosted copy) and 54716 (AVGO). Contract multipliers 2/10/10 and strike divisors 2/10/10 explain disclosed exercise quantities and strikes. Each adjusted contract delivers 100 shares.
- PANW 70 calls at $200 becomes 140 at $100 -> 14,000 shares; NVDA 50 at $120 becomes 500 at $12 -> 50,000 shares; AVGO 20 at $800 becomes 200 at $80 -> 20,000 shares. Arithmetic assertions pass.
- PANW expiration discrepancy is independent of stock split and remains unresolved. No exact current position inferred. Source URLs and calculations retained in option-corporate-action-reconciliation.json.

## Seventeenth pass: 2023 transactions, bidirectional matching
- Visually checked Schedule B pages 6–7 of annual 10059734. Seven source rows, not six: earlier range-only extraction missed Roblox's $1.00 option-expiration placeholder.
- Matched all seven source rows to all seven stored records with transaction dates in 2023; source count and stored count reconciled both directions. Captured exact identities and annual PDF hash.
- Applied local source/member-guarded corrections for three exercises into shares, Apple contribution, private REOF investment, NVDA range and Roblox expired-worthless classification. Roblox $1.00 retained as source amount but excluded from turnover; not represented as sale proceeds.
- Six regression tests and typecheck pass. No production DB changes or deployment. 2022 and earlier reconciliation/backfill remains open; all-history validity not certified.

## Pass 18 — 2022 annual identity reconciliation

Visually reviewed every Schedule B row and note on annual 10053231 pages 7–10. Reproducible `reconcile-2022-identities.py` matches 34 source rows to 34 distinct stored IDs; all five remaining 2022-date rows are already-reconciled originals superseded by 20020561. No unexplained stored 2022-date rows remain against this snapshot. This verifies identity coverage against the annual, not comprehensive historical source coverage.

Added eight source-scoped local corrections in `src/lib/pelosi-2022-review.json`: two REOF XX private investments incorrectly mapped to ONE; GOOG Class C exercise incorrectly stored GOOGL/OP (annual and original PTR agree); NVDA and TSLA exercises; worthless DIS expiration; WBD spinoff; CRM option sale with conflicting $9 amount versus 13,000 x $0.01 description. Preserve CRM source amount and discrepancy; exclude from aggregate totals rather than invent proceeds. Raw database unchanged. Missing AB sale ticker and broader amount normalization still require follow-through. These local corrections are not a production database migration or a full holdings ledger.

Validation: all seven amendment/correction tests pass; `npx tsc --noEmit` passes. 2022 reconciliation asserts 34 unique mappings, five superseded originals, and no unmatched stored rows. Detailed source text and SHA-256 retained in `2022-row-identity-reconciliation.json`.

## Application integration — September 17, 2026 UTC

The profile now consumes the reviewed 2025 baseline and 2026 events via `src/lib/pelosi-holdings.ts`. Stock quantities remain null; known additions are separate from totals. Option series use owner/ticker/type/strike/expiration keys; five exercised series become zero and eight series retain reconstructed quantities. None-valued baseline entries are excluded. Expiration without a reviewed outcome becomes unresolved, not an assumed exercise. Source links retain page references.

The profile checks the official 2026 House index at most hourly and displays review-needed or verification-unavailable states. It does not automatically approve or ingest new position changes. A new calendar year requires extending coverage; the old ledger is visibly marked for review. Live index checked during implementation: the same three PTRs, latest filed August 21, 2026. Index equality is not completeness certification.

Earlier “local/not deployed” notes describe the state at each historical pass. The guarded transaction corrections are now in the shared application layer. Raw database rows and staged historical backfill remain unchanged. The new holdings ledger deliberately bypasses the older midpoint-based portfolio estimator. Its source inputs are committed audit files, not a second transcription.

Maintenance: review each new filing and amendments against PDF pages, update the baseline/events with hashes and source references, reconcile superseded events, then run the standard backend tests. `test:backend` now includes both Pelosi amendment tests and holdings/coverage tests. Never add an unreviewed source event just to clear the freshness message. Newly discovered corporate actions or source conflicts require review before changing quantities.

## Full Schedule A inventory — September 17, 2026 UTC

Visually reviewed all seven Schedule A pages again and transcribed the 36 non-ticker entries into `src/lib/pelosi-other-assets.json`. Together with the existing 32 securities entries this accounts for all 68 annual asset rows. Seven rows across the two sets report value None and are separately displayed as excluded; one book contract reports Undetermined. Blank ownership cells remain blank. Financial Leasing Services spans pages 3–4. Property and associated corporate interests are retained as separate source entries but are never summed into a net-worth claim.

REOF XXV's July 2026 investment is linked to its existing annual entry, not counted as another entity. The page now leads with all 28 public stock/partnership positions inferred from the positive annual baseline plus subsequent acquisitions, followed by eight option series and 32 other asset entries. Continued ownership is an assumption based on the reviewed public record. Stock totals remain unknown; annual range values are explicitly historical. None-valued entries and consumed option series remain inspectable.

Re-downloaded the annual PDF and all three 2026 PTRs; all four SHA-256 values match the reviewed sources. Re-fetched 2025/2026 indexes: no additional Pelosi filings. Results retained in holdings-source-refresh.json. Runtime freshness now checks both baseline-year and subsequent-year indexes, so a newly indexed annual amendment triggers review.

Third-party check: Quiver's public Pelosi profile describes estimates based on trades, annual filings and price changes; it warns annual holdings may contain parsing errors and exclude later transactions. The retrieved public page did not expose holdings rows, so no position-by-position third-party validation is claimed. No external holdings have been copied or substituted for official evidence.
