# Backend quality and politician coverage plan

## Scope and baseline

The initial coverage target is every current U.S. House and Senate member, including House delegates and the Resident Commissioner, whether or not they have reported transactions. Former members must be covered for the supported disclosure history. Governors, state legislators, candidates, and other officials are outside this initial scope. A complete roster is not proof of complete disclosures or transactions.

Read-only baseline captured September 8, 2026 Pacific / September 9 UTC in `ops/congress-roster-baseline.json`:

- Official feeds identify 539 people: 439 House and 100 Senate. Two House entries have no Bioguide identity and are excluded from the person comparison; review their vacancy status. Counts must follow the source roster, not a hardcoded seat total.
- Database contains 1,120 member records and matches 532 current official identities.
- Seven current identities are missing: James Gallagher, Aisha Wahab, Everton Blair Jr., Clay Fuller, Analilia Mejia, Alan Armstrong, and Darline Graham.
- Eighty-eight stored active records are absent from the current official rosters. These require reconciliation; absence is not permission to delete their histories.
- Existing matched current identities have no detected active-flag or chamber mismatches.
- Existing seed code truncates historical imports to `historical_data[-500:]`, which is not a defensible historical coverage boundary.

Evidence: [House Clerk roster](https://clerk.house.gov/xml/lists/MemberData.xml), [Senate roster](https://www.senate.gov/legislative/LIS_MEMBER/cvc_member_data.xml). The baseline records source URLs, payload hashes, and the audit timestamp. Database queries are paginated and read-only. No production records were changed.

## Accepted scope exception

The user accepted the seven missing current members as a known gap. Leave these records unchanged and do not block the remaining backend work or UI rebuild solely on their absence. Continue reporting measured coverage honestly; do not label the roster complete. Reconciliation of these seven is deferred.

## 1. Establish canonical member identities

1. Replace the one-off seed with a roster reconciliation command that defaults to dry-run and produces additions, field corrections, status changes, and ambiguous matches.
2. Use Bioguide IDs as canonical identities; keep source-specific IDs and name aliases in a separately reviewed mapping. Never merge people using a name alone.
3. Import members independently of trades so a person with no disclosures can still be found and followed.
4. Resolve the seven missing identities and review the 88 active-status exceptions using official source evidence and term dates. Preserve historical records, trades, follows, and alert history.
5. Replace the last-500 historical slice with term overlap against a documented supported history window. The current House product cutoff is 2015; define Senate coverage explicitly before claiming comparable history.
6. Retain term/chamber history so a member moving between chambers does not invalidate old filings.

Acceptance: every identified current official member maps to exactly one canonical record; zero unexplained missing current IDs or active-status discrepancies; every historical filer within the supported window has a canonical mapping or a visible unresolved exception. Source fetch/parse failures must abort reconciliation, never mark everyone inactive.

## 2. Prove disclosure coverage

1. Build a filing inventory for each chamber and supported year from official indexes, separate from parsed transaction rows.
2. Record source filing ID, URL, filer identity, filing date, retrieval status, parser version, and outcome: parsed with transactions, parsed with none, unsupported, or failed.
3. Compare indexed filings with captured documents, parsed documents, and stored transactions. Include amendments, duplicate submissions, spouse/dependent ownership, and non-stock assets.
4. Reuse the existing House/Senate audit and parser tools after checking their write/logging behavior. Run controlled backfills for documented gaps; never rerun delivery or publishing jobs as an audit side effect.
5. Keep raw source amounts and dates. Unresolved tickers, partial ranges, and unknown values must remain distinguishable from zero.

Acceptance: 100% of inventoried in-scope filings have an accounted-for outcome; zero unexplained missing filings; every displayed transaction has source provenance and a valid identity mapping. Unsupported/failed filings must be quantified and visible, not counted as fully captured. No-trade status must not be inferred from a failed import.

## 3. Validate data integrity and regression behavior

- Audit orphan member IDs, duplicate economic transactions, impossible dates, truncated amount ranges, and conflicting chamber/party metadata.
- Use reviewed fixtures for House PDFs, Senate HTML/OCR, amendments, option trades, and ambiguous names.
- Run ingestion twice against an isolated database and verify idempotency, unchanged transaction counts, and no duplicate notifications.
- Reconcile a stratified sample from both chambers and multiple years directly against filings. Include each known parser failure category.

Acceptance: no unexplained identity or duplicate-transaction exceptions in release data; parser and idempotency regressions pass. Every remaining limitation has a count, reason, and product-visible handling rule.

## 4. Simplify and measure the API

- Define small contracts for the member directory, trade feed, member detail, company detail, follows, and account state before adding new UI.
- Audit public versus authenticated fields and row-level access with anonymous, ordinary-user, Pro-user, and ops test identities.
- Measure cold/warm API latency and inspect database query plans for real high-volume queries before adding indexes or caches.
- Fix nonunique ordering in ticker pagination; eliminate silent 1,000/1,200/2,000-row truncation or explicitly expose history limits.
- Unify pagination semantics, enforce input limits, and distinguish empty results from failed or partially failed queries.
- Consolidate duplicated dashboard/workspace calculations only after consumer contracts and regression fixtures exist.

Acceptance: no skipped/duplicated rows in stable-dataset pagination tests, explicit partial-result semantics, cross-user isolation tests pass, and recorded latency/query baselines meet a budget chosen from those measurements. No caching of private responses across users.

## 5. Establish operational quality gates

- Preserve billing tests; test auth, checkout, webhook retries, cancellations, follow limits, and delivery retries in sandbox accounts.
- Add roster and filing coverage reports to the existing pipeline/CI design with stored run evidence. Schedule production monitoring only as a separate, explicit rollout.
- Report separate health dimensions: roster completeness, filing capture, parse success, identity resolution, freshness, and API performance.
- Treat a stale source, blocked parser, or absent credentials as unknown/failed health, never a successful empty import.
- Keep dependency audit, build, typecheck, lint, Python tests, and API regressions as release checks.

## Rebuild gate and rollout

Begin the new UI once roster issues other than the accepted seven-member gap are resolved or explicitly accounted for, filing coverage is measured, critical data/API failures are resolved, and remaining historical/source limitations are documented. Then build Activity, Explore, and Following against the agreed contracts.

Before deploying: validate in staging, snapshot affected production data before any repair, review the exact reconciliation diff, make changes transactionally where possible, and retain a rollback path. Deploy UI and backend together only after customer workflows work again. The current local rebuild notice is not a finished customer release.

## Immediate next execution

1. Implement and test dry-run canonical roster reconciliation using the baseline exceptions.
2. Produce a filing-coverage matrix by chamber and year without triggering writes or notifications.
3. Resolve identity ambiguity and draft a concrete database repair diff.
4. Complete API/query measurements and implement prioritized fixes.

Re-run the read-only current-roster baseline with:

```bash
python ops/audit_congress_roster.py --output ops/congress-roster-baseline.json
```

This audit checks current identities/status/chamber only. It does not prove historical completeness, transaction correctness, or filing capture, and its JSON `current_roster_gate_passed` flag must be inspected separately from successful command execution.
