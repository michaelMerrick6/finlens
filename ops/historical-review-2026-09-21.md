# Historical source review — September 21, 2026

## Completed

Fresh database queue: **190** failed Congress filings from 2024 onward (176 House,
14 Senate). This was one more than the prior audit: Scott Peters filing
`house-2026-20035191` had subsequently failed the completeness check.

Reviewed original source images/PDF pages and atomically published four filings:

| Member / filing date | Filing | Previous rows | Verified rows |
| --- | --- | ---: | ---: |
| Richard Blumenthal / 2026-02-12 | `senate-068274e1-4b7a-4453-a242-563dde4c10d8` | 15 | 36 |
| Richard Blumenthal / 2025-08-13 | `senate-09694768-3b1e-4af0-8b46-8521c00cbd96` | 14 | 23 |
| Richard Blumenthal / 2024-10-16 | `senate-520e479e-3586-4de5-a16b-cb7c22414594` | 0 | 10 |
| Scott Peters / 2026-08-17 | `house-2026-20035191` | 15 | 15 |

**84 verified rows; 40 net additional records.** The Peters data already existed;
the repair makes the current parser complete and clears its failed ledger entry.
All four ledgers are complete, and database readback matches the complete parsed
row multisets (including amounts, asset names/types, dates, members and tickers).
Audit: `audits/2026-09-21-historical-recovery.json`.

## Source verification

- Blumenthal February: all five source images, including cover letter, reviewed.
  Four transaction pages contain 8/12/11/5 rows. Account headings and examples
  excluded. Identical-looking private fund/LLC transactions retained across
  separate spouse-owned accounts.
- Blumenthal August: all four images reviewed; transaction pages contain 7/10/6
  rows. July 3 ELCM2 sales are distinct from July 14 sales. Amount bands checked
  separately for each row.
- Blumenthal October: all three images reviewed; five rows on each transaction
  page. Two Brazilian government bond purchases and eight private LLC purchases.
  No public equity tickers inferred for these assets.
- Senate transcriptions include page/row/account provenance and SHA-256 hashes
  of all original RGB images. Publication re-fetched the sources and checked the
  hashes through the normal reviewed-filing parser. Changed images or filing
  metadata will fail closed rather than silently reusing a transcription.
- Peters: all three original PDF pages visually reviewed. The source has 15 rows,
  with three amounts labeled `Spouse/DC Over $1,000,000`. The text parser now
  accepts that exact prefix and retains the open-ended amount. It previously
  rejected these rows, while the layout parser found only 12; the existing
  completeness check correctly prevented partial publication.
- Peters Treasury bills keep the existing `US-TREAS` internal category and GS
  asset type. This category is excluded from public-stock analysis. Other
  private/municipal assets keep `N/A` tickers.

Original sources:

- [Blumenthal February 2026](https://efdsearch.senate.gov/search/view/paper/068274e1-4b7a-4453-a242-563dde4c10d8/)
- [Blumenthal August 2025](https://efdsearch.senate.gov/search/view/paper/09694768-3b1e-4af0-8b46-8521c00cbd96/)
- [Blumenthal October 2024](https://efdsearch.senate.gov/search/view/paper/520e479e-3586-4de5-a16b-cb7c22414594/)
- [Peters original PDF](https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20035191.pdf)

## Amendment resolved in a subsequent batch

The [September 12, 2025 original](https://efdsearch.senate.gov/search/view/paper/e08ef39c-87e8-4381-9f92-41218ecc7cd4/)
and [October 2, 2025 amendment](https://efdsearch.senate.gov/search/view/paper/f8c167e9-090d-471d-81c9-f8c7366420d1/)
are now reconciled. All five original source images and the amendment were
visually reviewed. The original contains 35 transactions; printed page 1, line 7
has no amount checked. Counsel's amendment explicitly replaces that spouse-owned
SERA sale in the Peter L. Malkin Family 2000 LLC account: August 14, 2025,
$1,001–$15,000.

- Original: 34 active rows, preserving the source index gap at the replaced row.
- Amendment: one corrected row, retaining its October 2 availability date.
- Combined: 35 transactions, up from 21 stored rows (**14 net restored**).
- Four separate SERA sales and five IRTC sales remain across distinct accounts;
  identical trade values do not establish duplicates.
- Original transaction pages retain 8/10/11/5 active rows; the excluded row and
  its replacement have reciprocal account/page/row/index provenance.
- Both sources were fetched again and their image hashes checked before publication.
- Both filings were published in one PostgreSQL transaction through the existing
  publication functions. Full row readbacks, complete statuses, reciprocal ledger
  metadata, and absence of the superseded row were checked before commit.
- Publication now rejects changed or resurrected rows for linked reviewed filings.
  Regression tests cover missing links, wrong member/amount/date, duplicate original
  rows, and rejected publication before any RPC call.

Audit: `audits/2026-09-21-blumenthal-amendment.json`.

## Remaining: 184 filings

175 House and 9 Senate failures from 2024 onward remain at this batch's readback.
These are failed filing reviews, not counts of missing trades. Historical holdings
and source completeness are not implied by clearing these two failures.

## Validation

All **63 pipeline test files passed** after these changes. Added regressions for
wrapped spouse/DC open-ended amounts, incomplete/unknown amount labels, source
image changes, physical row counts, and repeated transactions across accounts.
No notifications were manually dispatched. Unrelated strategy/UI work is excluded
from this change.
