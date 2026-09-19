# Historical source review — September 19, 2026

## Scope and results

The initial 3,380 failed Congress filings span 2012–2026. Only 216 were filed in
2024 or later. Prioritize that user-requested window; do not call these counts
missing trades, since some failed filings already have stored rows.

### House identity batch

Cleared 24 filing failures covering 58 existing transaction records:
18 Michael A. Collins filings, three Neal Patrick Dunn filings, two Michael Garcia
filings and one Anna Paulina Luna filing. Reparsed official PDFs, resolved the
member IDs, and published through the atomic filing publisher. All 24 ledger
entries completed. Detailed per-filing counts/IDs are in
`audits/2026-09-19-house-identity-recovery.json`.

Cause: missing Michael/Mike nickname equivalence, credentials treated as surnames,
and the Clerk index splitting Anna / Paulina Luna differently from our roster.
Ambiguous names still fail closed; no surname-only match was introduced.

Evidence includes original filing headers (name, state and district):
[Collins GA10](https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20033840.pdf),
[Dunn FL02](https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2024/20026250.pdf),
[Garcia CA27](https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2024/20025632.pdf),
[Luna FL13](https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2024/20025103.pdf).
Official Clerk profiles confirm [Collins C001129](https://clerk.house.gov/members/C001129),
[Dunn D000628](https://clerk.house.gov/members/D000628), and
[Luna L000596](https://clerk.house.gov/members/L000596).

Remaining from 2024 onward: **192** (168 House extraction/layout failures,
7 House transaction/filing-date conflicts, 2 House historical-chamber identity
failures, 15 Senate paper-form failures). Global unresolved total: 3,356.
Banks and Gallego require historical chamber handling, not a change to their
current Senate roster entries.

### Fund quarters

Restored only previously empty, validated target periods using atomic publication:

| Fund | Quarter | Stored rows | Unresolved supported source rows |
| --- | --- | ---: | ---: |
| Coatue | 2025-12-31 | 76 | 28 |
| Farallon | 2025-09-30 | 110 | 27 |

Database readback matched ticker/share/value sets. Source rows can aggregate into
fewer stored ticker positions; unresolved securities are not invented. These
restorations do not certify complete fund portfolios. Exact SEC filing URLs and
counts are in `audits/2026-09-19-fund-quarter-recovery.json`.

D. E. Shaw 2026-06-30 was already restored by the background worker before this
batch wrote it. Independently compared all 2,428 stored ticker/share/value rows:
no missing or extra rows against the parsed effective snapshot. No rewrite made.
There remain 1,205 supported source rows without resolved tickers for that filing.

### Amendment bug corrected

The old code selected the latest filing for a period even when that filing only
added holdings. New logic distinguishes ORIGINAL, RESTATEMENT and NEW HOLDINGS,
retains the original baseline for additions, and excludes unknown amendment types,
missing baselines and overlaps requiring further review. Accession deduplication
prevents adding the same amendment twice. Original source links remain on rows;
the assembled snapshot's availability date is the final amendment date.

Berkshire 2025-03-31 remains withheld pending overlap review: original
0000950123-25-005701 includes 152,572 LEN-B shares; NEW HOLDINGS amendment
0000950123-25-008361 adds an entry for 202 LEN-B shares alongside three other
positions. The source labels alone have not been treated as enough to resolve
whether overlapping aggregated entries can safely combine.

Reference: [SEC filing manual, amendment behavior](https://www.sec.gov/files/edgar/filermanual/archive/edgarfm-vol2-v76.pdf).

Also prevented comparisons across missing quarters from being labelled
quarter-over-quarter changes or generating false exited positions. A rejected
amendment no longer causes a silent fallback to an older quarter snapshot.

## Validation and next work

All 63 pipeline test files passed. The focused 13F suite passed 23 tests after the
final changes. Next: historical-chamber identity cases, source-specific paper form
layouts, Berkshire's overlapping amendment entries, and unresolved fund security
identities. Do not loosen parser validation merely to empty the queue.

## Follow-up source review

### Banks and Gallego historical House filings

Resolved `house-2024-20025626` (Jim / James E. Banks, B001299) and
`house-2024-20025670` (Ruben Gallego, G000574) using their reviewed House service
windows. Current Senate roster affiliations are unchanged. No cross-chamber
surname fallback was added; filings outside the reviewed years remain unresolved.
Three existing transaction rows were reparsed and atomically republished, not
three new trades. Both filing ledgers now report complete.

Sources: [Banks House service](https://history.house.gov/People/Detail/15032440317),
[Gallego House service](https://history.house.gov/People/Detail/15032409715), and the
original [Banks PDF](https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2024/20025626.pdf)
and [Gallego PDF](https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2024/20025670.pdf).
Audit: `audits/2026-09-19-house-historical-chamber-recovery.json`.

### Berkshire overlap resolved

The amendment explicitly says NEW HOLDINGS, confidential treatment expired,
original report dated May 15, 2025. SEC Form 13F Confidential Treatment Instruction
4 and Special Instruction 3 establish that these entries supplement the public
report. Direct review of both information tables found the same Class B CUSIP
526057302: original 152,572 shares / $16,641,028 plus newly disclosed 202 shares /
$22,032 = **152,774 shares / $16,663,060** as of March 31, 2025.

Added an exact-source review in `config/reviewed_13f_additions.json`, requiring
both accessions, document hashes, source URLs, quarter, overlap set, and matching
security identifiers. Other overlaps still fail closed. Repeated accessions do
not double-count additions; combined snapshots retain the amendment availability
date of August 14, 2025, and the audit preserves both source URLs.

Restored the previously empty Berkshire quarter: **38 stored ticker positions**,
102 resolved source rows out of 114 supported source rows; 12 still unresolved.
Database readback matched every ticker/share/value tuple. This is not a claim of
complete portfolio coverage. Audit: `audits/2026-09-19-berkshire-amendment-recovery.json`.

### Fetterman paper filing

Visually reviewed both original Senate images for
`senate-3b1affbf-2359-4fef-980d-1d2600cb8731`. The first is a counsel cover letter;
the second lists two dependent-child bond purchases: Nokia on June 6, 2024, and
Freeport McMoran on June 21, 2024, each $1,001–$15,000. The cover letter
corroborates the dates. Printed IBM/Microsoft examples were excluded. Bond
positions retain `N/A` tickers instead of being attributed to the issuers' stocks.

The parser had attempted to read the cover letter as a transaction table. A
source-image-hash-locked transcription now handles this filing. Two existing rows
were republished with a matching database readback, and the ledger is complete.
Audit: `audits/2026-09-19-fetterman-paper-recovery.json`.

### Updated remaining work and validation

**189 failed filings from 2024 onward** remain: 175 House (168 extraction/layout,
7 date conflicts) and 14 Senate paper filings. Fund security-resolution gaps
remain; the previously identified Berkshire overlap is resolved.

All 63 pipeline test files passed after the code changes. The final Fetterman
fixture and regression test then passed all 12 reviewed-filing tests. Tests cover
historical chamber boundaries, ambiguous/wrong names, exact amendment source
matching, security identity mismatch, duplicate accessions, and unchanged baseline
objects. No notifications were manually dispatched.
