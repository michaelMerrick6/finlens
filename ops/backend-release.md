# Backend handoff — September 11, 2026 (Pacific)

The backend is ready for frontend development. This is a bounded release check,
not a claim that all historical filings or OCR layouts are complete.

## Changes

- Senate ruled-table OCR uses local OpenCV/Tesseract with document-level rejection
  on unsupported or ambiguous input. House scan parsing rejects ambiguous marks
  and invalid date cells and preserves repeated physical transactions.
- Congress member lookups paginate the complete table, fail on lookup errors, and
  match complete compound names across first/last-name boundaries without dropping
  name tokens. Existing reviewed source-hash overrides remain in place.
- Activity statistics use exact counts. Database failures return 503 rather than
  plausible empty statistics. Autocomplete identifies partial source failures.
- Profile, workspace and disclosure reads paginate consistently. Disclosure
  histories exceeding 4,000 rows fail rather than returning partial portfolios.
- CI runs Python pipeline tests and backend API contract tests.

## Frontend contracts

- Auth stats: `fundHoldingRowsLastWeek` replaces `fundFilingsLastWeek`; it counts
  holding rows. Unavailable statistics return HTTP 503 with null stats.
- Search autocomplete: inspect `partial` and `failedSources`. Both sources failing
  returns 503; an overlong query returns 400.
- Politician profiles: inspect `history.hasMore`, `history.rowLimit` and
  `tradesTruncated`. Workspace pagination must follow the returned `nextOffset`.
- Ticker intelligence: inspect `history.potentiallyTruncatedSources` and
  `rowLimitPerSource`; paginated transaction counts include `totalCountIsExact`.

## Production data repair and verification

A fresh five-House/five-Senate source audit found 78 House rows attached to April
McClain Delaney's fallback ID and nine missing John Boozman trades across two newly
filed Senate documents. A backed-up transaction relinked the House rows to M001232,
corrected 78 raw and 134 signal payloads, and inserted the nine Senate trades with
matching raw filings and base signals. A subsequent backed-up transaction merged
18 obsolete derived signal identities into their existing canonical events,
preserving one delivery reference and six tweet-candidate references. No delivery
workers were run.

The subsequent ten-filing audit passed with zero parsing, transaction, date,
row-count or member-resolution discrepancies. See backend-release-audit.json.
Local backup files remain ignored because they contain operational database data.

Validation: all 33 Python test files; 6 backend, 12 billing and 4 pagination
JavaScript tests; TypeScript, ESLint and production build passed. Read-only API
smokes covered stats, autocomplete, workspace and unauthorized account access.
Production dependency audit reported zero vulnerabilities; pip check passed.

## Remaining boundaries

- The accepted seven-member coverage gap remains. Previously measured active-status
  anomalies are not automatically deactivated.
- Senate OCR evidence is 111 rows in one development filing, not a held-out corpus.
  Unknown layouts and degraded handwriting can still require source review. House
  scan coverage is not exhaustive; historical completeness is not established.
- Live authenticated multi-user billing and outbound delivery were not exercised.
  Validate these in a controlled environment before restoring those user flows.
- UI remains the intentional rebuild notice. Begin the frontend with search,
  politician profiles and transaction browsing, using the failure and pagination
  contracts above.
