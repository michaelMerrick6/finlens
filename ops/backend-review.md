# Backend review after UI reset

## Completed locally

- Removed ten library modules unreachable from app routes and middleware after the UI reset, including the former browser auth client, UI presentation helpers, and ops-page aggregations. Removed the obsolete auth-return helper tests; server authentication and access controls remain.
- Applied compatible dependency updates. Installed Next.js is 16.3.4; npm audit reports zero known vulnerabilities at review time.
- Fixed unsearched congressional feed pagination: application-level filtering now advances a raw-row cursor, scans for the next valid result, and does not mistake discarded rows for the end of the feed. Scanning is bounded to ten batches per request. Clients must use nextOffset and hasMore, including when a sparse page is empty.
- Added four pagination regression tests and registered them in CI alongside the twelve billing tests.
- Lint, typecheck, production build, and all sixteen JavaScript regression tests passed.

## Remaining review

- Inspect database query plans and actual indexes before proposing index migrations. No production database measurements or mutations were performed in this pass.
- Investigate duplicate politician identities against canonical member IDs; do not merge records solely by display name.
- Ticker aggregation loads up to 2,000 rows per source and sorts paginated database reads by a nonunique date column. Review stable ordering and expose truncated-history semantics before rebuilding holdings displays.
- Politician profiles request up to 1,200 rows in one database request. Verify the configured API row cap and add explicit pagination if necessary.
- Search paths and feed paths use different pagination strategies. Ranked search remains capped at 250 rows per query source; the new raw-row cursor fix applies to the unsearched feed only.
- Autocomplete currently converts search-source failures into empty arrays. Add explicit partial-failure/unavailable responses before the new UI relies on it.
- Consolidate duplicate dashboard/workspace aggregation only after defining the Activity, Explore, and Following API contracts.
- Validate auth, account flows, billing, and delivery end to end once an appropriate test environment is available. No real notifications or payments were triggered.

The UI reset and backend changes are local and have not been pushed or deployed.
