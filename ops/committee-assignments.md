# Committee assignments

## Scope and evidence

Current House: Clerk MemberData XML, including subcommittee membership and explicitly stated leadership.
Current Senate: official member XML plus the union of the committee index and member-listed committee codes. Every full committee membership in the member directory must also appear in its committee XML. Senate identity resolution requires a unique normalized last name + state in the official Bioguide directory; unknown/ambiguous identities reject the chamber.

House and Senate source-specific committee IDs intentionally stay separate (including the two chambers of joint committees). Archive Thomas IDs are a different namespace. Do not join these on names or count their combined catalog as distinct congressional committees. A researched ID crosswalk is needed before cross-source longitudinal committee screens.

Archive: all committee-membership-current.yaml revisions from unitedstates/congress-legislators since 2024-01-01, plus the last preceding revision as a baseline. Committee names come from the same Git revision. This is a secondary community archive, not an official effective-date record. Transitional empty/partial snapshots remain in the database as coverage gaps. An archive observation's Congress is the Congress at observation, not proof of every listed member's tenure. No inferred start/end dates. The UI lists top-level archived assignments; stored subcommittee assignments remain available for future research.

## Storage and publication

Apply `supabase_vail_phase18_committee_assignments.sql` before deploying the UI/worker. All new tables and publishing/history functions are service-role only. Snapshots retain raw documents and SHA-256 digests. New official Bioguide identities seed missing congress_members rows from explicit source name/state/party fields; existing profiles are never overwritten. Rechecking identical evidence refreshes verified_at without duplicating all rows. A changed source creates another immutable content snapshot; sync run records preserve verification outcomes. If content reverts, that existing content snapshot becomes the latest verified snapshot. Observation timestamps describe first capture of that exact content, not a continuous membership interval.

The publisher serializes each source with a transaction-scoped advisory lock. Assignments and snapshots publish atomically. Rejects unresolved references, empty current snapshots, losses over 10% (minimum five), additions over 25%, disappearing committees, out-of-order observations, and Congress transitions. Python additionally validates current chamber coverage floors, roster identities, duplicate rows, source consistency, and House Congress number. Failed parsing or publication leaves the last successful snapshot intact. Partial House/Senate success is allowed; failure of either chamber fails the overall job.

## Operation

- Dry run: `python scripts/sync_committee_assignments.py`
- Publish official current: `python scripts/sync_committee_assignments.py --write`
- Historical backfill: `python scripts/sync_committee_assignments.py --backfill --write`
- Requires SUPABASE_URL / SUPABASE_SERVICE_KEY; local .env.local is supported.
- GitHub workflow `Verify Committee Assignments` runs daily at 11:10 UTC and supports manual dispatch/backfill.
- Failed jobs use GitHub Actions failure status/notifications; repository notification delivery depends on the owner's GitHub settings. No external messaging credentials are required.
- `committee_sync_runs` records starts, successes, and failures. A stuck running entry or last successful verification older than 72 hours requires investigation.
- Politician profiles warn when a roster has not been successfully verified for 72 hours, including when the scheduler stops entirely.
- Successful HTTP requests cannot prove the government has updated a stale source. “Last verified” means successfully fetched and validated, not the government's effective date.

## Recovery / Congress transitions

Inspect saved evidence and source pages; repair the parser or mapping, run tests and dry run, then republish. Never bypass a failed validation by emptying the existing dataset. At a new Congress, review the complete proposed baseline and source changes before an explicit migration changes the guarded Congress baseline. Unexpected removals are deliberately held for review; do not silently lower thresholds.

## Validation

`python scripts/test_committee_assignments.py` runs offline parser contracts. Set VAIL_TEST_POSTGRES_DSN to an isolated PostgreSQL database to also test actual publication, preservation on failure, authorization, idempotency, and Congress transition gates. Production data must never be used for these fixtures.
