# Atomic follow writes

Apply `supabase_vail_phase20_atomic_follows.sql` before deploying the account API
changes. It adds the service-only `save_account_follow` function and preserves
all existing profiles and follows.

The function locks the account profile, verifies ownership of the watchlist,
checks the combined ticker and actor count, and saves the follow in one
transaction. Existing follows can still be updated when an account is at or
above its current limit. A full list returns HTTP 409 from the account API.

The migration is included in `scripts/apply_production_schema.py`. For an existing
installation, apply only the new file using your normal SQL migration process.
If it has not been applied, follow creation returns HTTP 503 rather than using
the old non-atomic writer.

Run the database regression checks against an isolated PostgreSQL database:

```bash
VAIL_TEST_POSTGRES_DSN=postgresql://localhost/vail_test python scripts/test_atomic_follows_postgres.py
```
