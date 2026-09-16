# Daily tracking email

Apply `supabase_vail_phase17_daily_digests.sql` before deploying the worker and Tracking UI.

- The existing scheduled `dispatch_email_alerts.py` now sends account digests only. It does not send individual event emails.
- Eligible email subscriptions across all of an account’s watchlists share one digest. New raw trades are queued individually, then deduplicated and grouped for the email. Older queued filing summaries are expanded completely, including entries beyond their ten-item preview.
- The first scheduled run after 8 a.m. America/New_York collects pending activity queued before that day’s midnight. Later activity carries forward. There is no empty daily email.
- PostgreSQL serializes account claims, enforces one digest per Eastern calendar day, and also checks a rolling 24-hour interval. Earlier individual sends count against the cap during migration.
- Personal per-event sends are blocked at the database boundary. Owner/system SMS and Discord broadcasts retain their existing behavior.
- A provider rate limit retries the same digest. An ambiguous provider result remains `uncertain`; a failure after provider acceptance remains `sending`. Never reset these without verifying the provider outcome. The provider idempotency key is `vail-daily-digest-<id>`.
- `preparing` means no provider call has started for that claim. An assembly failure needs investigation before requeueing that same digest; never create another digest to bypass the cap.
- The Sent tab uses authenticated, account-scoped history functions. New digests preserve the rendered text and every included item, independent of later source corrections. `sent_at` is recorded after provider acceptance, not a promise of inbox delivery. Earlier individual emails use their existing delivery records.
- Tests use fake recipients and mocked providers. Do not run the real dispatcher to test rendering. `render_digest` builds preview HTML/text without sending.
