# Private HQ

Open `/hq` and sign in with the verified email configured in the server-only `VAIL_HQ_OWNER_EMAIL` environment variable. Set it in `.env.local` for local access and the hosting environment for production. Without that setting, access fails closed. `/api/hq` validates the Supabase bearer session and confirmed email on every request; responses are private and not cached.

The page reports 30-day browser visitor analytics, retained politician and insider records inserted over 7 days, 30 days, and UTC year to date, plus the latest 40 scraper runs. Ingestion counts include backfills and are not trade-date counts. Missing queries display unavailable. The manual refresh control requests current values.

Browser analytics was restored after the UI rebuild; it excludes local development, HQ, API and ops pages and honors browser privacy signals. Historical collection gaps cannot be reconstructed. No customer addresses, access tokens or scraper log excerpts are exposed by HQ.

HQ also requires the separate password stored as a salted scrypt hash in server-only `VAIL_HQ_PASSWORD_HASH`. The local owner email and hash are configured. Both environment settings must also be configured on the production host before deployment. Password entry stays in page memory only, is sent in a request header, and clears on page reload/sign-out.
