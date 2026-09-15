# Vail

## UI reset in progress

The previous interface has been removed locally in preparation for a backend review and a new UI. Only a minimal rebuild notice and a not-found page remain. Former product, marketing, account, authentication (including callback/reset), and ops screens are unavailable. Do not deploy this reset expecting a usable customer application.

All 39 API endpoints retain their URLs; backend fixes are being applied during the review. Routes previously nested under `src/app/(app)/api` now live in `src/app/api`. Server libraries, database schemas, ingestion scripts, scheduled jobs, billing webhooks, and access controls are retained. Ten modules with no remaining application callers have been removed after a dependency review.

Next: review API contracts, data quality, query performance, authentication, billing, and pipeline reliability; then build Activity, Explore, and Following. Restore indexing when the new public experience is ready. The last complete UI is in git commit `b0dc4c4`.

Vail is a Next.js + Supabase product for tracking three public-market signal streams in one place:

- Congressional trades
- SEC Form 4 insider trades
- 13F fund position changes

The project is easiest to understand if you treat it as two layers:

1. A small production core that ingests data, compiles signals, and queues alerts
2. A larger ops toolbox for audits, repair jobs, historical backfills, and social posting

## Core Product Architecture

The core system is intentionally simple:

1. Ingest source data into canonical tables
2. Normalize raw source rows into `signal_events`
3. Compile higher-order signals such as cluster buys and cross-source accumulation
4. Match those signals against user follows and queue alert deliveries
5. Serve the same canonical data in the web app

That architecture is the project story to lead with on GitHub and in a resume.

## Core Files

- Temporary app shell: `src/app/page.tsx`
- API routes: `src/app/api/`
- Public data reads: `src/lib/public-data.ts`
- Ticker aggregation: `src/lib/ticker-intelligence.ts`
- Base schema: `supabase_schema.sql`
- Product pipeline schema: `supabase_vail_phase1.sql`
- Notification follows: `supabase_vail_phase2_notifications.sql`
- Follow modes: `supabase_vail_phase4_follow_modes.sql`
- User accounts: `supabase_vail_phase5_user_accounts.sql`
- Billing: `supabase_vail_phase6_billing.sql`
- Authoritative DB setup: `scripts/apply_production_schema.py`
- Core pipeline entrypoint: `scripts/run_core_pipeline.py`
- Raw signal emission: `scripts/emit_signal_events.py`
- Derived signal compilation: `scripts/compile_derived_signal_events.py`
- Alert queueing: `scripts/queue_alert_deliveries.py`

## Production Path

Run the app:

```bash
npm install
npm run build
npm run start
```

Apply the authoritative production schema path:

```bash
python3 scripts/apply_production_schema.py
```

Run the core production pipeline:

```bash
pip install -r scripts/requirements.txt
python3 scripts/run_core_pipeline.py
```

Run alert delivery workers by channel:

```bash
python3 scripts/dispatch_email_alerts.py
python3 scripts/dispatch_sms_alerts.py
python3 scripts/dispatch_discord_alerts.py
```

Use the full maintenance pipeline only when you want audits, repair jobs, and social/broadcast automation:

```bash
python3 scripts/daily_scraper.py
```

Ops-only entrypoints now live in `ops/`, and one-off SQL repair files live in `ops/sql/`.

## Scheduling

Congress capture uses a durable filing queue and atomic publication. Apply
`supabase_vail_phase14_congress_capture.sql` before deploying its workers. See
[Congress capture operations](ops/congress-capture.md) for deployment order,
retry behavior, validation, and remaining coverage limits.

Congress capture runs hourly through GitHub Actions. The current Vercel plan allows only daily crons, so its authenticated dispatch route provides a daily fallback and can re-enable workflows disabled after repository inactivity:

- `/api/cron/github-dispatch/capture-congress`
- `/api/cron/github-dispatch/capture-insider`
- `/api/cron/github-dispatch/process-signals`
- `/api/cron/github-dispatch/capture-13f`
- `/api/cron/github-dispatch/daily-scraper`

If you deploy on Vercel, `vercel.json` registers these cron jobs for the Production deployment. Configure `CRON_SECRET` plus `GITHUB_ACTIONS_DISPATCH_TOKEN`. If you use another host, point any external scheduler at the same endpoints.

## Local Development

1. Use Node `22.13.0` or newer (the version pinned by `.nvmrc` and `.node-version`)
2. Create your env from `.env.example`
3. Install frontend dependencies with `npm install`
4. Install pipeline dependencies with `pip install -r scripts/requirements.txt`

## Useful Checks

```bash
npm run lint
npm run typecheck
npm run build
npm run test:python
```

## Repo Boundary

The repository is split deliberately:

- `scripts/` is the production path
- `ops/` is the maintenance and remediation workspace

If you want the repository to stay strong as a flagship project, keep the public narrative centered on:

- Clean source tables
- A canonical `signal_events` layer
- A small derived-signal compiler
- User follows plus queued notifications
- A thin app surface on top of the same database

Everything else should be treated as supporting ops infrastructure.
