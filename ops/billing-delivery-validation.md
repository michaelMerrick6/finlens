# Billing and delivery validation — September 11, 2026 (Pacific)

Email delivery is verified through the real provider and owner-confirmed inbox
receipt. Billing has passed local lifecycle and account-isolation tests, plus
live configuration checks and correction of the webhook URL. A real Stripe sandbox checkout/payment is
still untested: only a live key was available, and no charge was created.

## Evidence

| Check | Result |
| --- | --- |
| Live Stripe price | Active, live, recurring monthly USD price |
| Live Stripe webhook | Corrected to the canonical www.vail.finance host; enabled event settings preserved; unsigned request rejected with 400 |
| Signed webhook lifecycle | Real Stripe SDK signature verification; isolated database and Stripe API fixtures |
| Account isolation | Checkout and portal use the authenticated bearer user, ignoring another user/customer in the body |
| Retry and entitlement tests | Duplicate events, invalid signature, database/Stripe failure, cancellation, replacement subscription, wrong customer metadata and unknown price covered |
| Production database access | RLS enabled on seven account/billing tables; no client mutation policies or public webhook-payload policies |
| Delivery claim | Real database compare-and-set and new statuses checked inside a rolled-back transaction |
| Email | One authorized message through the shared dispatcher and Resend; HTTP 200 and owner-confirmed inbox receipt |
| SMS and Discord | Local worker safety tests; no live messages sent; local SMS credentials absent |

Private provider results and database checks are retained under the ignored
`artifacts/backend-validation/` directory. No production delivery queue was drained,
no failed historical message was replayed, and no customer billing state changed.

## Fixes resulting from validation

The live endpoint was incorrectly configured as
`https://vail.finance.com/api/stripe/webhook`. It now uses
`https://www.vail.finance/api/stripe/webhook`. The non-www vail.finance host redirects
with 308, so the canonical www host is used directly. The existing endpoint was
updated after saving its configuration; event subscriptions and status were
preserved. This proves routing and unsigned-request rejection, not a successful
signed Stripe payment event. No payment was created.

- Webhook processing failures return 500; signature failures return 400. Failed
  billing updates remain unrecorded so the event can be retried.
- An unrelated active Stripe price cannot grant Pro. Conflicting customer/account
  metadata is rejected. A late cancellation resolves an active replacement before
  changing entitlements.
- A conditional pending-to-sending update prevents competing workers from sending
  the same queued record. The worker rechecks subscription status and destination
  immediately before sending.
- Missing provider configuration leaves queued rows unchanged. Explicit rate limits
  leave the affected row pending and stop the batch. Email requests are paced and
  use a stable provider idempotency key.
- Provider timeouts or ambiguous errors become `uncertain`. If the provider accepts
  a message but recording success fails, the row remains `sending`. Neither state
  is automatically resent. Scheduled signal work no longer cancels an in-flight run.

The historical delivery audit found 217 failed email rows, including 213 rate-limit
failures from June–July 2026. Those old failures remain historical records. There
are also legacy Discord/Telegram queue entries; this validation does not certify
or enable those broadcast channels.

## Operations and launch boundary

`sent` means the provider request completed locally, not guaranteed inbox receipt.
The one controlled email test additionally has human-confirmed inbox receipt.
Review `sending` and `uncertain` against provider logs before any manual retry;
never bulk-reset them to pending. Resend retains idempotency keys for 24 hours,
so they are an additional safeguard, not permanent exactly-once delivery.

Before enabling billing in the rebuilt frontend, use a Stripe sandbox/test key and
matching test price to complete checkout, renewal/failure and portal cancellation
against an isolated account database and test webhook. Local fixtures do not prove
that external payment lifecycle. Live multi-user browser sessions and live SMS/
broadcast delivery remain outside this pass.

All 34 Python test files and 32 JavaScript tests passed, together with TypeScript,
ESLint and the production build.

Validation commands:

```sh
npm run test:billing
npm run test:backend
npm run test:pagination
SUPABASE_URL=https://example.supabase.co SUPABASE_SERVICE_KEY=test-key python scripts/run_tests.py
npm run typecheck
npm run lint
npm run build
```

Provider contracts: [Stripe webhook retries and event ordering](https://docs.stripe.com/webhooks?lang=node),
[Resend idempotency keys](https://resend.com/docs/dashboard/emails/idempotency-keys),
[Stripe endpoint URL updates](https://docs.stripe.com/api/webhook_endpoints/update).
