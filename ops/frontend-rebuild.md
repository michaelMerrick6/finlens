# Frontend rebuild — September 12, 2026

The new frontend centers on Latest, Politicians, and Tracking. It uses live disclosure records, exact member and ticker scopes, source links, clear transaction/filing dates, responsive layouts, and optional email delivery. No new runtime dependencies were added.

## Validation

- Production build, TypeScript, ESLint, and 35 JavaScript tests passed.
- Live API checks passed for member/direction filters, distinct pagination, exact ticker filters, full-name search, empty results, and invalid scope rejection.
- Disposable-account checks passed for tracking creation/removal, account isolation, and email remaining disabled until opted in. All temporary accounts were deleted; no email was sent by these checks.
- Desktop and mobile previews checked, including disclosure details, profile navigation, sign-in dialog, repeated searches, and clearing URL filters. Mobile directory width was 390px without horizontal overflow.

## Before launch

Supabase Auth currently returns `http://localhost:3000` as the magic-link redirect even when the production tracking URL is requested. In Authentication → URL Configuration, set Site URL to `https://www.vail.finance` and allow `https://www.vail.finance/tracking` (plus `http://localhost:3017/tracking` for local review). Recheck the generated redirect after saving. Authenticated APIs passed independently; end-to-end magic-link sign-in remains blocked by this configuration.

The phase 12 migration was applied to production to default new profiles to email disabled. Existing preferences are preserved. The schema application script now includes this migration.

Email alerts currently cover disclosed activity, including purchases and sales. Purchase-only email preferences are not implemented. Stripe validation, unusual OCR expansion, and broader historical completeness remain outside this frontend rebuild; the UI explains available-record limitations.
