# Conversational research beta

## Scope

Analysis has a natural-language interpreter plus an independently usable filter form. One ticker, one SEC industry, one politician, one current committee, chamber, direction, 1–366 rolling days, date basis, and a minimum distinct politician count are supported. Stocks/ETFs only. Unsupported financial ratios, forecasts, options, legislative connections, fixed historical intervals, and trade-time committee membership must stop with an explanation. The LLM never executes SQL or calculates counts. It emits validated filters; deterministic code returns evidence and counts. Amounts in results are the underlying disclosed strings.

## Model and quota

Server-only OPENAI_API_KEY. RESEARCH_MODEL defaults to gpt-5.6-luna; gpt-5.4-mini is supported for evaluation. Responses API structured outputs, low reasoning, 2,500 maximum output tokens, 25-second timeout, store=false. Only the question, prior filters, and public classification/member/committee catalog are sent. No user emails, private follows, tokens, or complete trade history are sent to the model.

Interpretation requires an authenticated user and an atomic database quota of 20 requests per UTC day. Failed model requests consume an attempt. Manual filters remain available without model calls. Saved screens are scoped to authenticated user IDs and limited to 20 by a database lock/trigger. Saved screens rerun rolling windows; daily-email integration is intentionally not enabled in this first beta, as explained in the UI. No extra notification sender was introduced.

## Classification

The preexisting companies.industry values were placeholders (Unknown / 13F Filing). Do not use them for screening. The separate company_research_classifications table stores SEC SIC descriptions, CIK submission source URLs, and verification timestamps. The daily job checks recently disclosed tickers (90 days), fills missing SEC matches, and renews records older than 30 days, up to 400 per run. Exact ticker membership in the SEC submission JSON is required. ETFs, unmatched ticker aliases, or missing SIC descriptions remain unclassified. Company classifications are current, not historical sector membership. Classified/unclassified coverage is displayed per screening window.

## Data limits and semantics

Read paginated records, fail rather than return a partial result at 50,000 scanned trades. Catalog/trade reads cache for five minutes. Date ranges are UTC dates inclusive of today, with later future disclosure dates excluded. Committee filters use the latest verified official roster, requiring verification within 72 hours. They never infer tenure from archive snapshots. The UI labels current membership. Duplicate source row IDs and source-reviewed superseded records are excluded; unreconciled separate amendments can remain. This is not a claim of complete government coverage.

## Deployment

Apply supabase_vail_phase19_research_screens.sql. Run scripts/sync_research_classifications.py with service-role credentials. Enable the committed daily classification GitHub workflow by deploying to main. Configure OPENAI_API_KEY server-side in local and production environments; never use a NEXT_PUBLIC key. Missing API access disables the assistant with a visible message while manual screening remains available.

## Verification

npm run test:backend includes research filter/auth/ownership contracts. scripts/test_research_database.py with isolated VAIL_TEST_POSTGRES_DSN verifies concurrency, quota enforcement, private privileges, and saved-screen limits. Test actual model interpretations against fixed expected filters before changing the prompt/model. The initial six-question live evaluation covers counts, year-to-date all-activity, industry classification, forecasts, historical committee membership, and unsupported valuation criteria. It is a smoke evaluation, not proof that all natural-language queries are reliable; users can inspect/edit filters.

## Chat interface

The screen is entirely conversational: one composer remains available after successful answers, unsupported requests, and errors. No manual criteria panel or starting-criteria chips are shown. New search clears context. The last three question/answer pairs are bounded and validated server-side so clarification replies remain meaningful even when there was no successful screen. Saved screens retain validated query filters.

Popularity is defined as the number of distinct politicians in matching records, with ties explicitly reported. The model translates scope; deterministic application code supplies the ranking and counts. An accompanying request for an explanation does not reject a supported ranking; motives and causal explanations remain unknown. Unsupported selection conditions (such as PE ratios) still block a broader search. Dates and source evidence remain visible with results.

## Evidence-led briefs

Each successful turn now retains its own result snapshot and renders a newsletter-style brief with up to three featured companies. A deterministic writing step composes the finding and company paragraphs from the query results, including ties, distinct-politician counts, transaction counts, names and reported dates. It adds no outside company narrative or financial forecasts. Every featured section exposes its underlying disclosures, and remaining matches are expandable. Follow-up input follows the entire answer. Source and coverage details remain available in a collapsed section.

Price performance is not yet included. The existing Python market-price helper can fall back between adjusted and unadjusted closes and expose a raw current quote alongside adjusted history. Before using it for comparative performance, establish consistent adjustment semantics, observation dates, benchmark alignment, non-trading-day treatment, and missing-data behavior.

## Purchase amount ranking

Screens optionally store rank=politicians (legacy default) or rank=purchase_amount. The latter requires activity=buy and orders companies by summed lower bounds of complete disclosed purchase ranges. Incomplete, unbounded, and missing ranges are excluded, never reconstructed; every company carries known/missing counts and partial labels. Overlapping ranges mean actual dollar ordering is uncertain. "Most money received" is interpreted as reported purchases, not issuer capital inflows. Sale-dollar and net-flow rankings remain unsupported.
