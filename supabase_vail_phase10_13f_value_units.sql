-- Vail Phase 10: store SEC 13F values in actual dollars.
--
-- The XML information-table value is already dollar-denominated. Historical
-- ingestion multiplied it by 1,000, so correct the stored rows and the JSON
-- copies used by cluster detail. A migration marker makes this safe to rerun.

CREATE TABLE IF NOT EXISTS public.pipeline_data_migrations (
    migration_key TEXT PRIMARY KEY,
    applied_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

ALTER TABLE public.pipeline_data_migrations ENABLE ROW LEVEL SECURITY;

DO $$
DECLARE
    nonzero_value_count BIGINT;
    thousand_aligned_count BIGINT;
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.pipeline_data_migrations
        WHERE migration_key = '2026-07-14-13f-dollar-values'
    ) THEN
        RAISE NOTICE '13F dollar-value correction already applied; skipping.';
        RETURN;
    END IF;

    SELECT
        COUNT(*) FILTER (WHERE value_held IS NOT NULL AND value_held <> 0),
        COUNT(*) FILTER (
            WHERE value_held IS NOT NULL
              AND value_held <> 0
              AND value_held % 1000 = 0
        )
    INTO nonzero_value_count, thousand_aligned_count
    FROM public.institutional_holdings;

    IF nonzero_value_count <> thousand_aligned_count THEN
        RAISE EXCEPTION
            'Refusing 13F correction: % of % nonzero values are thousand-aligned',
            thousand_aligned_count,
            nonzero_value_count;
    END IF;

    UPDATE public.institutional_holdings
    SET value_held = value_held / 1000
    WHERE value_held IS NOT NULL
      AND value_held <> 0;

    UPDATE public.signal_events
    SET
        payload = jsonb_set(
            payload,
            '{value_held}',
            to_jsonb(((payload ->> 'value_held')::numeric / 1000)::bigint),
            false
        ),
        updated_at = NOW()
    WHERE source = 'hedge_fund'
      AND signal_type = 'fund_position_change'
      AND payload ? 'value_held'
      AND NULLIF(payload ->> 'value_held', '') IS NOT NULL;

    UPDATE public.raw_filings
    SET payload = jsonb_set(
        payload,
        '{value_held}',
        to_jsonb(((payload ->> 'value_held')::numeric / 1000)::bigint),
        false
    )
    WHERE source = 'hedge_fund'
      AND filing_type = '13f_holding'
      AND payload ? 'value_held'
      AND NULLIF(payload ->> 'value_held', '') IS NOT NULL;

    INSERT INTO public.pipeline_data_migrations (migration_key)
    VALUES ('2026-07-14-13f-dollar-values');
END;
$$;
