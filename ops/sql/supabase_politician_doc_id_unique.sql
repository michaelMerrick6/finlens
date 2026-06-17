-- Apply this only after duplicate doc_id values have been repaired/removed.
-- It will fail fast if any duplicates remain.

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.politician_trades
        WHERE doc_id IS NOT NULL
        GROUP BY doc_id
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'Cannot add unique politician_trades.doc_id index while duplicate doc_id values remain';
    END IF;
END;
$$;

DROP INDEX IF EXISTS public.idx_politician_trades_doc_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_politician_trades_doc_id_unique
ON public.politician_trades (doc_id)
WHERE doc_id IS NOT NULL;
