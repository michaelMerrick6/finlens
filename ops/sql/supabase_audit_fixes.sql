-- =============================================================
-- Audit Fixes: Add unique constraints to prevent duplicate trades
-- RUN EACH SECTION SEPARATELY in the Supabase SQL editor.
-- =============================================================

-- STEP 1: Add unique index on politician_trades.doc_id
-- (Run this block alone first)
CREATE UNIQUE INDEX IF NOT EXISTS idx_politician_trades_doc_id_unique
ON public.politician_trades(doc_id)
WHERE doc_id IS NOT NULL;


-- STEP 2: Add composite unique index on insider_trades
-- (Run this block alone second)
CREATE UNIQUE INDEX IF NOT EXISTS idx_insider_trades_source_dedup
ON public.insider_trades(source_url, ticker, transaction_date, filer_name)
WHERE source_url IS NOT NULL;
