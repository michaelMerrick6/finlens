BEGIN;

ALTER TABLE public.politician_trades
ADD COLUMN IF NOT EXISTS asset_name VARCHAR(255);

WITH doc_id_matches AS (
    SELECT
        pt.id AS trade_id,
        LEFT(NULLIF(BTRIM(rf.payload->>'asset_name'), ''), 255) AS asset_name
    FROM public.politician_trades pt
    JOIN public.raw_filings rf
      ON rf.source = 'congress'
     AND rf.source_document_id = pt.doc_id
    WHERE COALESCE(NULLIF(BTRIM(pt.asset_name), ''), '') = ''
      AND NULLIF(BTRIM(rf.payload->>'asset_name'), '') IS NOT NULL
)
UPDATE public.politician_trades pt
SET asset_name = doc_id_matches.asset_name
FROM doc_id_matches
WHERE pt.id = doc_id_matches.trade_id;

WITH signature_matches AS (
    SELECT DISTINCT ON (pt.id)
        pt.id AS trade_id,
        LEFT(NULLIF(BTRIM(rf.payload->>'asset_name'), ''), 255) AS asset_name
    FROM public.politician_trades pt
    JOIN public.raw_filings rf
      ON rf.source = 'congress'
     AND COALESCE(rf.payload->>'source_url', '') = COALESCE(pt.source_url, '')
     AND COALESCE(rf.payload->>'politician_name', '') = COALESCE(pt.politician_name, '')
     AND COALESCE(rf.payload->>'transaction_date', '') = COALESCE(pt.transaction_date::text, '')
     AND LOWER(COALESCE(rf.payload->>'transaction_type', '')) = LOWER(COALESCE(pt.transaction_type, ''))
     AND COALESCE(rf.payload->>'amount_range', '') = COALESCE(pt.amount_range, '')
    WHERE COALESCE(NULLIF(BTRIM(pt.asset_name), ''), '') = ''
      AND NULLIF(BTRIM(rf.payload->>'asset_name'), '') IS NOT NULL
    ORDER BY pt.id, rf.filed_at DESC, rf.created_at DESC
)
UPDATE public.politician_trades pt
SET asset_name = signature_matches.asset_name
FROM signature_matches
WHERE pt.id = signature_matches.trade_id;

UPDATE public.politician_trades
SET asset_name = 'U.S. Treasury'
WHERE COALESCE(NULLIF(BTRIM(asset_name), ''), '') = ''
  AND ticker = 'US-TREAS';

UPDATE public.politician_trades pt
SET asset_name = LEFT(c.name, 255)
FROM public.companies c
WHERE pt.ticker = c.ticker
  AND COALESCE(NULLIF(BTRIM(pt.asset_name), ''), '') = ''
  AND COALESCE(NULLIF(BTRIM(c.name), ''), '') <> '';

COMMIT;
