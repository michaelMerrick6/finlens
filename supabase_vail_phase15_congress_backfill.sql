-- Keep historical first-pass work separate from recurring verification and failed OCR retries.
BEGIN;
-- Verified against the official Senate CVC roster on 2026-09-16.
-- https://www.senate.gov/legislative/LIS_MEMBER/cvc_member_data.xml
INSERT INTO public.congress_members (id, first_name, last_name, state, party, chamber, active, source_url)
VALUES ('A000383', 'Alan', 'Armstrong', 'OK', 'Republican', 'Senate', true,
        'https://bioguide.congress.gov/search/bio/A000383')
ON CONFLICT (id) DO NOTHING;

CREATE INDEX IF NOT EXISTS congress_filings_backfill_idx
ON public.congress_filings (chamber, published_date DESC, filing_id)
WHERE status IN ('pending', 'processing');

CREATE OR REPLACE FUNCTION public.claim_congress_backfill(target_chamber text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE candidate congress_filings; token uuid := pg_catalog.gen_random_uuid();
BEGIN
    SELECT * INTO candidate FROM congress_filings
    WHERE chamber = target_chamber AND status IN ('pending', 'processing')
      AND next_attempt_at <= now() AND (lease_until IS NULL OR lease_until < now())
    ORDER BY published_date DESC, filing_id
    FOR UPDATE SKIP LOCKED LIMIT 1;
    IF NOT FOUND THEN RETURN NULL; END IF;
    UPDATE congress_filings SET status='processing', claim_token=token,
        lease_until=now() + interval '10 minutes', attempts=attempts+1, updated_at=now()
    WHERE filing_id=candidate.filing_id RETURNING * INTO candidate;
    RETURN to_jsonb(candidate);
END $$;
REVOKE ALL ON FUNCTION public.claim_congress_backfill(text) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.claim_congress_backfill(text) TO service_role;
COMMIT;
