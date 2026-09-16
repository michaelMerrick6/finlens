-- PostgREST hoists this function setting to the request transaction. Large
-- all-or-nothing filings exceed the inherited 8s API timeout (703 rows took 8s).
-- Keep the exemption on this service-role-only RPC, below the 60s API ceiling.
BEGIN;
ALTER FUNCTION public.publish_congress_filing(text,jsonb,jsonb,jsonb,uuid,text,text,jsonb,boolean)
    SET statement_timeout = '50s';
NOTIFY pgrst, 'reload schema';
COMMIT;
