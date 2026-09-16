BEGIN;
CREATE TABLE IF NOT EXISTS public.company_research_classifications (
 ticker text PRIMARY KEY, company_name text NOT NULL, industry text, sic text,
 source_url text NOT NULL, verified_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE public.company_research_classifications ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.company_research_classifications FROM anon,authenticated;
GRANT ALL ON public.company_research_classifications TO service_role;
CREATE TABLE IF NOT EXISTS public.research_screens (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(), user_id uuid NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
 name text NOT NULL CHECK(length(name) BETWEEN 1 AND 100), filters jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE public.research_screens ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.research_screens FROM anon,authenticated;
GRANT ALL ON public.research_screens TO service_role;
CREATE INDEX IF NOT EXISTS research_screens_user ON public.research_screens(user_id,created_at DESC);
CREATE TABLE IF NOT EXISTS public.research_usage (
 user_id uuid NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE, day date NOT NULL, requests int NOT NULL DEFAULT 0,
 PRIMARY KEY(user_id,day)
);
ALTER TABLE public.research_usage ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.research_usage FROM anon,authenticated;
GRANT ALL ON public.research_usage TO service_role;
CREATE OR REPLACE FUNCTION public.claim_research_request(p_user uuid) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE n int;
BEGIN
 INSERT INTO research_usage(user_id,day,requests) VALUES(p_user,(now() AT TIME ZONE 'UTC')::date,1)
 ON CONFLICT(user_id,day) DO UPDATE SET requests=research_usage.requests+1 WHERE research_usage.requests<20
 RETURNING requests INTO n;
 RETURN n IS NOT NULL;
END $$;
REVOKE ALL ON FUNCTION public.claim_research_request(uuid) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.claim_research_request(uuid) TO service_role;
CREATE OR REPLACE FUNCTION public.limit_saved_research_screens() RETURNS trigger
LANGUAGE plpgsql SET search_path=public AS $$
BEGIN
 PERFORM pg_advisory_xact_lock(hashtext('research-screens:' || NEW.user_id::text));
 IF (SELECT count(*) FROM research_screens WHERE user_id=NEW.user_id)>=20 THEN
   RAISE EXCEPTION 'Saved screen limit reached';
 END IF;
 RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS research_screen_limit ON public.research_screens;
CREATE TRIGGER research_screen_limit BEFORE INSERT ON public.research_screens FOR EACH ROW EXECUTE FUNCTION public.limit_saved_research_screens();
REVOKE ALL ON FUNCTION public.limit_saved_research_screens() FROM PUBLIC,anon,authenticated;
COMMIT;
