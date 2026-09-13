CREATE TABLE IF NOT EXISTS public.site_engagement (
 session_id uuid PRIMARY KEY,
 visitor_id uuid NOT NULL,
 user_id uuid REFERENCES auth.users(id) ON DELETE SET NULL,
 active_seconds integer NOT NULL DEFAULT 0 CHECK(active_seconds >= 0),
 started_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE public.site_engagement ENABLE ROW LEVEL SECURITY;
CREATE INDEX IF NOT EXISTS site_engagement_updated ON public.site_engagement(updated_at);
CREATE OR REPLACE FUNCTION public.record_site_engagement(p_session uuid,p_visitor uuid,p_user uuid,p_seconds integer)
RETURNS void LANGUAGE sql SECURITY DEFINER SET search_path='' AS $$
INSERT INTO public.site_engagement(session_id,visitor_id,user_id,active_seconds)
VALUES(p_session,p_visitor,p_user,least(greatest(p_seconds,0),30))
ON CONFLICT(session_id) DO UPDATE SET
 active_seconds=public.site_engagement.active_seconds + least(greatest(p_seconds,0),30,greatest(0,extract(epoch from (now()-public.site_engagement.updated_at))::integer)),
 user_id=coalesce(excluded.user_id,public.site_engagement.user_id),updated_at=now();
$$;
CREATE OR REPLACE FUNCTION public.get_hq_audience()
RETURNS jsonb LANGUAGE sql STABLE SECURITY DEFINER SET search_path='' AS $$
SELECT jsonb_build_object(
 'accounts', (SELECT jsonb_build_object('total',count(*),'last30',count(*) FILTER(WHERE created_at>=now()-interval '30 days'),'lastMonth',count(*) FILTER(WHERE created_at>=date_trunc('month',now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'-interval '1 month' AND created_at<date_trunc('month',now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC')) FROM auth.users),
 'engagement', (SELECT coalesce(jsonb_agg(x),'[]'::jsonb) FROM (
 SELECT days, count(DISTINCT visitor_id) AS browsers,count(DISTINCT user_id) AS accounts,
 coalesce(sum(active_seconds),0) AS seconds,
 coalesce(round(avg(active_seconds)),0) AS average_seconds
 FROM (VALUES(7),(30)) AS periods(days)
 LEFT JOIN public.site_engagement ON started_at>=now()-make_interval(days=>days)
 GROUP BY days ORDER BY days) x),
 'trackingSince',(SELECT min(started_at) FROM public.site_engagement));
$$;
REVOKE ALL ON FUNCTION public.record_site_engagement(uuid,uuid,uuid,integer) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION public.get_hq_audience() FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.record_site_engagement(uuid,uuid,uuid,integer) TO service_role;
GRANT EXECUTE ON FUNCTION public.get_hq_audience() TO service_role;
