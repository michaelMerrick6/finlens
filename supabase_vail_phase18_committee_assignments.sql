BEGIN;
-- Snapshot dates describe evidence, never legal appointment/effective dates.
CREATE TABLE IF NOT EXISTS public.committee_snapshots (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 source_key text NOT NULL,
 evidence_kind text NOT NULL CHECK(evidence_kind IN ('official_current','archive_snapshot')),
 source_version text NOT NULL,
 observed_at timestamptz NOT NULL,
 verified_at timestamptz NOT NULL DEFAULT now(),
 congress integer NOT NULL CHECK(congress >= 118),
 committees jsonb NOT NULL,
 members jsonb NOT NULL,
 documents jsonb NOT NULL,
 UNIQUE(source_key, source_version)
);
CREATE TABLE IF NOT EXISTS public.committee_assignments (
 snapshot_id uuid NOT NULL REFERENCES public.committee_snapshots(id) ON DELETE CASCADE,
 committee_id text NOT NULL,
 member_id text NOT NULL CHECK(member_id ~ '^[A-Z][0-9]{6}$'),
 chamber text NOT NULL CHECK(chamber IN ('House','Senate')),
 role text NOT NULL,
 PRIMARY KEY(snapshot_id, committee_id, member_id)
);
CREATE INDEX IF NOT EXISTS committee_assignment_member ON public.committee_assignments(member_id, snapshot_id);
CREATE INDEX IF NOT EXISTS committee_snapshot_latest ON public.committee_snapshots(source_key, observed_at DESC);
CREATE TABLE IF NOT EXISTS public.committee_sync_runs (
 id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
 source_key text NOT NULL,
 started_at timestamptz NOT NULL DEFAULT now(),
 finished_at timestamptz,
 status text NOT NULL CHECK(status IN ('running','success','failed')),
 detail text
);
ALTER TABLE public.committee_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.committee_assignments ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.committee_sync_runs ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.committee_snapshots, public.committee_assignments, public.committee_sync_runs FROM anon, authenticated;
GRANT ALL ON public.committee_snapshots, public.committee_assignments, public.committee_sync_runs TO service_role;

CREATE OR REPLACE FUNCTION public.publish_committee_snapshot(p jsonb) RETURNS uuid
LANGUAGE plpgsql SECURITY DEFINER SET search_path=public AS $$
DECLARE prior committee_snapshots; sid uuid; n integer; removed integer; previous_count integer;
BEGIN
 PERFORM pg_advisory_xact_lock(hashtext('committee:' || (p->>'source_key')));
 n := jsonb_array_length(p->'assignments');
 IF (n < 1 AND p->>'evidence_kind'='official_current') OR jsonb_array_length(p->'committees') < 1 OR jsonb_array_length(p->'documents') < 1 THEN
   RAISE EXCEPTION 'Empty committee snapshot rejected';
 END IF;
 IF EXISTS(SELECT FROM jsonb_array_elements(p->'assignments') a WHERE
   NOT EXISTS(SELECT FROM jsonb_array_elements(p->'committees') c WHERE c->>'id'=a->>'committee_id') OR
   NOT EXISTS(SELECT FROM jsonb_array_elements(p->'members') m WHERE m->>'id'=a->>'member_id')) THEN
   RAISE EXCEPTION 'Unresolved assignment rejected';
 END IF;
 IF p->>'evidence_kind'='official_current' THEN
   SELECT * INTO prior FROM committee_snapshots WHERE source_key=p->>'source_key' ORDER BY verified_at DESC LIMIT 1;
   IF prior.id IS NOT NULL THEN
     IF (p->>'observed_at')::timestamptz < prior.observed_at THEN RAISE EXCEPTION 'Out of order current snapshot'; END IF;
     IF prior.congress <> (p->>'congress')::integer THEN RAISE EXCEPTION 'Congress transition requires reviewed baseline'; END IF;
     SELECT count(*) INTO previous_count FROM committee_assignments WHERE snapshot_id=prior.id;
     SELECT count(*) INTO removed FROM committee_assignments a WHERE a.snapshot_id=prior.id AND NOT EXISTS(
       SELECT FROM jsonb_array_elements(p->'assignments') b WHERE b->>'committee_id'=a.committee_id AND b->>'member_id'=a.member_id);
     IF removed > greatest(5,previous_count * 0.10) OR n > previous_count * 1.25 THEN
       RAISE EXCEPTION 'Large roster change held for review';
     END IF;
     IF EXISTS(SELECT FROM committee_assignments a WHERE a.snapshot_id=prior.id
       AND NOT EXISTS(SELECT FROM jsonb_array_elements(p->'assignments') b WHERE b->>'committee_id'=a.committee_id)) THEN
       RAISE EXCEPTION 'Emptied committee held for review';
     END IF;
     -- A complete committee disappearing is suspicious even if total loss is small.
     IF EXISTS(SELECT FROM jsonb_array_elements(prior.committees) c WHERE NOT EXISTS(
       SELECT FROM jsonb_array_elements(p->'committees') d WHERE d->>'id'=c->>'id')) THEN
       RAISE EXCEPTION 'Missing committee held for review';
     END IF;
   END IF;
 END IF;
 INSERT INTO committee_snapshots(source_key,evidence_kind,source_version,observed_at,congress,committees,members,documents)
 VALUES(p->>'source_key',p->>'evidence_kind',p->>'source_version',(p->>'observed_at')::timestamptz,
 (p->>'congress')::integer,p->'committees',p->'members',p->'documents')
 ON CONFLICT(source_key,source_version) DO UPDATE SET verified_at=now()
 RETURNING id INTO sid;
 -- Seed newly seated members from official stable IDs; never overwrite existing profiles.
 IF p->>'evidence_kind'='official_current' THEN
   INSERT INTO congress_members(id,first_name,last_name,state,party,chamber,active,source_url)
   SELECT m->>'id',m->>'first_name',m->>'last_name',m->>'state',m->>'party',m->>'chamber',true,m->>'source_url'
   FROM jsonb_array_elements(p->'members') m
   WHERE coalesce(m->>'first_name','')<>'' AND coalesce(m->>'last_name','')<>''
     AND m->>'id' ~ '^[A-Z][0-9]{6}$'
   ON CONFLICT(id) DO NOTHING;
 END IF;
 INSERT INTO committee_assignments(snapshot_id,committee_id,member_id,chamber,role)
 SELECT sid,a->>'committee_id',a->>'member_id',a->>'chamber',a->>'role' FROM jsonb_array_elements(p->'assignments') a
 ON CONFLICT DO NOTHING;
 RETURN sid;
END $$;
REVOKE ALL ON FUNCTION public.publish_committee_snapshot(jsonb) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.publish_committee_snapshot(jsonb) TO service_role;
CREATE OR REPLACE FUNCTION public.member_committee_archive(p_member text)
RETURNS TABLE(observed_at timestamptz, names jsonb, source_url text)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=public AS $$
 SELECT s.observed_at, jsonb_agg(c->>'name' ORDER BY c->>'name'),
 'https://github.com/unitedstates/congress-legislators/blob/' || s.source_version || '/committee-membership-current.yaml'
 FROM committee_snapshots s JOIN committee_assignments a ON a.snapshot_id=s.id
 CROSS JOIN LATERAL jsonb_array_elements(s.committees) c
 WHERE s.source_key='archive_unitedstates' AND a.member_id=p_member
 AND c->>'id'=a.committee_id AND c->>'parent_id' IS NULL
 GROUP BY s.id ORDER BY s.observed_at DESC, s.source_version
$$;
REVOKE ALL ON FUNCTION public.member_committee_archive(text) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.member_committee_archive(text) TO service_role;
COMMIT;
