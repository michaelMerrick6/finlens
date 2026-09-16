-- One immutable daily tracking email per account. Apply after phases 5 and 16.
BEGIN;
CREATE TABLE IF NOT EXISTS public.daily_email_digests (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
    digest_date date NOT NULL,
    cutoff_at timestamptz NOT NULL,
    destination text NOT NULL,
    status text NOT NULL CHECK (status IN ('preparing','ready','sending','sent','uncertain','cancelled')),
    events jsonb NOT NULL DEFAULT '[]',
    items jsonb NOT NULL DEFAULT '[]',
    subject text,
    html_body text,
    text_body text,
    provider_message_id text,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    attempted_at timestamptz,
    sent_at timestamptz,
    UNIQUE(user_id,digest_date)
);
CREATE TABLE IF NOT EXISTS public.daily_email_digest_events (
    user_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
    event_id text NOT NULL,
    digest_id uuid NOT NULL REFERENCES public.daily_email_digests(id) ON DELETE CASCADE,
    PRIMARY KEY(user_id,event_id)
);
ALTER TABLE public.daily_email_digest_events ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.daily_email_digest_events FROM anon,authenticated;
GRANT ALL ON public.daily_email_digest_events TO service_role;
ALTER TABLE public.daily_email_digests ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.daily_email_digests FROM anon, authenticated;
GRANT ALL ON public.daily_email_digests TO service_role;
ALTER TABLE public.alert_deliveries ADD COLUMN IF NOT EXISTS digest_id uuid REFERENCES public.daily_email_digests(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_daily_email_history ON public.daily_email_digests(user_id,sent_at DESC) WHERE status='sent';
CREATE INDEX IF NOT EXISTS idx_alert_delivery_digest ON public.alert_deliveries(digest_id);
CREATE INDEX IF NOT EXISTS idx_alert_delivery_pending_email ON public.alert_deliveries(subscription_id,queued_at) WHERE channel='email' AND status='pending' AND digest_id IS NULL;

-- Old deployed per-event workers must not bypass the daily gate during rollout.
CREATE OR REPLACE FUNCTION public.guard_individual_tracking_send()
RETURNS trigger LANGUAGE plpgsql SET search_path=public,pg_temp AS $$
BEGIN
    IF NEW.status='pending' AND (OLD.digest_id IS NOT NULL OR OLD.sent_at IS NOT NULL) THEN RETURN OLD; END IF;
    IF NEW.status='sending' AND OLD.status IS DISTINCT FROM 'sending'
       AND (NEW.channel='email' OR EXISTS (
           SELECT 1 FROM alert_subscriptions s JOIN watchlists w ON w.id=s.watchlist_id
           WHERE s.id=NEW.subscription_id AND w.user_id IS NOT NULL)) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS guard_individual_tracking_send ON public.alert_deliveries;
CREATE TRIGGER guard_individual_tracking_send BEFORE UPDATE ON public.alert_deliveries
FOR EACH ROW EXECUTE FUNCTION public.guard_individual_tracking_send();

UPDATE public.alert_deliveries SET status='sent' WHERE sent_at IS NOT NULL AND status='pending' AND digest_id IS NULL;

CREATE OR REPLACE FUNCTION public.claim_daily_email_digest()
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
DECLARE p profiles; d daily_email_digests; target text;
    local_today date := (now() AT TIME ZONE 'America/New_York')::date;
    cutoff timestamptz := date_trunc('day',now() AT TIME ZONE 'America/New_York') AT TIME ZONE 'America/New_York';
    ids uuid[]; snapshot jsonb;
BEGIN
    -- Collect the complete preceding day, then send on the first job after 8am ET.
    IF (now() AT TIME ZONE 'America/New_York')::time < time '08:00' THEN RETURN NULL; END IF;
    FOR p IN SELECT * FROM profiles WHERE email_enabled ORDER BY id FOR UPDATE SKIP LOCKED LOOP
        -- A timed-out provider request may have succeeded. It blocks automatic replay.
        IF EXISTS (SELECT 1 FROM daily_email_digests WHERE user_id=p.id AND status IN ('preparing','sending','uncertain')) THEN CONTINUE; END IF;
        IF EXISTS (SELECT 1 FROM daily_email_digests WHERE user_id=p.id AND status='sent'
                   AND (digest_date=local_today OR sent_at>now()-interval '24 hours')) THEN CONTINUE; END IF;
        -- Respect emails sent before this migration, and unresolved legacy sends.
        IF EXISTS (SELECT 1 FROM alert_deliveries a JOIN alert_subscriptions s ON s.id=a.subscription_id
            JOIN watchlists w ON w.id=s.watchlist_id WHERE w.user_id=p.id
            AND (a.status IN ('sending','uncertain') OR (a.sent_at IS NOT NULL AND
                (a.sent_at>now()-interval '24 hours' OR (a.sent_at AT TIME ZONE 'America/New_York')::date=local_today)))) THEN CONTINUE; END IF;
        SELECT * INTO d FROM daily_email_digests WHERE user_id=p.id AND status='ready' ORDER BY created_at LIMIT 1 FOR UPDATE;
        IF FOUND THEN
            UPDATE daily_email_digests SET status='preparing' WHERE id=d.id RETURNING * INTO d;
            RETURN to_jsonb(d);
        END IF;
        IF EXISTS (SELECT 1 FROM daily_email_digests WHERE user_id=p.id AND digest_date=local_today) THEN CONTINUE; END IF;
        SELECT s.destination INTO target FROM alert_subscriptions s JOIN watchlists w ON w.id=s.watchlist_id
        WHERE w.user_id=p.id AND s.active AND s.channel='email' AND btrim(s.destination)<>''
        ORDER BY s.created_at,s.id LIMIT 1;
        IF NOT FOUND THEN CONTINUE; END IF;
        -- Pending items from earlier days carry forward; never cap or truncate a user's digest.
        SELECT array_agg(a.id ORDER BY a.queued_at,a.id), jsonb_agg(to_jsonb(e) ORDER BY a.queued_at,a.id)
        INTO ids,snapshot FROM alert_deliveries a JOIN alert_subscriptions s ON s.id=a.subscription_id
        JOIN watchlists w ON w.id=s.watchlist_id JOIN signal_events e ON e.id=a.signal_event_id
        WHERE w.user_id=p.id AND s.active AND s.channel='email' AND a.channel='email'
          AND lower(btrim(s.destination))=lower(btrim(target)) AND lower(btrim(a.destination))=lower(btrim(target))
          AND a.status='pending' AND a.digest_id IS NULL AND a.sent_at IS NULL AND a.queued_at<cutoff;
        IF ids IS NULL THEN CONTINUE; END IF;
        INSERT INTO daily_email_digests(user_id,digest_date,cutoff_at,destination,status,events)
        VALUES(p.id,local_today,cutoff,target,'preparing',snapshot) RETURNING * INTO d;
        UPDATE alert_deliveries SET digest_id=d.id,status='digest_pending' WHERE id=ANY(ids);
        RETURN to_jsonb(d);
    END LOOP;
    RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION public.daily_email_unseen_events(p_id uuid,p_events jsonb)
RETURNS jsonb LANGUAGE sql STABLE SECURITY DEFINER SET search_path=public,pg_temp AS $$
    SELECT coalesce(jsonb_agg(e.value ORDER BY e.ordinality),'[]'::jsonb)
    FROM daily_email_digests d CROSS JOIN LATERAL jsonb_array_elements(p_events) WITH ORDINALITY e
    WHERE d.id=p_id AND d.status='preparing'
      AND NOT EXISTS (SELECT 1 FROM daily_email_digest_events r WHERE r.user_id=d.user_id AND r.event_id=e.value->>'id')
      AND NOT EXISTS (SELECT 1 FROM alert_deliveries a JOIN alert_subscriptions s ON s.id=a.subscription_id
          JOIN watchlists w ON w.id=s.watchlist_id WHERE w.user_id=d.user_id
          AND a.channel='email' AND a.sent_at IS NOT NULL AND a.signal_event_id::text=e.value->>'id')
$$;
REVOKE ALL ON FUNCTION public.daily_email_unseen_events(uuid,jsonb) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.daily_email_unseen_events(uuid,jsonb) TO service_role;

CREATE OR REPLACE FUNCTION public.prepare_daily_email_digest(p_id uuid,p_subject text,p_html text,p_text text,p_items jsonb)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
DECLARE d daily_email_digests;
BEGIN
    SELECT * INTO d FROM daily_email_digests WHERE id=p_id AND status='preparing' FOR UPDATE;
    IF NOT FOUND THEN RETURN false; END IF;
    -- Check opt-in and the exact destination again immediately before sending.
    IF NOT EXISTS (SELECT 1 FROM profiles WHERE id=d.user_id AND email_enabled)
       OR NOT EXISTS (SELECT 1 FROM alert_subscriptions s JOIN watchlists w ON w.id=s.watchlist_id
           WHERE w.user_id=d.user_id AND s.active AND s.channel='email'
             AND lower(btrim(s.destination))=lower(btrim(d.destination))) THEN
        UPDATE daily_email_digests SET status='cancelled',last_error='Email preference or destination changed.' WHERE id=p_id;
        UPDATE alert_deliveries SET status='cancelled' WHERE digest_id=p_id;
        RETURN false;
    END IF;
    IF p_items='[]'::jsonb THEN
        UPDATE daily_email_digests SET status='cancelled',last_error='All matching events were already emailed.' WHERE id=p_id;
        UPDATE alert_deliveries SET status='superseded' WHERE digest_id=p_id;
        RETURN false;
    END IF;
    IF jsonb_typeof(p_items) IS DISTINCT FROM 'array'
       OR nullif(p_subject,'') IS NULL OR nullif(p_html,'') IS NULL OR nullif(p_text,'') IS NULL THEN
        RAISE EXCEPTION 'A nonempty digest is required';
    END IF;
    UPDATE daily_email_digests SET subject=p_subject,html_body=p_html,text_body=p_text,items=p_items,
        status='sending',attempted_at=now(),last_error=NULL WHERE id=p_id;
    RETURN true;
END $$;

CREATE OR REPLACE FUNCTION public.finish_daily_email_digest(p_id uuid,p_status text,p_provider_id text DEFAULT NULL)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
BEGIN
    IF p_status NOT IN ('sent','ready','uncertain') THEN RAISE EXCEPTION 'Invalid digest result'; END IF;
    UPDATE daily_email_digests SET status=p_status,provider_message_id=p_provider_id,
        sent_at=CASE WHEN p_status='sent' THEN now() ELSE NULL END,
        last_error=CASE WHEN p_status='uncertain' THEN 'Provider outcome needs verification; automatic sending paused.'
                       WHEN p_status='ready' THEN 'Provider rate limited; nothing accepted.' ELSE NULL END
    WHERE id=p_id AND status='sending';
    IF NOT FOUND THEN RAISE EXCEPTION 'Digest is not sending'; END IF;
    IF p_status='sent' THEN
        INSERT INTO daily_email_digest_events(user_id,event_id,digest_id)
        SELECT d.user_id,item->>'eventId',d.id FROM daily_email_digests d,
            jsonb_array_elements(d.items) item WHERE d.id=p_id
        ON CONFLICT(user_id,event_id) DO NOTHING;
        -- Legacy filing summaries may cover raw rows queued later that same day.
        UPDATE alert_deliveries a SET status='digested',sent_at=now(),digest_id=p_id,last_error=NULL
        FROM alert_subscriptions s,watchlists w,daily_email_digest_events r
        WHERE a.subscription_id=s.id AND s.watchlist_id=w.id AND w.user_id=r.user_id
          AND r.digest_id=p_id AND r.event_id=a.signal_event_id::text AND a.channel='email'
          AND a.status='pending' AND a.digest_id IS NULL;
        UPDATE alert_deliveries SET status='digested',sent_at=now(),last_error=NULL WHERE digest_id=p_id;
    END IF;
END $$;
REVOKE ALL ON FUNCTION public.claim_daily_email_digest() FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION public.prepare_daily_email_digest(uuid,text,text,text,jsonb) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION public.finish_daily_email_digest(uuid,text,text) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.claim_daily_email_digest() TO service_role;
GRANT EXECUTE ON FUNCTION public.prepare_daily_email_digest(uuid,text,text,text,jsonb) TO service_role;
GRANT EXECUTE ON FUNCTION public.finish_daily_email_digest(uuid,text,text) TO service_role;

CREATE OR REPLACE FUNCTION public.sent_email_history(p_user_id uuid,p_limit integer DEFAULT 20,p_offset integer DEFAULT 0)
RETURNS TABLE(id uuid,kind text,subject text,sent_at timestamptz,item_count integer,destination text)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path=public,pg_temp AS $$
    SELECT * FROM (
        SELECT d.id,'daily'::text,d.subject,d.sent_at,jsonb_array_length(d.items),d.destination
        FROM daily_email_digests d WHERE d.user_id=p_user_id AND d.status='sent' AND d.sent_at IS NOT NULL
        UNION ALL
        SELECT a.id,'individual'::text,coalesce(e.title,'Tracking email'),a.sent_at,1,a.destination
        FROM alert_deliveries a JOIN alert_subscriptions s ON s.id=a.subscription_id
        JOIN watchlists w ON w.id=s.watchlist_id LEFT JOIN signal_events e ON e.id=a.signal_event_id
        WHERE w.user_id=p_user_id AND a.channel='email' AND a.status='sent' AND a.sent_at IS NOT NULL AND a.digest_id IS NULL
    ) history ORDER BY sent_at DESC,id DESC LIMIT greatest(1,least(p_limit,50)) OFFSET greatest(p_offset,0)
$$;
CREATE OR REPLACE FUNCTION public.sent_email_detail(p_user_id uuid,p_id uuid)
RETURNS jsonb LANGUAGE sql STABLE SECURITY DEFINER SET search_path=public,pg_temp AS $$
    SELECT detail FROM (
        SELECT jsonb_build_object('id',d.id,'kind','daily','subject',d.subject,'sent_at',d.sent_at,
            'destination',d.destination,'items',d.items,'text_body',d.text_body) detail
        FROM daily_email_digests d WHERE d.id=p_id AND d.user_id=p_user_id AND d.status='sent' AND d.sent_at IS NOT NULL
        UNION ALL
        SELECT jsonb_build_object('id',a.id,'kind','individual','subject',coalesce(e.title,'Tracking email'),
            'sent_at',a.sent_at,'destination',a.destination,'items',jsonb_build_array(jsonb_build_object(
                'eventId',e.id,'title',e.title,'actor',e.actor_name,'ticker',e.ticker,'summary',e.summary,
                'sourceUrl',e.source_url,'filedAt',e.published_at)),'text_body',NULL)
        FROM alert_deliveries a JOIN alert_subscriptions s ON s.id=a.subscription_id
        JOIN watchlists w ON w.id=s.watchlist_id LEFT JOIN signal_events e ON e.id=a.signal_event_id
        WHERE a.id=p_id AND w.user_id=p_user_id AND a.channel='email' AND a.status='sent' AND a.sent_at IS NOT NULL AND a.digest_id IS NULL
    ) records LIMIT 1
$$;
REVOKE ALL ON FUNCTION public.sent_email_history(uuid,integer,integer) FROM PUBLIC,anon,authenticated;
REVOKE ALL ON FUNCTION public.sent_email_detail(uuid,uuid) FROM PUBLIC,anon,authenticated;
GRANT EXECUTE ON FUNCTION public.sent_email_history(uuid,integer,integer) TO service_role;
GRANT EXECUTE ON FUNCTION public.sent_email_detail(uuid,uuid) TO service_role;

NOTIFY pgrst, 'reload schema';
COMMIT;
