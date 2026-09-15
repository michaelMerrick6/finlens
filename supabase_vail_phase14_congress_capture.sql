-- Filing completion is independent of transaction presence. Apply before new workers.
BEGIN;
CREATE TABLE IF NOT EXISTS public.congress_filings (
    filing_id text PRIMARY KEY,
    chamber text NOT NULL CHECK (chamber IN ('House', 'Senate')),
    published_date date NOT NULL,
    filing jsonb NOT NULL DEFAULT '{}',
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','processing','failed','complete')),
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    claim_token uuid,
    lease_until timestamptz,
    last_error text,
    parser_version text,
    source_hash text,
    stored_rows integer,
    completed_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS congress_filings_queue ON public.congress_filings(chamber, next_attempt_at);
ALTER TABLE public.congress_filings ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.congress_filings FROM anon, authenticated;
GRANT ALL ON public.congress_filings TO service_role;

-- Preserve correction history before removing obsolete signals and their dependents.
CREATE TABLE IF NOT EXISTS public.congress_corrections (
    id uuid PRIMARY KEY DEFAULT pg_catalog.gen_random_uuid(),
    filing_id text NOT NULL,
    corrected_at timestamptz NOT NULL DEFAULT now(),
    snapshot jsonb NOT NULL
);
ALTER TABLE public.congress_corrections ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.congress_corrections FROM anon, authenticated;
GRANT ALL ON public.congress_corrections TO service_role;

-- Correction cleanup must not make already-delivered notifications eligible again.
CREATE TABLE IF NOT EXISTS public.congress_delivery_tombstones (
    kind text NOT NULL, delivery_key text NOT NULL, PRIMARY KEY(kind,delivery_key)
);
ALTER TABLE public.congress_delivery_tombstones ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.congress_delivery_tombstones FROM anon, authenticated;
GRANT ALL ON public.congress_delivery_tombstones TO service_role;

CREATE UNIQUE INDEX IF NOT EXISTS idx_politician_trades_doc_id_unique
    ON public.politician_trades(doc_id) WHERE doc_id IS NOT NULL;

-- Range predicates use these indexes even in cached RPC plans.
CREATE INDEX IF NOT EXISTS idx_congress_trade_filing_prefix ON public.politician_trades(lower(doc_id));
CREATE INDEX IF NOT EXISTS idx_congress_raw_filing_prefix ON public.raw_filings(lower(source_document_id)) WHERE source='congress';
CREATE INDEX IF NOT EXISTS idx_congress_signal_filing_prefix ON public.signal_events(lower(source_document_id)) WHERE source='congress' AND signal_type='politician_trade';

-- Index dependency IDs only, rather than scanning every signal payload per trade.
CREATE OR REPLACE FUNCTION public.signal_dependency_ids(payload jsonb)
RETURNS text[] LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT coalesce(array_agg(DISTINCT value), ARRAY[]::text[]) FROM (
        SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(payload->key)='array'
            THEN payload->key ELSE '[]'::jsonb END) AS value
        FROM unnest(ARRAY['group_event_ids','summary_event_ids','summary_unusual_event_ids',
                         'cluster_event_ids','cluster_raw_event_ids']) key
        UNION ALL
        SELECT payload->>key FROM unnest(ARRAY['source_trade_event_id','cluster_source_event_id']) key
        WHERE payload->>key IS NOT NULL
    ) dependencies;
$$;
CREATE INDEX IF NOT EXISTS idx_signal_event_dependency_ids
    ON public.signal_events USING gin(public.signal_dependency_ids(payload));

CREATE OR REPLACE FUNCTION public.register_congress_filings(entries jsonb)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
BEGIN
    INSERT INTO congress_filings(filing_id, chamber, published_date, filing)
    SELECT lower(x.filing_id), x.chamber, x.published_date, x.filing
    FROM jsonb_to_recordset(entries) AS x(filing_id text, chamber text, published_date date, filing jsonb)
    ON CONFLICT (filing_id) DO UPDATE SET
        published_date = EXCLUDED.published_date, filing = EXCLUDED.filing,
        status = CASE WHEN congress_filings.filing IS DISTINCT FROM EXCLUDED.filing THEN 'pending' ELSE congress_filings.status END,
        claim_token = CASE WHEN congress_filings.filing IS DISTINCT FROM EXCLUDED.filing THEN NULL ELSE congress_filings.claim_token END,
        lease_until = CASE WHEN congress_filings.filing IS DISTINCT FROM EXCLUDED.filing THEN NULL ELSE congress_filings.lease_until END,
        next_attempt_at = CASE WHEN congress_filings.filing IS DISTINCT FROM EXCLUDED.filing
            THEN now() ELSE congress_filings.next_attempt_at END;
END $$;

CREATE OR REPLACE FUNCTION public.claim_congress_filing(target_chamber text, recent_first boolean)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE candidate congress_filings; token uuid := pg_catalog.gen_random_uuid();
BEGIN
    SELECT * INTO candidate FROM congress_filings
    WHERE chamber = target_chamber AND next_attempt_at <= now()
      AND (lease_until IS NULL OR lease_until < now())
    ORDER BY CASE WHEN recent_first AND published_date >= current_date - 30 THEN 0 ELSE 1 END,
      CASE WHEN recent_first THEN published_date END DESC,
      next_attempt_at, filing_id
    FOR UPDATE SKIP LOCKED LIMIT 1;
    IF NOT FOUND THEN RETURN NULL; END IF;
    UPDATE congress_filings SET status='processing', claim_token=token,
        lease_until=now() + interval '10 minutes', attempts=attempts+1, updated_at=now()
    WHERE filing_id=candidate.filing_id RETURNING * INTO candidate;
    RETURN to_jsonb(candidate);
END $$;

CREATE OR REPLACE FUNCTION public.fail_congress_filing(target_filing_id text, token uuid, error_message text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
BEGIN
    UPDATE congress_filings SET status='failed', last_error=left(error_message,4000),
        lease_until=NULL, claim_token=NULL, updated_at=now(),
        next_attempt_at=now() + make_interval(mins => LEAST(1440, 15 * power(2, LEAST(attempts,7))::integer))
    WHERE filing_id=target_filing_id AND claim_token=token;
    IF NOT FOUND THEN RAISE EXCEPTION 'Filing claim expired or was replaced'; END IF;
END $$;

CREATE OR REPLACE FUNCTION public.publish_congress_filing(
    target_filing_id text, trades jsonb, raw_rows jsonb, events jsonb,
    claim_token uuid DEFAULT NULL, parser_version text DEFAULT NULL,
    source_hash text DEFAULT NULL, filing_metadata jsonb DEFAULT '{}', verified_no_trades boolean DEFAULT false
) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_temp AS $$
DECLARE prior_count integer; changed_ids uuid[]; removed_ids uuid[];
    snapshot jsonb; prior_state congress_filings; trade_keys text[]; changed boolean;
BEGIN
    IF target_filing_id !~ '^(house-[0-9]{4}-[0-9]+|senate-[a-f0-9-]+)$' THEN
        RAISE EXCEPTION 'Invalid filing key';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(target_filing_id,0));
    SELECT * INTO prior_state FROM congress_filings WHERE filing_id=target_filing_id FOR UPDATE;
    IF claim_token IS NOT NULL AND (prior_state.claim_token IS DISTINCT FROM claim_token
        OR prior_state.lease_until < now()) THEN
        RAISE EXCEPTION 'Filing claim expired or was replaced';
    END IF;
    IF claim_token IS NULL AND prior_state.lease_until > now() THEN
        RAISE EXCEPTION 'Filing is claimed by another worker';
    END IF;
    IF prior_state.filing_id IS NULL THEN RAISE EXCEPTION 'Register filing before publication'; END IF;
    IF jsonb_typeof(trades) IS DISTINCT FROM 'array' OR jsonb_typeof(events) IS DISTINCT FROM 'array'
       OR jsonb_typeof(raw_rows) IS DISTINCT FROM 'array' THEN
        RAISE EXCEPTION 'Publication requires transaction, signal and raw-record arrays';
    END IF;
    IF jsonb_array_length(trades)=0 AND verified_no_trades IS NOT TRUE THEN
        RAISE EXCEPTION 'Empty extraction is not verified no-transactions';
    END IF;
    IF jsonb_array_length(trades) <> jsonb_array_length(events)
       OR jsonb_array_length(trades) <> jsonb_array_length(raw_rows) THEN
        RAISE EXCEPTION 'Incomplete publication payload';
    END IF;
    IF EXISTS (SELECT 1 FROM jsonb_array_elements(trades) t WHERE
        t->>'doc_id' IS NULL OR t->>'doc_id' !~ ('^' || target_filing_id || '-[0-9]+$')
        OR (t->>'transaction_date')::date > (t->>'published_date')::date) THEN
        RAISE EXCEPTION 'Invalid transaction in filing';
    END IF;
    SELECT coalesce(array_agg(t->>'doc_id'), ARRAY[]::text[]) INTO trade_keys FROM jsonb_array_elements(trades) t;
    IF cardinality(trade_keys) <> (SELECT count(DISTINCT k) FROM unnest(trade_keys) k) THEN
        RAISE EXCEPTION 'Duplicate transaction keys';
    END IF;
    IF EXISTS (SELECT 1 FROM jsonb_array_elements(events || raw_rows) e
        WHERE e->>'source' IS DISTINCT FROM 'congress' OR NOT (e->>'source_document_id' = ANY(trade_keys))) THEN
        RAISE EXCEPTION 'Foreign signal or raw filing';
    END IF;
    SELECT count(*) INTO prior_count FROM politician_trades WHERE lower(doc_id) >= target_filing_id || '-' AND lower(doc_id) < target_filing_id || '.';
    SELECT EXISTS (
        (SELECT to_jsonb(t) - 'id' - 'created_at' FROM politician_trades t WHERE lower(doc_id) >= target_filing_id || '-' AND lower(doc_id) < target_filing_id || '.'
         EXCEPT SELECT value FROM jsonb_array_elements(trades))
        UNION ALL
        (SELECT value FROM jsonb_array_elements(trades)
         EXCEPT SELECT to_jsonb(t) - 'id' - 'created_at' FROM politician_trades t WHERE lower(doc_id) >= target_filing_id || '-' AND lower(doc_id) < target_filing_id || '.')
    ) INTO changed;

    IF changed THEN
        SELECT coalesce(array_agg(id), ARRAY[]::uuid[]) INTO changed_ids FROM signal_events
        WHERE source='congress' AND signal_type='politician_trade'
          AND lower(source_document_id) >= target_filing_id || '-' AND lower(source_document_id) < target_filing_id || '.';
        -- Invalidate derived events transitively; their compiler will rebuild from current rows.
        WITH RECURSIVE affected(id) AS (
            SELECT unnest(changed_ids)
            UNION
            SELECT e.id FROM signal_events e JOIN affected a ON
                signal_dependency_ids(e.payload) @> ARRAY[a.id::text]
        ) SELECT coalesce(array_agg(id), ARRAY[]::uuid[]) INTO removed_ids FROM affected
          WHERE NOT (id = ANY(changed_ids));
        removed_ids := removed_ids || ARRAY(SELECT id FROM signal_events WHERE id=ANY(changed_ids)
            AND NOT (source_document_id=ANY(trade_keys)));
        snapshot := jsonb_build_object(
            'trades', (SELECT coalesce(jsonb_agg(to_jsonb(t)), '[]') FROM politician_trades t WHERE lower(doc_id) >= target_filing_id || '-' AND lower(doc_id) < target_filing_id || '.'),
            'signals', (SELECT coalesce(jsonb_agg(to_jsonb(e)), '[]') FROM signal_events e WHERE id=ANY(removed_ids || changed_ids)),
            'raw_filings', (SELECT coalesce(jsonb_agg(to_jsonb(r)), '[]') FROM raw_filings r WHERE source='congress' AND lower(source_document_id) >= target_filing_id || '-' AND lower(source_document_id) < target_filing_id || '.'),
            'deliveries', (SELECT coalesce(jsonb_agg(to_jsonb(d)), '[]') FROM alert_deliveries d WHERE signal_event_id=ANY(removed_ids || changed_ids))
        );
        IF to_regclass('public.tweet_candidates') IS NOT NULL THEN
            EXECUTE 'SELECT $1 || jsonb_build_object(''tweet_candidates'', coalesce(jsonb_agg(to_jsonb(t)), ''[]'')) FROM public.tweet_candidates t WHERE signal_event_id=ANY($2)'
                INTO snapshot USING snapshot, removed_ids || changed_ids;
            EXECUTE 'INSERT INTO congress_delivery_tombstones(kind,delivery_key) SELECT ''tweet'', candidate_key FROM tweet_candidates WHERE signal_event_id=ANY($1) AND status IN (''posted'',''posting'') ON CONFLICT DO NOTHING' USING removed_ids;
            EXECUTE 'UPDATE tweet_candidates SET status=''rejected'', review_notes=''Source filing corrected; previous draft archived'' WHERE signal_event_id=ANY($1) AND status IN (''pending_review'',''approved'')' USING changed_ids;
        END IF;
        INSERT INTO congress_corrections(filing_id,snapshot) VALUES(target_filing_id,snapshot);
        INSERT INTO congress_delivery_tombstones(kind,delivery_key)
            SELECT 'alert',delivery_key FROM alert_deliveries WHERE signal_event_id=ANY(removed_ids)
                AND status IN ('sent','sending') ON CONFLICT DO NOTHING;
        UPDATE alert_deliveries SET status='superseded',last_error='Source filing corrected; previous payload archived'
            WHERE signal_event_id=ANY(changed_ids) AND status IN ('pending','failed');
        DELETE FROM signal_events WHERE id=ANY(removed_ids);
    END IF;
    INSERT INTO companies(ticker,name) SELECT 'N/A','Unmapped asset'
        WHERE EXISTS (SELECT 1 FROM jsonb_array_elements(trades) t WHERE t->>'ticker'='N/A')
        ON CONFLICT(ticker) DO NOTHING;
    DELETE FROM politician_trades WHERE lower(doc_id) >= target_filing_id || '-' AND lower(doc_id) < target_filing_id || '.' AND NOT (doc_id=ANY(trade_keys));
    INSERT INTO politician_trades(member_id,politician_name,chamber,party,ticker,asset_name,
        transaction_date,published_date,transaction_type,asset_type,amount_range,source_url,doc_id)
    SELECT member_id,politician_name,chamber,party,ticker,asset_name,transaction_date,published_date,
        transaction_type,asset_type,amount_range,source_url,doc_id
    FROM jsonb_populate_recordset(NULL::politician_trades,trades)
    ON CONFLICT (doc_id) WHERE doc_id IS NOT NULL DO UPDATE SET
        member_id=EXCLUDED.member_id,politician_name=EXCLUDED.politician_name,chamber=EXCLUDED.chamber,
        party=EXCLUDED.party,ticker=EXCLUDED.ticker,asset_name=EXCLUDED.asset_name,
        transaction_date=EXCLUDED.transaction_date,published_date=EXCLUDED.published_date,
        transaction_type=EXCLUDED.transaction_type,asset_type=EXCLUDED.asset_type,
        amount_range=EXCLUDED.amount_range,source_url=EXCLUDED.source_url;
    DELETE FROM raw_filings WHERE source='congress' AND lower(source_document_id) >= target_filing_id || '-' AND lower(source_document_id) < target_filing_id || '.'
        AND NOT (source_document_id=ANY(trade_keys));
    INSERT INTO raw_filings(source,filing_type,source_document_id,source_url,ticker,filer_name,filed_at,payload)
    SELECT source,filing_type,source_document_id,source_url,ticker,filer_name,filed_at,payload
    FROM jsonb_populate_recordset(NULL::raw_filings,raw_rows)
    ON CONFLICT(source,source_document_id) DO UPDATE SET source_url=EXCLUDED.source_url,ticker=EXCLUDED.ticker,
        filer_name=EXCLUDED.filer_name,filed_at=EXCLUDED.filed_at,payload=EXCLUDED.payload;
    INSERT INTO signal_events(source,signal_type,source_document_id,ticker,actor_name,actor_type,direction,
        occurred_at,published_at,importance_score,title,summary,source_url,payload)
    SELECT source,signal_type,source_document_id,ticker,actor_name,actor_type,direction,
        occurred_at,published_at,importance_score,title,summary,source_url,payload
    FROM jsonb_populate_recordset(NULL::signal_events,events)
    ON CONFLICT(source,source_document_id) DO UPDATE SET ticker=EXCLUDED.ticker,actor_name=EXCLUDED.actor_name,
        direction=EXCLUDED.direction,occurred_at=EXCLUDED.occurred_at,published_at=EXCLUDED.published_at,
        importance_score=EXCLUDED.importance_score,title=EXCLUDED.title,summary=EXCLUDED.summary,
        source_url=EXCLUDED.source_url,payload=EXCLUDED.payload;
    IF EXISTS (SELECT 1 FROM jsonb_array_elements(events) e WHERE NOT EXISTS
        (SELECT 1 FROM signal_events s WHERE s.source='congress' AND s.source_document_id=e->>'source_document_id' AND s.payload @> (e->'payload')))
       OR EXISTS (SELECT 1 FROM jsonb_array_elements(raw_rows) e WHERE NOT EXISTS
        (SELECT 1 FROM raw_filings r WHERE r.source='congress' AND r.source_document_id=e->>'source_document_id' AND r.payload @> (e->'payload'))) THEN
        RAISE EXCEPTION 'Signal/raw publication did not match canonical transactions';
    END IF;
    UPDATE congress_filings SET status='complete', last_error=NULL, claim_token=NULL, lease_until=NULL,
        completed_at=now(), updated_at=now(), stored_rows=jsonb_array_length(trades),
        parser_version=publish_congress_filing.parser_version, source_hash=publish_congress_filing.source_hash,
        next_attempt_at=now() + CASE WHEN published_date >= current_date - 30 THEN interval '1 day' ELSE interval '30 days' END
    WHERE filing_id=target_filing_id;
    RETURN jsonb_build_object('previous_rows',prior_count,'stored_rows',jsonb_array_length(trades));
END $$;

-- A legacy emitter may hold a pre-correction snapshot. It must not recreate a
-- removed trade or overwrite a correction after the publication transaction.
CREATE OR REPLACE FUNCTION public.guard_congress_snapshot()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
DECLARE prefix text; canonical jsonb; field text;
BEGIN
    IF NEW.source <> 'congress' THEN RETURN NEW; END IF;
    IF TG_TABLE_NAME='signal_events' AND to_jsonb(NEW)->>'signal_type' <> 'politician_trade' THEN RETURN NEW; END IF;
    prefix := regexp_replace(lower(NEW.source_document_id), '-[0-9]+$', '');
    IF NOT EXISTS (SELECT 1 FROM congress_filings WHERE filing_id=prefix) THEN RETURN NEW; END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended(prefix,0));
    SELECT to_jsonb(t) INTO canonical FROM politician_trades t WHERE doc_id=lower(NEW.source_document_id);
    IF canonical IS NULL THEN RETURN NULL; END IF;
    FOREACH field IN ARRAY ARRAY['member_id','politician_name','ticker','transaction_date',
        'published_date','transaction_type','amount_range','asset_name','asset_type'] LOOP
        IF coalesce(NEW.payload->>field,'') IS DISTINCT FROM coalesce(canonical->>field,'') THEN RETURN NULL; END IF;
    END LOOP;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS guard_congress_signal_snapshot ON public.signal_events;
CREATE TRIGGER guard_congress_signal_snapshot BEFORE INSERT OR UPDATE ON public.signal_events
    FOR EACH ROW EXECUTE FUNCTION public.guard_congress_snapshot();
DROP TRIGGER IF EXISTS guard_congress_raw_snapshot ON public.raw_filings;
CREATE TRIGGER guard_congress_raw_snapshot BEFORE INSERT OR UPDATE ON public.raw_filings
    FOR EACH ROW EXECUTE FUNCTION public.guard_congress_snapshot();
REVOKE ALL ON FUNCTION public.guard_congress_snapshot() FROM PUBLIC, anon, authenticated;

CREATE OR REPLACE FUNCTION public.guard_congress_redelivery()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=public,pg_temp AS $$
DECLARE target_kind text; target_key text;
BEGIN
    target_kind := CASE WHEN TG_TABLE_NAME='alert_deliveries' THEN 'alert' ELSE 'tweet' END;
    target_key := CASE WHEN target_kind='alert' THEN to_jsonb(NEW)->>'delivery_key' ELSE to_jsonb(NEW)->>'candidate_key' END;
    IF EXISTS(SELECT 1 FROM congress_delivery_tombstones t WHERE t.kind=target_kind AND t.delivery_key=target_key) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END $$;
REVOKE ALL ON FUNCTION public.guard_congress_redelivery() FROM PUBLIC, anon, authenticated;
DROP TRIGGER IF EXISTS guard_congress_redelivery ON public.alert_deliveries;
CREATE TRIGGER guard_congress_redelivery BEFORE INSERT ON public.alert_deliveries
    FOR EACH ROW EXECUTE FUNCTION public.guard_congress_redelivery();
DO $$ BEGIN
    IF to_regclass('public.tweet_candidates') IS NOT NULL THEN
        EXECUTE 'DROP TRIGGER IF EXISTS guard_congress_redelivery ON public.tweet_candidates';
        EXECUTE 'CREATE TRIGGER guard_congress_redelivery BEFORE INSERT ON public.tweet_candidates FOR EACH ROW EXECUTE FUNCTION public.guard_congress_redelivery()';
    END IF;
END $$;

REVOKE ALL ON FUNCTION public.register_congress_filings(jsonb) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.claim_congress_filing(text,boolean) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.fail_congress_filing(text,uuid,text) FROM PUBLIC, anon, authenticated;
REVOKE ALL ON FUNCTION public.publish_congress_filing(text,jsonb,jsonb,jsonb,uuid,text,text,jsonb,boolean) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.register_congress_filings(jsonb) TO service_role;
GRANT EXECUTE ON FUNCTION public.claim_congress_filing(text,boolean) TO service_role;
GRANT EXECUTE ON FUNCTION public.fail_congress_filing(text,uuid,text) TO service_role;
GRANT EXECUTE ON FUNCTION public.publish_congress_filing(text,jsonb,jsonb,jsonb,uuid,text,text,jsonb,boolean) TO service_role;
NOTIFY pgrst, 'reload schema';
COMMIT;
