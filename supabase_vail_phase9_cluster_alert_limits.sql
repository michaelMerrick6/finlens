-- Vail Phase 9 per-user cluster alert daily limits
-- Apply after phase 8. Non-destructive.

CREATE TABLE IF NOT EXISTS public.cluster_alert_daily_events (
    user_id UUID NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
    alert_date DATE NOT NULL,
    signal_event_id UUID NOT NULL REFERENCES public.signal_events(id) ON DELETE CASCADE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, alert_date, signal_event_id)
);

ALTER TABLE public.cluster_alert_daily_events ENABLE ROW LEVEL SECURITY;

-- Preserve today's already-queued cluster events when this migration is first applied.
INSERT INTO public.cluster_alert_daily_events (
    user_id,
    alert_date,
    signal_event_id
)
SELECT DISTINCT
    watchlists.user_id,
    (alert_deliveries.queued_at AT TIME ZONE 'UTC')::DATE,
    alert_deliveries.signal_event_id
FROM public.alert_deliveries
JOIN public.alert_subscriptions
  ON alert_subscriptions.id = alert_deliveries.subscription_id
JOIN public.watchlists
  ON watchlists.id = alert_subscriptions.watchlist_id
JOIN public.signal_events
  ON signal_events.id = alert_deliveries.signal_event_id
WHERE watchlists.user_id IS NOT NULL
  AND alert_deliveries.status IN ('pending', 'sent')
  AND (alert_deliveries.queued_at AT TIME ZONE 'UTC')::DATE = (NOW() AT TIME ZONE 'UTC')::DATE
  AND signal_events.signal_type IN (
      'politician_cluster',
      'insider_cluster',
      'cross_source_accumulation'
  )
ON CONFLICT (user_id, alert_date, signal_event_id) DO NOTHING;

CREATE OR REPLACE FUNCTION public.queue_cluster_alert_deliveries_capped(
    p_deliveries JSONB,
    p_daily_limit INTEGER DEFAULT 5
)
RETURNS TABLE (
    deliveries_queued INTEGER,
    cluster_events_reserved INTEGER,
    cluster_events_suppressed INTEGER
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    item JSONB;
    item_user_id UUID;
    item_signal_event_id UUID;
    item_subscription_id UUID;
    item_key TEXT;
    alert_date_utc DATE := (NOW() AT TIME ZONE 'UTC')::DATE;
    daily_count INTEGER;
    inserted_count INTEGER;
    event_reserved BOOLEAN;
    suppressed_key TEXT;
    suppressed_keys TEXT[] := ARRAY[]::TEXT[];
BEGIN
    IF auth.role() IS DISTINCT FROM 'service_role' THEN
        RAISE EXCEPTION 'queue_cluster_alert_deliveries_capped requires the service role';
    END IF;

    deliveries_queued := 0;
    cluster_events_reserved := 0;
    cluster_events_suppressed := 0;

    FOR item IN
        SELECT entry
        FROM jsonb_array_elements(COALESCE(p_deliveries, '[]'::JSONB)) AS entries(entry)
        ORDER BY
            entry ->> 'user_id',
            COALESCE(NULLIF(entry ->> 'importance_score', '')::NUMERIC, 0) DESC,
            entry ->> 'signal_event_id',
            entry ->> 'channel'
    LOOP
        item_user_id := (item ->> 'user_id')::UUID;
        item_signal_event_id := (item ->> 'signal_event_id')::UUID;
        item_subscription_id := NULLIF(item ->> 'subscription_id', '')::UUID;
        item_key := item ->> 'delivery_key';
        suppressed_key := item_user_id::TEXT || ':' || item_signal_event_id::TEXT;

        -- Serialize quota reservations for one user and UTC day across overlapping jobs.
        PERFORM pg_advisory_xact_lock(
            hashtextextended(item_user_id::TEXT || ':' || alert_date_utc::TEXT, 0)
        );

        SELECT EXISTS (
            SELECT 1
            FROM public.cluster_alert_daily_events
            WHERE user_id = item_user_id
              AND alert_date = alert_date_utc
              AND signal_event_id = item_signal_event_id
        )
        INTO event_reserved;

        IF NOT event_reserved THEN
            SELECT COUNT(*)
            INTO daily_count
            FROM public.cluster_alert_daily_events
            WHERE user_id = item_user_id
              AND alert_date = alert_date_utc;

            IF daily_count >= GREATEST(p_daily_limit, 0) THEN
                IF NOT (suppressed_key = ANY(suppressed_keys)) THEN
                    suppressed_keys := array_append(suppressed_keys, suppressed_key);
                    cluster_events_suppressed := cluster_events_suppressed + 1;
                END IF;
                CONTINUE;
            END IF;

            INSERT INTO public.cluster_alert_daily_events (
                user_id,
                alert_date,
                signal_event_id
            )
            VALUES (
                item_user_id,
                alert_date_utc,
                item_signal_event_id
            )
            ON CONFLICT (user_id, alert_date, signal_event_id) DO NOTHING;

            GET DIAGNOSTICS inserted_count = ROW_COUNT;
            cluster_events_reserved := cluster_events_reserved + inserted_count;
        END IF;

        INSERT INTO public.alert_deliveries (
            signal_event_id,
            subscription_id,
            delivery_key,
            channel,
            destination,
            status,
            payload
        )
        VALUES (
            item_signal_event_id,
            item_subscription_id,
            item_key,
            item ->> 'channel',
            item ->> 'destination',
            COALESCE(NULLIF(item ->> 'status', ''), 'pending'),
            COALESCE(item -> 'payload', '{}'::JSONB)
        )
        ON CONFLICT (delivery_key) DO NOTHING;

        GET DIAGNOSTICS inserted_count = ROW_COUNT;
        deliveries_queued := deliveries_queued + inserted_count;
    END LOOP;

    RETURN NEXT;
END;
$$;

REVOKE ALL ON FUNCTION public.queue_cluster_alert_deliveries_capped(JSONB, INTEGER) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.queue_cluster_alert_deliveries_capped(JSONB, INTEGER) TO service_role;
