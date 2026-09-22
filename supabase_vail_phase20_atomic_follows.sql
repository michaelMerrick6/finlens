-- Apply after phase 6, before deploying the atomic account-follow writer.
-- Existing follows are preserved, including accounts above their current limit.
BEGIN;

CREATE OR REPLACE FUNCTION public.save_account_follow(
    p_user_id UUID,
    p_watchlist_id UUID,
    p_kind TEXT,
    p_target TEXT,
    p_alert_mode TEXT,
    p_actor_type TEXT DEFAULT NULL,
    p_actor_name TEXT DEFAULT NULL,
    p_metadata JSONB DEFAULT '{}'::JSONB
)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    account_limit INTEGER;
    current_count BIGINT;
    already_followed BOOLEAN;
BEGIN
    IF auth.role() IS DISTINCT FROM 'service_role' THEN
        RAISE EXCEPTION 'save_account_follow requires the service role' USING ERRCODE = '42501';
    END IF;

    IF p_kind IS NULL OR p_kind NOT IN ('ticker', 'actor')
       OR p_target IS NULL OR btrim(p_target) = ''
       OR p_alert_mode IS NULL OR p_alert_mode NOT IN ('activity', 'unusual', 'both') THEN
        RAISE EXCEPTION 'Invalid follow' USING ERRCODE = '22023';
    END IF;
    IF p_kind = 'actor' AND (
        p_actor_type IS NULL OR p_actor_type NOT IN ('politician', 'insider', 'fund')
        OR p_actor_name IS NULL OR btrim(p_actor_name) = ''
    ) THEN
        RAISE EXCEPTION 'Invalid actor follow' USING ERRCODE = '22023';
    END IF;

    -- One lock coordinates both follow tables and concurrent billing-limit updates.
    SELECT follow_limit INTO account_limit
    FROM public.profiles WHERE id = p_user_id
    FOR UPDATE;
    IF NOT FOUND OR NOT EXISTS (
        SELECT 1 FROM public.watchlists
        WHERE id = p_watchlist_id AND user_id = p_user_id
    ) THEN
        RAISE EXCEPTION 'Account does not own this watchlist' USING ERRCODE = '42501';
    END IF;

    IF p_kind = 'ticker' THEN
        SELECT EXISTS (
            SELECT 1 FROM public.watchlist_tickers
            WHERE watchlist_id = p_watchlist_id AND ticker = p_target
        ) INTO already_followed;
    ELSE
        SELECT EXISTS (
            SELECT 1 FROM public.watchlist_actors
            WHERE watchlist_id = p_watchlist_id AND actor_type = p_actor_type AND actor_key = p_target
        ) INTO already_followed;
    END IF;

    -- Updating an existing follow remains possible at or above the current quota.
    IF NOT already_followed THEN
        SELECT (SELECT count(*) FROM public.watchlist_tickers WHERE watchlist_id = p_watchlist_id)
             + (SELECT count(*) FROM public.watchlist_actors WHERE watchlist_id = p_watchlist_id)
        INTO current_count;
        IF current_count >= account_limit THEN
            RAISE EXCEPTION 'FOLLOW_LIMIT_REACHED' USING ERRCODE = 'P0001';
        END IF;
    END IF;

    IF p_kind = 'ticker' THEN
        INSERT INTO public.watchlist_tickers (watchlist_id, ticker, alert_mode)
        VALUES (p_watchlist_id, p_target, p_alert_mode)
        ON CONFLICT (watchlist_id, ticker) DO UPDATE SET alert_mode = EXCLUDED.alert_mode;
    ELSE
        INSERT INTO public.watchlist_actors (watchlist_id, actor_type, actor_key, actor_name, alert_mode, metadata)
        VALUES (p_watchlist_id, p_actor_type, p_target, p_actor_name, p_alert_mode, COALESCE(p_metadata, '{}'::JSONB))
        ON CONFLICT (watchlist_id, actor_type, actor_key) DO UPDATE
        SET actor_name = EXCLUDED.actor_name, alert_mode = EXCLUDED.alert_mode, metadata = EXCLUDED.metadata;
    END IF;
END;
$$;

REVOKE ALL ON FUNCTION public.save_account_follow(UUID, UUID, TEXT, TEXT, TEXT, TEXT, TEXT, JSONB) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.save_account_follow(UUID, UUID, TEXT, TEXT, TEXT, TEXT, TEXT, JSONB) TO service_role;

COMMIT;
