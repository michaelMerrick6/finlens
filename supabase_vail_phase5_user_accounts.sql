-- Vail Phase 5 user account and self-serve alerts
-- Apply after phases 1, 2, 3, and 4. Non-destructive.

CREATE TABLE IF NOT EXISTS public.profiles (
    id UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
    email TEXT,
    display_name TEXT,
    alert_email TEXT,
    telegram_username TEXT,
    telegram_chat_id TEXT,
    email_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    telegram_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    follow_limit INTEGER NOT NULL DEFAULT 10,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_profiles_email ON public.profiles(email);
CREATE INDEX IF NOT EXISTS idx_profiles_alert_email ON public.profiles(alert_email);

DROP TRIGGER IF EXISTS set_profiles_updated_at ON public.profiles;
CREATE TRIGGER set_profiles_updated_at
BEFORE UPDATE ON public.profiles
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();

ALTER TABLE public.profiles ENABLE ROW LEVEL SECURITY;

ALTER TABLE public.watchlists
ADD COLUMN IF NOT EXISTS user_id UUID REFERENCES auth.users(id) ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS idx_watchlists_user_id ON public.watchlists(user_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_watchlists_user_name
ON public.watchlists(user_id, name)
WHERE user_id IS NOT NULL;

UPDATE public.watchlists
SET user_id = owner_key::uuid
WHERE user_id IS NULL
  AND owner_type = 'auth_user'
  AND owner_key ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

CREATE OR REPLACE FUNCTION public.handle_auth_user_created()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
    INSERT INTO public.profiles (
        id,
        email,
        display_name,
        alert_email
    )
    VALUES (
        NEW.id,
        NEW.email,
        COALESCE(
            NULLIF(NEW.raw_user_meta_data ->> 'display_name', ''),
            NULLIF(NEW.raw_user_meta_data ->> 'full_name', ''),
            NULLIF(split_part(COALESCE(NEW.email, ''), '@', 1), '')
        ),
        NEW.email
    )
    ON CONFLICT (id) DO UPDATE
    SET email = EXCLUDED.email,
        display_name = COALESCE(public.profiles.display_name, EXCLUDED.display_name),
        alert_email = COALESCE(public.profiles.alert_email, EXCLUDED.alert_email),
        updated_at = NOW();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS on_auth_user_created ON auth.users;
CREATE TRIGGER on_auth_user_created
AFTER INSERT ON auth.users
FOR EACH ROW
EXECUTE FUNCTION public.handle_auth_user_created();

INSERT INTO public.profiles (
    id,
    email,
    display_name,
    alert_email
)
SELECT
    users.id,
    users.email,
    COALESCE(
        NULLIF(users.raw_user_meta_data ->> 'display_name', ''),
        NULLIF(users.raw_user_meta_data ->> 'full_name', ''),
        NULLIF(split_part(COALESCE(users.email, ''), '@', 1), '')
    ),
    users.email
FROM auth.users AS users
ON CONFLICT (id) DO UPDATE
SET email = EXCLUDED.email,
    display_name = COALESCE(public.profiles.display_name, EXCLUDED.display_name),
    alert_email = COALESCE(public.profiles.alert_email, EXCLUDED.alert_email),
    updated_at = NOW();

CREATE OR REPLACE FUNCTION public.watchlist_belongs_to_auth_user(target_watchlist_id UUID)
RETURNS BOOLEAN
LANGUAGE SQL
STABLE
AS $$
    SELECT EXISTS (
        SELECT 1
        FROM public.watchlists
        WHERE id = target_watchlist_id
          AND (
              user_id = auth.uid()
              OR (
                  user_id IS NULL
                  AND owner_type = 'auth_user'
                  AND owner_key = auth.uid()::TEXT
              )
          )
    );
$$;

DROP POLICY IF EXISTS "Profiles self read" ON public.profiles;
CREATE POLICY "Profiles self read"
ON public.profiles
FOR SELECT
TO authenticated
USING (auth.uid() = id);

DROP POLICY IF EXISTS "Profiles self insert" ON public.profiles;
CREATE POLICY "Profiles self insert"
ON public.profiles
FOR INSERT
TO authenticated
WITH CHECK (auth.uid() = id);

DROP POLICY IF EXISTS "Profiles self update" ON public.profiles;
CREATE POLICY "Profiles self update"
ON public.profiles
FOR UPDATE
TO authenticated
USING (auth.uid() = id)
WITH CHECK (auth.uid() = id);

DROP POLICY IF EXISTS "Auth users manage own watchlists" ON public.watchlists;
CREATE POLICY "Auth users manage own watchlists"
ON public.watchlists
FOR ALL
TO authenticated
USING (
    user_id = auth.uid()
    OR (
        user_id IS NULL
        AND owner_type = 'auth_user'
        AND owner_key = auth.uid()::TEXT
    )
)
WITH CHECK (
    user_id = auth.uid()
    OR (
        owner_type = 'auth_user'
        AND owner_key = auth.uid()::TEXT
    )
);

DROP POLICY IF EXISTS "Auth users manage own ticker follows" ON public.watchlist_tickers;
CREATE POLICY "Auth users manage own ticker follows"
ON public.watchlist_tickers
FOR ALL
TO authenticated
USING (public.watchlist_belongs_to_auth_user(watchlist_id))
WITH CHECK (public.watchlist_belongs_to_auth_user(watchlist_id));

DROP POLICY IF EXISTS "Auth users manage own actor follows" ON public.watchlist_actors;
CREATE POLICY "Auth users manage own actor follows"
ON public.watchlist_actors
FOR ALL
TO authenticated
USING (public.watchlist_belongs_to_auth_user(watchlist_id))
WITH CHECK (public.watchlist_belongs_to_auth_user(watchlist_id));

DROP POLICY IF EXISTS "Auth users manage own subscriptions" ON public.alert_subscriptions;
CREATE POLICY "Auth users manage own subscriptions"
ON public.alert_subscriptions
FOR ALL
TO authenticated
USING (
    watchlist_id IS NOT NULL
    AND public.watchlist_belongs_to_auth_user(watchlist_id)
)
WITH CHECK (
    watchlist_id IS NOT NULL
    AND public.watchlist_belongs_to_auth_user(watchlist_id)
);

DROP POLICY IF EXISTS "Auth users read own alert deliveries" ON public.alert_deliveries;
CREATE POLICY "Auth users read own alert deliveries"
ON public.alert_deliveries
FOR SELECT
TO authenticated
USING (
    subscription_id IS NOT NULL
    AND EXISTS (
        SELECT 1
        FROM public.alert_subscriptions
        WHERE alert_subscriptions.id = alert_deliveries.subscription_id
          AND alert_subscriptions.watchlist_id IS NOT NULL
          AND public.watchlist_belongs_to_auth_user(alert_subscriptions.watchlist_id)
    )
);
