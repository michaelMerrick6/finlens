-- Vail Phase 2 notifications schema
-- Apply after supabase_vail_phase1.sql. This file is non-destructive.

CREATE TABLE IF NOT EXISTS public.watchlist_actors (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    watchlist_id UUID NOT NULL REFERENCES public.watchlists(id) ON DELETE CASCADE,
    actor_type TEXT NOT NULL,
    actor_key TEXT NOT NULL,
    actor_name TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE(watchlist_id, actor_type, actor_key)
);

CREATE INDEX IF NOT EXISTS idx_watchlist_actors_watchlist ON public.watchlist_actors(watchlist_id);
CREATE INDEX IF NOT EXISTS idx_watchlist_actors_lookup ON public.watchlist_actors(actor_type, actor_key);

DROP TRIGGER IF EXISTS set_watchlist_actors_updated_at ON public.watchlist_actors;
CREATE TRIGGER set_watchlist_actors_updated_at
BEFORE UPDATE ON public.watchlist_actors
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();

ALTER TABLE public.watchlist_actors ENABLE ROW LEVEL SECURITY;
