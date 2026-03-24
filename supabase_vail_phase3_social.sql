-- Vail Phase 3 social posting review queue
-- Apply this after the phase 1 and phase 2 migrations. Non-destructive.

CREATE TABLE IF NOT EXISTS public.tweet_candidates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    signal_event_id UUID NOT NULL REFERENCES public.signal_events(id) ON DELETE CASCADE,
    channel TEXT NOT NULL DEFAULT 'twitter',
    candidate_key TEXT NOT NULL,
    rule_key TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending_review',
    score NUMERIC(5,2) NOT NULL DEFAULT 0,
    title TEXT NOT NULL,
    draft_text TEXT NOT NULL,
    rationale TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    reviewed_by TEXT,
    review_notes TEXT,
    reviewed_at TIMESTAMP WITH TIME ZONE,
    posted_at TIMESTAMP WITH TIME ZONE,
    external_post_id TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE(channel, candidate_key)
);

CREATE INDEX IF NOT EXISTS idx_tweet_candidates_status_created_at
ON public.tweet_candidates(status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_tweet_candidates_signal_event
ON public.tweet_candidates(signal_event_id);

CREATE INDEX IF NOT EXISTS idx_tweet_candidates_rule_key
ON public.tweet_candidates(rule_key);

DROP TRIGGER IF EXISTS set_tweet_candidates_updated_at ON public.tweet_candidates;
CREATE TRIGGER set_tweet_candidates_updated_at
BEFORE UPDATE ON public.tweet_candidates
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();

ALTER TABLE public.tweet_candidates ENABLE ROW LEVEL SECURITY;
