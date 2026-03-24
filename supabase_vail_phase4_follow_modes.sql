-- Vail Phase 4 follow alert modes
-- Apply after phase 1 and phase 2. Non-destructive.

ALTER TABLE public.watchlist_tickers
ADD COLUMN IF NOT EXISTS alert_mode TEXT NOT NULL DEFAULT 'both';

ALTER TABLE public.watchlist_actors
ADD COLUMN IF NOT EXISTS alert_mode TEXT NOT NULL DEFAULT 'both';

CREATE INDEX IF NOT EXISTS idx_watchlist_tickers_alert_mode
ON public.watchlist_tickers(alert_mode);

CREATE INDEX IF NOT EXISTS idx_watchlist_actors_alert_mode
ON public.watchlist_actors(alert_mode);
