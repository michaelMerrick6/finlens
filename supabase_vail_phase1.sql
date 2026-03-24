-- Vail Phase 1 foundation schema
-- Apply this after the base schema. This file is intentionally non-destructive.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE OR REPLACE FUNCTION public.set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.increment_scraper_run_error_count(target_run_id UUID)
RETURNS VOID AS $$
BEGIN
    UPDATE public.scraper_runs
    SET error_count = COALESCE(error_count, 0) + 1,
        updated_at = NOW()
    WHERE id = target_run_id;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS public.scraper_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    parent_run_id UUID REFERENCES public.scraper_runs(id) ON DELETE SET NULL,
    scraper_name TEXT NOT NULL,
    source_name TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'manual',
    status TEXT NOT NULL DEFAULT 'running',
    started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMP WITH TIME ZONE,
    duration_ms BIGINT,
    records_seen INTEGER,
    records_inserted INTEGER,
    records_updated INTEGER,
    records_skipped INTEGER,
    signal_events_created INTEGER,
    error_count INTEGER NOT NULL DEFAULT 0,
    stdout_excerpt TEXT,
    stderr_excerpt TEXT,
    run_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scraper_runs_started_at ON public.scraper_runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_scraper_runs_status ON public.scraper_runs(status);
CREATE INDEX IF NOT EXISTS idx_scraper_runs_parent_run ON public.scraper_runs(parent_run_id);

DROP TRIGGER IF EXISTS set_scraper_runs_updated_at ON public.scraper_runs;
CREATE TRIGGER set_scraper_runs_updated_at
BEFORE UPDATE ON public.scraper_runs
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();

CREATE TABLE IF NOT EXISTS public.scraper_errors (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_id UUID NOT NULL REFERENCES public.scraper_runs(id) ON DELETE CASCADE,
    stage TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'error',
    message TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scraper_errors_run_id ON public.scraper_errors(run_id);
CREATE INDEX IF NOT EXISTS idx_scraper_errors_created_at ON public.scraper_errors(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_politician_trades_created_at ON public.politician_trades(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_politician_trades_doc_id ON public.politician_trades(doc_id);
CREATE INDEX IF NOT EXISTS idx_insider_trades_created_at ON public.insider_trades(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_institutional_holdings_created_at ON public.institutional_holdings(created_at DESC);

CREATE TABLE IF NOT EXISTS public.raw_filings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source TEXT NOT NULL,
    filing_type TEXT NOT NULL,
    source_document_id TEXT NOT NULL,
    source_url TEXT,
    ticker VARCHAR(10),
    filer_name TEXT,
    filed_at DATE,
    received_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    content_hash TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE(source, source_document_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_filings_source_filed_at ON public.raw_filings(source, filed_at DESC);
CREATE INDEX IF NOT EXISTS idx_raw_filings_ticker ON public.raw_filings(ticker);

DROP TRIGGER IF EXISTS set_raw_filings_updated_at ON public.raw_filings;
CREATE TRIGGER set_raw_filings_updated_at
BEFORE UPDATE ON public.raw_filings
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();

CREATE TABLE IF NOT EXISTS public.signal_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    raw_filing_id UUID REFERENCES public.raw_filings(id) ON DELETE SET NULL,
    source TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    source_document_id TEXT NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    actor_name TEXT NOT NULL,
    actor_type TEXT NOT NULL,
    direction TEXT,
    occurred_at DATE,
    published_at DATE,
    importance_score NUMERIC(5,2) NOT NULL DEFAULT 0,
    title TEXT NOT NULL,
    summary TEXT NOT NULL,
    source_url TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE(source, source_document_id)
);

CREATE INDEX IF NOT EXISTS idx_signal_events_ticker_published_at ON public.signal_events(ticker, published_at DESC);
CREATE INDEX IF NOT EXISTS idx_signal_events_source_created_at ON public.signal_events(source, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_signal_events_importance ON public.signal_events(importance_score DESC);

DROP TRIGGER IF EXISTS set_signal_events_updated_at ON public.signal_events;
CREATE TRIGGER set_signal_events_updated_at
BEFORE UPDATE ON public.signal_events
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();

CREATE TABLE IF NOT EXISTS public.watchlists (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    owner_type TEXT NOT NULL DEFAULT 'anonymous',
    owner_key TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT 'Default',
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE(owner_type, owner_key, name)
);

DROP TRIGGER IF EXISTS set_watchlists_updated_at ON public.watchlists;
CREATE TRIGGER set_watchlists_updated_at
BEFORE UPDATE ON public.watchlists
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();

CREATE TABLE IF NOT EXISTS public.watchlist_tickers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    watchlist_id UUID NOT NULL REFERENCES public.watchlists(id) ON DELETE CASCADE,
    ticker VARCHAR(10) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE(watchlist_id, ticker)
);

CREATE INDEX IF NOT EXISTS idx_watchlist_tickers_watchlist ON public.watchlist_tickers(watchlist_id);
CREATE INDEX IF NOT EXISTS idx_watchlist_tickers_ticker ON public.watchlist_tickers(ticker);

CREATE TABLE IF NOT EXISTS public.alert_subscriptions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    watchlist_id UUID REFERENCES public.watchlists(id) ON DELETE CASCADE,
    channel TEXT NOT NULL,
    destination TEXT NOT NULL,
    minimum_importance NUMERIC(5,2) NOT NULL DEFAULT 0.5,
    event_types TEXT[] NOT NULL DEFAULT '{}'::TEXT[],
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alert_subscriptions_watchlist ON public.alert_subscriptions(watchlist_id);
CREATE INDEX IF NOT EXISTS idx_alert_subscriptions_active ON public.alert_subscriptions(active);

DROP TRIGGER IF EXISTS set_alert_subscriptions_updated_at ON public.alert_subscriptions;
CREATE TRIGGER set_alert_subscriptions_updated_at
BEFORE UPDATE ON public.alert_subscriptions
FOR EACH ROW
EXECUTE FUNCTION public.set_updated_at();

CREATE TABLE IF NOT EXISTS public.alert_deliveries (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    signal_event_id UUID NOT NULL REFERENCES public.signal_events(id) ON DELETE CASCADE,
    subscription_id UUID REFERENCES public.alert_subscriptions(id) ON DELETE CASCADE,
    delivery_key TEXT NOT NULL,
    channel TEXT NOT NULL,
    destination TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    queued_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    sent_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_alert_deliveries_status ON public.alert_deliveries(status, queued_at DESC);
CREATE INDEX IF NOT EXISTS idx_alert_deliveries_signal_event ON public.alert_deliveries(signal_event_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_alert_deliveries_delivery_key
ON public.alert_deliveries(delivery_key);

ALTER TABLE public.scraper_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.scraper_errors ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.raw_filings ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.signal_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.watchlists ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.watchlist_tickers ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.alert_subscriptions ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.alert_deliveries ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Allow public read-only signal events" ON public.signal_events;
CREATE POLICY "Allow public read-only signal events"
ON public.signal_events
FOR SELECT
USING (true);
