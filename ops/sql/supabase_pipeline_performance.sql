-- Non-destructive indexes for scheduled capture, replay, and signal queries.
-- Apply this file directly to an existing production database.

CREATE INDEX IF NOT EXISTS idx_politician_trades_published_date
ON public.politician_trades(published_date DESC);

CREATE INDEX IF NOT EXISTS idx_politician_trades_transaction_date
ON public.politician_trades(transaction_date DESC);

CREATE INDEX IF NOT EXISTS idx_insider_trades_published_date
ON public.insider_trades(published_date DESC);

CREATE INDEX IF NOT EXISTS idx_insider_trades_transaction_date
ON public.insider_trades(transaction_date DESC);

CREATE INDEX IF NOT EXISTS idx_institutional_holdings_published_date
ON public.institutional_holdings(published_date DESC);

CREATE INDEX IF NOT EXISTS idx_institutional_holdings_report_period
ON public.institutional_holdings(report_period DESC);

CREATE INDEX IF NOT EXISTS idx_institutional_holdings_fund_period
ON public.institutional_holdings(fund_name, report_period DESC);

CREATE INDEX IF NOT EXISTS idx_signal_events_created_at
ON public.signal_events(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_signal_events_type_published_at
ON public.signal_events(signal_type, published_at DESC);

CREATE INDEX IF NOT EXISTS idx_signal_events_type_occurred_at
ON public.signal_events(signal_type, occurred_at DESC);
