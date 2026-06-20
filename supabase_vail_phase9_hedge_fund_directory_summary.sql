-- Fast hedge-fund directory summaries for 13F pages.
-- The app falls back to raw holdings if this view is not installed, but
-- production should use this view to avoid statement timeouts on large tables.

CREATE INDEX IF NOT EXISTS idx_institutional_holdings_report_period_value
ON public.institutional_holdings(report_period DESC, value_held DESC)
WHERE shares_held > 0;

CREATE INDEX IF NOT EXISTS idx_institutional_holdings_fund_report_period_value
ON public.institutional_holdings(fund_name, report_period DESC, value_held DESC)
WHERE shares_held > 0;

CREATE OR REPLACE VIEW public.fund_directory_period_summary AS
SELECT
  fund_name,
  report_period,
  MAX(published_date) AS last_filed_date,
  SUM(value_held) AS current_portfolio_value,
  COUNT(*)::integer AS current_holding_count
FROM public.institutional_holdings
WHERE
  fund_name IS NOT NULL
  AND report_period IS NOT NULL
  AND shares_held > 0
GROUP BY fund_name, report_period;

GRANT SELECT ON public.fund_directory_period_summary TO anon;
GRANT SELECT ON public.fund_directory_period_summary TO authenticated;
