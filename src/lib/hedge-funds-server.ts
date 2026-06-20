import 'server-only';

import type { InstitutionalHoldingRow, FundDirectoryEntry } from '@/lib/hedge-funds';
import { buildFundDirectory, estimateNext13fFiling } from '@/lib/hedge-funds';
import { getPublicSupabase } from '@/lib/supabase-server';

const PAGE_SIZE = 1000;
const DIRECTORY_HISTORY_PERIOD_COUNT = 2;
const FULL_SELECT =
  'id,fund_name,ticker,report_period,published_date,shares_held,value_held,qoq_change_shares,qoq_change_percent,source_url';

type FundDirectoryPeriodSummaryRow = {
  fund_name: string | null;
  report_period: string | null;
  last_filed_date: string | null;
  current_portfolio_value: number | null;
  current_holding_count: number | null;
};

function quarterEndDate(year: number, month: number) {
  const quarterEndMonth = Math.ceil(month / 3) * 3;
  return new Date(Date.UTC(year, quarterEndMonth, 0, 12));
}

function recent13fReportPeriods(limit: number, now = new Date()) {
  const periods: string[] = [];
  let cursor = quarterEndDate(now.getUTCFullYear(), now.getUTCMonth() + 1);
  if (cursor.getTime() > now.getTime()) {
    cursor = quarterEndDate(cursor.getUTCFullYear(), cursor.getUTCMonth() - 2);
  }

  while (periods.length < limit) {
    periods.push(cursor.toISOString().slice(0, 10));
    cursor = quarterEndDate(cursor.getUTCFullYear(), cursor.getUTCMonth() - 2);
  }

  return periods;
}

function buildFundDirectoryFromSummaryRows(rows: FundDirectoryPeriodSummaryRow[]) {
  const summariesByFund = new Map<string, FundDirectoryPeriodSummaryRow[]>();

  for (const row of rows) {
    const fundName = String(row.fund_name || '').trim();
    const reportPeriod = String(row.report_period || '').slice(0, 10);
    if (!fundName || !reportPeriod) {
      continue;
    }
    const existing = summariesByFund.get(fundName) || [];
    existing.push({ ...row, report_period: reportPeriod });
    summariesByFund.set(fundName, existing);
  }

  const entries: FundDirectoryEntry[] = [];
  for (const [fundName, fundRows] of summariesByFund.entries()) {
    const latest = [...fundRows].sort((left, right) =>
      String(right.report_period || '').localeCompare(String(left.report_period || '')),
    )[0];
    if (!latest?.report_period) {
      continue;
    }

    entries.push({
      fundName,
      latestReportPeriod: latest.report_period,
      lastFiledDate: latest.last_filed_date ? String(latest.last_filed_date).slice(0, 10) : null,
      nextExpectedFiling: estimateNext13fFiling(latest.report_period),
      currentPortfolioValue: Number(latest.current_portfolio_value || 0),
      currentHoldingCount: Number(latest.current_holding_count || 0),
      estimatedReturn: null,
      estimatedReturnStartPeriod: null,
      estimatedReturnEndPeriod: null,
      estimatedReturnCoverage: null,
    });
  }

  return entries.sort((left, right) => {
    const valueDiff = right.currentPortfolioValue - left.currentPortfolioValue;
    if (valueDiff !== 0) {
      return valueDiff;
    }
    return left.fundName.localeCompare(right.fundName);
  });
}

async function loadFundDirectoryFromSummary(periodsToFetch: string[]) {
  const supabase = getPublicSupabase();
  const { data, error } = await supabase
    .from('fund_directory_period_summary')
    .select('fund_name,report_period,last_filed_date,current_portfolio_value,current_holding_count')
    .in('report_period', periodsToFetch)
    .order('current_portfolio_value', { ascending: false });

  if (error) {
    throw new Error(error.message);
  }

  return buildFundDirectoryFromSummaryRows((data || []) as FundDirectoryPeriodSummaryRow[]);
}

/**
 * Load ALL rows for a specific fund (used on the detail page).
 * Filtered to one fund, so row count stays manageable.
 */
export async function loadInstitutionalHoldingRows(fundName: string) {
  const supabase = getPublicSupabase();
  const rows: InstitutionalHoldingRow[] = [];
  let offset = 0;

  while (true) {
    const { data, error } = await supabase
      .from('institutional_holdings')
      .select(FULL_SELECT)
      .eq('fund_name', fundName)
      .order('report_period', { ascending: false })
      .order('published_date', { ascending: false })
      .order('value_held', { ascending: false })
      .range(offset, offset + PAGE_SIZE - 1);

    if (error) throw new Error(error.message);
    const batch = (data || []) as InstitutionalHoldingRow[];
    if (!batch.length) break;
    rows.push(...batch);
    if (batch.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
  }

  return rows;
}

/**
 * Build the fund directory efficiently.
 *
 * Strategy: use the database summary view when available. If a database has
 * not received the summary migration yet, fall back to the bounded raw rows
 * for the latest completed 13F quarter and the previous quarter.
 */
export async function loadFundDirectory(): Promise<FundDirectoryEntry[]> {
  const periodsToFetch = recent13fReportPeriods(DIRECTORY_HISTORY_PERIOD_COUNT);
  if (!periodsToFetch.length) return [];

  try {
    const summaryDirectory = await loadFundDirectoryFromSummary(periodsToFetch);
    if (summaryDirectory.length) {
      return summaryDirectory;
    }
  } catch (error) {
    console.warn(
      '[hedge-funds] Falling back to raw holdings directory load.',
      error instanceof Error ? error.message : error,
    );
  }

  const supabase = getPublicSupabase();

  // Fetch a recent quarter window so we can estimate holding returns between
  // comparable filings without pulling the full institutional_holdings table.
  const rows: InstitutionalHoldingRow[] = [];
  let offset = 0;

  while (true) {
    const { data, error } = await supabase
      .from('institutional_holdings')
      .select('fund_name,ticker,report_period,published_date,shares_held,value_held')
      .in('report_period', periodsToFetch)
      .gt('shares_held', 0)
      .order('value_held', { ascending: false })
      .range(offset, offset + PAGE_SIZE - 1);

    if (error) throw new Error(error.message);
    const batch = (data || []) as InstitutionalHoldingRow[];
    if (!batch.length) break;
    rows.push(...batch);
    if (batch.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
  }

  return buildFundDirectory(rows);
}
