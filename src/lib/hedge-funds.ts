import { hasFundChange } from '@/lib/fund-holdings';

export type InstitutionalHoldingRow = {
  id?: string;
  fund_name?: string | null;
  ticker?: string | null;
  report_period?: string | null;
  published_date?: string | null;
  shares_held?: number | null;
  value_held?: number | null;
  qoq_change_shares?: number | null;
  qoq_change_percent?: number | null;
  source_url?: string | null;
};

export type FundDirectoryEntry = {
  fundName: string;
  latestReportPeriod: string | null;
  lastFiledDate: string | null;
  nextExpectedFiling: string | null;
  currentPortfolioValue: number;
  currentHoldingCount: number;
  estimatedReturn: number | null;
  estimatedReturnStartPeriod: string | null;
  estimatedReturnEndPeriod: string | null;
  estimatedReturnCoverage: number | null;
};

export type FundQuarterSnapshot = {
  reportPeriod: string;
  quarterLabel: string;
  publishedDate: string | null;
  nextExpectedFiling: string | null;
  totalValue: number;
  holdingCount: number;
  holdings: InstitutionalHoldingRow[];
  changes: InstitutionalHoldingRow[];
};

function normalize13fCurrencyValue(value: number | null | undefined) {
  const amount = Number(value || 0);
  if (!Number.isFinite(amount) || amount <= 0) {
    return 0;
  }

  // 13F values in our holdings pipeline are stored in filing units that
  // render 1,000x too large for the product UI unless normalized here.
  return amount / 1_000;
}

export function fundPath(name: string) {
  return `/hedge-funds/${encodeURIComponent(name)}`;
}

export function formatCompactCurrency(value: number | null | undefined) {
  const amount = normalize13fCurrencyValue(value);
  if (amount <= 0) {
    return '$0';
  }
  if (amount >= 1_000_000_000_000) {
    return `$${(amount / 1_000_000_000_000).toFixed(1)}T`;
  }
  if (amount >= 1_000_000_000) {
    return `$${(amount / 1_000_000_000).toFixed(1)}B`;
  }
  if (amount >= 1_000_000) {
    return `$${(amount / 1_000_000).toFixed(1)}M`;
  }
  return `$${Math.round(amount / 1_000).toLocaleString()}K`;
}

export function formatFullCurrency(value: number | null | undefined) {
  const amount = normalize13fCurrencyValue(value);
  if (amount <= 0) {
    return '$0';
  }

  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 0,
  }).format(amount);
}

export function formatShareCount(value: number | null | undefined) {
  const amount = Number(value || 0);
  if (!Number.isFinite(amount) || amount <= 0) {
    return '0';
  }
  if (amount >= 1_000_000_000) {
    return `${(amount / 1_000_000_000).toFixed(1)}B`;
  }
  if (amount >= 1_000_000) {
    return `${(amount / 1_000_000).toFixed(1)}M`;
  }
  if (amount >= 1_000) {
    return `${(amount / 1_000).toFixed(1)}K`;
  }
  return Math.round(amount).toLocaleString();
}

export function formatSignedPercent(value: number | null | undefined) {
  const amount = Number(value);
  if (!Number.isFinite(amount)) {
    return 'N/A';
  }
  const sign = amount > 0 ? '+' : '';
  return `${sign}${(amount * 100).toFixed(1)}%`;
}

type FundTickerPosition = {
  shares: number;
  value: number;
};

type FundPeriodStats = {
  value: number;
  holdingCount: number;
  lastFiledDate: string | null;
  holdingsByTicker: Map<string, FundTickerPosition>;
};

const MIN_REASONABLE_PERIOD_RETURN = -0.95;
const MAX_REASONABLE_PERIOD_RETURN = 5;

function normalizeTicker(value: string | null | undefined) {
  const ticker = String(value || '').trim().toUpperCase();
  return ticker && /^[A-Z0-9.-]{1,12}$/.test(ticker) ? ticker : null;
}

function calculateEstimatedPeriodReturn(previousStats: FundPeriodStats | null | undefined, latestStats: FundPeriodStats | null | undefined) {
  if (!previousStats || !latestStats || previousStats.value <= 0) {
    return { value: null, coverage: null };
  }

  let comparableValue = 0;
  let weightedReturn = 0;

  for (const [ticker, previousPosition] of previousStats.holdingsByTicker.entries()) {
    const latestPosition = latestStats.holdingsByTicker.get(ticker);
    if (!latestPosition) {
      continue;
    }

    const previousShares = Number(previousPosition.shares || 0);
    const latestShares = Number(latestPosition.shares || 0);
    const previousValue = Number(previousPosition.value || 0);
    const latestValue = Number(latestPosition.value || 0);
    if (previousShares <= 0 || latestShares <= 0 || previousValue <= 0 || latestValue <= 0) {
      continue;
    }

    const previousPrice = previousValue / previousShares;
    const latestPrice = latestValue / latestShares;
    const holdingReturn = latestPrice / previousPrice - 1;
    if (
      !Number.isFinite(holdingReturn) ||
      holdingReturn < MIN_REASONABLE_PERIOD_RETURN ||
      holdingReturn > MAX_REASONABLE_PERIOD_RETURN
    ) {
      continue;
    }

    comparableValue += previousValue;
    weightedReturn += holdingReturn * previousValue;
  }

  if (comparableValue <= 0) {
    return { value: null, coverage: null };
  }

  return {
    value: weightedReturn / comparableValue,
    coverage: comparableValue / previousStats.value,
  };
}

function sortByValueDesc(rows: InstitutionalHoldingRow[]) {
  return [...rows].sort((left, right) => {
    const valueDiff = Number(right.value_held || 0) - Number(left.value_held || 0);
    if (valueDiff !== 0) {
      return valueDiff;
    }
    return String(left.ticker || '').localeCompare(String(right.ticker || ''));
  });
}

export function quarterLabelFromReportPeriod(reportPeriod: string | null | undefined) {
  if (!reportPeriod) {
    return 'Unknown quarter';
  }
  const [yearString, monthString] = reportPeriod.split('-');
  const year = Number(yearString);
  const month = Number(monthString);
  if (!Number.isFinite(year) || !Number.isFinite(month) || month < 1 || month > 12) {
    return reportPeriod;
  }
  const quarter = Math.ceil(month / 3);
  return `Q${quarter} ${year}`;
}

export function estimateNext13fFiling(reportPeriod: string | null | undefined) {
  if (!reportPeriod) {
    return null;
  }
  const date = new Date(`${reportPeriod}T12:00:00Z`);
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  const nextQuarterEnd = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 4, 0, 12));
  nextQuarterEnd.setUTCDate(nextQuarterEnd.getUTCDate() + 45);
  return nextQuarterEnd.toISOString().slice(0, 10);
}

export function buildFundDirectory(rows: InstitutionalHoldingRow[]) {
  const rowsByFund = new Map<string, InstitutionalHoldingRow[]>();

  for (const row of rows) {
    const fundName = String(row.fund_name || '').trim();
    if (!fundName) {
      continue;
    }
    const existing = rowsByFund.get(fundName) || [];
    existing.push(row);
    rowsByFund.set(fundName, existing);
  }

  const entries: FundDirectoryEntry[] = [];
  for (const [fundName, fundRows] of rowsByFund.entries()) {
    const periodStats = new Map<string, FundPeriodStats>();

    for (const row of fundRows) {
      const reportPeriod = String(row.report_period || '').slice(0, 10);
      if (!reportPeriod || Number(row.shares_held || 0) <= 0) {
        continue;
      }

      const existing = periodStats.get(reportPeriod) || {
        value: 0,
        holdingCount: 0,
        lastFiledDate: null,
        holdingsByTicker: new Map<string, FundTickerPosition>(),
      };
      const sharesHeld = Number(row.shares_held || 0);
      const valueHeld = Number(row.value_held || 0);
      existing.value += valueHeld;
      existing.holdingCount += 1;

      const ticker = normalizeTicker(row.ticker);
      if (ticker) {
        const position = existing.holdingsByTicker.get(ticker) || { shares: 0, value: 0 };
        position.shares += sharesHeld;
        position.value += valueHeld;
        existing.holdingsByTicker.set(ticker, position);
      }

      const publishedDate = String(row.published_date || '').slice(0, 10);
      if (publishedDate && (!existing.lastFiledDate || publishedDate > existing.lastFiledDate)) {
        existing.lastFiledDate = publishedDate;
      }
      periodStats.set(reportPeriod, existing);
    }

    const reportPeriods = [...periodStats.keys()].sort();
    const latestReportPeriod = reportPeriods[reportPeriods.length - 1] || null;
    const latestStats = latestReportPeriod ? periodStats.get(latestReportPeriod) : null;
    if (!latestReportPeriod || !latestStats) {
      continue;
    }

    const previousReportPeriod = [...reportPeriods].reverse().find((period) => period < latestReportPeriod) || null;
    const previousStats = previousReportPeriod ? periodStats.get(previousReportPeriod) : null;
    const estimatedReturn = calculateEstimatedPeriodReturn(previousStats, latestStats);

    entries.push({
      fundName,
      latestReportPeriod,
      lastFiledDate: latestStats.lastFiledDate,
      nextExpectedFiling: estimateNext13fFiling(latestReportPeriod),
      currentPortfolioValue: latestStats.value,
      currentHoldingCount: latestStats.holdingCount,
      estimatedReturn: estimatedReturn.value,
      estimatedReturnStartPeriod: estimatedReturn.value === null ? null : previousReportPeriod,
      estimatedReturnEndPeriod: estimatedReturn.value === null ? null : latestReportPeriod,
      estimatedReturnCoverage: estimatedReturn.coverage,
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

export function buildFundQuarterSnapshots(rows: InstitutionalHoldingRow[]) {
  const rowsByPeriod = new Map<string, InstitutionalHoldingRow[]>();

  for (const row of rows) {
    const reportPeriod = String(row.report_period || '').slice(0, 10);
    if (!reportPeriod) {
      continue;
    }
    const existing = rowsByPeriod.get(reportPeriod) || [];
    existing.push(row);
    rowsByPeriod.set(reportPeriod, existing);
  }

  return [...rowsByPeriod.entries()]
    .sort(([left], [right]) => right.localeCompare(left))
    .map(([reportPeriod, periodRows]) => {
      const holdings = sortByValueDesc(periodRows.filter((row) => Number(row.shares_held || 0) > 0));
      const changes = [...periodRows]
        .filter(hasFundChange)
        .sort((left, right) => {
          const rightWeight = Math.max(Math.abs(Number(right.value_held || 0)), Math.abs(Number(right.qoq_change_shares || 0)));
          const leftWeight = Math.max(Math.abs(Number(left.value_held || 0)), Math.abs(Number(left.qoq_change_shares || 0)));
          if (rightWeight !== leftWeight) {
            return rightWeight - leftWeight;
          }
          return String(left.ticker || '').localeCompare(String(right.ticker || ''));
        });

      return {
        reportPeriod,
        quarterLabel: quarterLabelFromReportPeriod(reportPeriod),
        publishedDate: periodRows
          .map((row) => String(row.published_date || '').slice(0, 10))
          .filter(Boolean)
          .sort()
          .reverse()[0] || null,
        nextExpectedFiling: estimateNext13fFiling(reportPeriod),
        totalValue: holdings.reduce((sum, row) => sum + Number(row.value_held || 0), 0),
        holdingCount: holdings.length,
        holdings,
        changes,
      } satisfies FundQuarterSnapshot;
    });
}
