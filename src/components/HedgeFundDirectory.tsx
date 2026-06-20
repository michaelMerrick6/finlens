'use client';

import Link from 'next/link';
import { useDeferredValue, useMemo, useState } from 'react';
import {
  Building2,
  CalendarClock,
  ChevronLeft,
  ChevronRight,
  Search,
  Wallet,
} from 'lucide-react';

import { formatCalendarDate } from '@/lib/date-format';
import {
  formatCompactCurrency,
  formatFullCurrency,
  fundPath,
  quarterLabelFromReportPeriod,
  type FundDirectoryEntry,
} from '@/lib/hedge-funds';

// ─── Helpers ──────────────────────────────────────────────────────────────────

function matchesQuery(fund: FundDirectoryEntry, query: string) {
  if (!query) return true;
  return fund.fundName.toLowerCase().includes(query);
}

type SortKey = 'value' | 'holdings' | 'name';

function sortFunds(funds: FundDirectoryEntry[], key: SortKey) {
  return [...funds].sort((a, b) => {
    if (key === 'value') return b.currentPortfolioValue - a.currentPortfolioValue;
    if (key === 'holdings') return b.currentHoldingCount - a.currentHoldingCount;
    return a.fundName.localeCompare(b.fundName);
  });
}

// ─── Calendar Helpers ─────────────────────────────────────────────────────────

const MONTH_NAMES = [
  'January', 'February', 'March', 'April', 'May', 'June',
  'July', 'August', 'September', 'October', 'November', 'December',
];
const DAY_LABELS = ['Su', 'Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa'];

type FilingEvent = {
  date: string;        // estimated filing date YYYY-MM-DD
  fundName: string;
  filingQuarter: string; // e.g. "Q2 2026"
};

/** Compute the next estimated filing date for a fund based on its historical
 *  filing pattern.  Uses the offset between the fund's latest quarter-end and
 *  its actual filing date, then applies that offset to subsequent quarters
 *  until a future date is found.  Falls back to the SEC 45-day deadline when
 *  no historical offset can be derived. */
function getNextFutureFilingInfo(
  latestReportPeriod: string | null,
  lastFiledDate: string | null,
): { date: string; quarter: string } | null {
  if (!latestReportPeriod) return null;

  const now = new Date();
  const todayKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`;

  const periodDate = new Date(`${latestReportPeriod}T12:00:00Z`);
  if (Number.isNaN(periodDate.getTime())) return null;

  // Derive the fund's typical filing offset (days after quarter end)
  let filingOffsetDays = 45; // SEC deadline fallback

  if (lastFiledDate) {
    const filedDate = new Date(`${lastFiledDate}T12:00:00Z`);
    if (!Number.isNaN(filedDate.getTime())) {
      // latestReportPeriod IS the quarter end (e.g. 2025-12-31)
      const diffMs = filedDate.getTime() - periodDate.getTime();
      const diffDays = Math.round(diffMs / (1000 * 60 * 60 * 24));
      if (diffDays > 0 && diffDays <= 90) {
        filingOffsetDays = diffDays;
      }
    }
  }

  // Walk forward quarter by quarter until the estimated date is in the future
  let period = periodDate;
  for (let i = 0; i < 8; i++) {
    const nextQuarterEnd = new Date(
      Date.UTC(period.getUTCFullYear(), period.getUTCMonth() + 4, 0, 12),
    );
    const estimatedFiling = new Date(nextQuarterEnd);
    estimatedFiling.setUTCDate(estimatedFiling.getUTCDate() + filingOffsetDays);
    const filingKey = estimatedFiling.toISOString().slice(0, 10);

    if (filingKey >= todayKey) {
      // The quarter this filing reports ON is the quarter ending at nextQuarterEnd
      const quarter = quarterLabelFromReportPeriod(
        nextQuarterEnd.toISOString().slice(0, 10),
      );
      return { date: filingKey, quarter };
    }

    period = nextQuarterEnd;
  }

  return null;
}

function getUpcomingFilings(funds: FundDirectoryEntry[]): FilingEvent[] {
  const events: FilingEvent[] = [];
  for (const fund of funds) {
    const info = getNextFutureFilingInfo(fund.latestReportPeriod, fund.lastFiledDate);
    if (info) {
      events.push({
        date: info.date,
        fundName: fund.fundName,
        filingQuarter: info.quarter,
      });
    }
  }
  return events.sort((a, b) => a.date.localeCompare(b.date));
}

function getDaysInMonth(year: number, month: number) {
  return new Date(year, month + 1, 0).getDate();
}

function getFirstDayOfWeek(year: number, month: number) {
  return new Date(year, month, 1).getDay();
}

function toDateKey(year: number, month: number, day: number) {
  return `${year}-${String(month + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
}

function formatFilingDate(dateStr: string) {
  const d = new Date(`${dateStr}T12:00:00Z`);
  if (Number.isNaN(d.getTime())) return dateStr;
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' });
}

function isToday(year: number, month: number, day: number) {
  const now = new Date();
  return now.getFullYear() === year && now.getMonth() === month && now.getDate() === day;
}

function isPast(dateStr: string) {
  const now = new Date();
  const todayKey = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')}`;
  return dateStr < todayKey;
}

// ─── Stats Strip (2 columns) ─────────────────────────────────────────────────

function StatsStrip({ funds }: { funds: FundDirectoryEntry[] }) {
  const totalValue = funds.reduce((s, f) => s + f.currentPortfolioValue, 0);
  const latestPeriod = funds.length > 0
    ? funds.reduce((best, f) => (f.latestReportPeriod ?? '') > (best ?? '') ? f.latestReportPeriod : best, funds[0].latestReportPeriod)
    : null;

  const stats = [
    { icon: Wallet, label: 'Total AUM', value: formatCompactCurrency(totalValue), detail: 'Current tracked 13F value', color: 'text-blue-400 bg-blue-500/10' },
    { icon: CalendarClock, label: 'Latest Period', value: quarterLabelFromReportPeriod(latestPeriod), detail: 'Latest tracked filing period', color: 'text-amber-400 bg-amber-500/10' },
  ];

  return (
    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
      {stats.map(({ icon: Icon, label, value, detail, color }) => (
        <div key={label} className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-4">
          <div className="flex items-center gap-2">
            <div className={`flex h-7 w-7 items-center justify-center rounded-lg ${color}`}>
              <Icon className="h-3.5 w-3.5" />
            </div>
            <span className="text-[10px] font-semibold uppercase tracking-[0.15em] text-zinc-600">{label}</span>
          </div>
          <div className="mt-2.5 text-xl font-semibold text-white">{value}</div>
          <div className="mt-1 text-[10px] text-zinc-600">{detail}</div>
        </div>
      ))}
    </div>
  );
}

// ─── 13F Filing Calendar ──────────────────────────────────────────────────────

function FilingCalendar({ funds }: { funds: FundDirectoryEntry[] }) {
  const today = new Date();
  const [viewYear, setViewYear] = useState(today.getFullYear());
  const [viewMonth, setViewMonth] = useState(today.getMonth());
  const [selectedDate, setSelectedDate] = useState<string | null>(null);

  const allFilings = useMemo(() => getUpcomingFilings(funds), [funds]);

  // Build a map of filing date → events for quick lookup
  const filingsByDate = useMemo(() => {
    const map = new Map<string, FilingEvent[]>();
    for (const filing of allFilings) {
      const existing = map.get(filing.date) || [];
      existing.push(filing);
      map.set(filing.date, existing);
    }
    return map;
  }, [allFilings]);

  // Unique filing dates in the currently viewed month
  const filingDatesInMonth = useMemo(() => {
    const set = new Set<string>();
    for (const dateKey of filingsByDate.keys()) {
      const [y, m] = dateKey.split('-').map(Number);
      if (y === viewYear && m === viewMonth + 1) {
        set.add(dateKey);
      }
    }
    return set;
  }, [filingsByDate, viewYear, viewMonth]);

  // Upcoming filings list — selected date, or all future filings
  const upcomingFilings = useMemo(() => {
    if (selectedDate) {
      return filingsByDate.get(selectedDate) || [];
    }
    return allFilings;
  }, [allFilings, filingsByDate, selectedDate]);

  const daysInMonth = getDaysInMonth(viewYear, viewMonth);
  const firstDay = getFirstDayOfWeek(viewYear, viewMonth);

  function goToPrevMonth() {
    if (viewMonth === 0) {
      setViewMonth(11);
      setViewYear(viewYear - 1);
    } else {
      setViewMonth(viewMonth - 1);
    }
    setSelectedDate(null);
  }

  function goToNextMonth() {
    if (viewMonth === 11) {
      setViewMonth(0);
      setViewYear(viewYear + 1);
    } else {
      setViewMonth(viewMonth + 1);
    }
    setSelectedDate(null);
  }

  return (
    <div className="mt-3 rounded-2xl border border-white/[0.06] bg-white/[0.02] p-4 sm:p-5">
      {/* Calendar header */}
      <div className="flex items-center gap-2 mb-4">
        <div className="flex h-7 w-7 items-center justify-center rounded-lg bg-violet-500/10 text-violet-400">
          <CalendarClock className="h-3.5 w-3.5" />
        </div>
        <span className="text-[10px] font-semibold uppercase tracking-[0.15em] text-zinc-600">
          Upcoming 13F-HR Filings
        </span>
      </div>

      <div className="flex flex-col gap-4 sm:flex-row sm:gap-5">
        {/* Mini Calendar */}
        <div className="shrink-0 sm:w-[260px]">
          {/* Month navigation */}
          <div className="flex items-center justify-between mb-3">
            <button
              onClick={goToPrevMonth}
              className="flex h-7 w-7 items-center justify-center rounded-lg text-zinc-500 transition hover:bg-white/[0.06] hover:text-zinc-300"
            >
              <ChevronLeft className="h-3.5 w-3.5" />
            </button>
            <span className="text-xs font-semibold text-zinc-300">
              {MONTH_NAMES[viewMonth]} {viewYear}
            </span>
            <button
              onClick={goToNextMonth}
              className="flex h-7 w-7 items-center justify-center rounded-lg text-zinc-500 transition hover:bg-white/[0.06] hover:text-zinc-300"
            >
              <ChevronRight className="h-3.5 w-3.5" />
            </button>
          </div>

          {/* Day headers */}
          <div className="grid grid-cols-7 gap-0.5 mb-1">
            {DAY_LABELS.map((d) => (
              <div key={d} className="text-center text-[9px] font-semibold uppercase tracking-wider text-zinc-600 py-1">
                {d}
              </div>
            ))}
          </div>

          {/* Day cells */}
          <div className="grid grid-cols-7 gap-0.5">
            {/* Empty cells for offset */}
            {Array.from({ length: firstDay }).map((_, i) => (
              <div key={`empty-${i}`} className="h-8" />
            ))}

            {Array.from({ length: daysInMonth }).map((_, i) => {
              const day = i + 1;
              const dateKey = toDateKey(viewYear, viewMonth, day);
              const hasFiling = filingDatesInMonth.has(dateKey);
              const isSelected = selectedDate === dateKey;
              const isTodayCell = isToday(viewYear, viewMonth, day);
              const filingsOnDay = filingsByDate.get(dateKey);
              const filingCount = filingsOnDay?.length || 0;

              return (
                <button
                  key={day}
                  onClick={() => {
                    if (hasFiling) {
                      setSelectedDate(isSelected ? null : dateKey);
                    }
                  }}
                  className={`
                    relative flex h-8 items-center justify-center rounded-lg text-xs font-medium transition-all
                    ${hasFiling ? 'cursor-pointer' : 'cursor-default'}
                    ${isSelected
                      ? 'bg-violet-500/20 text-violet-300 ring-1 ring-violet-500/40'
                      : isTodayCell
                        ? 'bg-white/[0.06] text-white'
                        : hasFiling
                          ? 'text-white hover:bg-white/[0.06]'
                          : 'text-zinc-600'
                    }
                  `}
                >
                  {day}
                  {/* Filing indicator dot(s) */}
                  {hasFiling && (
                    <span className="absolute bottom-1 left-1/2 flex -translate-x-1/2 gap-[2px]">
                      {filingCount <= 3 ? (
                        Array.from({ length: filingCount }).map((_, dotIdx) => (
                          <span
                            key={dotIdx}
                            className={`block h-[3px] w-[3px] rounded-full ${
                              isSelected ? 'bg-violet-400' : 'bg-emerald-400'
                            }`}
                          />
                        ))
                      ) : (
                        <>
                          <span className={`block h-[3px] w-[3px] rounded-full ${isSelected ? 'bg-violet-400' : 'bg-emerald-400'}`} />
                          <span className={`block h-[3px] w-[3px] rounded-full ${isSelected ? 'bg-violet-400' : 'bg-emerald-400'}`} />
                          <span className={`block h-[3px] w-[3px] rounded-full ${isSelected ? 'bg-violet-400' : 'bg-amber-400'}`} />
                        </>
                      )}
                    </span>
                  )}
                </button>
              );
            })}
          </div>
        </div>

        {/* Filing list sidebar */}
        <div className="flex-1 min-w-0">
          <div className="text-[10px] font-semibold uppercase tracking-[0.15em] text-zinc-600 mb-2">
            {selectedDate
              ? `Filings on ${formatFilingDate(selectedDate)}`
              : 'Next upcoming filings'
            }
          </div>

          {upcomingFilings.length === 0 ? (
            <div className="flex items-center justify-center rounded-xl border border-white/[0.04] bg-white/[0.01] py-8 text-xs text-zinc-600">
              {selectedDate ? 'No filings on this date' : 'No upcoming filings found'}
            </div>
          ) : (
            <div className="space-y-1.5 max-h-[240px] overflow-y-auto pr-1 scrollbar-thin">
              {upcomingFilings.map((filing, idx) => {
                const past = isPast(filing.date);
                return (
                  <Link
                    key={`${filing.fundName}-${idx}`}
                    href={fundPath(filing.fundName)}
                    className="group flex items-center gap-3 rounded-xl border border-white/[0.04] bg-white/[0.01] px-3 py-2.5 transition-all hover:border-white/[0.10] hover:bg-white/[0.04]"
                  >
                    {/* Date badge */}
                    <div className={`flex h-9 w-9 shrink-0 flex-col items-center justify-center rounded-lg text-center ${
                      past
                        ? 'bg-zinc-800/50 text-zinc-500'
                        : 'bg-emerald-500/10 text-emerald-400'
                    }`}>
                      <span className="text-[8px] font-bold uppercase leading-none">
                        {new Date(`${filing.date}T12:00:00Z`).toLocaleDateString('en-US', { month: 'short', timeZone: 'UTC' })}
                      </span>
                      <span className="text-sm font-bold leading-tight">
                        {new Date(`${filing.date}T12:00:00Z`).getUTCDate()}
                      </span>
                    </div>

                    {/* Fund info */}
                    <div className="min-w-0 flex-1">
                      <div className="truncate text-xs font-medium text-zinc-200 group-hover:text-emerald-300 transition-colors">
                        {filing.fundName}
                      </div>
                      <div className="text-[10px] text-zinc-600">
                        {filing.filingQuarter} 13F-HR · est. filing
                      </div>
                    </div>

                    <ChevronRight className="h-3.5 w-3.5 shrink-0 text-zinc-700 transition-colors group-hover:text-zinc-400" />
                  </Link>
                );
              })}
            </div>
          )}

          {!selectedDate && upcomingFilings.length > 0 && (
            <div className="mt-2 text-[10px] text-zinc-600 text-center">
              Estimated from each fund&apos;s historical filing pattern
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

// ─── Fund Card ────────────────────────────────────────────────────────────────

function FundCard({ fund, rank }: { fund: FundDirectoryEntry; rank: number }) {
  return (
    <Link
      href={fundPath(fund.fundName)}
      className="group flex items-center gap-4 rounded-2xl border border-white/[0.06] bg-white/[0.02] p-4 transition-all hover:border-white/[0.12] hover:bg-white/[0.04]"
    >
      {/* Rank */}
      <div className="w-8 shrink-0 text-right text-sm font-semibold tabular-nums text-zinc-500">
        {rank}
      </div>

      {/* Info */}
      <div className="min-w-0 flex-1">
        <div className="flex items-center gap-2">
          <span className="truncate text-sm font-medium text-white group-hover:text-emerald-300 transition-colors">
            {fund.fundName}
          </span>
        </div>
        <div className="mt-0.5 flex items-center gap-2 text-[11px] text-zinc-600">
          <span>{fund.currentHoldingCount.toLocaleString()} holdings</span>
          {fund.latestReportPeriod && (
            <>
              <span className="text-white/10">·</span>
              <span>{quarterLabelFromReportPeriod(fund.latestReportPeriod)}</span>
            </>
          )}
        </div>
      </div>

      {/* Portfolio value */}
      <div className="hidden text-right sm:block">
        <div className="text-sm font-semibold text-white">
          {formatFullCurrency(fund.currentPortfolioValue)}
        </div>
        <div className="mt-0.5 text-[10px] text-zinc-600">
          Filed {formatCalendarDate(fund.lastFiledDate)}
        </div>
      </div>

      <ChevronRight className="h-4 w-4 shrink-0 text-zinc-700 transition-colors group-hover:text-zinc-400" />
    </Link>
  );
}

// ─── Main Directory Component ─────────────────────────────────────────────────

export function HedgeFundDirectory({
  funds,
  loadError,
}: {
  funds: FundDirectoryEntry[];
  loadError?: string | null;
}) {
  const [query, setQuery] = useState('');
  const [sortBy, setSortBy] = useState<SortKey>('value');
  const deferredQuery = useDeferredValue(query.trim().toLowerCase());

  const displayFunds = useMemo(() => {
    const filtered = funds.filter((f) => matchesQuery(f, deferredQuery));
    return sortFunds(filtered, sortBy);
  }, [funds, deferredQuery, sortBy]);

  return (
    <div className="mx-auto max-w-4xl px-4 py-6 sm:px-6 lg:px-8">
      {/* Header */}
      <div className="mb-6">
        <div className="inline-flex items-center gap-2 rounded-full border border-emerald-500/20 bg-emerald-500/10 px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.2em] text-emerald-400">
          <Building2 className="h-3 w-3" />
          13F Filings
        </div>
        <h1 className="mt-3 text-2xl font-semibold tracking-tight text-white">Hedge Fund Directory</h1>
        <p className="mt-1 text-sm text-zinc-500">
          Institutional 13F-HR holdings, sorted by portfolio value.
        </p>
      </div>

      {/* Stats */}
      {funds.length > 0 && <StatsStrip funds={funds} />}

      {/* 13F Filing Calendar */}
      {funds.length > 0 && <FilingCalendar funds={funds} />}

      {/* Search + Sort */}
      <div className="mt-6 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <label className="relative block w-full sm:w-72">
          <Search className="pointer-events-none absolute left-3 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-zinc-600" />
          <input
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search funds…"
            className="h-10 w-full rounded-xl border border-white/[0.08] bg-white/[0.03] pl-9 pr-3 text-sm text-white outline-none transition placeholder:text-zinc-600 focus:border-[#10b981]/30 focus:bg-white/[0.05]"
          />
        </label>

        <div className="flex gap-1">
          {([
            { key: 'value' as SortKey, label: 'By Value' },
            { key: 'holdings' as SortKey, label: 'By Holdings' },
            { key: 'name' as SortKey, label: 'A–Z' },
          ]).map(({ key, label }) => (
            <button
              key={key}
              onClick={() => setSortBy(key)}
              className={`rounded-lg px-3 py-1.5 text-xs font-medium transition ${
                sortBy === key
                  ? 'bg-white/10 text-white'
                  : 'text-zinc-500 hover:text-zinc-300'
              }`}
            >
              {label}
            </button>
          ))}
        </div>
      </div>

      <div className="mt-1 text-xs text-zinc-600">
        {displayFunds.length.toLocaleString()} of {funds.length.toLocaleString()} managers
      </div>

      {/* Fund List */}
      <div className="mt-4 space-y-2">
        {displayFunds.map((fund, index) => (
          <FundCard key={fund.fundName} fund={fund} rank={index + 1} />
        ))}

        {loadError && (
          <div className="rounded-2xl border border-red-500/20 bg-red-500/5 px-6 py-12 text-center text-sm text-red-400">
            {loadError}
          </div>
        )}

        {!loadError && displayFunds.length === 0 && (
          <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] px-6 py-16 text-center">
            {funds.length > 0 ? (
              <>
                <Search className="mx-auto h-6 w-6 text-zinc-700" />
                <div className="mt-3 text-sm text-zinc-400">No funds match &quot;{query}&quot;</div>
                <button onClick={() => setQuery('')} className="mt-2 text-xs text-[#10b981] hover:underline">
                  Clear search
                </button>
              </>
            ) : (
              <>
                <Building2 className="mx-auto h-8 w-8 text-zinc-700" />
                <div className="mt-3 text-sm text-zinc-400">No institutional holdings available yet</div>
                <div className="mt-1 text-xs text-zinc-600">13F filings will appear here once ingested.</div>
              </>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
