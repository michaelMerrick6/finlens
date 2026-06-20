'use client';

import Image, { type ImageLoaderProps } from 'next/image';
import { useEffect, useRef, useState } from 'react';
import {
  ArrowDownRight,
  ArrowUpRight,
  ExternalLink,
  Loader2,
  Search,
} from 'lucide-react';

import { getTickerLogoUrl } from '@/lib/company-logos';
import { formatCalendarDate } from '@/lib/date-format';

interface InsiderTrade {
  id: string;
  ticker?: string | null;
  filer_name?: string | null;
  filer_relation?: string | null;
  transaction_date?: string | null;
  published_date?: string | null;
  transaction_code?: string | null;
  amount?: number | null;
  price?: number | null;
  value?: number | null;
  source_url?: string | null;
}

const DIRECTION_OPTIONS = [
  { value: 'All', label: 'All', tone: 'default' as const },
  { value: 'buy', label: 'Buy', tone: 'buy' as const },
  { value: 'sell', label: 'Sell', tone: 'sell' as const },
];
const MIN_LIVE_FALLBACK_ROWS = 25;
const passthroughImageLoader = ({ src }: ImageLoaderProps) => src;

function FilterPill({
  active,
  onClick,
  children,
  tone = 'default',
}: {
  active: boolean;
  onClick: () => void;
  children: string;
  tone?: 'default' | 'buy' | 'sell';
}) {
  const activeClass =
    tone === 'buy'
      ? 'border-emerald-500/25 bg-emerald-500/10 text-emerald-300'
      : tone === 'sell'
        ? 'border-red-500/25 bg-red-500/10 text-red-300'
        : 'border-white/12 bg-white/[0.06] text-white';

  return (
    <button
      type="button"
      onClick={onClick}
      className={`rounded-md border px-2.5 py-1.5 text-[11px] font-medium transition ${
        active ? activeClass : 'border-transparent text-zinc-500 hover:bg-white/[0.03] hover:text-zinc-300'
      }`}
    >
      {children}
    </button>
  );
}

function toFiniteNumber(value: number | null | undefined): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function formatCompactCurrency(value: number | null | undefined): string {
  const amount = toFiniteNumber(value);
  if (amount === null || amount === 0) {
    return '—';
  }

  const abs = Math.abs(amount);
  const prefix = amount < 0 ? '-$' : '$';

  if (abs >= 1_000_000) return `${prefix}${(abs / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `${prefix}${(abs / 1_000).toFixed(0)}K`;
  return `${prefix}${abs.toLocaleString()}`;
}

function formatShares(value: number | null | undefined): string {
  const amount = toFiniteNumber(value);
  if (amount === null) {
    return '—';
  }
  if (amount >= 1_000_000) return `${(amount / 1_000_000).toFixed(1)}M`;
  if (amount >= 1_000) return `${(amount / 1_000).toFixed(1)}K`;
  return amount.toLocaleString();
}

function formatPrice(value: number | null | undefined): string {
  const amount = toFiniteNumber(value);
  return amount !== null && amount > 0 ? `$${amount.toFixed(2)}` : '—';
}

function normalizeDirection(value: string | null | undefined): 'buy' | 'sell' | 'other' {
  const normalized = String(value || '').trim().toLowerCase();
  if (normalized === 'buy' || normalized === 'p' || normalized === 'a') return 'buy';
  if (normalized === 'sell' || normalized === 's' || normalized === 'd') return 'sell';
  return 'other';
}

function displayTicker(value: string | null | undefined): string {
  const ticker = String(value || '').trim().toUpperCase();
  return ticker || 'N/A';
}

function displayFilerName(value: string | null | undefined): string {
  const name = String(value || '').trim();
  return name || 'Unknown insider';
}

function displayRelation(value: string | null | undefined): string {
  const relation = String(value || '').trim();
  return relation || 'Insider';
}

function displayTradeLabel(direction: 'buy' | 'sell' | 'other'): string {
  if (direction === 'buy') return 'Buy';
  if (direction === 'sell') return 'Sell';
  return 'Filed';
}

function TickerLogo({ ticker }: { ticker: string | null | undefined }) {
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  const label = displayTicker(ticker);
  const normalizedTicker = ['N/A', 'NA', 'UNKNOWN'].includes(label) ? '' : label;
  const resolvedLogoUrl = normalizedTicker ? getTickerLogoUrl(normalizedTicker, 40) : null;
  const logoUrl = resolvedLogoUrl && failedUrl !== resolvedLogoUrl ? resolvedLogoUrl : null;

  if (logoUrl) {
    return (
      <span className="flex h-7 w-7 shrink-0 items-center justify-center overflow-hidden rounded-lg border border-white/[0.08] bg-black/35">
        <Image
          loader={passthroughImageLoader}
          unoptimized
          src={logoUrl}
          alt=""
          aria-hidden="true"
          width={28}
          height={28}
          sizes="28px"
          className="h-full w-full object-contain p-0.5"
          onError={() => setFailedUrl(logoUrl)}
        />
      </span>
    );
  }

  return (
    <span className="flex h-7 w-7 shrink-0 items-center justify-center rounded-lg border border-white/[0.08] bg-white/[0.04] text-[9px] font-semibold tracking-tight text-zinc-400">
      {normalizedTicker.slice(0, 2) || '—'}
    </span>
  );
}

export default function InsidersFeed({ initialTrades }: { initialTrades: InsiderTrade[] }) {
  const [baseTrades, setBaseTrades] = useState<InsiderTrade[]>(initialTrades);
  const [trades, setTrades] = useState<InsiderTrade[]>(initialTrades);
  const [searchQuery, setSearchQuery] = useState('');
  const [directionFilter, setDirectionFilter] = useState<(typeof DIRECTION_OPTIONS)[number]['value']>('All');
  const [isSearching, setIsSearching] = useState(false);
  const debounceRef = useRef<NodeJS.Timeout | null>(null);
  const attemptedLiveInitialLoadRef = useRef(false);
  const hasFilters = Boolean(searchQuery.trim() || directionFilter !== 'All');
  const displayedTrades = hasFilters ? trades : baseTrades;

  useEffect(() => {
    if (initialTrades.length > 0) {
      setBaseTrades(initialTrades);
      setTrades(initialTrades);
    }
  }, [initialTrades]);

  useEffect(() => {
    if (hasFilters || baseTrades.length >= MIN_LIVE_FALLBACK_ROWS || attemptedLiveInitialLoadRef.current) {
      return;
    }

    let cancelled = false;
    attemptedLiveInitialLoadRef.current = true;
    setIsSearching(true);

    (async () => {
      try {
        const res = await fetch('/api/search-insider-trades', { cache: 'no-store' });
        const json = await res.json();
        if (!cancelled) {
          const liveTrades = (json.trades || []) as InsiderTrade[];
          setBaseTrades(liveTrades);
          setTrades(liveTrades);
        }
      } catch (error) {
        console.error('Initial insider feed load failed:', error);
      } finally {
        if (!cancelled) {
          setIsSearching(false);
        }
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [baseTrades.length, hasFilters]);

  useEffect(() => {
    if (!hasFilters) {
      return;
    }

    if (debounceRef.current) clearTimeout(debounceRef.current);

    debounceRef.current = setTimeout(async () => {
      setIsSearching(true);
      try {
        const params = new URLSearchParams();
        if (searchQuery.trim()) params.set('q', searchQuery.trim());
        if (directionFilter !== 'All') params.set('direction', directionFilter);

        const res = await fetch(`/api/search-insider-trades?${params.toString()}`);
        const json = await res.json();
        setTrades(json.trades || []);
      } catch (error) {
        console.error('Search failed:', error);
      } finally {
        setIsSearching(false);
      }
    }, 400);

    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
    };
  }, [hasFilters, searchQuery, directionFilter]);

  function clearFilters() {
    setSearchQuery('');
    setDirectionFilter('All');
  }

  return (
    <div className="space-y-3">
      <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] px-3.5 py-3">
        <div className="flex flex-col gap-3 xl:flex-row xl:items-center xl:justify-between">
          <div className="w-full max-w-lg">
            <label className="mb-1.5 block text-[10px] font-semibold uppercase tracking-[0.16em] text-zinc-600">
              Search
            </label>
            <div className="relative">
              <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-zinc-600" />
              {isSearching ? (
                <Loader2 className="absolute right-3 top-1/2 h-4 w-4 -translate-y-1/2 animate-spin text-amber-400" />
              ) : null}
              <input
                type="text"
                placeholder="Search insider, ticker, or company..."
                value={searchQuery}
                onChange={(event) => setSearchQuery(event.target.value)}
                className="h-10 w-full rounded-lg border border-white/[0.08] bg-white/[0.03] pl-10 pr-10 text-sm text-white outline-none transition placeholder:text-zinc-600 focus:border-amber-500/30 focus:bg-white/[0.05]"
              />
            </div>
          </div>

          <div className="flex flex-col gap-2.5 sm:flex-row sm:flex-wrap sm:items-end xl:justify-end">
            <div className="min-w-0">
              <div className="mb-1.5 text-[10px] font-semibold uppercase tracking-[0.16em] text-zinc-600">
                Direction
              </div>
              <div className="inline-flex rounded-lg border border-white/[0.06] bg-white/[0.02] p-0.5">
                {DIRECTION_OPTIONS.map((option) => (
                  <FilterPill
                    key={option.value}
                    active={directionFilter === option.value}
                    onClick={() => setDirectionFilter(option.value)}
                    tone={option.tone}
                  >
                    {option.label}
                  </FilterPill>
                ))}
              </div>
            </div>

            {hasFilters ? (
              <button
                type="button"
                onClick={clearFilters}
                className="h-9 rounded-lg px-3 text-xs font-medium text-zinc-500 transition hover:bg-white/[0.03] hover:text-zinc-300"
              >
                Clear filters
              </button>
            ) : null}
          </div>
        </div>

        <div className="mt-2.5 text-xs text-zinc-500">
          {isSearching ? (
            <span className="inline-flex items-center gap-2">
              <Loader2 className="h-3.5 w-3.5 animate-spin text-amber-400" />
              Searching insider filings...
            </span>
          ) : (
            <>
              {displayedTrades.length.toLocaleString()} filings
              {searchQuery.trim() ? (
                <>
                  {' '}
                  matching <span className="text-amber-300">{searchQuery.trim()}</span>
                </>
              ) : null}
              {directionFilter !== 'All' ? (
                <>
                  {' '}
                  filtered to{' '}
                  <span className={directionFilter === 'buy' ? 'text-emerald-300' : 'text-red-300'}>
                    {directionFilter === 'buy' ? 'buys' : 'sells'}
                  </span>
                </>
              ) : null}
            </>
          )}
        </div>
      </div>

      <div className="overflow-hidden rounded-2xl border border-white/[0.06] bg-white/[0.02]">
        <div className="overflow-x-auto">
          <table className="min-w-[1080px] w-full border-collapse text-left">
            <thead>
              <tr className="border-b border-white/[0.08] bg-white/[0.03]">
                <th className="px-4 py-3 text-[11px] font-semibold uppercase tracking-[0.16em] text-zinc-600">
                  Insider
                </th>
                <th className="px-4 py-3 text-[11px] font-semibold uppercase tracking-[0.16em] text-zinc-600">
                  Ticker
                </th>
                <th className="px-4 py-3 text-[11px] font-semibold uppercase tracking-[0.16em] text-zinc-600">
                  Type
                </th>
                <th className="px-4 py-3 text-[11px] font-semibold uppercase tracking-[0.16em] text-zinc-600">
                  Shares
                </th>
                <th className="px-4 py-3 text-[11px] font-semibold uppercase tracking-[0.16em] text-zinc-600">
                  Price
                </th>
                <th className="px-4 py-3 text-[11px] font-semibold uppercase tracking-[0.16em] text-zinc-600">
                  Value
                </th>
                <th className="px-4 py-3 text-[11px] font-semibold uppercase tracking-[0.16em] text-zinc-600">
                  Tx Date
                </th>
                <th className="px-4 py-3 text-right text-[11px] font-semibold uppercase tracking-[0.16em] text-zinc-600">
                  Filing Link
                </th>
              </tr>
            </thead>
            <tbody>
              {displayedTrades.map((trade) => {
                const direction = normalizeDirection(trade.transaction_code);
                const typeToneClass =
                  direction === 'buy'
                    ? 'border-emerald-500/25 bg-emerald-500/10 text-emerald-300'
                    : direction === 'sell'
                      ? 'border-red-500/25 bg-red-500/10 text-red-300'
                      : 'border-white/10 bg-white/[0.05] text-zinc-300';
                const valueToneClass =
                  direction === 'buy'
                    ? 'text-emerald-300'
                    : direction === 'sell'
                      ? 'text-red-300'
                      : 'text-zinc-300';
                const typeIcon =
                  direction === 'buy' ? (
                    <ArrowUpRight className="h-3.5 w-3.5" />
                  ) : direction === 'sell' ? (
                    <ArrowDownRight className="h-3.5 w-3.5" />
                  ) : null;

                return (
                  <tr
                    key={trade.id}
                    className="border-b border-white/[0.06] align-top transition hover:bg-white/[0.03] last:border-0"
                  >
                    <td className="px-4 py-4">
                      <div className="text-sm font-medium text-white">
                        {displayFilerName(trade.filer_name)}
                      </div>
                      <div className="mt-1 text-xs text-zinc-500">
                        {displayRelation(trade.filer_relation)}
                      </div>
                    </td>
                    <td className="px-4 py-4">
                      <span className="inline-flex items-center gap-1.5 rounded-lg border border-white/[0.08] bg-white/[0.03] px-2 py-1 text-[13px] font-semibold text-white">
                        <TickerLogo ticker={trade.ticker} />
                        {displayTicker(trade.ticker)}
                      </span>
                    </td>
                    <td className="px-4 py-4">
                      <span className={`inline-flex items-center gap-1.5 rounded-lg border px-3 py-1.5 text-[12px] font-medium ${typeToneClass}`}>
                        {typeIcon}
                        {displayTradeLabel(direction)}
                      </span>
                    </td>
                    <td className="px-4 py-4 text-sm font-medium text-zinc-300 whitespace-nowrap">
                      {formatShares(trade.amount)}
                    </td>
                    <td className="px-4 py-4 text-sm text-zinc-400 whitespace-nowrap">
                      {formatPrice(trade.price)}
                    </td>
                    <td className={`px-4 py-4 text-sm font-semibold whitespace-nowrap ${valueToneClass}`}>
                      {formatCompactCurrency(trade.value)}
                    </td>
                    <td className="px-4 py-4 text-sm font-medium text-white whitespace-nowrap">
                      {formatCalendarDate(trade.transaction_date)}
                    </td>
                    <td className="px-4 py-4 text-right">
                      {trade.source_url ? (
                        <a
                          href={trade.source_url}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="inline-flex items-center gap-1.5 text-sm text-amber-300 transition hover:text-amber-200"
                        >
                          SEC Filing
                          <ExternalLink className="h-3.5 w-3.5" />
                        </a>
                      ) : (
                        <span className="text-sm text-zinc-600">N/A</span>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>

          {displayedTrades.length === 0 && !isSearching ? (
            <div className="px-4 py-12 text-center text-sm text-zinc-500">
              No insider trades match your search. Try a different insider or ticker.
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}
