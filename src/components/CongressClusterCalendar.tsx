'use client';

import Image, { type ImageLoaderProps } from 'next/image';
import Link from 'next/link';
import { useState } from 'react';
import { ArrowUpRight, Landmark } from 'lucide-react';

import { getTickerLogoUrl } from '@/lib/company-logos';
import type {
  CongressClusterCalendarData,
  CongressClusterPlay,
  CongressClusterRange,
} from '@/lib/congress-cluster-calendar-types';
import { formatCalendarDate } from '@/lib/date-format';
import { formatCompactCurrency } from '@/lib/hedge-funds';

const ROWS_PER_PAGE = 10;
const passthroughImageLoader = ({ src }: ImageLoaderProps) => src;

const RANGE_OPTIONS: Array<{ value: CongressClusterRange; label: string }> = [
  { value: 'week', label: '7 days' },
  { value: 'month', label: '30 days' },
  { value: 'ytd', label: 'YTD' },
];

const RANGE_PHRASES: Record<CongressClusterRange, string> = {
  week: 'in the last 7 days',
  month: 'in the last 30 days',
  ytd: 'year to date',
};

function TickerLogo({ ticker }: { ticker: string }) {
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  const logoUrl = getTickerLogoUrl(ticker, 42);
  const activeLogoUrl = logoUrl && failedUrl !== logoUrl ? logoUrl : null;
  const hue = ticker.split('').reduce((sum, character) => sum + character.charCodeAt(0), 0) % 360;

  if (activeLogoUrl) {
    return (
      <span className="flex h-[42px] w-[42px] shrink-0 items-center justify-center overflow-hidden rounded-xl border border-white/[0.12] bg-zinc-100 shadow-sm shadow-black/30">
        <Image
          loader={passthroughImageLoader}
          unoptimized
          src={activeLogoUrl}
          alt=""
          width={42}
          height={42}
          sizes="42px"
          className="h-full w-full object-contain p-1.5"
          onError={() => setFailedUrl(activeLogoUrl)}
        />
      </span>
    );
  }

  return (
    <span
      aria-hidden="true"
      className="flex h-[42px] w-[42px] shrink-0 items-center justify-center rounded-xl border border-white/[0.08] text-xs font-bold text-white"
      style={{
        background: `linear-gradient(135deg, hsl(${hue},55%,35%), hsl(${(hue + 38) % 360},55%,25%))`,
      }}
    >
      {ticker.slice(0, 2)}
    </span>
  );
}

function minimumLabel(value: number) {
  const compact = formatCompactCurrency(value);
  return compact ? `${compact}+` : 'Not reported';
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0 px-3 py-3 sm:px-4">
      <div className="truncate text-lg font-semibold tabular-nums text-white sm:text-xl">{value}</div>
      <div className="mt-0.5 text-[9px] font-semibold uppercase tracking-[0.12em] text-zinc-600 sm:text-[10px]">
        {label}
      </div>
    </div>
  );
}

function Fact({ label, value, emphasis = false }: { label: string; value: string; emphasis?: boolean }) {
  return (
    <div className="min-w-0">
      <div className={`truncate text-xs font-semibold tabular-nums ${emphasis ? 'text-emerald-200' : 'text-zinc-200'}`}>
        {value}
      </div>
      <div className="mt-0.5 text-[9px] uppercase tracking-[0.1em] text-zinc-600">{label}</div>
    </div>
  );
}

function RankedCompany({ cluster, rank }: { cluster: CongressClusterPlay; rank: number }) {
  return (
    <Link
      href={`/ticker/${encodeURIComponent(cluster.ticker)}`}
      className={`group grid grid-cols-[24px_42px_minmax(0,1fr)_auto] items-center gap-x-3 px-4 py-4 transition hover:bg-white/[0.025] sm:grid-cols-[28px_42px_minmax(170px,1fr)_92px_80px_112px_112px_16px] sm:gap-x-4 sm:px-5 ${
        rank === 1 ? 'bg-emerald-400/[0.025]' : ''
      }`}
    >
      <span className={`text-center text-xs font-semibold tabular-nums ${rank === 1 ? 'text-emerald-300' : 'text-zinc-700'}`}>
        {rank}
      </span>
      <TickerLogo ticker={cluster.ticker} />
      <div className="min-w-0">
        <div className="text-sm font-bold tracking-[0.07em] text-cyan-100">{cluster.ticker}</div>
        <div className="mt-0.5 truncate text-xs text-zinc-600">
          {cluster.companyName || `${cluster.ticker} purchases`}
        </div>
      </div>

      <div className="col-start-2 col-end-5 mt-3 grid grid-cols-2 gap-x-4 gap-y-3 border-t border-white/[0.05] pt-3 sm:contents">
        <Fact label="Lawmakers" value={cluster.actorCount.toLocaleString()} emphasis={rank === 1} />
        <Fact label="Disclosed buys" value={cluster.tradeCount.toLocaleString()} />
        <Fact label="Minimum disclosed" value={minimumLabel(cluster.amountFloor)} />
        <Fact
          label="Latest trade"
          value={cluster.latestTransactionDate ? formatCalendarDate(cluster.latestTransactionDate, 'UTC') : 'Not reported'}
        />
      </div>

      <ArrowUpRight className="col-start-4 row-start-1 h-3.5 w-3.5 text-zinc-700 transition group-hover:text-zinc-400 sm:col-start-8" />
    </Link>
  );
}

export default function CongressClusterCalendar({
  data,
  range,
  loading,
  error,
  onRangeChange,
}: {
  data: CongressClusterCalendarData;
  range: CongressClusterRange;
  loading: boolean;
  error: string;
  onRangeChange: (range: CongressClusterRange) => void;
}) {
  const [visibleCount, setVisibleCount] = useState(ROWS_PER_PAGE);
  const visibleClusters = data.topClusters.slice(0, visibleCount);
  const remainingCount = Math.max(data.topClusters.length - visibleClusters.length, 0);

  return (
    <div className="mx-auto max-w-[1040px] space-y-4">
      <header className="flex flex-col gap-4 border-b border-white/[0.06] pb-5 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <div className="flex items-center gap-2 text-[10px] font-semibold uppercase tracking-[0.18em] text-emerald-300/80">
            <Landmark className="h-3.5 w-3.5" />
            Congress
          </div>
          <h1 className="mt-2 text-2xl font-semibold tracking-tight text-white sm:text-3xl">Congress buying</h1>
          <p className="mt-1.5 max-w-xl text-sm leading-6 text-zinc-500">
            Stocks bought by 2+ lawmakers, based on buys newly disclosed {RANGE_PHRASES[range]}.
          </p>
        </div>

        <div
          role="group"
          aria-label="Accumulation time period"
          className="inline-grid w-full grid-cols-3 rounded-xl border border-white/[0.08] bg-white/[0.02] p-1 sm:w-auto"
        >
          {RANGE_OPTIONS.map((option) => (
            <button
              key={option.value}
              type="button"
              aria-pressed={range === option.value}
              onClick={() => {
                setVisibleCount(ROWS_PER_PAGE);
                onRangeChange(option.value);
              }}
              className={`rounded-lg px-3.5 py-2 text-xs font-medium transition ${
                range === option.value
                  ? 'bg-white/[0.1] text-white shadow-sm'
                  : 'text-zinc-600 hover:text-zinc-300'
              }`}
            >
              {option.label}
            </button>
          ))}
        </div>
      </header>

      {error ? (
        <div role="alert" className="rounded-xl border border-red-400/15 bg-red-400/[0.06] px-4 py-3 text-xs text-red-200">
          {error}
        </div>
      ) : null}

      <div
        aria-live="polite"
        aria-busy={loading}
        className={`space-y-4 transition-opacity ${loading ? 'pointer-events-none opacity-55' : 'opacity-100'}`}
      >
        <section className="grid grid-cols-3 divide-x divide-white/[0.055] rounded-2xl border border-white/[0.07] bg-white/[0.014]">
          <Metric label="Active lawmakers" value={data.totals.actorCount.toLocaleString()} />
          <Metric label="Disclosed buys" value={data.totals.tradeCount.toLocaleString()} />
          <Metric label="Minimum disclosed" value={minimumLabel(data.totals.amountFloor)} />
        </section>

        {data.topClusters.length ? (
          <section className="overflow-hidden rounded-2xl border border-white/[0.07] bg-white/[0.012]">
            <div className="flex items-center justify-between gap-4 border-b border-white/[0.055] px-4 py-3.5 sm:px-5">
              <div>
                <h2 className="text-sm font-semibold text-white">Ranked accumulation</h2>
                <p className="mt-0.5 text-[11px] text-zinc-600">Most distinct lawmakers first</p>
              </div>
              <div className="text-xs font-medium tabular-nums text-zinc-500">
                {data.totals.clusterCount.toLocaleString()} companies
              </div>
            </div>

            <div className="hidden grid-cols-[28px_42px_minmax(170px,1fr)_92px_80px_112px_112px_16px] gap-x-4 border-b border-white/[0.045] px-5 py-2.5 text-[9px] font-semibold uppercase tracking-[0.11em] text-zinc-700 sm:grid">
              <span className="text-center">#</span>
              <span />
              <span>Company</span>
              <span>Lawmakers</span>
              <span>Buys</span>
              <span>Minimum</span>
              <span>Latest trade</span>
              <span />
            </div>

            <div className="divide-y divide-white/[0.045]">
              {visibleClusters.map((cluster, index) => (
                <RankedCompany key={cluster.ticker} cluster={cluster} rank={index + 1} />
              ))}
            </div>

            {remainingCount ? (
              <div className="border-t border-white/[0.055] p-3">
                <button
                  type="button"
                  onClick={() => setVisibleCount(data.topClusters.length)}
                  className="w-full rounded-xl border border-white/[0.07] bg-white/[0.018] px-4 py-2.5 text-xs font-medium text-zinc-400 transition hover:border-white/[0.12] hover:bg-white/[0.035] hover:text-white"
                >
                  Show all {data.topClusters.length.toLocaleString()} companies
                  <span className="ml-2 text-zinc-700">{remainingCount} more</span>
                </button>
              </div>
            ) : null}
          </section>
        ) : (
          <section className="rounded-2xl border border-white/[0.07] bg-white/[0.015] px-5 py-14 text-center">
            <div className="text-sm font-medium text-zinc-300">No stocks were bought by 2+ lawmakers in this period.</div>
            <div className="mt-1 text-xs text-zinc-600">Try a longer time range.</div>
          </section>
        )}

        <div className="flex flex-col gap-1 px-1 text-[11px] leading-5 text-zinc-700 sm:flex-row sm:items-center sm:justify-between">
          <span>Repeated copies of the same trade are counted once. Dollar totals sum the minimum of disclosed ranges.</span>
          <span className="shrink-0">
            Data through {data.latestDisclosureDate ? formatCalendarDate(data.latestDisclosureDate, 'UTC') : 'the latest filing'}
          </span>
        </div>
      </div>
    </div>
  );
}
