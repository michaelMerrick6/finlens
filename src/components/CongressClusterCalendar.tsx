'use client';

import Image, { type ImageLoaderProps } from 'next/image';
import Link from 'next/link';
import { useState } from 'react';
import { ArrowUpRight, Landmark } from 'lucide-react';

import { getTickerLogoUrl } from '@/lib/company-logos';
import type {
  CongressClusterCalendarData,
  CongressClusterRange,
} from '@/lib/congress-cluster-calendar-types';
import { formatCalendarDate } from '@/lib/date-format';
import { formatCompactCurrency } from '@/lib/hedge-funds';

const passthroughImageLoader = ({ src }: ImageLoaderProps) => src;

const RANGE_OPTIONS: Array<{ value: CongressClusterRange; label: string }> = [
  { value: 'week', label: '7 days' },
  { value: 'month', label: '30 days' },
  { value: 'year', label: '1 year' },
];

const RANGE_PHRASES: Record<CongressClusterRange, string> = {
  week: 'the past 7 days',
  month: 'the past 30 days',
  year: 'the past year',
};

function TickerLogo({ ticker, prominent = false }: { ticker: string; prominent?: boolean }) {
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  const size = prominent ? 52 : 42;
  const logoUrl = getTickerLogoUrl(ticker, size);
  const activeLogoUrl = logoUrl && failedUrl !== logoUrl ? logoUrl : null;
  const hue = ticker.split('').reduce((sum, character) => sum + character.charCodeAt(0), 0) % 360;
  const sizeClass = prominent ? 'h-[52px] w-[52px] rounded-2xl' : 'h-[42px] w-[42px] rounded-xl';

  if (activeLogoUrl) {
    return (
      <span className={`flex shrink-0 items-center justify-center overflow-hidden border border-white/[0.08] bg-black/30 ${sizeClass}`}>
        <Image
          loader={passthroughImageLoader}
          unoptimized
          src={activeLogoUrl}
          alt=""
          width={size}
          height={size}
          sizes={`${size}px`}
          className="h-full w-full object-contain p-1"
          onError={() => setFailedUrl(activeLogoUrl)}
        />
      </span>
    );
  }

  return (
    <span
      aria-hidden="true"
      className={`flex shrink-0 items-center justify-center border border-white/[0.08] text-xs font-bold text-white ${sizeClass}`}
      style={{
        background: `linear-gradient(135deg, hsl(${hue},55%,35%), hsl(${(hue + 38) % 360},55%,25%))`,
      }}
    >
      {ticker.slice(0, 2)}
    </span>
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
  const leader = data.topClusters[0] || null;
  const otherLeaders = data.topClusters.slice(1);
  const maximumActors = Math.max(...data.topClusters.map((cluster) => cluster.actorCount), 1);

  return (
    <div className="mx-auto max-w-[980px] space-y-5">
      <header className="flex flex-col gap-4 border-b border-white/[0.06] pb-5 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <div className="flex items-center gap-2 text-[10px] font-semibold uppercase tracking-[0.18em] text-emerald-300/80">
            <Landmark className="h-3.5 w-3.5" />
            Congress
          </div>
          <h1 className="mt-2 text-2xl font-semibold tracking-tight text-white sm:text-3xl">
            Where Congress is accumulating
          </h1>
          <p className="mt-1.5 text-sm text-zinc-500">
            Companies bought by the most distinct lawmakers.
          </p>
        </div>

        <div
          role="group"
          aria-label="Accumulation time period"
          className="inline-flex w-fit rounded-xl border border-white/[0.08] bg-white/[0.02] p-1"
        >
          {RANGE_OPTIONS.map((option) => (
            <button
              key={option.value}
              type="button"
              aria-pressed={range === option.value}
              onClick={() => onRangeChange(option.value)}
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
        {leader ? (
          <section className="rounded-2xl border border-emerald-400/[0.14] bg-[linear-gradient(135deg,rgba(16,185,129,0.08),rgba(255,255,255,0.015)_55%)] p-4 sm:p-5">
            <div className="flex flex-col gap-4 sm:flex-row sm:items-center">
              <TickerLogo ticker={leader.ticker} prominent />
              <div className="min-w-0 flex-1">
                <div className="text-[10px] font-semibold uppercase tracking-[0.15em] text-emerald-300/75">
                  Strongest accumulation
                </div>
                <div className="mt-1 flex flex-wrap items-baseline gap-x-2">
                  <Link
                    href={`/ticker/${encodeURIComponent(leader.ticker)}`}
                    className="text-xl font-bold tracking-[0.06em] text-white transition hover:text-emerald-200"
                  >
                    {leader.ticker}
                  </Link>
                  <span className="truncate text-sm text-zinc-500">
                    {leader.companyName || `${leader.ticker} purchases`}
                  </span>
                </div>
                <p className="mt-2 text-sm leading-6 text-zinc-300">
                  <span className="font-semibold text-white">
                    {leader.actorCount} of {data.totals.actorCount} active lawmakers
                  </span>{' '}
                  bought this company in {RANGE_PHRASES[data.range]}.
                </p>
              </div>
              <div className="shrink-0 sm:text-right">
                <div className="text-3xl font-semibold tracking-tight text-emerald-200">
                  {leader.lawmakersSharePct}%
                </div>
                <div className="mt-0.5 text-[10px] uppercase tracking-[0.12em] text-zinc-600">
                  of active buyers
                </div>
              </div>
            </div>

            <div className="mt-4 h-1.5 overflow-hidden rounded-full bg-white/[0.05]">
              <div
                className="h-full rounded-full bg-gradient-to-r from-emerald-500 to-emerald-300"
                style={{ width: `${Math.max(5, leader.lawmakersSharePct)}%` }}
              />
            </div>
            <div className="mt-3 flex flex-wrap gap-x-4 gap-y-1 text-[11px] text-zinc-600">
              <span>{leader.tradeCount} disclosed buys</span>
              <span>{formatCompactCurrency(leader.amountFloor)}+ tracked minimum</span>
              <span>Latest {formatCalendarDate(leader.latestDisclosureDate, 'UTC')}</span>
            </div>
          </section>
        ) : (
          <section className="rounded-2xl border border-white/[0.07] bg-white/[0.015] px-5 py-14 text-center">
            <div className="text-sm font-medium text-zinc-300">No coordinated buying in this period.</div>
            <div className="mt-1 text-xs text-zinc-600">Try a longer time range.</div>
          </section>
        )}

        {otherLeaders.length ? (
          <section className="overflow-hidden rounded-2xl border border-white/[0.07] bg-white/[0.012]">
            <div className="flex flex-col gap-1 border-b border-white/[0.055] px-4 py-3.5 sm:flex-row sm:items-center sm:justify-between sm:px-5">
              <h2 className="text-sm font-semibold text-white">Other leading destinations</h2>
              <div className="text-[11px] text-zinc-600">
                {data.totals.clusterCount} companies bought by 2+ lawmakers
              </div>
            </div>

            <div className="divide-y divide-white/[0.045]">
              {otherLeaders.map((cluster, index) => (
                <Link
                  key={cluster.ticker}
                  href={`/ticker/${encodeURIComponent(cluster.ticker)}`}
                  className="group flex items-center gap-3 px-4 py-3.5 transition hover:bg-white/[0.025] sm:grid sm:grid-cols-[26px_42px_minmax(150px,0.85fr)_minmax(180px,1fr)_130px_16px] sm:px-5"
                >
                  <span className="hidden text-center text-xs font-semibold tabular-nums text-zinc-700 sm:block">
                    {index + 2}
                  </span>
                  <TickerLogo ticker={cluster.ticker} />
                  <div className="min-w-0">
                    <div className="flex items-center gap-2">
                      <span className="text-sm font-bold tracking-[0.07em] text-cyan-100">{cluster.ticker}</span>
                      {cluster.conviction === 'high' ? (
                        <span className="rounded border border-emerald-400/15 bg-emerald-400/[0.06] px-1.5 py-0.5 text-[8px] font-semibold uppercase tracking-[0.08em] text-emerald-300/80">
                          High conviction
                        </span>
                      ) : null}
                    </div>
                    <div className="mt-0.5 truncate text-xs text-zinc-600">
                      {cluster.companyName || `${cluster.ticker} purchases`}
                    </div>
                  </div>
                  <div className="hidden sm:block">
                    <div className="h-1.5 overflow-hidden rounded-full bg-white/[0.045]">
                      <div
                        className="h-full rounded-full bg-gradient-to-r from-cyan-500/45 to-emerald-300/75"
                        style={{ width: `${Math.max(12, (cluster.actorCount / maximumActors) * 100)}%` }}
                      />
                    </div>
                    <div className="mt-1.5 text-[10px] text-zinc-700">
                      {formatCompactCurrency(cluster.amountFloor)}+ minimum
                    </div>
                  </div>
                  <div className="ml-auto shrink-0 text-right sm:ml-0">
                    <div className="text-sm font-semibold text-white">
                      {cluster.actorCount} lawmakers
                    </div>
                    <div className="mt-0.5 text-[10px] text-zinc-600">
                      {cluster.lawmakersSharePct}% of active buyers
                    </div>
                  </div>
                  <ArrowUpRight className="hidden h-3.5 w-3.5 text-zinc-700 transition group-hover:text-zinc-400 sm:block" />
                </Link>
              ))}
            </div>
          </section>
        ) : null}

        <div className="flex flex-col gap-1 px-1 text-[11px] leading-5 text-zinc-700 sm:flex-row sm:items-center sm:justify-between">
          <span>
            {data.totals.actorCount} lawmakers · {data.totals.tradeCount.toLocaleString()} disclosed buys ·{' '}
            {formatCompactCurrency(data.totals.amountFloor)}+ tracked minimum
          </span>
          <span>
            Updated through {data.latestDisclosureDate ? formatCalendarDate(data.latestDisclosureDate, 'UTC') : 'the latest filing'}
          </span>
        </div>
        <p className="px-1 text-[11px] leading-5 text-zinc-700">
          Ranked by distinct lawmakers. Repeated filings from the same politician count once per company.
        </p>
      </div>
    </div>
  );
}
