'use client';

import Image, { type ImageLoaderProps } from 'next/image';
import Link from 'next/link';
import { useMemo, useState } from 'react';
import {
  ArrowUpRight,
  CalendarDays,
  Clock3,
  Flame,
  Landmark,
  Users,
} from 'lucide-react';

import { getTickerLogoUrl } from '@/lib/company-logos';
import type {
  CongressClusterCalendarData,
  CongressClusterDay,
  CongressClusterRange,
} from '@/lib/congress-cluster-calendar-types';
import { formatCalendarDate } from '@/lib/date-format';
import { formatCompactCurrency } from '@/lib/hedge-funds';

const passthroughImageLoader = ({ src }: ImageLoaderProps) => src;

const RANGE_OPTIONS: Array<{ value: CongressClusterRange; label: string; shortLabel: string }> = [
  { value: 'week', label: 'Last week', shortLabel: '7D' },
  { value: 'month', label: 'Last month', shortLabel: '30D' },
  { value: 'year', label: 'Last year', shortLabel: '1Y' },
];

function TickerLogo({ ticker }: { ticker: string }) {
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  const logoUrl = getTickerLogoUrl(ticker, 48);
  const activeLogoUrl = logoUrl && failedUrl !== logoUrl ? logoUrl : null;
  const hue = ticker.split('').reduce((sum, character) => sum + character.charCodeAt(0), 0) % 360;

  if (activeLogoUrl) {
    return (
      <span className="flex h-10 w-10 shrink-0 items-center justify-center overflow-hidden rounded-xl border border-white/[0.08] bg-black/30">
        <Image
          loader={passthroughImageLoader}
          unoptimized
          src={activeLogoUrl}
          alt=""
          width={40}
          height={40}
          sizes="40px"
          className="h-full w-full object-contain p-1"
          onError={() => setFailedUrl(activeLogoUrl)}
        />
      </span>
    );
  }

  return (
    <span
      aria-hidden="true"
      className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl border border-white/[0.08] text-[11px] font-bold text-white"
      style={{
        background: `linear-gradient(135deg, hsl(${hue},55%,35%), hsl(${(hue + 38) % 360},55%,25%))`,
      }}
    >
      {ticker.slice(0, 2)}
    </span>
  );
}

function StatCard({
  icon,
  label,
  value,
  detail,
}: {
  icon: React.ReactNode;
  label: string;
  value: string;
  detail: string;
}) {
  return (
    <div className="rounded-2xl border border-white/[0.065] bg-white/[0.018] p-3.5">
      <div className="flex items-center gap-2 text-[10px] font-semibold uppercase tracking-[0.14em] text-zinc-600">
        <span className="text-emerald-400/70">{icon}</span>
        {label}
      </div>
      <div className="mt-2 text-xl font-semibold tracking-tight text-white">{value}</div>
      <div className="mt-0.5 text-[11px] text-zinc-600">{detail}</div>
    </div>
  );
}

function dayLabel(day: CongressClusterDay) {
  const date = formatCalendarDate(day.date, 'UTC');
  if (!day.tradeCount) return `${date}: no disclosed buys`;
  const clusterSummary = day.clusterCount
    ? `${day.clusterCount} coordinated ${day.clusterCount === 1 ? 'company' : 'companies'}; `
    : '';
  return `${date}: ${clusterSummary}${day.tradeCount} disclosed buy${day.tradeCount === 1 ? '' : 's'} from ${day.actorCount} lawmaker${day.actorCount === 1 ? '' : 's'}`;
}

function intensityStyle(day: CongressClusterDay, maximum: number) {
  if (!day.tradeCount) {
    return {
      backgroundColor: 'rgba(255,255,255,0.025)',
      borderColor: 'rgba(255,255,255,0.045)',
    };
  }
  if (!day.clusterCount || !maximum) {
    return {
      backgroundColor: 'rgba(34, 211, 238, 0.07)',
      borderColor: 'rgba(34, 211, 238, 0.11)',
    };
  }
  const ratio = Math.max(0.25, Math.min(day.clusterCount / maximum, 1));
  return {
    backgroundColor: `rgba(16, 185, 129, ${0.1 + ratio * 0.58})`,
    borderColor: `rgba(52, 211, 153, ${0.16 + ratio * 0.4})`,
  };
}

function ActivityCalendar({
  data,
  selectedDate,
  onSelectDate,
}: {
  data: CongressClusterCalendarData;
  selectedDate: string;
  onSelectDate: (date: string) => void;
}) {
  const maximum = Math.max(...data.days.map((day) => day.clusterCount), 0);
  const leadingDays = data.days.length
    ? new Date(`${data.days[0].date}T12:00:00Z`).getUTCDay()
    : 0;
  const cells: Array<CongressClusterDay | null> = [
    ...Array.from({ length: leadingDays }, () => null),
    ...data.days,
  ];
  const compact = data.range === 'year';

  return (
    <div className="rounded-2xl border border-white/[0.065] bg-white/[0.016] p-4 sm:p-5">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <div>
          <div className="flex items-center gap-2">
            <CalendarDays className="h-4 w-4 text-emerald-300" />
            <h2 className="text-sm font-semibold text-white">Disclosure activity calendar</h2>
          </div>
          <p className="mt-1 text-xs leading-5 text-zinc-600">
            Darker days contain more companies bought by multiple lawmakers. Select a day for context.
          </p>
        </div>
        <div className="shrink-0 text-[11px] text-zinc-600">
          {formatCalendarDate(data.rangeStart, 'UTC')} –{' '}
          {formatCalendarDate(data.rangeEnd, 'UTC')}
        </div>
      </div>

      <div className="mt-5 flex min-w-0 gap-2.5">
        <div className="grid shrink-0 grid-rows-7 gap-1.5 pt-px text-[9px] font-medium uppercase text-zinc-700">
          {['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'].map((label) => (
            <span key={label} className={`flex items-center ${compact ? 'h-3.5' : 'h-8'}`}>
              {compact ? label.slice(0, 1) : label}
            </span>
          ))}
        </div>

        <div className="min-w-0 flex-1 overflow-x-auto pb-2">
          <div
            className="grid w-full grid-flow-col grid-rows-7 gap-1.5"
            style={{
              gridAutoColumns: compact ? '14px' : data.range === 'week' ? 'minmax(54px, 1fr)' : 'minmax(30px, 1fr)',
              minWidth: compact ? `${Math.ceil(cells.length / 7) * 20}px` : undefined,
            }}
          >
            {cells.map((day, index) =>
              day ? (
                <button
                  key={day.date}
                  type="button"
                  title={dayLabel(day)}
                  aria-label={dayLabel(day)}
                  aria-pressed={selectedDate === day.date}
                  onClick={() => onSelectDate(day.date)}
                  className={`relative rounded-[5px] border transition hover:scale-110 hover:brightness-125 focus:outline-none focus:ring-2 focus:ring-emerald-300/60 ${
                    compact ? 'h-3.5' : 'h-8'
                  } ${selectedDate === day.date ? 'ring-2 ring-emerald-300/70 ring-offset-1 ring-offset-[#080808]' : ''}`}
                  style={intensityStyle(day, maximum)}
                >
                  {!compact && data.range === 'week' ? (
                    <span className="text-[9px] font-semibold text-white/75">
                      {new Date(`${day.date}T12:00:00Z`).getUTCDate()}
                    </span>
                  ) : null}
                </button>
              ) : (
                <span key={`padding-${index}`} aria-hidden="true" className={compact ? 'h-3.5' : 'h-8'} />
              ),
            )}
          </div>
        </div>
      </div>
    </div>
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
  const latestActiveDay = useMemo(
    () =>
      [...data.days].reverse().find((day) => day.clusterCount > 0)?.date ||
      [...data.days].reverse().find((day) => day.tradeCount > 0)?.date ||
      data.rangeEnd,
    [data.days, data.rangeEnd],
  );
  const [selectedDate, setSelectedDate] = useState<string | null>(null);
  const activeSelectedDate = data.days.some((day) => day.date === selectedDate)
    ? selectedDate as string
    : latestActiveDay;
  const selectedDay =
    data.days.find((day) => day.date === activeSelectedDate) ||
    data.days[data.days.length - 1];
  const maximumActors = Math.max(...data.topClusters.map((cluster) => cluster.actorCount), 1);

  return (
    <div className="space-y-4">
      <section className="overflow-hidden rounded-3xl border border-emerald-400/[0.12] bg-[radial-gradient(circle_at_top_left,rgba(16,185,129,0.1),transparent_38%),rgba(255,255,255,0.016)] p-4 sm:p-5">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <div className="flex items-center gap-2 text-[10px] font-semibold uppercase tracking-[0.18em] text-emerald-300/80">
              <Landmark className="h-3.5 w-3.5" />
              Congress buy radar
            </div>
            <h1 className="mt-2 text-2xl font-semibold tracking-tight text-white sm:text-3xl">
              Congress cluster calendar
            </h1>
            <p className="mt-1.5 max-w-2xl text-sm leading-6 text-zinc-500">
              See the companies multiple lawmakers are buying, when those disclosures landed, and which moves carry the most conviction.
            </p>
          </div>

          <div className="flex flex-col gap-2 sm:items-end">
            <div className="flex w-fit items-center gap-2 rounded-full border border-emerald-400/[0.14] bg-emerald-400/[0.06] px-3 py-1.5 text-[10px] font-medium text-emerald-200/80">
              <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-emerald-300" />
              Latest disclosure {data.latestDisclosureDate ? formatCalendarDate(data.latestDisclosureDate, 'UTC') : 'pending'}
            </div>
            <div className="inline-flex w-fit rounded-xl border border-white/[0.07] bg-black/20 p-1">
              {RANGE_OPTIONS.map((option) => (
                <button
                  key={option.value}
                  type="button"
                  aria-pressed={range === option.value}
                  onClick={() => onRangeChange(option.value)}
                  className={`rounded-lg px-3 py-2 text-xs font-medium transition sm:px-3.5 ${
                    range === option.value
                      ? 'bg-white/[0.09] text-white shadow-sm'
                      : 'text-zinc-600 hover:text-zinc-300'
                  }`}
                >
                  <span className="sm:hidden">{option.shortLabel}</span>
                  <span className="hidden sm:inline">{option.label}</span>
                </button>
              ))}
            </div>
          </div>
        </div>
      </section>

      {error ? (
        <div role="alert" className="rounded-xl border border-red-400/15 bg-red-400/[0.06] px-4 py-3 text-xs text-red-200">
          {error}
        </div>
      ) : null}

      <div className={`space-y-4 transition-opacity ${loading ? 'pointer-events-none opacity-50' : 'opacity-100'}`}>
        <div className="grid gap-2.5 sm:grid-cols-2 lg:grid-cols-4">
          <StatCard
            icon={<Users className="h-3.5 w-3.5" />}
            label="Lawmakers"
            value={data.totals.actorCount.toLocaleString()}
            detail="Distinct buyers"
          />
          <StatCard
            icon={<Flame className="h-3.5 w-3.5" />}
            label="Companies"
            value={data.totals.tickerCount.toLocaleString()}
            detail="With disclosed buying"
          />
          <StatCard
            icon={<Landmark className="h-3.5 w-3.5" />}
            label="Buy filings"
            value={data.totals.tradeCount.toLocaleString()}
            detail="Published in this period"
          />
          <StatCard
            icon={<Clock3 className="h-3.5 w-3.5" />}
            label="Tracked floor"
            value={formatCompactCurrency(data.totals.amountFloor)}
            detail="Minimum disclosed value"
          />
        </div>

        <div className="grid gap-4 xl:grid-cols-[minmax(0,1.05fr)_minmax(420px,0.95fr)]">
          <section className="overflow-hidden rounded-2xl border border-white/[0.065] bg-white/[0.016]">
            <div className="flex items-start justify-between border-b border-white/[0.055] px-4 py-3.5 sm:px-5">
              <div>
                <h2 className="text-sm font-semibold text-white">Top 5 congressional accumulation plays</h2>
                <p className="mt-1 text-xs text-zinc-600">Ranked by distinct lawmakers, then disclosed dollar floor.</p>
              </div>
              <span className="rounded-md border border-white/[0.06] bg-white/[0.025] px-2 py-1 text-[9px] font-semibold uppercase tracking-[0.12em] text-zinc-600">
                Buying
              </span>
            </div>

            {data.topClusters.length ? (
              <div className="divide-y divide-white/[0.045]">
                {data.topClusters.map((cluster, index) => (
                  <Link
                    key={cluster.ticker}
                    href={`/ticker/${encodeURIComponent(cluster.ticker)}`}
                    className="group block px-4 py-3.5 transition hover:bg-white/[0.025] sm:px-5"
                  >
                    <div className="flex items-center gap-3">
                      <span className="w-5 shrink-0 text-center text-xs font-semibold tabular-nums text-zinc-700">
                        {index + 1}
                      </span>
                      <TickerLogo ticker={cluster.ticker} />
                      <div className="min-w-0 flex-1">
                        <div className="flex flex-wrap items-center gap-2">
                          <span className="text-sm font-bold tracking-[0.08em] text-cyan-100">{cluster.ticker}</span>
                          <span
                            className={`rounded-md border px-1.5 py-0.5 text-[9px] font-semibold uppercase tracking-[0.1em] ${
                              cluster.conviction === 'high'
                                ? 'border-emerald-400/20 bg-emerald-400/[0.08] text-emerald-300'
                                : 'border-amber-300/15 bg-amber-300/[0.06] text-amber-200/75'
                            }`}
                          >
                            {cluster.conviction === 'high' ? 'High conviction' : 'Building'}
                          </span>
                        </div>
                        <div className="mt-0.5 truncate text-xs text-zinc-500">
                          {cluster.companyName || `${cluster.ticker} disclosed purchases`}
                        </div>
                        <div className="mt-2 h-1 overflow-hidden rounded-full bg-white/[0.04]">
                          <div
                            className="h-full rounded-full bg-gradient-to-r from-emerald-500/50 to-cyan-300/70"
                            style={{ width: `${Math.max(16, (cluster.actorCount / maximumActors) * 100)}%` }}
                          />
                        </div>
                      </div>
                      <div className="shrink-0 text-right">
                        <div className="text-sm font-semibold tabular-nums text-white">
                          {cluster.actorCount} lawmaker{cluster.actorCount === 1 ? '' : 's'}
                        </div>
                        <div className="mt-0.5 text-[11px] text-zinc-600">
                          {cluster.tradeCount} buys · {formatCompactCurrency(cluster.amountFloor)}+
                        </div>
                      </div>
                      <ArrowUpRight className="hidden h-3.5 w-3.5 text-zinc-700 transition group-hover:text-zinc-400 sm:block" />
                    </div>
                    <div className="ml-20 mt-2 truncate text-[10px] text-zinc-700">
                      {cluster.politicianNames.join(' · ')}
                      {cluster.actorCount > cluster.politicianNames.length
                        ? ` · +${cluster.actorCount - cluster.politicianNames.length} more`
                        : ''}
                    </div>
                  </Link>
                ))}
              </div>
            ) : (
              <div className="px-5 py-14 text-center">
                <div className="text-sm font-medium text-zinc-300">No coordinated buys in this period.</div>
                <div className="mt-1 text-xs text-zinc-600">Try a longer time range.</div>
              </div>
            )}
          </section>

          <div className="space-y-3">
            <ActivityCalendar
              data={data}
              selectedDate={activeSelectedDate}
              onSelectDate={setSelectedDate}
            />
            {selectedDay ? (
              <div className="rounded-2xl border border-white/[0.06] bg-white/[0.014] px-4 py-3.5">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <div className="text-[10px] font-semibold uppercase tracking-[0.14em] text-zinc-700">Selected day</div>
                    <div className="mt-1 text-sm font-semibold text-white">{formatCalendarDate(selectedDay.date, 'UTC')}</div>
                  </div>
                  {selectedDay.topTicker ? (
                    <span className="rounded-lg border border-cyan-400/15 bg-cyan-400/[0.05] px-2.5 py-1.5 text-xs font-semibold text-cyan-100">
                      Most active: {selectedDay.topTicker}
                    </span>
                  ) : null}
                </div>
                <div className="mt-3 grid grid-cols-3 gap-2 text-xs">
                  <div>
                    <div className="text-zinc-700">Lawmakers</div>
                    <div className="mt-1 font-semibold text-zinc-300">{selectedDay.actorCount}</div>
                  </div>
                  <div>
                    <div className="text-zinc-700">Clusters</div>
                    <div className="mt-1 font-semibold text-zinc-300">{selectedDay.clusterCount}</div>
                  </div>
                  <div>
                    <div className="text-zinc-700">Buy filings</div>
                    <div className="mt-1 font-semibold text-zinc-300">{selectedDay.tradeCount}</div>
                  </div>
                </div>
              </div>
            ) : null}
          </div>
        </div>
      </div>

      <p className="px-1 text-[11px] leading-5 text-zinc-700">
        Based on purchase disclosures published during the selected period. Dollar figures use the minimum of each official disclosure range; filing dates can trail trade dates.
      </p>
    </div>
  );
}
