import type { Metadata } from 'next';
import Link from 'next/link';
import type { ReactNode } from 'react';
import {
  ArrowLeft,
  BarChart3,
  Clock3,
  Eye,
  Globe2,
  Minus,
  MonitorSmartphone,
  MousePointerClick,
  ShieldCheck,
  TrendingDown,
  TrendingUp,
  UserPlus,
  Users,
} from 'lucide-react';

import {
  ANALYTICS_RANGES,
  getSiteAnalytics,
  parseAnalyticsRange,
  type DeviceMetric,
  type RankedMetric,
} from '@/lib/site-analytics';

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

export const metadata: Metadata = {
  title: 'Site Analytics — Vail',
  description: 'Private Vail traffic and audience analytics.',
  robots: {
    index: false,
    follow: false,
  },
};

type PageProps = {
  searchParams: Promise<{ range?: string | string[] }>;
};

function formatNumber(value: number) {
  return new Intl.NumberFormat('en-US').format(value);
}

function formatDecimal(value: number) {
  return new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 }).format(value);
}

function formatDate(value: string, options?: Intl.DateTimeFormatOptions) {
  return new Intl.DateTimeFormat('en-US', {
    month: 'short',
    day: 'numeric',
    timeZone: 'UTC',
    ...options,
  }).format(new Date(value));
}

function formatDateTime(value: string) {
  return new Intl.DateTimeFormat('en-US', {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  }).format(new Date(value));
}

function percentChange(current: number, previous: number) {
  if (previous === 0) return current === 0 ? 0 : null;
  return Math.round(((current - previous) / previous) * 100);
}

function ChangeBadge({ current, previous }: { current: number; previous: number }) {
  const change = percentChange(current, previous);
  if (change === null) {
    return <span className="text-emerald-300">New activity</span>;
  }

  const Icon = change > 0 ? TrendingUp : change < 0 ? TrendingDown : Minus;
  const tone = change > 0 ? 'text-emerald-300' : change < 0 ? 'text-amber-300' : 'text-zinc-500';
  return (
    <span className={`inline-flex items-center gap-1 ${tone}`}>
      <Icon className="h-3 w-3" />
      {Math.abs(change)}% vs previous period
    </span>
  );
}

function MetricCard({
  icon,
  label,
  value,
  detail,
  change,
  tone,
}: {
  icon: ReactNode;
  label: string;
  value: string;
  detail: string;
  change?: ReactNode;
  tone: string;
}) {
  return (
    <div className="rounded-2xl border border-white/[0.07] bg-white/[0.025] p-5">
      <div className="flex items-start justify-between gap-4">
        <div>
          <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-zinc-600">{label}</div>
          <div className="mt-2 text-3xl font-semibold tracking-[-0.04em] text-white">{value}</div>
        </div>
        <div className={`flex h-10 w-10 items-center justify-center rounded-xl bg-white/[0.04] ${tone}`}>
          {icon}
        </div>
      </div>
      <div className="mt-4 text-xs text-zinc-500">{detail}</div>
      {change ? <div className="mt-2 text-[11px]">{change}</div> : null}
    </div>
  );
}

function PanelHeading({ eyebrow, title, detail }: { eyebrow: string; title: string; detail?: string }) {
  return (
    <div className="flex flex-wrap items-end justify-between gap-3">
      <div>
        <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-zinc-600">{eyebrow}</div>
        <h2 className="mt-1 text-lg font-semibold text-white">{title}</h2>
      </div>
      {detail ? <div className="text-xs text-zinc-600">{detail}</div> : null}
    </div>
  );
}

function RankedList<T extends RankedMetric>({
  rows,
  label,
  emptyLabel,
}: {
  rows: T[];
  label: (row: T) => string;
  emptyLabel: string;
}) {
  const maxViews = Math.max(1, ...rows.map((row) => row.views));
  if (!rows.length) {
    return <div className="mt-6 text-sm text-zinc-600">{emptyLabel}</div>;
  }

  return (
    <div className="mt-5 space-y-4">
      {rows.map((row, index) => (
        <div key={`${label(row)}-${index}`}>
          <div className="mb-2 flex items-center justify-between gap-4 text-xs">
            <span className="truncate font-medium text-zinc-300">{label(row)}</span>
            <span className="shrink-0 text-zinc-600">
              {formatNumber(row.visitors)} people / {formatNumber(row.views)} views
            </span>
          </div>
          <div className="h-1.5 overflow-hidden rounded-full bg-white/[0.05]">
            <div
              className="h-full rounded-full bg-gradient-to-r from-emerald-500 to-cyan-400"
              style={{ width: `${Math.max(3, (row.views / maxViews) * 100)}%` }}
            />
          </div>
        </div>
      ))}
    </div>
  );
}

function countryFlag(countryCode: string | null) {
  const code = String(countryCode || '').toUpperCase();
  if (!/^[A-Z]{2}$/.test(code) || code === 'XX') return '◌';
  return String.fromCodePoint(...[...code].map((character) => 127397 + character.charCodeAt(0)));
}

function countryName(countryCode: string | null) {
  const code = String(countryCode || '').toUpperCase();
  if (!/^[A-Z]{2}$/.test(code) || code === 'XX') return 'Unknown country';
  try {
    return new Intl.DisplayNames(['en'], { type: 'region' }).of(code) || code;
  } catch {
    return code;
  }
}

function deviceIcon(device: DeviceMetric) {
  return device.device_type === 'Mobile' ? 'Mobile' : device.device_type === 'Tablet' ? 'Tablet' : 'Desktop';
}

export default async function AnalyticsPage({ searchParams }: PageProps) {
  const params = await searchParams;
  const days = parseAnalyticsRange(params.range);

  let data;
  try {
    data = await getSiteAnalytics(days);
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown analytics error.';
    return (
      <div className="mx-auto max-w-[920px] px-4 py-8 sm:px-6">
        <Link href="/ops" className="inline-flex items-center gap-2 text-xs font-medium text-zinc-500 transition hover:text-white">
          <ArrowLeft className="h-3.5 w-3.5" /> Mission Control
        </Link>
        <section className="mt-5 rounded-2xl border border-amber-500/20 bg-amber-500/10 p-6">
          <BarChart3 className="h-6 w-6 text-amber-300" />
          <h1 className="mt-4 text-2xl font-semibold text-white">Analytics setup is not finished</h1>
          <p className="mt-3 text-sm leading-6 text-amber-100/70">{message}</p>
        </section>
      </div>
    );
  }

  const { summary } = data;
  const maxDailyViews = Math.max(1, ...data.daily.map((metric) => metric.views));
  const chartLabelEvery = days === 7 ? 1 : days === 30 ? 5 : 15;

  return (
    <div className="mx-auto max-w-[1320px] space-y-5 px-4 py-6 sm:px-6 lg:px-8">
      <section className="flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <Link href="/ops" className="inline-flex items-center gap-2 text-xs font-medium text-zinc-500 transition hover:text-white">
            <ArrowLeft className="h-3.5 w-3.5" />
            Mission Control
          </Link>
          <div className="mt-5 inline-flex items-center gap-2 rounded-full border border-emerald-500/20 bg-emerald-500/10 px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.2em] text-emerald-300">
            <ShieldCheck className="h-3.5 w-3.5" />
            Private Analytics
          </div>
          <h1 className="mt-4 text-3xl font-semibold tracking-[-0.04em] text-white md:text-4xl">Your audience, at a glance</h1>
          <p className="mt-2 max-w-2xl text-sm leading-6 text-zinc-500">
            First-party traffic, acquisition, and location data for Vail. Automated bots and private ops pages are excluded.
          </p>
        </div>

        <div className="inline-flex self-start rounded-xl border border-white/[0.07] bg-white/[0.025] p-1 lg:self-auto">
          {ANALYTICS_RANGES.map((range) => (
            <Link
              key={range}
              href={`/ops/analytics?range=${range}`}
              className={`rounded-lg px-3 py-2 text-xs font-semibold transition ${
                range === days ? 'bg-white/[0.09] text-white shadow-sm' : 'text-zinc-500 hover:text-zinc-200'
              }`}
            >
              {range} days
            </Link>
          ))}
        </div>
      </section>

      <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <MetricCard
          icon={<Users className="h-5 w-5" />}
          label="Total Visitors"
          value={formatNumber(summary.allTimeVisitors)}
          detail={`${formatNumber(summary.visitors)} unique in the last ${days} days`}
          change={<ChangeBadge current={summary.visitors} previous={summary.previousVisitors} />}
          tone="text-emerald-300"
        />
        <MetricCard
          icon={<Eye className="h-5 w-5" />}
          label="Page Views"
          value={formatNumber(summary.views)}
          detail={`${formatNumber(summary.allTimeViews)} all time / ${formatNumber(summary.todayViews)} today`}
          change={<ChangeBadge current={summary.views} previous={summary.previousViews} />}
          tone="text-cyan-300"
        />
        <MetricCard
          icon={<MousePointerClick className="h-5 w-5" />}
          label="Sessions"
          value={formatNumber(summary.sessions)}
          detail={`${formatDecimal(summary.viewsPerSession)} pages per session / ${formatDecimal(summary.bounceRate)}% bounce`}
          change={<ChangeBadge current={summary.sessions} previous={summary.previousSessions} />}
          tone="text-blue-300"
        />
        <MetricCard
          icon={<UserPlus className="h-5 w-5" />}
          label="Registered Users"
          value={formatNumber(data.signups.allTime)}
          detail={`${formatNumber(data.signups.inRange)} joined in the last ${days} days`}
          tone="text-violet-300"
        />
      </section>

      {summary.allTimeViews === 0 ? (
        <section className="rounded-2xl border border-cyan-500/20 bg-cyan-500/[0.07] p-5">
          <div className="flex items-start gap-3">
            <BarChart3 className="mt-0.5 h-5 w-5 shrink-0 text-cyan-300" />
            <div>
              <div className="text-sm font-semibold text-white">Collection is active now</div>
              <p className="mt-1 text-sm leading-6 text-zinc-400">
                This is a new first-party report, so historical traffic is not backfilled. The first real visitor will appear here automatically.
              </p>
            </div>
          </div>
        </section>
      ) : null}

      <section className="rounded-2xl border border-white/[0.07] bg-white/[0.025] p-5 sm:p-6">
        <PanelHeading
          eyebrow="Traffic Trend"
          title={`Visitors over ${days} days`}
          detail={`${formatNumber(summary.todayVisitors)} unique today`}
        />

        <div className="mt-7 overflow-x-auto pb-2">
          <div
            role="img"
            aria-label={`Daily page views for the last ${days} days`}
            className="flex h-56 min-w-[620px] items-end gap-1.5 border-b border-white/[0.07]"
            style={{ width: days === 90 ? '1280px' : '100%' }}
          >
            {data.daily.map((metric, index) => {
              const barHeight = metric.views ? Math.max(5, (metric.views / maxDailyViews) * 100) : 1;
              const showLabel = index % chartLabelEvery === 0 || index === data.daily.length - 1;
              return (
                <div key={metric.day} className="group relative flex h-full min-w-0 flex-1 items-end" title={`${formatDate(metric.day)}: ${metric.views} views, ${metric.visitors} visitors`}>
                  <div
                    className="w-full rounded-t-sm bg-gradient-to-t from-emerald-600/70 to-cyan-300/90 transition group-hover:brightness-125"
                    style={{ height: `${barHeight}%` }}
                  />
                  {showLabel ? (
                    <span className="absolute -bottom-6 left-1/2 -translate-x-1/2 whitespace-nowrap text-[9px] text-zinc-700">
                      {formatDate(metric.day)}
                    </span>
                  ) : null}
                </div>
              );
            })}
          </div>
        </div>
      </section>

      <section className="grid gap-5 xl:grid-cols-2">
        <div className="rounded-2xl border border-white/[0.07] bg-white/[0.025] p-5 sm:p-6">
          <PanelHeading eyebrow="Acquisition" title="Where people found Vail" />
          <RankedList rows={data.sources} label={(row) => row.source} emptyLabel="Traffic sources will appear after the first visit." />
        </div>

        <div className="rounded-2xl border border-white/[0.07] bg-white/[0.025] p-5 sm:p-6">
          <PanelHeading eyebrow="Content" title="Most visited pages" />
          <RankedList rows={data.pages} label={(row) => row.path} emptyLabel="Popular pages will appear after the first visit." />
        </div>
      </section>

      <section className="grid gap-5 xl:grid-cols-[1.2fr_0.8fr]">
        <div className="rounded-2xl border border-white/[0.07] bg-white/[0.025] p-5 sm:p-6">
          <PanelHeading eyebrow="Geography" title="Where visitors are located" />
          {data.locations.length ? (
            <div className="mt-5 overflow-hidden rounded-xl border border-white/[0.06]">
              <div className="grid grid-cols-[1fr_auto] gap-4 border-b border-white/[0.06] bg-white/[0.025] px-4 py-2.5 text-[10px] font-semibold uppercase tracking-[0.16em] text-zinc-600 sm:grid-cols-[1fr_1fr_auto]">
                <span>Location</span>
                <span className="hidden sm:block">Country</span>
                <span>People</span>
              </div>
              {data.locations.map((location, index) => (
                <div key={`${location.country_code}-${location.region}-${location.city}-${index}`} className="grid grid-cols-[1fr_auto] items-center gap-4 border-b border-white/[0.05] px-4 py-3 last:border-0 sm:grid-cols-[1fr_1fr_auto]">
                  <div className="flex min-w-0 items-center gap-3">
                    <span className="text-lg" aria-hidden="true">{countryFlag(location.country_code)}</span>
                    <div className="min-w-0">
                      <div className="truncate text-sm font-medium text-zinc-200">{location.city}</div>
                      <div className="truncate text-[11px] text-zinc-600">{location.region}</div>
                    </div>
                  </div>
                  <div className="hidden text-xs text-zinc-500 sm:block">{countryName(location.country_code)}</div>
                  <div className="text-sm font-semibold text-white">{formatNumber(location.visitors)}</div>
                </div>
              ))}
            </div>
          ) : (
            <div className="mt-6 flex items-center gap-3 text-sm text-zinc-600">
              <Globe2 className="h-4 w-4" /> Location data will appear on supported production hosts.
            </div>
          )}
        </div>

        <div className="rounded-2xl border border-white/[0.07] bg-white/[0.025] p-5 sm:p-6">
          <PanelHeading eyebrow="Technology" title="Devices" />
          <div className="mt-5">
            <RankedList rows={data.devices} label={deviceIcon} emptyLabel="Device details will appear after the first visit." />
          </div>
          <div className="mt-6 flex items-start gap-3 rounded-xl border border-white/[0.06] bg-black/20 p-4">
            <MonitorSmartphone className="mt-0.5 h-4 w-4 shrink-0 text-cyan-300" />
            <p className="text-xs leading-5 text-zinc-500">
              Device and browser categories come from the visitor&apos;s user agent. No fingerprinting is used.
            </p>
          </div>
        </div>
      </section>

      <section className="rounded-2xl border border-white/[0.07] bg-white/[0.025] p-5 sm:p-6">
        <PanelHeading eyebrow="Live Feed" title="Recent visits" detail={`${data.recent.length} latest page views`} />
        {data.recent.length ? (
          <div className="mt-5 divide-y divide-white/[0.05] overflow-hidden rounded-xl border border-white/[0.06]">
            {data.recent.slice(0, 12).map((visit, index) => (
              <div key={`${visit.occurred_at}-${visit.path}-${index}`} className="grid gap-3 px-4 py-3 md:grid-cols-[1fr_0.7fr_0.8fr_auto] md:items-center">
                <div className="min-w-0">
                  <div className="truncate text-sm font-medium text-zinc-200">{visit.path}</div>
                  <div className="mt-1 text-[11px] text-zinc-600">{visit.source}</div>
                </div>
                <div className="text-xs text-zinc-500">
                  {countryFlag(visit.country_code)} {visit.city || countryName(visit.country_code)}
                </div>
                <div className="text-xs text-zinc-500">{visit.device_type} / {visit.browser || 'Other'}</div>
                <div className="inline-flex items-center gap-1.5 text-[11px] text-zinc-600">
                  <Clock3 className="h-3 w-3" />
                  {formatDateTime(visit.occurred_at)}
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="mt-6 text-sm text-zinc-600">No visits recorded yet.</div>
        )}
      </section>

      <footer className="flex flex-col gap-2 border-t border-white/[0.06] py-4 text-[11px] leading-5 text-zinc-700 sm:flex-row sm:items-center sm:justify-between">
        <span>First-party analytics. Raw IP addresses are never stored.</span>
        <span>Updated {formatDateTime(data.generatedAt)}</span>
      </footer>
    </div>
  );
}
