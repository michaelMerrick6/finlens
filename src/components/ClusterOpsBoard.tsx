'use client';

import Image, { type ImageLoaderProps } from 'next/image';
import Link from 'next/link';
import { useEffect, useMemo, useState } from 'react';
import { useRouter } from 'next/navigation';
import { ArrowUpRight, Search } from 'lucide-react';

import DashboardClusterModal from '@/components/DashboardClusterModal';
import { getTickerLogoUrl } from '@/lib/company-logos';
import type { ClusterOpsData, ClusterOpsStory, ClusterStoryStatus } from '@/lib/cluster-ops';
import type { DashboardClusterDetail } from '@/lib/dashboard-cluster-types';
import { formatCalendarDate } from '@/lib/date-format';

const STATUS_OPTIONS = [
  { value: 'all', label: 'All' },
  { value: 'pending_review', label: 'Pending' },
  { value: 'approved', label: 'Approved' },
  { value: 'posted', label: 'Posted' },
  { value: 'rejected', label: 'Rejected' },
  { value: 'mixed', label: 'Mixed' },
] as const;

const SOURCE_OPTIONS = [
  { value: 'all', label: 'All' },
  { value: 'congress', label: 'Congress' },
  { value: 'insiders', label: 'Insiders' },
  { value: 'cross-source', label: 'Cross-Source' },
] as const;

type StatusFilter = (typeof STATUS_OPTIONS)[number]['value'];
type SourceFilter = (typeof SOURCE_OPTIONS)[number]['value'];

const passthroughImageLoader = ({ src }: ImageLoaderProps) => src;

function FilterPill({
  active,
  children,
  onClick,
}: {
  active: boolean;
  children: string;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`rounded-md border px-2.5 py-1.5 text-[11px] font-medium transition ${
        active
          ? 'border-white/12 bg-white/[0.06] text-white'
          : 'border-transparent text-zinc-500 hover:bg-white/[0.03] hover:text-zinc-300'
      }`}
    >
      {children}
    </button>
  );
}

function metricToneClass(tone: ClusterOpsData['metrics'][number]['tone']) {
  if (tone === 'success') {
    return 'border-emerald-500/18 bg-emerald-500/[0.07]';
  }
  if (tone === 'warn') {
    return 'border-amber-500/18 bg-amber-500/[0.07]';
  }
  return 'border-white/[0.06] bg-white/[0.02]';
}

function statusToneClass(status: ClusterStoryStatus) {
  if (status === 'pending_review') {
    return 'border-amber-500/20 bg-amber-500/10 text-amber-200';
  }
  if (status === 'approved') {
    return 'border-blue-500/20 bg-blue-500/10 text-blue-200';
  }
  if (status === 'posted') {
    return 'border-emerald-500/20 bg-emerald-500/10 text-emerald-200';
  }
  if (status === 'rejected') {
    return 'border-red-500/20 bg-red-500/10 text-red-200';
  }
  return 'border-white/10 bg-white/[0.05] text-zinc-300';
}

function statusLabel(status: ClusterStoryStatus) {
  if (status === 'pending_review') return 'Pending';
  if (status === 'approved') return 'Approved';
  if (status === 'posted') return 'Posted';
  if (status === 'rejected') return 'Rejected';
  return 'Mixed';
}

function formatDateShort(value: string | null | undefined) {
  if (!value) return '—';
  return formatCalendarDate(value, 'UTC');
}

function TickerLogo({ ticker }: { ticker: string }) {
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  const logoUrl = getTickerLogoUrl(ticker, 44);
  const activeLogoUrl = logoUrl && failedUrl !== logoUrl ? logoUrl : null;
  const hue = ticker.split('').reduce((acc, ch) => acc + ch.charCodeAt(0), 0) % 360;

  if (activeLogoUrl) {
    return (
      <div className="h-8 w-8 shrink-0 overflow-hidden rounded-full border border-white/10 bg-black/30">
        <Image
          loader={passthroughImageLoader}
          unoptimized
          src={activeLogoUrl}
          alt={ticker}
          width={32}
          height={32}
          sizes="32px"
          className="h-full w-full object-contain p-0.5"
          onError={() => setFailedUrl(activeLogoUrl)}
        />
      </div>
    );
  }

  return (
    <div
      className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full border border-white/10 text-[11px] font-bold text-white"
      style={{
        background: `linear-gradient(135deg, hsl(${hue},65%,45%), hsl(${(hue + 40) % 360},65%,38%))`,
      }}
    >
      {ticker.slice(0, 2)}
    </div>
  );
}

export default function ClusterOpsBoard({ data }: { data: ClusterOpsData }) {
  const router = useRouter();
  const [searchQuery, setSearchQuery] = useState('');
  const [statusFilter, setStatusFilter] = useState<StatusFilter>('all');
  const [sourceFilter, setSourceFilter] = useState<SourceFilter>('all');
  const [selectedCluster, setSelectedCluster] = useState<ClusterOpsStory | null>(null);
  const [clusterDetail, setClusterDetail] = useState<DashboardClusterDetail | null>(null);
  const [clusterDetailLoading, setClusterDetailLoading] = useState(false);
  const [clusterDetailError, setClusterDetailError] = useState('');

  const visibleStories = useMemo(() => {
    const query = searchQuery.trim().toLowerCase();
    return data.stories.filter((story) => {
      if (statusFilter !== 'all' && story.status !== statusFilter) {
        return false;
      }
      if (sourceFilter !== 'all' && story.sourceGroup !== sourceFilter) {
        return false;
      }
      if (!query) {
        return true;
      }

      return [
        story.ticker,
        story.title,
        story.summary,
        story.ruleLabel,
        story.actorPreview || '',
      ]
        .join(' ')
        .toLowerCase()
        .includes(query);
    });
  }, [data.stories, searchQuery, sourceFilter, statusFilter]);

  useEffect(() => {
    if (!selectedCluster) {
      return;
    }

    const controller = new AbortController();
    let cancelled = false;

    fetch(`/api/ops/dashboard-cluster?key=${encodeURIComponent(selectedCluster.id)}`, {
      signal: controller.signal,
    })
      .then(async (response) => {
        if (!response.ok) {
          const payload = (await response.json().catch(() => null)) as { error?: string } | null;
          throw new Error(payload?.error || 'Could not load this cluster right now.');
        }
        return (await response.json()) as DashboardClusterDetail;
      })
      .then((payload) => {
        if (!cancelled) {
          setClusterDetail(payload);
        }
      })
      .catch((error) => {
        if (cancelled || (error instanceof Error && error.name === 'AbortError')) {
          return;
        }
        setClusterDetailError(error instanceof Error ? error.message : 'Could not load this cluster right now.');
      })
      .finally(() => {
        if (!cancelled) {
          setClusterDetailLoading(false);
        }
      });

    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [selectedCluster]);

  return (
    <>
      <div className="space-y-6">
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
          {data.metrics.map((metric) => (
            <div key={metric.label} className={`rounded-2xl border px-4 py-4 ${metricToneClass(metric.tone)}`}>
              <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-zinc-500">{metric.label}</div>
              <div className="mt-2 text-2xl font-semibold text-white">{metric.value}</div>
              <div className="mt-2 text-sm text-zinc-500">{metric.sublabel}</div>
            </div>
          ))}
        </div>

        <div className="grid gap-4 xl:grid-cols-[0.9fr_1.1fr]">
          <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-4">
            <div className="flex items-center justify-between gap-3">
              <div>
                <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-zinc-500">Current Gates</div>
                <h2 className="mt-2 text-lg font-semibold text-white">Cluster Logic Snapshot</h2>
              </div>
              <Link
                href="/ops/policy"
                className="inline-flex items-center gap-1.5 rounded-lg border border-white/[0.08] bg-white/[0.03] px-3 py-2 text-xs font-medium text-zinc-300 transition hover:border-white/[0.16] hover:bg-white/[0.05] hover:text-white"
              >
                Signal Policy
                <ArrowUpRight className="h-3.5 w-3.5" />
              </Link>
            </div>

            <div className="mt-4 grid gap-3 sm:grid-cols-2">
              <div className="rounded-xl border border-white/[0.06] bg-black/20 p-3">
                <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-zinc-600">Congress Cluster</div>
                <div className="mt-2 text-sm text-white">{data.policy.congressClusterMinMembers} members in {data.policy.congressClusterWindowDays} days</div>
              </div>
              <div className="rounded-xl border border-white/[0.06] bg-black/20 p-3">
                <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-zinc-600">Cross-Source</div>
                <div className="mt-2 text-sm text-white">Congress + insiders in {data.policy.crossSourceClusterWindowDays}d</div>
                <div className="mt-1 text-xs text-zinc-500">Funds aligned across {data.policy.fundAlignmentWindowDays}d</div>
              </div>
              <div className="rounded-xl border border-white/[0.06] bg-black/20 p-3">
                <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-zinc-600">Score Floor</div>
                <div className="mt-2 text-sm text-white">{data.policy.minimumImportance.toFixed(2)} minimum importance</div>
                <div className="mt-1 text-xs text-zinc-500">{data.policy.minimumGroupCount} grouped rows minimum</div>
              </div>
              <div className="rounded-xl border border-white/[0.06] bg-black/20 p-3">
                <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-zinc-600">Insider Gate</div>
                <div className="mt-2 text-sm text-white">
                  {(data.policy.meaningfulInsiderChangeMinPct * 100).toFixed(0)}% holding change
                </div>
                <div className="mt-1 text-xs text-zinc-500">
                  ${data.policy.meaningfulInsiderChangeMinValue.toLocaleString()} value floor
                </div>
              </div>
            </div>

            <div className="mt-3 rounded-xl border border-white/[0.06] bg-black/20 p-3 text-xs text-zinc-500">
              Large politician buy floor: ${data.policy.largePoliticianBuyMinLowerBound.toLocaleString()}.
              Committee relevance buy floor: ${data.policy.committeeRelevanceBuyMinLowerBound.toLocaleString()}.
            </div>
          </div>

          <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-4">
            <div className="flex items-center justify-between gap-3">
              <div>
                <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-zinc-500">Rule Yield</div>
                <h2 className="mt-2 text-lg font-semibold text-white">Which cluster rules are producing</h2>
              </div>
              <Link
                href="/clusters"
                className="inline-flex items-center gap-1.5 rounded-lg border border-white/[0.08] bg-white/[0.03] px-3 py-2 text-xs font-medium text-zinc-300 transition hover:border-white/[0.16] hover:bg-white/[0.05] hover:text-white"
              >
                Public Feed
                <ArrowUpRight className="h-3.5 w-3.5" />
              </Link>
            </div>

            <div className="mt-4 space-y-3">
              {data.rules.slice(0, 6).map((rule) => (
                <div key={rule.ruleKey} className="rounded-xl border border-white/[0.06] bg-black/20 px-3 py-3">
                  <div className="flex items-center justify-between gap-3">
                    <div className="text-sm font-medium text-white">{rule.ruleLabel}</div>
                    <div className="text-sm font-semibold text-white">{rule.total}</div>
                  </div>
                  <div className="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-zinc-500">
                    <span>{rule.last30d} in last 30d</span>
                    <span>{rule.pendingReview} pending</span>
                    <span>{rule.publicArchive} retained</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] px-3.5 py-3">
          <div className="flex flex-col gap-3 xl:flex-row xl:items-center xl:justify-between">
            <div className="w-full max-w-lg">
              <label className="mb-1.5 block text-[10px] font-semibold uppercase tracking-[0.16em] text-zinc-600">
                Search
              </label>
              <div className="relative">
                <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-zinc-600" />
                <input
                  type="text"
                  placeholder="Search ticker, rule, or actor..."
                  value={searchQuery}
                  onChange={(event) => setSearchQuery(event.target.value)}
                  className="h-10 w-full rounded-lg border border-white/[0.08] bg-white/[0.03] pl-10 pr-4 text-sm text-white outline-none transition placeholder:text-zinc-600 focus:border-cyan-500/30 focus:bg-white/[0.05]"
                />
              </div>
            </div>

            <div className="flex flex-col gap-2.5 sm:flex-row sm:flex-wrap sm:items-end xl:justify-end">
              <div className="min-w-0">
                <div className="mb-1.5 text-[10px] font-semibold uppercase tracking-[0.16em] text-zinc-600">Status</div>
                <div className="inline-flex rounded-lg border border-white/[0.06] bg-white/[0.02] p-0.5">
                  {STATUS_OPTIONS.map((option) => (
                    <FilterPill
                      key={option.value}
                      active={statusFilter === option.value}
                      onClick={() => setStatusFilter(option.value)}
                    >
                      {option.label}
                    </FilterPill>
                  ))}
                </div>
              </div>

              <div className="min-w-0">
                <div className="mb-1.5 text-[10px] font-semibold uppercase tracking-[0.16em] text-zinc-600">Source</div>
                <div className="inline-flex rounded-lg border border-white/[0.06] bg-white/[0.02] p-0.5">
                  {SOURCE_OPTIONS.map((option) => (
                    <FilterPill
                      key={option.value}
                      active={sourceFilter === option.value}
                      onClick={() => setSourceFilter(option.value)}
                    >
                      {option.label}
                    </FilterPill>
                  ))}
                </div>
              </div>
            </div>
          </div>

          <div className="mt-2.5 text-xs text-zinc-500">{visibleStories.length.toLocaleString()} visible stories in this view</div>
        </div>

        <div className="overflow-hidden rounded-2xl border border-white/[0.06] bg-white/[0.02]">
          {visibleStories.length ? (
            visibleStories.map((story) => (
              <div
                key={story.id}
                role="button"
                tabIndex={0}
                onClick={() => {
                  setClusterDetail(null);
                  setClusterDetailError('');
                  setClusterDetailLoading(true);
                  setSelectedCluster(story);
                }}
                onKeyDown={(event) => {
                  if (event.key === 'Enter' || event.key === ' ') {
                    event.preventDefault();
                    setClusterDetail(null);
                    setClusterDetailError('');
                    setClusterDetailLoading(true);
                    setSelectedCluster(story);
                  }
                }}
                className="flex cursor-pointer flex-col gap-4 border-b border-white/[0.06] px-4 py-3 transition hover:bg-white/[0.03] last:border-0 focus:outline-none focus:ring-1 focus:ring-cyan-500/25 sm:flex-row sm:items-center sm:px-5"
              >
                <button
                  type="button"
                  onClick={(event) => {
                    event.stopPropagation();
                    router.push(`/dashboard?ticker=${encodeURIComponent(story.ticker)}`);
                  }}
                  className="inline-flex w-fit shrink-0 items-center gap-2 rounded-2xl border border-white/[0.08] bg-white/[0.03] px-2.5 py-1.5 text-[11px] font-semibold tracking-[0.18em] text-cyan-200 transition hover:border-cyan-400/20 hover:bg-cyan-400/10 hover:text-white"
                >
                  <TickerLogo ticker={story.ticker} />
                  <span>{story.ticker}</span>
                </button>

                <div className="min-w-0 flex-1">
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="inline-flex items-center rounded-full border border-white/[0.08] bg-white/[0.03] px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.14em] text-white">
                      {story.ruleLabel}
                    </span>
                    <span className={`inline-flex items-center rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.14em] ${statusToneClass(story.status)}`}>
                      {statusLabel(story.status)}
                    </span>
                    {story.amountLabel ? (
                      <span className="inline-flex items-center rounded-full border border-amber-400/20 bg-amber-400/10 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.14em] text-amber-200">
                        {story.amountLabel}
                      </span>
                    ) : null}
                  </div>

                  <div className="mt-1 truncate text-sm font-medium text-white">{story.title}</div>
                  <div className="mt-1 flex flex-wrap items-center gap-x-2 gap-y-1 text-xs text-zinc-500">
                    <span>{story.summary}</span>
                    {story.actorPreview ? (
                      <>
                        <span className="text-white/20">·</span>
                        <span className="truncate">{story.actorPreview}</span>
                      </>
                    ) : null}
                  </div>
                </div>

                <div className="shrink-0 text-left sm:text-right">
                  <div className="text-sm font-semibold text-white">{story.score.toFixed(2)}</div>
                  <div className="mt-1 text-xs text-zinc-500">
                    {story.actorCount} actor{story.actorCount === 1 ? '' : 's'}
                  </div>
                  <div className="mt-1 text-[11px] text-zinc-600">{formatDateShort(story.publishedAt)}</div>
                </div>
              </div>
            ))
          ) : (
            <div className="px-5 py-12 text-sm text-zinc-500">
              No cluster stories match this view. Try a different status, source family, or ticker.
            </div>
          )}
        </div>
      </div>

      <DashboardClusterModal
        cluster={selectedCluster}
        detail={clusterDetail}
        loading={clusterDetailLoading}
        error={clusterDetailError}
        open={Boolean(selectedCluster)}
        onClose={() => {
          setSelectedCluster(null);
          setClusterDetail(null);
          setClusterDetailError('');
          setClusterDetailLoading(false);
        }}
      />
    </>
  );
}
