'use client';

import Image, { type ImageLoaderProps } from 'next/image';
import { useEffect, useMemo, useState } from 'react';
import { useRouter } from 'next/navigation';
import { ChevronRight, Search } from 'lucide-react';

import DashboardClusterModal from '@/components/DashboardClusterModal';
import { getTickerLogoUrl } from '@/lib/company-logos';
import type { DashboardClusterDetail } from '@/lib/dashboard-cluster-types';
import { formatCalendarDate } from '@/lib/date-format';

type ClusterSignal = {
  id: string;
  ticker: string;
  title: string;
  summary: string;
  ruleLabel: string;
  actorPreview: string | null;
  actorCount: number;
  amountLabel: string | null;
  sourceLabel: string;
  publishedAt: string | null;
  direction: 'buy' | 'sell' | null;
  ruleKey: string;
  sourceGroup: 'congress' | 'insiders' | 'cross-source';
  score: number;
};

const SOURCE_OPTIONS = [
  { value: 'all', label: 'All' },
  { value: 'congress', label: 'Congress' },
  { value: 'insiders', label: 'Insiders' },
  { value: 'cross-source', label: 'Cross-Source' },
] as const;

type SourceFilter = (typeof SOURCE_OPTIONS)[number]['value'];
type DirectionFilter = 'all' | 'buy' | 'sell';

const passthroughImageLoader = ({ src }: ImageLoaderProps) => src;

function FilterPill({
  active,
  children,
  onClick,
  tone = 'default',
}: {
  active: boolean;
  children: string;
  onClick: () => void;
  tone?: 'default' | 'neutral' | 'buy' | 'sell';
}) {
  const activeClass =
    tone === 'buy'
      ? 'border-emerald-500/25 bg-emerald-500/[0.1] text-emerald-300'
      : tone === 'sell'
        ? 'border-red-500/25 bg-red-500/[0.1] text-red-300'
        : tone === 'neutral'
          ? 'border-white/[0.12] bg-white/[0.045] text-zinc-200'
          : 'border-emerald-500/25 bg-emerald-500/[0.1] text-emerald-300';

  return (
    <button
      type="button"
      onClick={onClick}
      className={`rounded-full border px-3 py-1.5 text-[11px] font-medium transition ${
        active
          ? activeClass
          : 'border-white/[0.06] bg-white/[0.015] text-zinc-500 hover:border-white/[0.12] hover:text-zinc-300'
      }`}
    >
      {children}
    </button>
  );
}

function TickerLogo({ ticker }: { ticker: string }) {
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  const logoUrl = getTickerLogoUrl(ticker, 48);
  const activeLogoUrl = logoUrl && failedUrl !== logoUrl ? logoUrl : null;
  const hue = ticker.split('').reduce((acc, ch) => acc + ch.charCodeAt(0), 0) % 360;

  if (activeLogoUrl) {
    return (
      <div className="h-9 w-9 shrink-0 overflow-hidden rounded-full border border-white/10 bg-black/30">
        <Image
          loader={passthroughImageLoader}
          unoptimized
          src={activeLogoUrl}
          alt={ticker}
          width={36}
          height={36}
          sizes="36px"
          className="h-full w-full object-contain p-0.5"
          onError={() => setFailedUrl(activeLogoUrl)}
        />
      </div>
    );
  }

  return (
    <div
      className="flex h-9 w-9 shrink-0 items-center justify-center rounded-full border border-white/10 text-[12px] font-bold text-white"
      style={{
        background: `linear-gradient(135deg, hsl(${hue},65%,45%), hsl(${(hue + 40) % 360},65%,38%))`,
      }}
    >
      {ticker.slice(0, 2)}
    </div>
  );
}

function formatDateShort(value: string | null | undefined) {
  if (!value) {
    return '—';
  }
  return formatCalendarDate(value, 'UTC');
}

export default function ClustersPage({ signals }: { signals: ClusterSignal[] }) {
  const router = useRouter();
  const [searchQuery, setSearchQuery] = useState('');
  const [sourceFilter, setSourceFilter] = useState<SourceFilter>('all');
  const [directionFilter, setDirectionFilter] = useState<DirectionFilter>('all');
  const [visibleCount, setVisibleCount] = useState(18);
  const [selectedCluster, setSelectedCluster] = useState<ClusterSignal | null>(null);
  const [clusterDetail, setClusterDetail] = useState<DashboardClusterDetail | null>(null);
  const [clusterDetailLoading, setClusterDetailLoading] = useState(false);
  const [clusterDetailError, setClusterDetailError] = useState('');

  const filteredSignals = useMemo(() => {
    const query = searchQuery.trim().toLowerCase();

    return signals.filter((signal) => {
      if (sourceFilter !== 'all' && signal.sourceGroup !== sourceFilter) {
        return false;
      }

      if (directionFilter !== 'all' && signal.direction !== directionFilter) {
        return false;
      }

      if (!query) {
        return true;
      }

      const haystack = [
        signal.ticker,
        signal.title,
        signal.summary,
        signal.actorPreview || '',
        signal.ruleLabel,
      ]
        .join(' ')
        .toLowerCase();

      return haystack.includes(query);
    });
  }, [directionFilter, searchQuery, signals, sourceFilter]);

  const visibleSignals = filteredSignals.slice(0, visibleCount);

  const crossSourceCount = useMemo(
    () => signals.filter((signal) => signal.sourceGroup === 'cross-source').length,
    [signals],
  );

  useEffect(() => {
    if (!selectedCluster) {
      return;
    }

    const controller = new AbortController();
    let cancelled = false;

    fetch(`/api/dashboard-cluster?key=${encodeURIComponent(selectedCluster.id)}`, {
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
      <div className="space-y-3">
        <div className="rounded-2xl border border-white/[0.06] bg-white/[0.015] p-3">
          <div className="flex flex-col gap-3 lg:flex-row lg:items-center">
            <div className="w-full lg:max-w-lg">
              <div className="relative">
                <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-zinc-600" />
                <input
                  type="text"
                  placeholder="Search ticker or name..."
                  value={searchQuery}
                  onChange={(event) => {
                    setSearchQuery(event.target.value);
                    setVisibleCount(18);
                  }}
                  className="h-10 w-full rounded-lg border border-white/[0.08] bg-white/[0.03] pl-10 pr-4 text-sm text-white outline-none transition placeholder:text-zinc-600 focus:border-emerald-500/30 focus:bg-white/[0.05]"
                />
              </div>
            </div>

            <div className="flex flex-wrap items-center gap-1.5">
              {SOURCE_OPTIONS.map((option) => (
                <FilterPill
                  key={option.value}
                  active={sourceFilter === option.value}
                  onClick={() => {
                    setSourceFilter(option.value);
                    setVisibleCount(18);
                  }}
                >
                  {option.label}
                </FilterPill>
              ))}
            </div>

            <div className="hidden h-5 w-px bg-white/[0.08] lg:block" />

            <div className="flex flex-wrap items-center gap-1.5">
              <FilterPill
                active={directionFilter === 'all'}
                tone="neutral"
                onClick={() => {
                  setDirectionFilter('all');
                  setVisibleCount(18);
                }}
              >
                Both
              </FilterPill>
              <FilterPill
                active={directionFilter === 'buy'}
                tone="buy"
                onClick={() => {
                  setDirectionFilter('buy');
                  setVisibleCount(18);
                }}
              >
                Buy
              </FilterPill>
              <FilterPill
                active={directionFilter === 'sell'}
                tone="sell"
                onClick={() => {
                  setDirectionFilter('sell');
                  setVisibleCount(18);
                }}
              >
                Sell
              </FilterPill>
            </div>
          </div>

          <div className="mt-2.5 flex flex-wrap items-center gap-x-2 gap-y-1 text-[11px] text-zinc-600">
            <span>{filteredSignals.length.toLocaleString()} clusters</span>
            <span className="text-white/15">•</span>
            <span>{crossSourceCount.toLocaleString()} cross-source</span>
          </div>
        </div>

        <div className="overflow-hidden rounded-2xl border border-white/[0.06] bg-white/[0.015]">
          {filteredSignals.length ? (
            visibleSignals.map((signal) => {
              const toneColor = signal.direction === 'sell' ? 'text-red-300' : 'text-emerald-300';
              const toneBg = signal.direction === 'sell' ? 'bg-red-500/10' : 'bg-emerald-500/10';
              const toneBorder = signal.direction === 'sell' ? 'border-red-500/20' : 'border-emerald-500/20';

              return (
                <div
                  key={signal.id}
                  role="button"
                  tabIndex={0}
                  onClick={() => {
                    setClusterDetail(null);
                    setClusterDetailError('');
                    setClusterDetailLoading(true);
                    setSelectedCluster(signal);
                  }}
                  onKeyDown={(event) => {
                    if (event.key === 'Enter' || event.key === ' ') {
                      event.preventDefault();
                      setClusterDetail(null);
                      setClusterDetailError('');
                      setClusterDetailLoading(true);
                      setSelectedCluster(signal);
                    }
                  }}
                  className="flex cursor-pointer items-center gap-3 border-b border-white/[0.06] px-4 py-3.5 transition hover:bg-white/[0.03] last:border-0 focus:outline-none focus:ring-1 focus:ring-[#10b981]/25 sm:px-5"
                >
                  <button
                    type="button"
                    onClick={(event) => {
                      event.stopPropagation();
                      router.push(`/dashboard?ticker=${encodeURIComponent(signal.ticker)}`);
                    }}
                    className="shrink-0 rounded-full transition hover:ring-2 hover:ring-cyan-400/20"
                    aria-label={`Open ${signal.ticker}`}
                  >
                    <TickerLogo ticker={signal.ticker} />
                  </button>

                  <div className="min-w-0 flex-1">
                    <div className="flex min-w-0 items-center gap-2">
                      <span className="shrink-0 text-xs font-semibold tracking-[0.16em] text-cyan-200">{signal.ticker}</span>
                      <span
                        className={`inline-flex shrink-0 items-center rounded-full border px-2 py-0.5 text-[10px] font-semibold ${toneBorder} ${toneBg} ${toneColor}`}
                      >
                        {signal.ruleLabel}
                      </span>
                    </div>

                    <div className="mt-1 truncate text-sm font-medium text-white">{signal.title}</div>
                    <div className="mt-0.5 hidden min-w-0 items-center gap-2 truncate text-xs text-zinc-600 sm:flex">
                      <span>{signal.summary}</span>
                      {signal.actorPreview ? (
                        <>
                          <span className="text-white/20">·</span>
                          <span className="truncate">{signal.actorPreview}</span>
                        </>
                      ) : null}
                    </div>
                  </div>

                  <div className="ml-auto hidden shrink-0 text-right sm:block">
                    {signal.amountLabel ? <div className="text-xs font-semibold text-zinc-300">{signal.amountLabel}</div> : null}
                    <div className="mt-1 text-[11px] text-zinc-600">
                      {signal.actorCount} actor{signal.actorCount === 1 ? '' : 's'} · {formatDateShort(signal.publishedAt)}
                    </div>
                  </div>
                  <ChevronRight className="h-4 w-4 shrink-0 text-zinc-700" />
                </div>
              );
            })
          ) : (
            <div className="px-5 py-12 text-sm text-zinc-500">
              No clusters match this filter yet. Try a ticker, an actor name, or a different source mix.
            </div>
          )}
        </div>

        {visibleSignals.length < filteredSignals.length ? (
          <button
            type="button"
            onClick={() => setVisibleCount((count) => count + 18)}
            className="rounded-lg border border-white/[0.08] bg-white/[0.015] px-3.5 py-2 text-xs font-medium text-zinc-400 transition hover:border-white/[0.14] hover:bg-white/[0.03] hover:text-white"
          >
            Load more clusters
          </button>
        ) : null}
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
