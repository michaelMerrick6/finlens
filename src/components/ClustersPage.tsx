'use client';

import Image, { type ImageLoaderProps } from 'next/image';
import { useDeferredValue, useEffect, useMemo, useState } from 'react';
import { ChevronRight, Landmark, Loader2, Search, UserRound } from 'lucide-react';

import DashboardClusterModal from '@/components/DashboardClusterModal';
import { clusterHeadline, clusterReason } from '@/lib/cluster-presentation';
import {
  isHighConvictionCluster,
  MIN_CONGRESS_CLUSTER_ACTORS,
  MIN_INSIDER_CLUSTER_ACTORS,
} from '@/lib/cluster-quality';
import { getTickerLogoUrl } from '@/lib/company-logos';
import type { DashboardClusterDetail } from '@/lib/dashboard-cluster-types';
import { formatCalendarDate } from '@/lib/date-format';

export type ClusterFeedSource = 'politicians' | 'insiders';

export type ClusterSignal = {
  id: string;
  ticker: string;
  title: string;
  summary: string;
  ruleLabel: string;
  actorPreview: string | null;
  actorCount: number;
  amountLabel: string | null;
  amountFloor: number;
  includesCongress: boolean;
  sourceLabel: string;
  publishedAt: string | null;
  direction: 'buy' | 'sell' | null;
  ruleKey: string;
  sourceGroup: 'congress' | 'insiders' | 'cross-source';
  sourceCounts: {
    congress: number;
    insiders: number;
    funds: number;
  };
  score: number;
  windowDays: number | null;
};

type DirectionFilter = 'all' | 'buy' | 'sell';

const CLUSTERS_PER_PAGE = 20;
const passthroughImageLoader = ({ src }: ImageLoaderProps) => src;
const clusterDetailCache = new Map<string, DashboardClusterDetail>();
const clusterDetailRequests = new Map<string, Promise<DashboardClusterDetail>>();

const SOURCE_CONFIG: Record<ClusterFeedSource, {
  description: string;
  emptyLabel: string;
  label: string;
  ruleKey: 'congress_cluster' | 'insider_cluster';
  title: string;
}> = {
  politicians: {
    label: 'Politicians',
    title: 'Politician clusters',
    description: `${MIN_CONGRESS_CLUSTER_ACTORS}+ distinct members of Congress moving in the same direction, grouped once per stock and direction.`,
    emptyLabel: 'politician',
    ruleKey: 'congress_cluster',
  },
  insiders: {
    label: 'Insiders',
    title: 'Insider clusters',
    description: `${MIN_INSIDER_CLUSTER_ACTORS}+ distinct company insiders moving in the same direction, grouped once per stock and direction.`,
    emptyLabel: 'insider',
    ruleKey: 'insider_cluster',
  },
};

function requestClusterDetail(clusterId: string, accessToken?: string) {
  const cached = clusterDetailCache.get(clusterId);
  if (cached) return Promise.resolve(cached);

  const pending = clusterDetailRequests.get(clusterId);
  if (pending) return pending;

  const request = fetch(`/api/dashboard-cluster?key=${encodeURIComponent(clusterId)}`, {
    headers: accessToken ? { Authorization: `Bearer ${accessToken}` } : undefined,
  })
    .then(async (response) => {
      if (!response.ok) {
        const payload = (await response.json().catch(() => null)) as { error?: string } | null;
        throw new Error(payload?.error || 'Could not load this cluster right now.');
      }
      return (await response.json()) as DashboardClusterDetail;
    })
    .then((detail) => {
      clusterDetailCache.set(clusterId, detail);
      return detail;
    })
    .finally(() => {
      clusterDetailRequests.delete(clusterId);
    });

  clusterDetailRequests.set(clusterId, request);
  return request;
}

function TickerLogo({ ticker }: { ticker: string }) {
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  const logoUrl = getTickerLogoUrl(ticker, 48);
  const activeLogoUrl = logoUrl && failedUrl !== logoUrl ? logoUrl : null;
  const hue = ticker.split('').reduce((sum, character) => sum + character.charCodeAt(0), 0) % 360;

  if (activeLogoUrl) {
    return (
      <span className="flex h-11 w-11 shrink-0 items-center justify-center overflow-hidden rounded-xl border border-white/[0.08] bg-black/30">
        <Image
          loader={passthroughImageLoader}
          unoptimized
          src={activeLogoUrl}
          alt=""
          width={44}
          height={44}
          sizes="44px"
          className="h-full w-full object-contain p-1"
          onError={() => setFailedUrl(activeLogoUrl)}
        />
      </span>
    );
  }

  return (
    <span
      aria-hidden="true"
      className="flex h-11 w-11 shrink-0 items-center justify-center rounded-xl border border-white/[0.08] text-xs font-bold text-white"
      style={{
        background: `linear-gradient(135deg, hsl(${hue},58%,38%), hsl(${(hue + 42) % 360},58%,28%))`,
      }}
    >
      {ticker.slice(0, 2)}
    </span>
  );
}

function formatDateShort(value: string | null | undefined) {
  return value ? formatCalendarDate(value, 'UTC') : '—';
}

function SourceButton({
  active,
  icon,
  label,
  onClick,
}: {
  active: boolean;
  icon: React.ReactNode;
  label: string;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      aria-pressed={active}
      onClick={onClick}
      className={`inline-flex min-h-10 items-center justify-center gap-2 rounded-lg px-4 py-2 text-sm font-medium transition ${
        active
          ? 'bg-white/[0.1] text-white shadow-sm'
          : 'text-zinc-600 hover:bg-white/[0.035] hover:text-zinc-300'
      }`}
    >
      {icon}
      {label}
    </button>
  );
}

function DirectionButton({
  active,
  label,
  onClick,
}: {
  active: boolean;
  label: string;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      aria-pressed={active}
      onClick={onClick}
      className={`rounded-lg px-3 py-2 text-xs font-medium transition ${
        active ? 'bg-white/[0.08] text-white' : 'text-zinc-600 hover:text-zinc-300'
      }`}
    >
      {label}
    </button>
  );
}

export default function ClustersPage({
  signals,
  source,
  onSourceChange,
  accessToken,
  archiveLoaded,
  loadingMore,
  loadMoreError,
  onLoadMore,
}: {
  signals: ClusterSignal[];
  source: ClusterFeedSource;
  onSourceChange: (source: ClusterFeedSource) => void;
  accessToken?: string;
  archiveLoaded: boolean;
  loadingMore: boolean;
  loadMoreError: string;
  onLoadMore: () => Promise<void>;
}) {
  const [searchQuery, setSearchQuery] = useState('');
  const deferredSearchQuery = useDeferredValue(searchQuery);
  const [directionFilter, setDirectionFilter] = useState<DirectionFilter>('all');
  const [visibleCount, setVisibleCount] = useState(CLUSTERS_PER_PAGE);
  const [selectedCluster, setSelectedCluster] = useState<ClusterSignal | null>(null);
  const [clusterDetail, setClusterDetail] = useState<DashboardClusterDetail | null>(null);
  const [clusterDetailLoading, setClusterDetailLoading] = useState(false);
  const [clusterDetailError, setClusterDetailError] = useState('');
  const sourceConfig = SOURCE_CONFIG[source];

  const curatedSignals = useMemo(() => {
    const seen = new Set<string>();
    return signals.filter((signal) => {
      if (signal.ruleKey !== sourceConfig.ruleKey || !isHighConvictionCluster(signal)) return false;
      const key = `${signal.ticker}::${signal.direction || 'mixed'}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  }, [signals, sourceConfig.ruleKey]);

  const filteredSignals = useMemo(() => {
    const query = deferredSearchQuery.trim().toLowerCase();
    return curatedSignals.filter((signal) => {
      if (directionFilter !== 'all' && signal.direction !== directionFilter) return false;
      if (!query) return true;
      return [signal.ticker, signal.title, signal.summary, signal.sourceLabel, signal.actorPreview || '']
        .join(' ')
        .toLowerCase()
        .includes(query);
    });
  }, [curatedSignals, deferredSearchQuery, directionFilter]);

  const visibleSignals = filteredSignals.slice(0, visibleCount);
  const hasMoreVisibleSignals = visibleSignals.length < filteredSignals.length;

  useEffect(() => {
    if (!selectedCluster || clusterDetailCache.has(selectedCluster.id)) return;

    let active = true;
    requestClusterDetail(selectedCluster.id, accessToken)
      .then((detail) => {
        if (active) setClusterDetail(detail);
      })
      .catch((error) => {
        if (active) {
          setClusterDetailError(error instanceof Error ? error.message : 'Could not load this cluster right now.');
        }
      })
      .finally(() => {
        if (active) setClusterDetailLoading(false);
      });

    return () => {
      active = false;
    };
  }, [accessToken, selectedCluster]);

  function openCluster(signal: ClusterSignal) {
    const cached = clusterDetailCache.get(signal.id) || null;
    setClusterDetail(cached);
    setClusterDetailError('');
    setClusterDetailLoading(!cached);
    setSelectedCluster(signal);
  }

  function prefetchCluster(signal: ClusterSignal) {
    if (clusterDetailCache.has(signal.id)) return;
    void requestClusterDetail(signal.id, accessToken).catch(() => undefined);
  }

  function changeSource(nextSource: ClusterFeedSource) {
    if (nextSource === source) return;
    setSearchQuery('');
    setDirectionFilter('all');
    setVisibleCount(CLUSTERS_PER_PAGE);
    onSourceChange(nextSource);
  }

  function changeDirection(direction: DirectionFilter) {
    setDirectionFilter(direction);
    setVisibleCount(CLUSTERS_PER_PAGE);
  }

  async function loadMoreSignals() {
    if (hasMoreVisibleSignals) {
      setVisibleCount((count) => count + CLUSTERS_PER_PAGE);
      return;
    }
    if (!archiveLoaded) {
      await onLoadMore();
      setVisibleCount((count) => count + CLUSTERS_PER_PAGE);
    }
  }

  return (
    <>
      <div className="space-y-3">
        <section className="rounded-2xl border border-white/[0.07] bg-white/[0.016] p-3.5 sm:p-4">
          <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <div
              role="group"
              aria-label="Cluster source"
              className="inline-grid w-full grid-cols-2 rounded-xl border border-white/[0.07] bg-black/20 p-1 sm:w-auto"
            >
              <SourceButton
                active={source === 'politicians'}
                icon={<Landmark className="h-3.5 w-3.5" />}
                label="Politicians"
                onClick={() => changeSource('politicians')}
              />
              <SourceButton
                active={source === 'insiders'}
                icon={<UserRound className="h-3.5 w-3.5" />}
                label="Insiders"
                onClick={() => changeSource('insiders')}
              />
            </div>
            <div className="text-xs font-medium text-zinc-600">
              {curatedSignals.length.toLocaleString()} high-conviction cluster{curatedSignals.length === 1 ? '' : 's'}
            </div>
          </div>

          <div className="mt-4 border-t border-white/[0.055] pt-4">
            <h2 className="text-base font-semibold text-white">{sourceConfig.title}</h2>
            <p className="mt-1 text-xs leading-5 text-zinc-500">{sourceConfig.description}</p>
          </div>

          <div className="mt-3 flex flex-col gap-2.5 sm:flex-row sm:items-center sm:justify-between">
            <div className="relative w-full sm:max-w-md">
              <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-zinc-600" />
              <input
                type="search"
                aria-label={`Search ${sourceConfig.emptyLabel} clusters`}
                placeholder="Search ticker or company"
                value={searchQuery}
                onChange={(event) => {
                  setSearchQuery(event.target.value);
                  setVisibleCount(CLUSTERS_PER_PAGE);
                }}
                className="h-10 w-full rounded-xl border border-white/[0.08] bg-black/15 pl-10 pr-4 text-sm text-white outline-none transition placeholder:text-zinc-600 focus:border-emerald-400/25 focus:bg-white/[0.025]"
              />
            </div>
            <div
              role="group"
              aria-label="Trade direction"
              className="inline-flex w-fit rounded-xl border border-white/[0.065] bg-black/15 p-1"
            >
              <DirectionButton active={directionFilter === 'all'} label="All" onClick={() => changeDirection('all')} />
              <DirectionButton active={directionFilter === 'buy'} label="Buying" onClick={() => changeDirection('buy')} />
              <DirectionButton active={directionFilter === 'sell'} label="Selling" onClick={() => changeDirection('sell')} />
            </div>
          </div>
        </section>

        {filteredSignals.length ? (
          <div className="space-y-2" aria-live="polite">
            {visibleSignals.map((signal) => {
              const isSell = signal.direction === 'sell';
              return (
                <button
                  key={signal.id}
                  type="button"
                  data-testid="cluster-card"
                  data-cluster-source={source}
                  onClick={() => openCluster(signal)}
                  onMouseEnter={() => prefetchCluster(signal)}
                  onFocus={() => prefetchCluster(signal)}
                  aria-label={`Open ${signal.ticker} cluster: ${clusterHeadline(signal)}`}
                  className="group w-full rounded-2xl border border-white/[0.065] bg-white/[0.012] px-4 py-4 text-left transition hover:border-white/[0.12] hover:bg-white/[0.027] focus:outline-none focus:ring-1 focus:ring-emerald-400/25 sm:px-5"
                >
                  <div className="flex items-start gap-3.5">
                    <TickerLogo ticker={signal.ticker} />
                    <div className="min-w-0 flex-1">
                      <div className="flex flex-wrap items-center gap-2">
                        <span className="text-xs font-bold tracking-[0.15em] text-cyan-100">{signal.ticker}</span>
                        <span
                          className={`rounded-md px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-[0.11em] ${
                            isSell ? 'bg-red-400/[0.09] text-red-300' : 'bg-emerald-400/[0.09] text-emerald-300'
                          }`}
                        >
                          {isSell ? 'Selling' : 'Buying'}
                        </span>
                      </div>
                      <h3 className="mt-1.5 text-[15px] font-semibold leading-6 text-white sm:text-base">
                        {clusterHeadline(signal)}
                      </h3>
                      <p className="mt-0.5 text-xs leading-5 text-zinc-500">{clusterReason(signal)}</p>
                      <div className="mt-2.5 flex flex-wrap gap-x-3 gap-y-1 text-[10px] text-zinc-600">
                        {signal.windowDays ? <span>{signal.windowDays}-day window</span> : null}
                        {signal.amountLabel ? <span>{signal.amountLabel} tracked minimum</span> : null}
                        <span>Updated {formatDateShort(signal.publishedAt)}</span>
                      </div>
                    </div>
                    <ChevronRight className="mt-3 h-4 w-4 shrink-0 text-zinc-700 transition group-hover:translate-x-0.5 group-hover:text-zinc-400" />
                  </div>
                </button>
              );
            })}
          </div>
        ) : (
          <div className="rounded-2xl border border-white/[0.06] bg-white/[0.012] px-5 py-12 text-center">
            <div className="text-sm font-medium text-zinc-300">
              No high-conviction {sourceConfig.emptyLabel} clusters match this view.
            </div>
            <div className="mt-1 text-xs text-zinc-600">Try another ticker or direction.</div>
          </div>
        )}

        {hasMoreVisibleSignals || !archiveLoaded ? (
          <button
            type="button"
            onClick={loadMoreSignals}
            disabled={loadingMore}
            className="inline-flex items-center justify-center gap-2 rounded-xl border border-white/[0.08] bg-white/[0.018] px-4 py-2.5 text-xs font-medium text-zinc-400 transition hover:border-white/[0.14] hover:bg-white/[0.035] hover:text-white disabled:cursor-wait disabled:opacity-60"
          >
            {loadingMore ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : null}
            {loadingMore
              ? `Loading older ${sourceConfig.emptyLabel} clusters…`
              : hasMoreVisibleSignals
                ? `Load ${Math.min(CLUSTERS_PER_PAGE, filteredSignals.length - visibleSignals.length)} more`
                : `Load older ${sourceConfig.emptyLabel} clusters`}
          </button>
        ) : null}
        {loadMoreError ? <p role="alert" className="text-xs text-red-300">{loadMoreError}</p> : null}
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
