'use client';

import Image, { type ImageLoaderProps } from 'next/image';
import { useDeferredValue, useEffect, useMemo, useState } from 'react';
import { ChevronRight, Search, ShieldCheck } from 'lucide-react';

import DashboardClusterModal from '@/components/DashboardClusterModal';
import {
  clusterCategoryLabel,
  clusterEvidenceItems,
  clusterHeadline,
  clusterReason,
} from '@/lib/cluster-presentation';
import { isHighConvictionCluster } from '@/lib/cluster-quality';
import { getTickerLogoUrl } from '@/lib/company-logos';
import type { DashboardClusterDetail } from '@/lib/dashboard-cluster-types';
import { formatCalendarDate } from '@/lib/date-format';

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

const CLUSTERS_PER_PAGE = 12;
const passthroughImageLoader = ({ src }: ImageLoaderProps) => src;
const clusterDetailCache = new Map<string, DashboardClusterDetail>();
const clusterDetailRequests = new Map<string, Promise<DashboardClusterDetail>>();

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

function DirectionPill({
  active,
  children,
  onClick,
  tone = 'neutral',
}: {
  active: boolean;
  children: string;
  onClick: () => void;
  tone?: 'neutral' | 'buy' | 'sell';
}) {
  const activeClass =
    tone === 'buy'
      ? 'border-emerald-400/25 bg-emerald-400/[0.1] text-emerald-300'
      : tone === 'sell'
        ? 'border-red-400/25 bg-red-400/[0.1] text-red-300'
        : 'border-white/[0.12] bg-white/[0.055] text-white';

  return (
    <button
      type="button"
      onClick={onClick}
      aria-pressed={active}
      className={`rounded-lg border px-3 py-2 text-xs font-medium transition ${
        active
          ? activeClass
          : 'border-transparent text-zinc-500 hover:border-white/[0.08] hover:text-zinc-300'
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
      <div className="h-10 w-10 shrink-0 overflow-hidden rounded-xl border border-white/10 bg-black/30">
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
      </div>
    );
  }

  return (
    <div
      aria-hidden="true"
      className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl border border-white/10 text-[12px] font-bold text-white"
      style={{
        background: `linear-gradient(135deg, hsl(${hue},65%,45%), hsl(${(hue + 40) % 360},65%,38%))`,
      }}
    >
      {ticker.slice(0, 2)}
    </div>
  );
}

function formatDateShort(value: string | null | undefined) {
  if (!value) return '—';
  return formatCalendarDate(value, 'UTC');
}

function evidenceTone(key: ReturnType<typeof clusterEvidenceItems>[number]['key']) {
  if (key === 'insiders') return 'border-violet-400/15 bg-violet-400/[0.06] text-violet-200/75';
  if (key === 'congress') return 'border-cyan-400/15 bg-cyan-400/[0.06] text-cyan-200/75';
  if (key === 'funds') return 'border-emerald-400/15 bg-emerald-400/[0.06] text-emerald-200/75';
  return 'border-white/[0.07] bg-white/[0.025] text-zinc-500';
}

export default function ClustersPage({ signals, accessToken }: { signals: ClusterSignal[]; accessToken?: string }) {
  const [searchQuery, setSearchQuery] = useState('');
  const deferredSearchQuery = useDeferredValue(searchQuery);
  const [directionFilter, setDirectionFilter] = useState<DirectionFilter>('all');
  const [visibleCount, setVisibleCount] = useState(CLUSTERS_PER_PAGE);
  const [selectedCluster, setSelectedCluster] = useState<ClusterSignal | null>(null);
  const [clusterDetail, setClusterDetail] = useState<DashboardClusterDetail | null>(null);
  const [clusterDetailLoading, setClusterDetailLoading] = useState(false);
  const [clusterDetailError, setClusterDetailError] = useState('');

  const curatedSignals = useMemo(() => {
    const seen = new Set<string>();
    return signals.filter((signal) => {
      if (!isHighConvictionCluster(signal)) return false;
      const key = `${signal.ticker}::${signal.direction || 'mixed'}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  }, [signals]);

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

  useEffect(() => {
    if (!selectedCluster) return;

    const cached = clusterDetailCache.get(selectedCluster.id);
    if (cached) {
      return;
    }

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

  function setDirection(direction: DirectionFilter) {
    setDirectionFilter(direction);
    setVisibleCount(CLUSTERS_PER_PAGE);
  }

  return (
    <>
      <div className="space-y-3">
        <div className="rounded-2xl border border-white/[0.07] bg-white/[0.018] p-3.5 sm:p-4">
          <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
            <div className="flex items-center gap-2.5 text-xs text-zinc-500">
              <div className="flex h-8 w-8 items-center justify-center rounded-lg border border-emerald-400/15 bg-emerald-400/[0.07] text-emerald-300">
                <ShieldCheck className="h-4 w-4" />
              </div>
              <div>
                <div className="font-semibold text-zinc-200">Strict high-conviction feed</div>
                <div className="mt-0.5">Distinct actors, material activity, one story per stock and direction.</div>
              </div>
            </div>
            <div className="text-xs font-medium text-zinc-500">
              {curatedSignals.length.toLocaleString()} active signal{curatedSignals.length === 1 ? '' : 's'}
            </div>
          </div>

          <div className="mt-3 flex flex-col gap-2.5 border-t border-white/[0.055] pt-3 sm:flex-row sm:items-center sm:justify-between">
            <div className="relative w-full sm:max-w-md">
              <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-zinc-600" />
              <input
                type="search"
                placeholder="Search a ticker or company"
                value={searchQuery}
                onChange={(event) => {
                  setSearchQuery(event.target.value);
                  setVisibleCount(CLUSTERS_PER_PAGE);
                }}
                className="h-10 w-full rounded-xl border border-white/[0.08] bg-black/15 pl-10 pr-4 text-sm text-white outline-none transition placeholder:text-zinc-600 focus:border-emerald-400/25 focus:bg-white/[0.025]"
              />
            </div>

            <div className="flex w-fit items-center rounded-xl border border-white/[0.065] bg-black/15 p-1">
              <DirectionPill active={directionFilter === 'all'} onClick={() => setDirection('all')}>
                All
              </DirectionPill>
              <DirectionPill active={directionFilter === 'buy'} tone="buy" onClick={() => setDirection('buy')}>
                Buying
              </DirectionPill>
              <DirectionPill active={directionFilter === 'sell'} tone="sell" onClick={() => setDirection('sell')}>
                Selling
              </DirectionPill>
            </div>
          </div>
        </div>

        {filteredSignals.length ? (
          <div className="space-y-2">
            {visibleSignals.map((signal) => {
              const isSell = signal.direction === 'sell';
              const evidence = clusterEvidenceItems(signal);

              return (
                <button
                  key={signal.id}
                  type="button"
                  data-testid="cluster-card"
                  onClick={() => openCluster(signal)}
                  onMouseEnter={() => prefetchCluster(signal)}
                  onFocus={() => prefetchCluster(signal)}
                  aria-label={`Open ${signal.ticker} cluster: ${clusterHeadline(signal)}`}
                  className="group w-full rounded-2xl border border-white/[0.065] bg-white/[0.014] px-4 py-4 text-left transition hover:border-white/[0.11] hover:bg-white/[0.028] focus:outline-none focus:ring-1 focus:ring-emerald-400/25 sm:px-5"
                >
                  <div className="flex items-start gap-3.5">
                    <TickerLogo ticker={signal.ticker} />

                    <div className="min-w-0 flex-1">
                      <div className="flex flex-wrap items-center gap-2">
                        <span className="text-xs font-bold tracking-[0.16em] text-cyan-100">{signal.ticker}</span>
                        <span className="text-[10px] font-semibold uppercase tracking-[0.12em] text-zinc-600">
                          {clusterCategoryLabel(signal)}
                        </span>
                        <span
                          className={`rounded-md px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-[0.12em] ${
                            isSell ? 'bg-red-400/[0.09] text-red-300' : 'bg-emerald-400/[0.09] text-emerald-300'
                          }`}
                        >
                          {isSell ? 'Selling' : 'Buying'}
                        </span>
                      </div>

                      <h2 className="mt-1.5 text-[15px] font-semibold leading-6 text-white sm:text-base">
                        {clusterHeadline(signal)}
                      </h2>
                      <p className="mt-1 text-xs leading-5 text-zinc-500">{clusterReason(signal)}</p>

                      <div className="mt-2.5 flex flex-wrap gap-1.5">
                        {evidence.map((item) => (
                          <span
                            key={item.key}
                            className={`rounded-md border px-2 py-1 text-[10px] font-medium ${evidenceTone(item.key)}`}
                          >
                            {item.label}
                          </span>
                        ))}
                      </div>
                    </div>

                    <div className="hidden shrink-0 items-center gap-4 sm:flex">
                      <div className="text-right">
                        {signal.amountLabel ? (
                          <>
                            <div className="text-[10px] font-medium uppercase tracking-[0.12em] text-zinc-700">Tracked floor</div>
                            <div className="mt-1 text-sm font-semibold tabular-nums text-zinc-200">{signal.amountLabel}</div>
                          </>
                        ) : null}
                        <div className={`${signal.amountLabel ? 'mt-2' : ''} text-[11px] text-zinc-600`}>
                          Updated {formatDateShort(signal.publishedAt)}
                        </div>
                      </div>
                      <ChevronRight className="h-4 w-4 text-zinc-700 transition group-hover:translate-x-0.5 group-hover:text-zinc-400" />
                    </div>
                  </div>

                  <div className="mt-3 flex items-center justify-between border-t border-white/[0.045] pt-2.5 text-[11px] text-zinc-600 sm:hidden">
                    <span>{signal.amountLabel ? `Tracked floor ${signal.amountLabel}` : `Updated ${formatDateShort(signal.publishedAt)}`}</span>
                    <ChevronRight className="h-3.5 w-3.5" />
                  </div>
                </button>
              );
            })}
          </div>
        ) : (
          <div className="rounded-2xl border border-white/[0.06] bg-white/[0.014] px-5 py-12 text-center">
            <div className="text-sm font-medium text-zinc-300">No high-conviction clusters match this view.</div>
            <div className="mt-1 text-xs text-zinc-600">Try another ticker or switch the direction filter.</div>
          </div>
        )}

        {visibleSignals.length < filteredSignals.length ? (
          <button
            type="button"
            onClick={() => setVisibleCount((count) => count + CLUSTERS_PER_PAGE)}
            className="rounded-xl border border-white/[0.08] bg-white/[0.018] px-4 py-2.5 text-xs font-medium text-zinc-400 transition hover:border-white/[0.14] hover:bg-white/[0.035] hover:text-white"
          >
            Show more
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
