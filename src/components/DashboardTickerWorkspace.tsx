'use client';

import Image, { type ImageLoaderProps } from 'next/image';
import Link from 'next/link';
import { useEffect, useState } from 'react';
import {
  ArrowUpRight,
  BriefcaseBusiness,
  Building2,
  Landmark,
  Loader2,
  RefreshCcw,
  SearchX,
  ShieldAlert,
  X,
} from 'lucide-react';

import PoliticianHeadshot from '@/components/PoliticianHeadshot';
import SignalActionButton from '@/components/SignalActionButton';
import { getTickerLogoUrl } from '@/lib/company-logos';
import type {
  DashboardTickerActivity,
  DashboardTickerActivityDirection,
  DashboardTickerActivityFilter,
  DashboardTickerWorkspaceData,
} from '@/lib/ticker-workspace-types';
import { formatCalendarDate } from '@/lib/date-format';

type DashboardTickerWorkspaceProps = {
  data: DashboardTickerWorkspaceData | null;
  requestedTicker: string;
  loading: boolean;
  error: string;
  onRetry: () => void;
  onDismiss: () => void;
  onOpenPriceAlert: () => void;
};

const ACTIVITY_PAGE_SIZE = 10;
const passthroughImageLoader = ({ src }: ImageLoaderProps) => src;
const ACTIVITY_TABS: Array<{ key: DashboardTickerActivityFilter; label: string }> = [
  { key: 'all', label: 'All' },
  { key: 'politician', label: 'Politicians' },
  { key: 'insider', label: 'Insiders' },
  { key: 'fund', label: 'Hedge Funds' },
];

type ActivityTabState = Record<
  DashboardTickerActivityFilter,
  {
    activity: DashboardTickerActivity[];
    nextOffset: number | null;
    loaded: boolean;
  }
>;

function createEmptyTabState(): ActivityTabState {
  return {
    all: { activity: [], nextOffset: null, loaded: false },
    politician: { activity: [], nextOffset: null, loaded: false },
    insider: { activity: [], nextOffset: null, loaded: false },
    fund: { activity: [], nextOffset: null, loaded: false },
  };
}

function tabLabel(tab: DashboardTickerActivityFilter) {
  return ACTIVITY_TABS.find((item) => item.key === tab)?.label || 'All';
}

function directionClass(direction: DashboardTickerActivityDirection) {
  if (direction === 'buy' || direction === 'increase' || direction === 'new') {
    return 'border-emerald-400/20 bg-emerald-400/10 text-emerald-300';
  }
  if (direction === 'sell' || direction === 'decrease' || direction === 'exit') {
    return 'border-red-400/20 bg-red-400/10 text-red-300';
  }
  return 'border-white/10 bg-white/[0.04] text-zinc-300';
}

function sourceClass(sourceType: DashboardTickerActivity['sourceType']) {
  if (sourceType === 'politician') {
    return 'border-blue-400/20 bg-blue-400/10 text-blue-300';
  }
  if (sourceType === 'insider') {
    return 'border-amber-400/20 bg-amber-400/10 text-amber-300';
  }
  return 'border-emerald-400/20 bg-emerald-400/10 text-emerald-300';
}

function sourceLabel(sourceType: DashboardTickerActivity['sourceType']) {
  if (sourceType === 'politician') return 'Congress';
  if (sourceType === 'insider') return 'Insider';
  return 'Fund';
}

function sourceIcon(sourceType: DashboardTickerActivity['sourceType']) {
  if (sourceType === 'politician') return <Landmark className="h-4 w-4" />;
  if (sourceType === 'insider') return <ShieldAlert className="h-4 w-4" />;
  return <BriefcaseBusiness className="h-4 w-4" />;
}

function TickerLogo({ symbol, size = 64 }: { symbol: string; size?: number }) {
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  const logoUrl = getTickerLogoUrl(symbol, size);
  const activeLogoUrl = logoUrl && failedUrl !== logoUrl ? logoUrl : null;

  if (activeLogoUrl) {
    return (
      <div
        className="flex shrink-0 items-center justify-center overflow-hidden rounded-2xl border border-white/10 bg-black/35"
        style={{ width: size, height: size }}
      >
        <Image
          loader={passthroughImageLoader}
          unoptimized
          src={activeLogoUrl}
          alt={`${symbol} logo`}
          width={size}
          height={size}
          sizes={`${size}px`}
          className="h-full w-full object-contain p-2"
          onError={() => setFailedUrl(activeLogoUrl)}
        />
      </div>
    );
  }

  return (
    <div
      className="flex shrink-0 items-center justify-center rounded-2xl border border-white/10 bg-white/[0.04] text-sm font-semibold tracking-[0.18em] text-zinc-300"
      style={{ width: size, height: size }}
    >
      {symbol.slice(0, 3)}
    </div>
  );
}

function ActivityAvatar({ item }: { item: DashboardTickerActivity }) {
  if (item.sourceType === 'politician' && item.memberId) {
    return (
      <PoliticianHeadshot
        memberId={item.memberId}
        name={item.actorName}
        party={item.party}
        size={40}
      />
    );
  }

  return (
    <div className={`flex h-10 w-10 shrink-0 items-center justify-center rounded-xl border ${sourceClass(item.sourceType)}`}>
      {sourceIcon(item.sourceType)}
    </div>
  );
}

function SourcePill({ item }: { item: DashboardTickerActivity }) {
  return (
    <span className={`inline-flex rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-[0.12em] ${sourceClass(item.sourceType)}`}>
      {sourceLabel(item.sourceType)}
    </span>
  );
}

function fundActionLabel(direction: DashboardTickerActivityDirection) {
  if (direction === 'increase') return 'Increase';
  if (direction === 'decrease') return 'Decrease';
  if (direction === 'new') return 'New';
  if (direction === 'exit') return 'Exit';
  if (direction === 'flat') return 'Neutral';
  return '13F';
}

function fundDeltaClass(direction: DashboardTickerActivityDirection) {
  if (direction === 'increase' || direction === 'new') {
    return 'text-emerald-300';
  }
  if (direction === 'decrease' || direction === 'exit') {
    return 'text-red-300';
  }
  return 'text-zinc-400';
}

function FundActivityRow({ item }: { item: DashboardTickerActivity }) {
  const filingDate = item.filingDate || item.date;
  const changeLabel = item.direction === 'new'
    ? 'Position opened'
    : item.amountLabel || 'Position change unavailable';

  return (
    <div className="group flex min-h-[74px] items-center gap-3 border-b border-white/[0.06] px-4 py-3 transition hover:bg-white/[0.025] last:border-b-0 sm:px-5">
      <ActivityAvatar item={item} />

      <div className="min-w-0 flex-1">
        <div className="flex min-w-0 items-center gap-2">
          <div className="truncate text-[15px] font-semibold text-zinc-100">{item.actorName}</div>
          <SourcePill item={item} />
        </div>
        <div className="mt-1.5 flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1 text-xs">
          <span className={`inline-flex whitespace-nowrap rounded-full border px-2 py-0.5 font-semibold ${directionClass(item.direction)}`}>
            {fundActionLabel(item.direction)}
          </span>
          {filingDate ? (
            <span className="whitespace-nowrap tabular-nums text-zinc-500">
              {formatCalendarDate(filingDate)}
            </span>
          ) : null}
          <span className={`truncate font-medium tabular-nums ${fundDeltaClass(item.direction)}`}>
            {changeLabel}
          </span>
        </div>
      </div>

      <div className="hidden min-w-[118px] text-right sm:block">
        <div className="text-sm font-semibold tabular-nums text-zinc-100">{item.metricLabel || '—'}</div>
        <div className="mt-0.5 whitespace-nowrap text-xs tabular-nums text-zinc-500">{item.secondaryMetricLabel || '—'}</div>
      </div>

      {item.sourceUrl ? (
        <Link
          href={item.sourceUrl}
          target="_blank"
          rel="noreferrer"
          className="inline-flex shrink-0 items-center justify-center gap-1 rounded-full border border-white/10 bg-white/[0.03] px-3 py-1.5 text-xs font-semibold text-zinc-300 transition hover:border-cyan-400/40 hover:text-cyan-300"
        >
          Source <ArrowUpRight className="h-3.5 w-3.5" />
        </Link>
      ) : null}
    </div>
  );
}

function ActivityRow({ item }: { item: DashboardTickerActivity }) {
  if (item.sourceType === 'fund') {
    return <FundActivityRow item={item} />;
  }

  return (
    <div className="group flex min-h-[74px] items-center gap-3 border-b border-white/[0.06] px-4 py-3 transition hover:bg-white/[0.025] last:border-b-0 sm:px-5">
      <ActivityAvatar item={item} />

      <div className="min-w-0 flex-1">
        <div className="flex min-w-0 items-center gap-2">
          <div className="truncate text-[15px] font-semibold text-zinc-100">{item.actorName}</div>
          <SourcePill item={item} />
        </div>
        <div className="mt-1.5 flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1 text-xs">
          <span className={`inline-flex whitespace-nowrap rounded-full border px-2 py-0.5 font-semibold ${directionClass(item.direction)}`}>
            {item.directionLabel}
          </span>
          {item.date ? (
            <span className="whitespace-nowrap tabular-nums text-zinc-500">
              {formatCalendarDate(item.date)}
            </span>
          ) : null}
          {item.actorSubtitle ? (
            <span className="truncate text-zinc-500">{item.actorSubtitle}</span>
          ) : null}
        </div>
      </div>

      <div className="hidden min-w-[132px] text-right sm:block">
        <div className="whitespace-nowrap text-sm font-semibold tabular-nums text-zinc-100">{item.amountLabel || item.metricLabel || '—'}</div>
        <div className="mt-0.5 text-[10px] font-semibold uppercase tracking-[0.14em] text-zinc-600">
          {item.metricCaption || (item.amountLabel ? 'reported' : 'amount')}
        </div>
      </div>

      {item.sourceUrl ? (
        <Link
          href={item.sourceUrl}
          target="_blank"
          rel="noreferrer"
          className="inline-flex shrink-0 items-center justify-center gap-1 rounded-full border border-white/10 bg-white/[0.03] px-3 py-1.5 text-xs font-semibold text-zinc-300 transition hover:border-cyan-400/40 hover:text-cyan-300"
        >
          Source <ArrowUpRight className="h-3.5 w-3.5" />
        </Link>
      ) : null}
    </div>
  );
}

function LoadingState({ requestedTicker, onDismiss }: Pick<DashboardTickerWorkspaceProps, 'requestedTicker' | 'onDismiss'>) {
  return (
    <section className="dash-fade-in overflow-hidden rounded-[1.75rem] border border-white/[0.06] bg-white/[0.025] p-4 shadow-[0_24px_80px_rgba(0,0,0,0.35)] sm:p-6">
      <div className="flex items-start justify-between gap-4">
        <div className="flex items-center gap-4">
          <TickerLogo symbol={requestedTicker} />
          <div>
            <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-zinc-600">Stock workspace</p>
            <h2 className="mt-2 text-3xl font-semibold tracking-tight text-white">{requestedTicker}</h2>
          </div>
        </div>
        <button
          type="button"
          onClick={onDismiss}
          className="flex h-10 w-10 items-center justify-center rounded-full border border-white/[0.08] bg-white/[0.04] text-zinc-500 transition hover:border-white/[0.16] hover:bg-white/[0.08] hover:text-white"
          aria-label="Close stock workspace"
        >
          <X className="h-5 w-5" />
        </button>
      </div>
      <div className="mt-8 flex min-h-[180px] items-center justify-center rounded-3xl border border-white/10 bg-white/[0.025] text-sm text-zinc-500">
        <Loader2 className="mr-2 h-4 w-4 animate-spin" />
        Loading latest activity...
      </div>
    </section>
  );
}

function ErrorState({
  requestedTicker,
  error,
  onRetry,
  onDismiss,
}: Pick<DashboardTickerWorkspaceProps, 'requestedTicker' | 'error' | 'onRetry' | 'onDismiss'>) {
  return (
    <section className="dash-fade-in overflow-hidden rounded-[1.75rem] border border-white/[0.06] bg-white/[0.025] p-4 shadow-[0_24px_80px_rgba(0,0,0,0.35)] sm:p-6">
      <div className="flex items-start justify-between gap-4">
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-zinc-600">Stock workspace</p>
          <h2 className="mt-2 text-3xl font-semibold tracking-tight text-white">{requestedTicker}</h2>
        </div>
        <button
          type="button"
          onClick={onDismiss}
          className="flex h-10 w-10 items-center justify-center rounded-full border border-white/[0.08] bg-white/[0.04] text-zinc-500 transition hover:border-white/[0.16] hover:bg-white/[0.08] hover:text-white"
          aria-label="Close stock workspace"
        >
          <X className="h-5 w-5" />
        </button>
      </div>
      <div className="mt-8 rounded-3xl border border-red-500/15 bg-red-500/[0.04] p-6 text-sm text-red-200">
        <div className="flex items-center gap-2 font-semibold">
          <SearchX className="h-4 w-4" />
          Could not load {requestedTicker}
        </div>
        <p className="mt-2 text-red-200/70">{error || 'Try again in a moment.'}</p>
        <button
          type="button"
          onClick={onRetry}
          className="mt-4 inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/[0.04] px-4 py-2 text-sm font-medium text-white transition hover:border-white/20 hover:bg-white/[0.08]"
        >
          <RefreshCcw className="h-4 w-4" />
          Retry
        </button>
      </div>
    </section>
  );
}

export default function DashboardTickerWorkspace({
  data,
  requestedTicker,
  loading,
  error,
  onRetry,
  onDismiss,
  onOpenPriceAlert,
}: DashboardTickerWorkspaceProps) {
  const [activeTab, setActiveTab] = useState<DashboardTickerActivityFilter>('all');
  const [tabState, setTabState] = useState<ActivityTabState>(() => createEmptyTabState());
  const [loadingTab, setLoadingTab] = useState<DashboardTickerActivityFilter | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const [loadMoreError, setLoadMoreError] = useState('');

  useEffect(() => {
    const nextState = createEmptyTabState();
    if (data) {
      nextState.all = {
        activity: data.recentActivity,
        nextOffset: data.nextOffset,
        loaded: true,
      };
    }
    setActiveTab('all');
    setTabState(nextState);
    setLoadingTab(null);
    setLoadMoreError('');
    setLoadingMore(false);
  }, [data]);

  useEffect(() => {
    if (!data || activeTab === 'all' || tabState[activeTab].loaded) {
      return;
    }

    let cancelled = false;
    const controller = new AbortController();
    const loadTab = async () => {
      setLoadingTab(activeTab);
      setLoadMoreError('');
      try {
        const response = await fetch(
          `/api/ticker-workspace/${encodeURIComponent(data.symbol)}/lite?source=${activeTab}&limit=${ACTIVITY_PAGE_SIZE}`,
          { signal: controller.signal, cache: 'no-store' },
        );
        if (!response.ok) {
          const payload = (await response.json().catch(() => null)) as { error?: string } | null;
          throw new Error(payload?.error || `Could not load ${tabLabel(activeTab).toLowerCase()}.`);
        }
        const payload = (await response.json()) as DashboardTickerWorkspaceData;
        if (cancelled) {
          return;
        }
        setTabState((current) => ({
          ...current,
          [activeTab]: {
            activity: payload.recentActivity,
            nextOffset: payload.nextOffset,
            loaded: true,
          },
        }));
      } catch (tabError) {
        if (cancelled || (tabError instanceof Error && tabError.name === 'AbortError')) {
          return;
        }
        setLoadMoreError(tabError instanceof Error ? tabError.message : `Could not load ${tabLabel(activeTab).toLowerCase()}.`);
      } finally {
        if (!cancelled) {
          setLoadingTab((current) => (current === activeTab ? null : current));
        }
      }
    };

    void loadTab();
    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [activeTab, data, tabState]);

  if (loading && !data) {
    return <LoadingState requestedTicker={requestedTicker} onDismiss={onDismiss} />;
  }

  if (error && !data) {
    return <ErrorState requestedTicker={requestedTicker} error={error} onRetry={onRetry} onDismiss={onDismiss} />;
  }

  if (!data) {
    return null;
  }

  const currentTabState = tabState[activeTab];
  const activity = currentTabState.activity;
  const nextOffset = currentTabState.nextOffset;
  const tabIsLoading = loadingTab === activeTab;
  const currentTabLabel = tabLabel(activeTab);
  const latestActivityDate = activity[0]?.date || (activeTab === 'all' ? data.latestActivityDate : null);

  const handleLoadMore = async () => {
    if (nextOffset == null || loadingMore) {
      return;
    }

    setLoadingMore(true);
    setLoadMoreError('');
    try {
      const response = await fetch(
        `/api/ticker-workspace/${encodeURIComponent(data.symbol)}/lite?source=${activeTab}&offset=${nextOffset}&limit=${ACTIVITY_PAGE_SIZE}`,
        { cache: 'no-store' },
      );
      if (!response.ok) {
        const payload = (await response.json().catch(() => null)) as { error?: string } | null;
        throw new Error(payload?.error || 'Could not load more activity.');
      }
      const payload = (await response.json()) as DashboardTickerWorkspaceData;
      setTabState((current) => {
        const activeState = current[activeTab];
        const seen = new Set(activeState.activity.map((item) => item.id));
        const additions = payload.recentActivity.filter((item) => !seen.has(item.id));
        return {
          ...current,
          [activeTab]: {
            activity: [...activeState.activity, ...additions],
            nextOffset: payload.nextOffset,
            loaded: true,
          },
        };
      });
    } catch (loadError) {
      setLoadMoreError(loadError instanceof Error ? loadError.message : 'Could not load more activity.');
    } finally {
      setLoadingMore(false);
    }
  };

  return (
    <section className="dash-fade-in overflow-hidden rounded-[1.75rem] border border-white/[0.06] bg-white/[0.025] p-4 shadow-[0_24px_80px_rgba(0,0,0,0.35)] sm:p-6">
      <div className="flex flex-col gap-5 sm:flex-row sm:items-start sm:justify-between">
        <div className="flex min-w-0 items-center gap-4">
          <TickerLogo symbol={data.symbol} />
          <div className="min-w-0">
            <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-zinc-600">Stock workspace</p>
            <h2 className="mt-2 truncate text-3xl font-semibold tracking-tight text-white">
              {data.symbol}
            </h2>
            <p className="mt-1 truncate text-sm text-zinc-500">
              {[data.companyName, data.sector || data.industry].filter(Boolean).join(' · ')}
            </p>
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2 sm:justify-end">
          <SignalActionButton onClick={onOpenPriceAlert} />
          <button
            type="button"
            onClick={onDismiss}
            className="flex h-10 w-10 items-center justify-center rounded-full border border-white/[0.08] bg-white/[0.04] text-zinc-500 transition hover:border-white/[0.16] hover:bg-white/[0.08] hover:text-white"
            aria-label="Close stock workspace"
          >
            <X className="h-5 w-5" />
          </button>
        </div>
      </div>

      <div className="mt-8 grid gap-3 sm:grid-cols-3">
        <div className="rounded-2xl border border-white/[0.06] bg-black/20 p-4">
          <span className="block text-[10px] font-semibold uppercase tracking-[0.18em] text-zinc-600">Total transactions</span>
          <strong className="mt-2 block text-lg font-semibold text-white">{activity.length}</strong>
        </div>
        <div className="rounded-2xl border border-white/[0.06] bg-black/20 p-4">
          <span className="block text-[10px] font-semibold uppercase tracking-[0.18em] text-zinc-600">Latest activity</span>
          <strong className="mt-2 block text-lg font-semibold text-white">{latestActivityDate ? formatCalendarDate(latestActivityDate) : 'No data'}</strong>
        </div>
        <div className="rounded-2xl border border-white/[0.06] bg-black/20 p-4">
          <span className="block text-[10px] font-semibold uppercase tracking-[0.18em] text-zinc-600">View</span>
          <strong className="mt-2 block text-lg font-semibold text-white">{currentTabLabel}</strong>
        </div>
      </div>

      <div className="mt-8 overflow-hidden rounded-3xl border border-white/10 bg-white/[0.025]">
        <div className="flex items-center justify-between border-b border-white/[0.06] px-4 py-4 sm:px-5">
          <div>
            <h3 className="text-base font-semibold text-white">Recent Activity</h3>
            <p className="mt-1 text-xs text-zinc-500">
              Load more from all sources or focus on one data stream.
            </p>
          </div>
          <Building2 className="hidden h-5 w-5 text-zinc-600 sm:block" />
        </div>

        <div className="flex flex-wrap gap-2 border-b border-white/[0.06] px-4 py-3 sm:px-5">
          {ACTIVITY_TABS.map((tab) => {
            const selected = activeTab === tab.key;
            return (
              <button
                key={tab.key}
                type="button"
                onClick={() => {
                  setActiveTab(tab.key);
                  setLoadMoreError('');
                }}
                className={`rounded-full border px-3 py-1.5 text-xs font-semibold transition ${
                  selected
                    ? 'border-emerald-400/25 bg-emerald-400/10 text-emerald-200'
                    : 'border-white/[0.08] bg-white/[0.025] text-zinc-500 hover:border-white/[0.16] hover:text-zinc-200'
                }`}
              >
                {tab.label}
              </button>
            );
          })}
        </div>

        {tabIsLoading && !activity.length ? (
          <div className="flex min-h-[180px] items-center justify-center px-6 text-sm text-zinc-500">
            <Loader2 className="mr-2 h-4 w-4 animate-spin text-emerald-300" />
            Loading {currentTabLabel.toLowerCase()}...
          </div>
        ) : activity.length ? (
          <div>
            {activity.map((item) => (
              <ActivityRow key={item.id} item={item} />
            ))}
          </div>
        ) : (
          <div className="flex min-h-[180px] flex-col items-center justify-center px-6 text-center text-sm text-zinc-500">
            <SearchX className="mb-3 h-8 w-8 text-zinc-700" />
            No recent {currentTabLabel.toLowerCase()} activity found for {data.symbol}.
          </div>
        )}
      </div>

      {loadMoreError ? (
        <p className="mt-3 text-sm text-red-300">{loadMoreError}</p>
      ) : null}

      {nextOffset != null ? (
        <button
          type="button"
          onClick={handleLoadMore}
          disabled={loadingMore}
          className="mx-auto mt-5 inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/[0.04] px-4 py-2 text-sm font-medium text-white transition hover:border-white/20 hover:bg-white/[0.08] disabled:cursor-not-allowed disabled:opacity-60"
        >
          {loadingMore ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
          {loadingMore ? 'Loading...' : `Load 10 more ${activeTab === 'all' ? '' : currentTabLabel.toLowerCase()}`}
        </button>
      ) : null}
    </section>
  );
}
