'use client';

import Image, { type ImageLoaderProps } from 'next/image';
import Link from 'next/link';
import { useEffect, useState } from 'react';
import {
  ArrowUpRight,
  BriefcaseBusiness,
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
  DashboardCongressOverviewData,
  DashboardCongressOverviewRange,
  DashboardCongressTransaction,
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
const CONGRESS_RANGES: Array<{ key: DashboardCongressOverviewRange; label: string }> = [
  { key: '7d', label: '7 days' },
  { key: '30d', label: '30 days' },
  { key: 'ytd', label: 'YTD' },
];
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

type CongressOverviewState = Record<
  DashboardCongressOverviewRange,
  {
    data: DashboardCongressOverviewData | null;
    error: string;
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

function createEmptyCongressOverviewState(): CongressOverviewState {
  return {
    '7d': { data: null, error: '', loaded: false },
    '30d': { data: null, error: '', loaded: false },
    ytd: { data: null, error: '', loaded: false },
  };
}

function formatMinimumCurrency(value: number) {
  if (!Number.isFinite(value) || value <= 0) return '$0';
  const formatter = new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    notation: value >= 1_000 ? 'compact' : 'standard',
    maximumFractionDigits: value >= 1_000_000 ? 1 : 0,
  });
  return `${formatter.format(value)}+`;
}

function countLabel(count: number, singular: string, plural = `${singular}s`) {
  return `${count.toLocaleString()} ${count === 1 ? singular : plural}`;
}

function tabLabel(tab: DashboardTickerActivityFilter) {
  return ACTIVITY_TABS.find((item) => item.key === tab)?.label || 'All';
}

function directionTextClass(direction: DashboardTickerActivityDirection) {
  if (direction === 'buy' || direction === 'increase' || direction === 'new') {
    return 'text-emerald-300';
  }
  if (direction === 'sell' || direction === 'decrease' || direction === 'exit') {
    return 'text-red-300';
  }
  return 'text-zinc-400';
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

function sourceTextClass(sourceType: DashboardTickerActivity['sourceType']) {
  if (sourceType === 'politician') return 'text-blue-300';
  if (sourceType === 'insider') return 'text-amber-300';
  return 'text-emerald-300';
}

function sourceIcon(sourceType: DashboardTickerActivity['sourceType']) {
  if (sourceType === 'politician') return <Landmark className="h-4 w-4" />;
  if (sourceType === 'insider') return <ShieldAlert className="h-4 w-4" />;
  return <BriefcaseBusiness className="h-4 w-4" />;
}

function TickerLogo({ symbol, size = 52 }: { symbol: string; size?: number }) {
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  const logoUrl = getTickerLogoUrl(symbol, size);
  const activeLogoUrl = logoUrl && failedUrl !== logoUrl ? logoUrl : null;

  if (activeLogoUrl) {
    return (
      <div
        className="flex shrink-0 items-center justify-center overflow-hidden rounded-xl border border-white/10 bg-zinc-100"
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
      className="flex shrink-0 items-center justify-center rounded-xl border border-white/10 bg-white/[0.04] text-sm font-semibold tracking-[0.18em] text-zinc-300"
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
        size={36}
      />
    );
  }

  return (
    <div className={`flex h-9 w-9 shrink-0 items-center justify-center rounded-lg border ${sourceClass(item.sourceType)}`}>
      {sourceIcon(item.sourceType)}
    </div>
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

function ActivityRow({ item, showSource }: { item: DashboardTickerActivity; showSource: boolean }) {
  const isFund = item.sourceType === 'fund';
  const displayDate = isFund ? item.filingDate || item.date : item.date;
  const actionLabel = isFund ? fundActionLabel(item.direction) : item.directionLabel;
  const contextLabel = isFund
    ? item.direction === 'new'
      ? 'Position opened'
      : item.amountLabel || 'Position change unavailable'
    : item.actorSubtitle;
  const primaryMetric = isFund ? item.metricLabel : item.amountLabel || item.metricLabel;
  const secondaryMetric = isFund
    ? item.secondaryMetricLabel
    : item.metricCaption || (item.amountLabel ? 'reported range' : null);

  const metric = (
    <div className="min-w-[96px] max-w-[150px] text-right sm:min-w-[132px]">
      <div className="flex items-center justify-end gap-1 whitespace-nowrap text-xs font-semibold tabular-nums text-white sm:text-sm">
        <span>{primaryMetric || '—'}</span>
        {item.sourceUrl ? <ArrowUpRight className="h-3 w-3 text-zinc-600" /> : null}
      </div>
      {secondaryMetric ? (
        <div className="mt-0.5 truncate text-[9px] font-medium uppercase tracking-[0.08em] text-zinc-600 sm:text-[10px]">
          {secondaryMetric}
        </div>
      ) : null}
    </div>
  );

  return (
    <div className="group flex min-h-[68px] items-center gap-3 border-b border-white/[0.055] px-4 py-3 transition hover:bg-white/[0.025] last:border-b-0 sm:px-5">
      <ActivityAvatar item={item} />

      <div className="min-w-0 flex-1">
        <div className="flex min-w-0 items-center gap-2">
          <div className="truncate text-sm font-semibold text-zinc-100 sm:text-[15px]">{item.actorName}</div>
          {showSource ? (
            <span className={`shrink-0 text-[9px] font-semibold uppercase tracking-[0.12em] ${sourceTextClass(item.sourceType)}`}>
              {sourceLabel(item.sourceType)}
            </span>
          ) : null}
        </div>
        <div className="mt-1 flex min-w-0 items-center gap-x-2 text-[11px] sm:text-xs">
          <span className={`shrink-0 font-semibold ${directionTextClass(item.direction)}`}>
            {actionLabel}
          </span>
          {displayDate ? (
            <span className="shrink-0 tabular-nums text-zinc-300">
              {formatCalendarDate(displayDate)}
            </span>
          ) : null}
          {contextLabel ? (
            <span className={`truncate ${isFund ? fundDeltaClass(item.direction) : 'text-zinc-600'}`}>
              {contextLabel}
            </span>
          ) : null}
        </div>
      </div>

      {item.sourceUrl ? (
        <Link
          href={item.sourceUrl}
          target="_blank"
          rel="noreferrer"
          aria-label={`Open source filing for ${item.actorName}`}
          className="shrink-0 transition hover:text-cyan-200"
        >
          {metric}
        </Link>
      ) : metric}
    </div>
  );
}

function CongressTransactionRow({ transaction }: { transaction: DashboardCongressTransaction }) {
  const isBuy = transaction.direction === 'buy';
  const amount = transaction.amountRange || (transaction.amountFloor ? formatMinimumCurrency(transaction.amountFloor) : '—');
  const metric = (
    <div className="min-w-[116px] text-right">
      <div className="flex items-center justify-end gap-1 whitespace-nowrap text-xs font-semibold tabular-nums text-white sm:text-sm">
        <span>{amount}</span>
        {transaction.sourceUrl ? <ArrowUpRight className="h-3 w-3 text-zinc-600" /> : null}
      </div>
      <div className="mt-0.5 whitespace-nowrap text-[9px] font-medium uppercase tracking-[0.08em] text-zinc-600 sm:text-[10px]">
        reported range
      </div>
    </div>
  );

  return (
    <div className="flex min-h-[72px] items-center gap-3 border-b border-white/[0.055] px-4 py-3 last:border-b-0 sm:px-5">
      {transaction.memberId ? (
        <PoliticianHeadshot
          memberId={transaction.memberId}
          name={transaction.politicianName}
          party={transaction.party}
          size={38}
        />
      ) : (
        <div className="flex h-[38px] w-[38px] shrink-0 items-center justify-center rounded-full border border-blue-400/20 bg-blue-400/10 text-blue-300">
          <Landmark className="h-4 w-4" />
        </div>
      )}

      <div className="min-w-0 flex-1">
        <div className="truncate text-sm font-semibold text-zinc-100 sm:text-[15px]">
          {transaction.politicianName}
        </div>
        <div className="mt-1 flex min-w-0 flex-wrap items-center gap-x-2 gap-y-0.5 text-[11px] sm:text-xs">
          <span className={`font-semibold ${isBuy ? 'text-emerald-300' : 'text-red-300'}`}>
            {isBuy ? 'Buy' : 'Sell'}
          </span>
          {transaction.transactionDate ? (
            <span className="tabular-nums text-zinc-300">
              Traded {formatCalendarDate(transaction.transactionDate)}
            </span>
          ) : null}
          <span className="tabular-nums text-zinc-600">
            Disclosed {formatCalendarDate(transaction.publishedDate)}
          </span>
        </div>
      </div>

      {transaction.sourceUrl ? (
        <Link
          href={transaction.sourceUrl}
          target="_blank"
          rel="noreferrer"
          aria-label={`Open source filing for ${transaction.politicianName}`}
          className="shrink-0 transition hover:text-cyan-200"
        >
          {metric}
        </Link>
      ) : metric}
    </div>
  );
}

function CongressOverview({
  symbol,
  range,
  onRangeChange,
  data,
  loading,
  error,
  onRetry,
}: {
  symbol: string;
  range: DashboardCongressOverviewRange;
  onRangeChange: (range: DashboardCongressOverviewRange) => void;
  data: DashboardCongressOverviewData | null;
  loading: boolean;
  error: string;
  onRetry: () => void;
}) {
  const [showAllTransactions, setShowAllTransactions] = useState(false);

  const displayedTransactions = showAllTransactions ? data?.transactions || [] : data?.transactions.slice(0, 8) || [];

  return (
    <div>
      <div className="border-b border-white/[0.06] px-4 py-4 sm:px-5">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
          <div>
            <h3 className="text-sm font-semibold text-white">Congress activity</h3>
            <p className="mt-1 text-xs leading-5 text-zinc-600">
              Newly disclosed filings. Trade dates may be earlier.
            </p>
          </div>
          <div className="flex w-full gap-1 rounded-xl border border-white/[0.07] bg-black/20 p-1 sm:w-fit">
            {CONGRESS_RANGES.map((option) => {
              const selected = range === option.key;
              return (
                <button
                  key={option.key}
                  type="button"
                  aria-pressed={selected}
                  onClick={() => onRangeChange(option.key)}
                  className={`flex-1 rounded-lg px-3 py-1.5 text-xs font-medium transition sm:flex-none ${
                    selected ? 'bg-white/[0.09] text-white' : 'text-zinc-600 hover:text-zinc-300'
                  }`}
                >
                  {option.label}
                </button>
              );
            })}
          </div>
        </div>
      </div>

      {loading && !data ? (
        <div className="flex min-h-[240px] items-center justify-center px-6 text-sm text-zinc-500">
          <Loader2 className="mr-2 h-4 w-4 animate-spin text-emerald-300" />
          Loading Congress overview...
        </div>
      ) : error && !data ? (
        <div className="m-4 rounded-xl border border-red-500/15 bg-red-500/[0.04] p-4 text-sm text-red-200 sm:m-5">
          <p>{error}</p>
          <button
            type="button"
            onClick={onRetry}
            className="mt-3 inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/[0.04] px-3 py-1.5 text-xs font-medium text-white transition hover:bg-white/[0.08]"
          >
            <RefreshCcw className="h-3.5 w-3.5" />
            Retry
          </button>
        </div>
      ) : data ? (
        <>
          <div className="grid grid-cols-3 border-b border-white/[0.06]">
            <div className="px-4 py-4 sm:px-5">
              <div className="text-xl font-semibold tabular-nums text-white sm:text-2xl">
                {data.totals.lawmakerCount.toLocaleString()}
              </div>
              <div className="mt-1 text-[9px] font-medium uppercase tracking-[0.12em] text-zinc-600 sm:text-[10px]">
                Lawmakers
              </div>
            </div>
            <div className="border-l border-white/[0.06] px-4 py-4 sm:px-5">
              <div className="text-xl font-semibold tabular-nums text-emerald-300 sm:text-2xl">
                {formatMinimumCurrency(data.totals.buyAmountFloor)}
              </div>
              <div className="mt-1 text-[9px] font-medium uppercase tracking-[0.12em] text-zinc-600 sm:text-[10px]">
                Bought · {countLabel(data.totals.buyCount, 'buy')}
              </div>
            </div>
            <div className="border-l border-white/[0.06] px-4 py-4 sm:px-5">
              <div className="text-xl font-semibold tabular-nums text-red-300 sm:text-2xl">
                {formatMinimumCurrency(data.totals.sellAmountFloor)}
              </div>
              <div className="mt-1 text-[9px] font-medium uppercase tracking-[0.12em] text-zinc-600 sm:text-[10px]">
                Sold · {countLabel(data.totals.sellCount, 'sale', 'sales')}
              </div>
            </div>
          </div>

          <div className="flex items-center justify-between gap-3 border-b border-white/[0.06] px-4 py-3 sm:px-5">
            <div>
              <h4 className="text-xs font-semibold text-zinc-200">Disclosed transactions</h4>
              <p className="mt-0.5 text-[10px] text-zinc-600">Dollar totals use the minimum of each reported range.</p>
            </div>
            {data.latestDisclosureDate ? (
              <div className="shrink-0 text-[10px] tabular-nums text-zinc-600">
                Latest <span className="text-zinc-400">{formatCalendarDate(data.latestDisclosureDate)}</span>
              </div>
            ) : null}
          </div>

          {data.transactions.length ? (
            <div>
              {displayedTransactions.map((transaction) => (
                <CongressTransactionRow key={transaction.id} transaction={transaction} />
              ))}
              {data.transactions.length > 8 ? (
                <div className="border-t border-white/[0.06] p-3">
                  <button
                    type="button"
                    onClick={() => setShowAllTransactions((current) => !current)}
                    className="inline-flex w-full items-center justify-center rounded-xl border border-white/[0.07] bg-white/[0.018] px-4 py-2.5 text-xs font-medium text-zinc-400 transition hover:border-white/[0.12] hover:bg-white/[0.035] hover:text-white"
                  >
                    {showAllTransactions
                      ? 'Show fewer transactions'
                      : `Show all ${data.transactions.length.toLocaleString()} transactions`}
                  </button>
                </div>
              ) : null}
            </div>
          ) : (
            <div className="flex min-h-[180px] flex-col items-center justify-center px-6 text-center text-sm text-zinc-500">
              <Landmark className="mb-3 h-7 w-7 text-zinc-700" />
              <p>No congressional trades were newly disclosed for {symbol} in this period.</p>
              <p className="mt-1 text-xs text-zinc-700">Try 30 days or YTD.</p>
            </div>
          )}
        </>
      ) : null}
    </div>
  );
}

function LoadingState({ requestedTicker, onDismiss }: Pick<DashboardTickerWorkspaceProps, 'requestedTicker' | 'onDismiss'>) {
  return (
    <section className="dash-fade-in overflow-hidden rounded-2xl border border-white/[0.07] bg-white/[0.018]">
      <div className="flex items-center justify-between gap-4 border-b border-white/[0.06] px-4 py-4 sm:px-5">
        <div className="flex items-center gap-3.5">
          <TickerLogo symbol={requestedTicker} />
          <div>
            <h2 className="text-2xl font-semibold tracking-tight text-white">{requestedTicker}</h2>
            <p className="mt-0.5 text-sm text-zinc-600">Loading company activity</p>
          </div>
        </div>
        <button
          type="button"
          onClick={onDismiss}
          className="flex h-9 w-9 items-center justify-center rounded-xl border border-white/[0.08] bg-white/[0.025] text-zinc-500 transition hover:bg-white/[0.06] hover:text-white"
          aria-label="Close stock workspace"
        >
          <X className="h-4 w-4" />
        </button>
      </div>
      <div className="flex min-h-[160px] items-center justify-center text-sm text-zinc-500">
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
    <section className="dash-fade-in overflow-hidden rounded-2xl border border-white/[0.07] bg-white/[0.018]">
      <div className="flex items-center justify-between gap-4 border-b border-white/[0.06] px-4 py-4 sm:px-5">
        <div>
          <h2 className="text-2xl font-semibold tracking-tight text-white">{requestedTicker}</h2>
          <p className="mt-0.5 text-sm text-zinc-600">Company activity</p>
        </div>
        <button
          type="button"
          onClick={onDismiss}
          className="flex h-9 w-9 items-center justify-center rounded-xl border border-white/[0.08] bg-white/[0.025] text-zinc-500 transition hover:bg-white/[0.06] hover:text-white"
          aria-label="Close stock workspace"
        >
          <X className="h-4 w-4" />
        </button>
      </div>
      <div className="m-4 rounded-xl border border-red-500/15 bg-red-500/[0.04] p-4 text-sm text-red-200 sm:m-5">
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
  const [workspaceView, setWorkspaceView] = useState<'congress' | 'activity'>('congress');
  const [congressRange, setCongressRange] = useState<DashboardCongressOverviewRange>('30d');
  const [congressOverviewState, setCongressOverviewState] = useState<CongressOverviewState>(() =>
    createEmptyCongressOverviewState(),
  );
  const [loadingCongressRange, setLoadingCongressRange] = useState<DashboardCongressOverviewRange | null>(null);
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
    setWorkspaceView('congress');
    setCongressRange('30d');
    setCongressOverviewState(createEmptyCongressOverviewState());
    setLoadingCongressRange(null);
  }, [data]);

  useEffect(() => {
    const rangeState = congressOverviewState[congressRange];
    if (!data || workspaceView !== 'congress' || rangeState.loaded) {
      return;
    }

    let cancelled = false;
    const controller = new AbortController();
    const loadOverview = async () => {
      setLoadingCongressRange(congressRange);
      try {
        const response = await fetch(
          `/api/ticker-workspace/${encodeURIComponent(data.symbol)}/congress-overview?range=${congressRange}`,
          { signal: controller.signal, cache: 'no-store' },
        );
        if (!response.ok) {
          const payload = (await response.json().catch(() => null)) as { error?: string } | null;
          throw new Error(payload?.error || 'Could not load Congress overview.');
        }
        const payload = (await response.json()) as DashboardCongressOverviewData;
        if (cancelled) return;
        setCongressOverviewState((current) => ({
          ...current,
          [congressRange]: { data: payload, error: '', loaded: true },
        }));
      } catch (overviewError) {
        if (cancelled || (overviewError instanceof Error && overviewError.name === 'AbortError')) return;
        setCongressOverviewState((current) => ({
          ...current,
          [congressRange]: {
            data: null,
            error: overviewError instanceof Error ? overviewError.message : 'Could not load Congress overview.',
            loaded: true,
          },
        }));
      } finally {
        if (!cancelled) {
          setLoadingCongressRange((current) => (current === congressRange ? null : current));
        }
      }
    };

    void loadOverview();
    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [congressOverviewState, congressRange, data, workspaceView]);

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
  const currentCongressOverview = congressOverviewState[congressRange];

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
    <section className="dash-fade-in overflow-hidden rounded-2xl border border-white/[0.07] bg-white/[0.018]">
      <header className="flex items-center justify-between gap-4 border-b border-white/[0.06] px-4 py-4 sm:px-5">
        <div className="flex min-w-0 items-center gap-3.5">
          <TickerLogo symbol={data.symbol} />
          <div className="min-w-0">
            <h2 className="truncate text-2xl font-semibold tracking-tight text-white">
              {data.symbol}
            </h2>
            <p className="mt-0.5 truncate text-sm text-zinc-500">{data.companyName}</p>
          </div>
        </div>

        <div className="flex shrink-0 items-center gap-2">
          <SignalActionButton onClick={onOpenPriceAlert} label="Alert me" showLiveStatus={false} />
          <button
            type="button"
            onClick={onDismiss}
            className="flex h-9 w-9 items-center justify-center rounded-xl border border-white/[0.08] bg-white/[0.025] text-zinc-500 transition hover:border-white/[0.16] hover:bg-white/[0.06] hover:text-white"
            aria-label="Close stock workspace"
          >
            <X className="h-4 w-4" />
          </button>
        </div>
      </header>

      <div className="flex gap-1 border-b border-white/[0.06] px-4 py-2.5 sm:px-5">
        <button
          type="button"
          aria-pressed={workspaceView === 'congress'}
          onClick={() => setWorkspaceView('congress')}
          className={`rounded-lg px-3 py-1.5 text-xs font-medium transition ${
            workspaceView === 'congress' ? 'bg-white/[0.09] text-white' : 'text-zinc-600 hover:text-zinc-300'
          }`}
        >
          Congress overview
        </button>
        <button
          type="button"
          aria-pressed={workspaceView === 'activity'}
          onClick={() => setWorkspaceView('activity')}
          className={`rounded-lg px-3 py-1.5 text-xs font-medium transition ${
            workspaceView === 'activity' ? 'bg-white/[0.09] text-white' : 'text-zinc-600 hover:text-zinc-300'
          }`}
        >
          All activity
        </button>
      </div>

      {workspaceView === 'congress' ? (
        <CongressOverview
          key={`${data.symbol}:${congressRange}`}
          symbol={data.symbol}
          range={congressRange}
          onRangeChange={setCongressRange}
          data={currentCongressOverview.data}
          loading={loadingCongressRange === congressRange}
          error={currentCongressOverview.error}
          onRetry={() => {
            setCongressOverviewState((current) => ({
              ...current,
              [congressRange]: { data: null, error: '', loaded: false },
            }));
          }}
        />
      ) : (
        <>
      <div className="border-b border-white/[0.06] px-4 py-3.5 sm:px-5">
        <div className="flex items-center justify-between gap-3">
          <h3 className="text-sm font-semibold text-white">Activity</h3>
          {latestActivityDate ? (
            <div className="text-xs tabular-nums text-zinc-500">
              Latest <span className="font-medium text-zinc-300">{formatCalendarDate(latestActivityDate)}</span>
            </div>
          ) : null}
        </div>

        <div className="mt-3 flex w-full gap-1 rounded-xl border border-white/[0.07] bg-black/20 p-1 sm:w-fit">
          {ACTIVITY_TABS.map((tab) => {
            const selected = activeTab === tab.key;
            return (
              <button
                key={tab.key}
                type="button"
                aria-pressed={selected}
                onClick={() => {
                  setActiveTab(tab.key);
                  setLoadMoreError('');
                }}
                className={`flex-1 rounded-lg px-3 py-1.5 text-xs font-medium transition sm:flex-none ${
                  selected
                    ? 'bg-white/[0.09] text-white'
                    : 'text-zinc-600 hover:text-zinc-300'
                }`}
              >
                {tab.label}
              </button>
            );
          })}
        </div>
      </div>

      {loadMoreError ? (
        <p className="border-b border-red-400/10 bg-red-400/[0.04] px-4 py-2.5 text-xs text-red-300 sm:px-5">{loadMoreError}</p>
      ) : null}

      {tabIsLoading && !activity.length ? (
        <div className="flex min-h-[180px] items-center justify-center px-6 text-sm text-zinc-500">
          <Loader2 className="mr-2 h-4 w-4 animate-spin text-emerald-300" />
          Loading {currentTabLabel.toLowerCase()}...
        </div>
      ) : activity.length ? (
        <div>
          {activity.map((item) => (
            <ActivityRow key={item.id} item={item} showSource={activeTab === 'all'} />
          ))}
        </div>
      ) : (
        <div className="flex min-h-[180px] flex-col items-center justify-center px-6 text-center text-sm text-zinc-500">
          <SearchX className="mb-3 h-7 w-7 text-zinc-700" />
          No recent {currentTabLabel.toLowerCase()} activity found for {data.symbol}.
        </div>
      )}

      {nextOffset != null ? (
        <div className="border-t border-white/[0.06] p-3">
          <button
            type="button"
            onClick={handleLoadMore}
            disabled={loadingMore}
            className="inline-flex w-full items-center justify-center gap-2 rounded-xl border border-white/[0.07] bg-white/[0.018] px-4 py-2.5 text-xs font-medium text-zinc-400 transition hover:border-white/[0.12] hover:bg-white/[0.035] hover:text-white disabled:cursor-not-allowed disabled:opacity-60"
          >
            {loadingMore ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : null}
            {loadingMore ? 'Loading...' : `Load 10 more ${activeTab === 'all' ? 'transactions' : currentTabLabel.toLowerCase()}`}
          </button>
        </div>
      ) : null}
        </>
      )}
    </section>
  );
}
