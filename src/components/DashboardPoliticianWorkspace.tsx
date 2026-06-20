'use client';

import { useEffect, useState } from 'react';
import Image, { type ImageLoaderProps } from 'next/image';
import { ExternalLink, LoaderCircle, TrendingDown, TrendingUp, X } from 'lucide-react';

import OptionTradeBadge from '@/components/OptionTradeBadge';
import PoliticianHeadshot from '@/components/PoliticianHeadshot';
import SignalActionButton from '@/components/SignalActionButton';
import { getTickerLogoUrl } from '@/lib/company-logos';
import { formatCalendarDate } from '@/lib/date-format';
import {
  parsePoliticianOptionDetails,
  stripPoliticianOptionMetadata,
} from '@/lib/politician-option-trades';
import type { PoliticianProfileTrade } from '@/lib/politician-profile';
import type { DashboardPoliticianWorkspaceData } from '@/lib/politician-workspace-types';
import { getPartyPresentation } from '@/lib/politics';

type DashboardPoliticianWorkspaceProps = {
  data: DashboardPoliticianWorkspaceData | null;
  requestedMemberId: string | null;
  requestedMemberName: string;
  loading: boolean;
  error: string | null;
  onDismiss: () => void;
  onRetry: () => void;
  onOpenSignals: () => void;
};

const TRADE_PAGE_SIZE = 8;
const passthroughImageLoader = ({ src }: ImageLoaderProps) => src;

function tradeLabel(direction: string | null | undefined) {
  const normalized = String(direction || '').trim().toLowerCase();
  if (normalized.startsWith('buy') || normalized === 'purchase') {
    return {
      label: 'Buy',
      className: 'border-emerald-500/20 bg-emerald-500/10 text-emerald-300',
      icon: TrendingUp,
    };
  }
  if (normalized.startsWith('sell') || normalized === 'sale') {
    return {
      label: 'Sell',
      className: 'border-red-500/20 bg-red-500/10 text-red-300',
      icon: TrendingDown,
    };
  }
  return {
    label: 'Activity',
    className: 'border-white/10 bg-white/[0.04] text-zinc-300',
    icon: TrendingUp,
  };
}

function tradeAssetLabel(trade: PoliticianProfileTrade) {
  if (trade.ticker === 'US-TREAS') {
    return stripPoliticianOptionMetadata(trade.asset_name) || 'U.S. Treasury';
  }
  if (trade.ticker && !['N/A', 'NA', 'UNKNOWN', 'MULTI'].includes(trade.ticker.toUpperCase())) {
    return trade.ticker.toUpperCase();
  }
  return stripPoliticianOptionMetadata(trade.asset_name) || 'Unmapped asset';
}

function TickerAssetLogo({
  ticker,
  label,
}: {
  ticker: string | null | undefined;
  label: string;
}) {
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  const normalizedTicker = String(ticker || '').trim().toUpperCase();
  const resolvedLogoUrl = normalizedTicker ? getTickerLogoUrl(normalizedTicker, 56) : null;
  const logoUrl = resolvedLogoUrl && failedUrl !== resolvedLogoUrl ? resolvedLogoUrl : null;
  const monogram = label
    .split(/\s+/)
    .map((part) => part.charAt(0))
    .join('')
    .slice(0, 2)
    .toUpperCase();
  const hue = label.split('').reduce((acc, ch) => acc + ch.charCodeAt(0), 0) % 360;

  if (logoUrl) {
    return (
      <div className="h-10 w-10 shrink-0 overflow-hidden rounded-2xl border border-white/[0.08] bg-black/30">
        <Image
          loader={passthroughImageLoader}
          unoptimized
          src={logoUrl}
          alt={label}
          width={40}
          height={40}
          sizes="40px"
          className="h-full w-full object-contain p-1"
          onError={() => setFailedUrl(logoUrl)}
        />
      </div>
    );
  }

  return (
    <div
      className="flex h-10 w-10 shrink-0 items-center justify-center rounded-2xl border border-white/[0.08] text-[10px] font-semibold text-white"
      style={{
        background: `linear-gradient(135deg, hsl(${hue},65%,38%), hsl(${(hue + 42) % 360},55%,24%))`,
      }}
    >
      {normalizedTicker.slice(0, 2) || monogram || 'NA'}
    </div>
  );
}

function LoadingState({
  memberName,
  onDismiss,
}: {
  memberName: string;
  onDismiss: () => void;
}) {
  return (
    <div className="dash-fade-in overflow-hidden rounded-[1.75rem] border border-white/[0.06] bg-white/[0.025] p-4 shadow-[0_24px_80px_rgba(0,0,0,0.35)]">
      <div className="flex items-center justify-between gap-3">
        <div className="flex items-center gap-3">
          <div className="h-12 w-12 animate-pulse rounded-full bg-white/[0.06]" />
          <div>
            <div className="h-3 w-32 animate-pulse rounded bg-white/[0.05]" />
            <div className="mt-2 h-5 w-48 animate-pulse rounded bg-white/[0.07]" />
          </div>
        </div>
        <button
          type="button"
          onClick={onDismiss}
          className="flex h-9 w-9 items-center justify-center rounded-full border border-white/[0.06] bg-white/[0.03] text-zinc-500 transition hover:text-white"
          aria-label="Close workspace"
        >
          <X className="h-4 w-4" />
        </button>
      </div>
      <div className="mt-5 rounded-2xl border border-white/[0.05] bg-black/20 p-3">
        <div className="mb-3 flex items-center gap-2 text-xs text-zinc-500">
          <LoaderCircle className="h-3.5 w-3.5 animate-spin text-emerald-300" />
          Loading recent trades for {memberName}
        </div>
        <div className="space-y-2">
          {Array.from({ length: 5 }).map((_, index) => (
            <div key={index} className="h-16 animate-pulse rounded-2xl bg-white/[0.035]" />
          ))}
        </div>
      </div>
    </div>
  );
}

function ErrorState({
  memberName,
  error,
  onDismiss,
  onRetry,
}: {
  memberName: string;
  error: string;
  onDismiss: () => void;
  onRetry: () => void;
}) {
  return (
    <div className="dash-fade-in rounded-[1.75rem] border border-red-500/20 bg-red-500/5 p-5">
      <div className="flex items-start justify-between gap-4">
        <div>
          <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-red-300/80">
            Politician workspace
          </div>
          <div className="mt-2 text-xl font-semibold text-white">Could not load {memberName}</div>
          <div className="mt-2 max-w-2xl text-sm text-red-200/80">{error}</div>
        </div>
        <button
          type="button"
          onClick={onDismiss}
          className="flex h-9 w-9 items-center justify-center rounded-full border border-white/[0.08] bg-white/[0.04] text-zinc-500 transition hover:text-white"
          aria-label="Close workspace"
        >
          <X className="h-4 w-4" />
        </button>
      </div>
      <button
        type="button"
        onClick={onRetry}
        className="mt-4 rounded-full border border-white/10 bg-white/[0.04] px-4 py-2 text-sm font-medium text-white transition hover:border-white/20 hover:bg-white/[0.08]"
      >
        Retry
      </button>
    </div>
  );
}

function TradeRow({
  trade,
  index,
}: {
  trade: PoliticianProfileTrade;
  index: number;
}) {
  const assetLabel = tradeAssetLabel(trade);
  const tradeTone = tradeLabel(trade.transaction_type);
  const TradeIcon = tradeTone.icon;
  const optionDetails = parsePoliticianOptionDetails(trade);

  return (
    <div
      className="dash-fade-in rounded-2xl border border-white/[0.055] bg-white/[0.025] px-3 py-3 transition hover:border-emerald-400/15 hover:bg-emerald-400/[0.035]"
      style={{ animationDelay: `${index * 24}ms` }}
    >
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex min-w-0 items-center gap-3">
          <TickerAssetLogo ticker={trade.ticker} label={assetLabel} />
          <div className="min-w-0">
            <div className="flex min-w-0 flex-wrap items-center gap-2">
              <div className="truncate text-sm font-semibold text-white">{assetLabel}</div>
              {optionDetails ? (
                <OptionTradeBadge
                  label={optionDetails.badgeLabel}
                  tooltip={optionDetails.tooltip}
                  className="inline-flex items-center rounded-full border border-orange-500/25 bg-orange-500/10 px-2 py-0.5 text-[10px] font-bold text-orange-300"
                />
              ) : null}
            </div>
            <div className="mt-1 flex flex-wrap items-center gap-2 text-xs text-zinc-500">
              <span>{trade.amount_range || 'Unknown range'}</span>
              {trade.transaction_date ? (
                <>
                  <span className="text-white/10">•</span>
                  <span>{formatCalendarDate(trade.transaction_date)}</span>
                </>
              ) : null}
            </div>
          </div>
        </div>

        <div className="ml-auto flex shrink-0 items-center gap-2">
          <span
            className={`inline-flex items-center gap-1.5 rounded-full border px-3 py-1 text-[11px] font-semibold ${tradeTone.className}`}
          >
            <TradeIcon className="h-3.5 w-3.5" />
            {tradeTone.label}
          </span>
          {trade.source_url ? (
            <a
              href={trade.source_url}
              target="_blank"
              rel="noreferrer"
              className="inline-flex items-center gap-1 rounded-full border border-white/[0.08] bg-white/[0.035] px-3 py-1 text-[11px] font-medium text-zinc-400 transition hover:border-white/[0.16] hover:bg-white/[0.08] hover:text-white"
            >
              Source
              <ExternalLink className="h-3.5 w-3.5" />
            </a>
          ) : null}
        </div>
      </div>
    </div>
  );
}

export default function DashboardPoliticianWorkspace({
  data,
  requestedMemberId,
  requestedMemberName,
  loading,
  error,
  onDismiss,
  onRetry,
  onOpenSignals,
}: DashboardPoliticianWorkspaceProps) {
  const [trades, setTrades] = useState<PoliticianProfileTrade[]>([]);
  const [nextOffset, setNextOffset] = useState<number | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);
  const [loadMoreError, setLoadMoreError] = useState('');

  useEffect(() => {
    setTrades(data?.trades || []);
    setNextOffset(data?.nextOffset ?? null);
    setLoadingMore(false);
    setLoadMoreError('');
  }, [data]);

  if (!requestedMemberId) {
    return null;
  }

  if (loading) {
    return <LoadingState memberName={requestedMemberName} onDismiss={onDismiss} />;
  }

  if (error) {
    return (
      <ErrorState
        memberName={requestedMemberName}
        error={error}
        onDismiss={onDismiss}
        onRetry={onRetry}
      />
    );
  }

  if (!data) {
    return (
      <ErrorState
        memberName={requestedMemberName}
        error="No recent trade workspace is available for this member yet."
        onDismiss={onDismiss}
        onRetry={onRetry}
      />
    );
  }

  const summary = data.summary;
  const partyPresentation = getPartyPresentation(summary.party, requestedMemberId);

  const handleLoadMore = async () => {
    if (nextOffset === null || loadingMore) {
      return;
    }

    setLoadingMore(true);
    setLoadMoreError('');

    try {
      const response = await fetch(
        `/api/politician-workspace/${encodeURIComponent(requestedMemberId)}?offset=${nextOffset}&limit=${TRADE_PAGE_SIZE}`,
      );
      const payload = (await response.json().catch(() => null)) as (DashboardPoliticianWorkspaceData & { error?: string }) | null;

      if (!response.ok || !payload) {
        throw new Error(payload?.error || 'Could not load more trades.');
      }

      setTrades((current) => {
        const seenIds = new Set(current.map((trade) => trade.id));
        return [...current, ...payload.trades.filter((trade) => !seenIds.has(trade.id))];
      });
      setNextOffset(payload.nextOffset);
    } catch (fetchError) {
      setLoadMoreError(fetchError instanceof Error ? fetchError.message : 'Could not load more trades.');
    } finally {
      setLoadingMore(false);
    }
  };

  return (
    <div className="dash-fade-in relative overflow-hidden rounded-[1.75rem] border border-white/[0.06] bg-white/[0.025] p-4 shadow-[0_24px_80px_rgba(0,0,0,0.35)]">
      <div className="pointer-events-none absolute -top-24 left-12 h-44 w-72 rounded-full bg-emerald-500/[0.045] blur-3xl" />
      <div className="pointer-events-none absolute -right-24 top-10 h-48 w-64 rounded-full bg-cyan-500/[0.035] blur-3xl" />

      <div className="relative flex flex-wrap items-center justify-between gap-4">
        <div className="flex min-w-0 items-center gap-4">
          <PoliticianHeadshot
            memberId={requestedMemberId}
            name={summary.displayName}
            party={summary.party}
            size={56}
          />
          <div className="min-w-0">
            <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-zinc-600">
              Recent congressional trades
            </div>
            <div className="mt-1 truncate text-[24px] font-semibold tracking-[-0.03em] text-white">
              {summary.displayName}
            </div>
            <div className="mt-1 flex flex-wrap items-center gap-2 text-xs text-zinc-500">
              <span style={{ color: partyPresentation.color }}>{partyPresentation.label}</span>
              {summary.chamber ? (
                <>
                  <span className="text-white/10">•</span>
                  <span>{summary.chamber}</span>
                </>
              ) : null}
              {summary.state ? (
                <>
                  <span className="text-white/10">•</span>
                  <span>{summary.state}</span>
                </>
              ) : null}
              {summary.latestTradeDate ? (
                <>
                  <span className="text-white/10">•</span>
                  <span>Latest {formatCalendarDate(summary.latestTradeDate)}</span>
                </>
              ) : null}
            </div>
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <SignalActionButton onClick={onOpenSignals} />
          <button
            type="button"
            onClick={onDismiss}
            className="inline-flex h-9 w-9 items-center justify-center rounded-full border border-white/[0.08] bg-white/[0.035] text-zinc-400 transition hover:bg-white/[0.08] hover:text-white"
            aria-label="Close workspace"
          >
            <X className="h-4 w-4" />
          </button>
        </div>
      </div>

      <div className="relative mt-5 rounded-2xl border border-white/[0.05] bg-black/20 p-3">
        <div className="mb-3 flex flex-wrap items-end justify-between gap-3 px-1">
          <div>
            <div className="text-sm font-semibold text-white">Recent trades</div>
            <div className="mt-1 text-xs text-zinc-500">Newest displayable ticker trades first. More loads only when requested.</div>
          </div>
          <div className="text-[10px] uppercase tracking-[0.16em] text-zinc-600">{trades.length} loaded</div>
        </div>

        {trades.length ? (
          <div className="space-y-2">
            {trades.map((trade, index) => (
              <TradeRow key={trade.id} trade={trade} index={index} />
            ))}
          </div>
        ) : (
          <div className="rounded-2xl border border-dashed border-white/[0.08] bg-white/[0.02] px-4 py-10 text-center text-sm text-zinc-500">
            No recent displayable ticker trades were found for this member.
          </div>
        )}

        {loadMoreError ? (
          <div className="mt-3 rounded-xl border border-red-500/15 bg-red-500/5 px-3 py-2 text-xs text-red-300">
            {loadMoreError}
          </div>
        ) : null}

        {nextOffset !== null ? (
          <div className="mt-3 flex justify-center">
            <button
              type="button"
              onClick={handleLoadMore}
              disabled={loadingMore}
              className="inline-flex items-center gap-2 rounded-full border border-white/[0.08] bg-white/[0.035] px-4 py-2 text-xs font-semibold text-zinc-200 transition hover:border-emerald-400/20 hover:bg-emerald-400/10 hover:text-white disabled:cursor-not-allowed disabled:opacity-60"
            >
              {loadingMore ? <LoaderCircle className="h-3.5 w-3.5 animate-spin" /> : null}
              {loadingMore ? 'Loading trades' : `Load ${TRADE_PAGE_SIZE} more trades`}
            </button>
          </div>
        ) : null}
      </div>
    </div>
  );
}
