'use client';

import Image, { type ImageLoaderProps } from 'next/image';
import { useEffect, useMemo, useState } from 'react';
import type { Session } from '@supabase/supabase-js';
import { useRouter } from 'next/navigation';
import {
  BellPlus,
  ExternalLink,
  TrendingDown,
  TrendingUp,
  X,
  Briefcase,
  List,
} from 'lucide-react';

import { CreateSignalModal } from '@/components/CreateSignalModal';
import OptionTradeBadge from '@/components/OptionTradeBadge';
import PoliticianHeadshot from '@/components/PoliticianHeadshot';
import { getTickerLogoUrl } from '@/lib/company-logos';
import { formatCalendarDate } from '@/lib/date-format';
import { parsePoliticianOptionDetails, stripPoliticianOptionMetadata } from '@/lib/politician-option-trades';
import { type PoliticianProfileData } from '@/lib/politician-profile-server';
import type { PoliticianHoldingEstimate, PoliticianLivePortfolioHolding } from '@/lib/politician-profile';
import { getPartyPresentation } from '@/lib/politics';
import { supabase } from '@/lib/supabase';

type PoliticianProfileModalProps = {
  memberId: string | null;
  memberName: string;
  fallbackSummary?: PoliticianProfileFallbackSummary | null;
  open: boolean;
  loading: boolean;
  error: string | null;
  profile: PoliticianProfileData | null;
  onClose: () => void;
};

export type PoliticianProfileFallbackSummary = {
  displayName: string;
  party?: string | null;
  chamber?: string | null;
  state?: string | null;
  totalTrades?: number | null;
  latestTradeDate?: string | null;
};

type TabId = 'transactions' | 'holdings';

const POLITICIAN_TRADE_PAGE_SIZE = 16;

const passthroughImageLoader = ({ src }: ImageLoaderProps) => src;

function tradeAssetLabel(trade: PoliticianProfileData['trades'][number]): string {
  if (trade.ticker === 'US-TREAS') {
    return stripPoliticianOptionMetadata(trade.asset_name) || 'U.S. Treasury';
  }
  if (trade.ticker && !['N/A', 'UNKNOWN'].includes(trade.ticker)) {
    return trade.ticker;
  }
  return stripPoliticianOptionMetadata(trade.asset_name) || 'Unmapped asset';
}

function TickerAssetLogo({ ticker, label, size = 36 }: { ticker: string | null | undefined; label: string; size?: number }) {
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  const normalizedTicker = String(ticker || '').trim().toUpperCase();
  const logoUrl = normalizedTicker ? getTickerLogoUrl(normalizedTicker, size * 2) : null;
  const activeLogoUrl = logoUrl && failedUrl !== logoUrl ? logoUrl : null;
  const monogram = label
    .split(/\s+/)
    .map((part) => part.charAt(0))
    .join('')
    .slice(0, 2)
    .toUpperCase();
  const hue = label.split('').reduce((acc, ch) => acc + ch.charCodeAt(0), 0) % 360;
  const px = `${size}px`;

  if (activeLogoUrl) {
    return (
      <div
        className="shrink-0 overflow-hidden rounded-xl border border-white/[0.08] bg-black/30"
        style={{ width: size, height: size }}
      >
        <Image
          loader={passthroughImageLoader}
          unoptimized
          src={activeLogoUrl}
          alt={label}
          width={size}
          height={size}
          sizes={px}
          className="h-full w-full object-contain p-0.5"
          onError={() => setFailedUrl(activeLogoUrl)}
        />
      </div>
    );
  }

  return (
    <div
      className="flex shrink-0 items-center justify-center rounded-xl border border-white/[0.08] text-[10px] font-semibold text-white"
      style={{
        width: size,
        height: size,
        background: `linear-gradient(135deg, hsl(${hue},65%,45%), hsl(${(hue + 40) % 360},65%,38%))`,
      }}
    >
      {normalizedTicker.slice(0, 2) || monogram || 'NA'}
    </div>
  );
}

/* ─── Formatting helpers ─── */

function formatCompactUsd(value: number): string {
  if (value >= 1_000_000) {
    return `$${(value / 1_000_000).toFixed(1)}M`;
  }
  if (value >= 1_000) {
    return `$${(value / 1_000).toFixed(0)}K`;
  }
  return `$${value.toLocaleString('en-US', { maximumFractionDigits: 0 })}`;
}

function formatPct(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '—';
  const sign = value >= 0 ? '+' : '';
  return `${sign}${(value * 100).toFixed(1)}%`;
}

/* ─── Holdings tab content ─── */

type MergedHolding = {
  key: string;
  ticker: string | null;
  label: string;
  estimatedValue: number;
  tradeCount: number;
  lastTradeDate: string | null;
  // Live data (if available)
  currentPrice: number | null;
  currentValue: number | null;
  unrealizedReturnPct: number | null;
  allocationPct: number | null;
};

function mergeHoldings(
  summaryHoldings: PoliticianHoldingEstimate[],
  liveHoldings: PoliticianLivePortfolioHolding[],
): MergedHolding[] {
  const liveMap = new Map<string, PoliticianLivePortfolioHolding>();
  for (const lh of liveHoldings) {
    liveMap.set(`ticker:${lh.ticker}`, lh);
  }

  const merged: MergedHolding[] = [];
  const usedKeys = new Set<string>();

  // First: take summary holdings (which have net buys > sells)
  for (const sh of summaryHoldings) {
    const live = liveMap.get(sh.key);
    usedKeys.add(sh.key);
    merged.push({
      key: sh.key,
      ticker: sh.ticker,
      label: sh.label,
      estimatedValue: live?.estimatedCurrentValue ?? sh.netAmount,
      tradeCount: sh.tradeCount,
      lastTradeDate: sh.lastTradeDate,
      currentPrice: live?.estimatedCurrentPrice ?? null,
      currentValue: live?.estimatedCurrentValue ?? null,
      unrealizedReturnPct: live?.estimatedUnrealizedReturnPct ?? null,
      allocationPct: live?.allocationPct ?? null,
    });
  }

  // Add live-only holdings (from disclosure snapshots)
  for (const lh of liveHoldings) {
    const key = `ticker:${lh.ticker}`;
    if (!usedKeys.has(key)) {
      merged.push({
        key,
        ticker: lh.ticker,
        label: lh.label || lh.ticker,
        estimatedValue: lh.estimatedCurrentValue,
        tradeCount: lh.tradeCount,
        lastTradeDate: lh.lastTradeDate,
        currentPrice: lh.estimatedCurrentPrice,
        currentValue: lh.estimatedCurrentValue,
        unrealizedReturnPct: lh.estimatedUnrealizedReturnPct,
        allocationPct: lh.allocationPct,
      });
    }
  }

  return merged.sort((a, b) => b.estimatedValue - a.estimatedValue);
}

function HoldingsTab({ profile }: { profile: PoliticianProfileData }) {
  const holdings = useMemo(
    () => mergeHoldings(profile.summary.holdings, profile.livePortfolio.holdings),
    [profile],
  );

  const totalValue = useMemo(
    () => holdings.reduce((sum, h) => sum + h.estimatedValue, 0),
    [holdings],
  );

  if (!holdings.length) {
    return (
      <div className="py-16 text-center text-sm text-zinc-600">
        No estimated holdings available for this politician.
      </div>
    );
  }

  const hasLiveData = holdings.some((h) => h.currentValue != null);

  return (
    <div>
      {/* Summary bar */}
      <div className="mb-5 flex flex-wrap items-center gap-x-6 gap-y-2">
        <div>
          <div className="text-[10px] font-semibold uppercase tracking-[0.2em] text-zinc-600">Est. Total Value</div>
          <div className="mt-0.5 text-xl font-semibold text-white">{formatCompactUsd(totalValue)}</div>
        </div>
        <div>
          <div className="text-[10px] font-semibold uppercase tracking-[0.2em] text-zinc-600">Positions</div>
          <div className="mt-0.5 text-xl font-semibold text-white">{holdings.length}</div>
        </div>
        {!hasLiveData ? (
          <div className="ml-auto rounded-lg bg-white/[0.03] px-3 py-1.5 text-[10px] text-zinc-500">
            Based on disclosed trade ranges
          </div>
        ) : null}
      </div>

      {/* Holdings list */}
      <div className="space-y-1">
        {holdings.map((holding) => {
          const barWidth = totalValue > 0 ? Math.max(2, (holding.estimatedValue / totalValue) * 100) : 0;

          return (
            <div
              key={holding.key}
              className="group relative overflow-hidden rounded-xl px-4 py-3.5 transition hover:bg-white/[0.025]"
            >
              {/* Background allocation bar */}
              <div
                className="absolute inset-y-0 left-0 bg-white/[0.015] transition-all group-hover:bg-white/[0.025]"
                style={{ width: `${barWidth}%` }}
              />

              <div className="relative flex items-center gap-4">
                <TickerAssetLogo ticker={holding.ticker} label={holding.label} size={38} />

                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2">
                    <span className="text-sm font-semibold text-white">{holding.label}</span>
                    {holding.ticker && holding.ticker !== holding.label ? (
                      <span className="text-[11px] text-zinc-600">{holding.ticker}</span>
                    ) : null}
                  </div>
                  <div className="mt-0.5 flex items-center gap-x-2 text-[11px] text-zinc-600">
                    <span>{holding.tradeCount} trade{holding.tradeCount === 1 ? '' : 's'}</span>
                    {holding.lastTradeDate ? (
                      <>
                        <span className="text-white/10">·</span>
                        <span>Last {formatCalendarDate(holding.lastTradeDate)}</span>
                      </>
                    ) : null}
                  </div>
                </div>

                <div className="shrink-0 text-right">
                  <div className="text-sm font-semibold tabular-nums text-white">
                    {formatCompactUsd(holding.estimatedValue)}
                  </div>
                  <div className="mt-0.5 flex items-center justify-end gap-2 text-[11px]">
                    {holding.unrealizedReturnPct != null ? (
                      <span
                        className={
                          holding.unrealizedReturnPct >= 0
                            ? 'text-emerald-400/80'
                            : 'text-red-400/80'
                        }
                      >
                        {formatPct(holding.unrealizedReturnPct)}
                      </span>
                    ) : null}
                    {holding.allocationPct != null && holding.allocationPct > 0 ? (
                      <span className="text-zinc-600">
                        {(holding.allocationPct * 100).toFixed(1)}%
                      </span>
                    ) : null}
                  </div>
                </div>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

/* ─── Main component ─── */

export default function PoliticianProfileModal({
  memberId,
  memberName,
  fallbackSummary = null,
  open,
  loading,
  error,
  profile,
  onClose,
}: PoliticianProfileModalProps) {
  const router = useRouter();
  const [session, setSession] = useState<Session | null>(null);
  const [showCreateSignal, setShowCreateSignal] = useState(false);
  const [signalMessage, setSignalMessage] = useState('');
  const [visibleTradeCount, setVisibleTradeCount] = useState(POLITICIAN_TRADE_PAGE_SIZE);
  const [isVisible, setIsVisible] = useState(false);
  const [activeTab, setActiveTab] = useState<TabId>('transactions');

  useEffect(() => {
    let frame = 0;
    let nestedFrame = 0;

    if (!open) {
      frame = requestAnimationFrame(() => {
        setIsVisible(false);
      });
      return () => cancelAnimationFrame(frame);
    }

    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';

    frame = requestAnimationFrame(() => {
      nestedFrame = requestAnimationFrame(() => {
        setIsVisible(true);
      });
    });

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        onClose();
      }
    };

    window.addEventListener('keydown', onKeyDown);
    return () => {
      cancelAnimationFrame(frame);
      cancelAnimationFrame(nestedFrame);
      document.body.style.overflow = previousOverflow;
      window.removeEventListener('keydown', onKeyDown);
    };
  }, [open, onClose]);

  useEffect(() => {
    if (!open) {
      return;
    }

    let mounted = true;

    supabase.auth.getSession().then(({ data }) => {
      if (mounted) {
        setSession(data.session);
      }
    });

    const { data } = supabase.auth.onAuthStateChange((_event, nextSession) => {
      setSession(nextSession);
    });

    return () => {
      mounted = false;
      data.subscription.unsubscribe();
    };
  }, [open]);

  useEffect(() => {
    if (!signalMessage) {
      return;
    }

    const timer = setTimeout(() => setSignalMessage(''), 3200);
    return () => clearTimeout(timer);
  }, [signalMessage]);

  if (!open) {
    return null;
  }

  const summary = profile?.summary;
  const headerSummary = summary || fallbackSummary;
  const headerName = headerSummary?.displayName || memberName;
  const partyPresentation = headerSummary ? getPartyPresentation(headerSummary.party || null, memberId) : null;
  const allTrades = profile?.trades || [];
  const visibleTrades = allTrades.slice(0, visibleTradeCount);
  const hasMoreTrades = visibleTradeCount < allTrades.length;

  function handleAlertClick() {
    if (!memberId) {
      return;
    }

    if (!session) {
      router.push('/auth?mode=signup');
      return;
    }

    setShowCreateSignal(true);
  }

  const tabs: { id: TabId; label: string; icon: typeof List }[] = [
    { id: 'transactions', label: 'Transactions', icon: List },
    { id: 'holdings', label: 'Est. Holdings', icon: Briefcase },
  ];

  return (
    <>
      <div
        className="fixed inset-0 z-[70] flex items-start justify-center overflow-y-auto px-4 py-6 sm:py-10"
        style={{
          backgroundColor: isVisible ? 'rgba(0,0,0,0.65)' : 'rgba(0,0,0,0)',
          transition: 'background-color 0.25s ease',
        }}
      >
        <button className="absolute inset-0" aria-label="Close profile overlay" onClick={onClose} />

        <div
          className="relative z-[71] w-full max-w-[860px]"
          style={{
            opacity: isVisible ? 1 : 0,
            transform: isVisible ? 'translateY(0) scale(1)' : 'translateY(12px) scale(0.98)',
            transition: 'opacity 0.2s ease, transform 0.2s ease',
          }}
        >
          <div
            className="flex max-h-[90vh] flex-col overflow-hidden rounded-2xl border border-white/[0.08]"
            style={{
              background: 'linear-gradient(180deg, rgba(16,22,30,0.98) 0%, rgba(10,14,20,0.98) 100%)',
              boxShadow: '0 24px 80px rgba(0,0,0,0.5), 0 0 0 1px rgba(255,255,255,0.03) inset',
            }}
          >
            {/* Accent strip */}
            <div
              className="h-[2px] w-full"
              style={{
                background: partyPresentation?.color
                  ? `linear-gradient(90deg, transparent, ${partyPresentation.color}, ${partyPresentation.color}88, transparent)`
                  : 'linear-gradient(90deg, transparent, #6366f1, #818cf8, transparent)',
              }}
            />

            {/* Header */}
            <div className="px-6 pb-0 pt-6 sm:px-8">
              <div className="flex items-start justify-between gap-4">
                <div className="flex min-w-0 items-center gap-4">
                  <PoliticianHeadshot
                    memberId={memberId}
                    name={headerName}
                    party={headerSummary?.party || null}
                    size={56}
                  />
                  <div className="min-w-0">
                    <h3 className="truncate text-2xl font-semibold tracking-[-0.02em] text-white">
                      {headerName}
                    </h3>
                    <div className="mt-1.5 flex flex-wrap items-center gap-x-2.5 gap-y-0.5 text-sm text-zinc-500">
                      {partyPresentation?.label ? (
                        <span style={{ color: partyPresentation.color }}>{partyPresentation.label}</span>
                      ) : null}
                      {headerSummary?.chamber ? (
                        <>
                          {partyPresentation?.label ? <span className="text-white/10">·</span> : null}
                          <span>{headerSummary.chamber}</span>
                        </>
                      ) : null}
                      {headerSummary?.state ? (
                        <>
                          <span className="text-white/10">·</span>
                          <span>{headerSummary.state}</span>
                        </>
                      ) : null}
                      {headerSummary?.latestTradeDate ? (
                        <>
                          <span className="text-white/10">·</span>
                          <span>Latest {formatCalendarDate(headerSummary.latestTradeDate)}</span>
                        </>
                      ) : null}
                    </div>
                  </div>
                </div>

                <div className="flex items-center gap-2.5 shrink-0">
                  {signalMessage ? (
                    <div className="rounded-lg bg-emerald-500/[0.1] px-3 py-1.5 text-xs font-medium text-emerald-400">
                      {signalMessage}
                    </div>
                  ) : null}
                  <button
                    type="button"
                    onClick={handleAlertClick}
                    className="inline-flex items-center gap-1.5 rounded-lg bg-emerald-500/[0.1] px-3.5 py-2 text-xs font-medium text-emerald-400 transition hover:bg-emerald-500/[0.18] hover:text-emerald-300"
                  >
                    <BellPlus className="h-3.5 w-3.5" />
                    {session ? 'Turn On Signals' : 'Sign In For Signals'}
                  </button>
                  <button
                    onClick={onClose}
                    className="inline-flex h-9 w-9 items-center justify-center rounded-lg text-zinc-500 transition hover:bg-white/[0.06] hover:text-zinc-300"
                    aria-label="Close profile"
                  >
                    <X className="h-4 w-4" />
                  </button>
                </div>
              </div>

              {/* Tabs */}
              <div className="mt-5 flex gap-1">
                {tabs.map((tab) => {
                  const isActive = activeTab === tab.id;
                  const Icon = tab.icon;
                  return (
                    <button
                      key={tab.id}
                      type="button"
                      onClick={() => setActiveTab(tab.id)}
                      className={`inline-flex items-center gap-1.5 rounded-t-lg border-b-2 px-4 py-2.5 text-xs font-medium transition ${
                        isActive
                          ? 'border-white/20 bg-white/[0.04] text-white'
                          : 'border-transparent text-zinc-500 hover:bg-white/[0.02] hover:text-zinc-300'
                      }`}
                    >
                      <Icon className="h-3.5 w-3.5" />
                      {tab.label}
                    </button>
                  );
                })}
              </div>
            </div>

            {/* Divider */}
            <div className="border-t border-white/[0.06]" />

            {/* Content */}
            <div className="min-h-0 flex-1 overflow-y-auto px-6 pb-6 pt-5 sm:px-8">
              {loading ? (
                <div className="space-y-3 py-4">
                  <div className="h-14 animate-pulse rounded-xl bg-white/[0.03]" />
                  <div className="h-14 animate-pulse rounded-xl bg-white/[0.03]" />
                  <div className="h-14 animate-pulse rounded-xl bg-white/[0.03]" />
                  <div className="h-14 animate-pulse rounded-xl bg-white/[0.03]" />
                  <div className="h-14 animate-pulse rounded-xl bg-white/[0.03]" />
                </div>
              ) : error ? (
                <div className="rounded-xl bg-red-500/[0.06] px-5 py-4 text-sm text-red-400/90">
                  {error}
                </div>
              ) : profile && summary ? (
                <>
                  {activeTab === 'transactions' ? (
                    <>
                      <div className="mb-4 flex items-center justify-between">
                        <span className="text-[10px] font-semibold uppercase tracking-[0.2em] text-zinc-600">
                          Recent Transactions
                        </span>
                        <span className="text-xs text-zinc-600">
                          {visibleTrades.length} of {allTrades.length}
                        </span>
                      </div>

                      <div className="space-y-1">
                        {visibleTrades.map((trade) => {
                          const direction = String(trade.transaction_type || '').toLowerCase();
                          const isBuy = direction.startsWith('buy') || direction === 'purchase';
                          const optionDetails = parsePoliticianOptionDetails(trade);
                          const assetLabel = tradeAssetLabel(trade);
                          const tickerLabel =
                            trade.ticker && !['N/A', 'UNKNOWN'].includes(trade.ticker) ? trade.ticker : null;

                          return (
                            <div
                              key={trade.id}
                              className="group flex items-center gap-4 rounded-xl px-4 py-3.5 transition hover:bg-white/[0.025]"
                            >
                              <TickerAssetLogo ticker={tickerLabel} label={assetLabel} size={38} />

                              <div className="min-w-0 flex-1">
                                <div className="flex flex-wrap items-center gap-2">
                                  <span className="text-sm font-medium text-white">{assetLabel}</span>
                                  <span
                                    className={`inline-flex items-center gap-1 rounded-md px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wider ${
                                      isBuy
                                        ? 'bg-emerald-500/[0.1] text-emerald-400/90'
                                        : 'bg-red-500/[0.1] text-red-400/90'
                                    }`}
                                  >
                                    {isBuy ? <TrendingUp className="h-2.5 w-2.5" /> : <TrendingDown className="h-2.5 w-2.5" />}
                                    {isBuy ? 'Buy' : 'Sell'}
                                  </span>
                                  {optionDetails ? (
                                    <OptionTradeBadge
                                      label={optionDetails.badgeLabel}
                                      tooltip={optionDetails.tooltip}
                                      className="inline-flex items-center rounded-md bg-orange-500/[0.08] px-1.5 py-0.5 text-[10px] font-semibold text-orange-400/90"
                                    />
                                  ) : null}
                                </div>

                                <div className="mt-1 flex flex-wrap items-center gap-x-2 text-xs text-zinc-600">
                                  <span>{trade.amount_range || 'Unknown range'}</span>
                                  {tickerLabel ? (
                                    <>
                                      <span className="text-white/10">·</span>
                                      <span>{tickerLabel}</span>
                                    </>
                                  ) : null}
                                </div>
                              </div>

                              <div className="shrink-0 text-right">
                                <div className="text-sm text-zinc-300">{formatCalendarDate(trade.transaction_date)}</div>
                                <div className="mt-0.5 text-[11px] text-zinc-600">
                                  Filed {formatCalendarDate(trade.published_date)}
                                </div>
                              </div>

                              <div className="w-14 shrink-0 text-right">
                                {trade.source_url ? (
                                  <a
                                    href={trade.source_url}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    className="inline-flex items-center gap-1 text-xs text-zinc-600 transition hover:text-zinc-400"
                                  >
                                    {trade.source_url.includes('efdsearch.senate.gov') ? 'Filing' : 'PDF'}
                                    <ExternalLink className="h-3 w-3" />
                                  </a>
                                ) : null}
                              </div>
                            </div>
                          );
                        })}

                        {!visibleTrades.length ? (
                          <div className="py-12 text-center text-sm text-zinc-600">
                            No trades available for this politician.
                          </div>
                        ) : null}
                      </div>

                      {hasMoreTrades ? (
                        <div className="mt-4">
                          <button
                            type="button"
                            onClick={() => setVisibleTradeCount((count) => count + POLITICIAN_TRADE_PAGE_SIZE)}
                            className="w-full rounded-xl bg-white/[0.03] px-4 py-3 text-sm font-medium text-zinc-500 transition hover:bg-white/[0.05] hover:text-zinc-300"
                          >
                            Show more trades
                          </button>
                        </div>
                      ) : null}
                    </>
                  ) : (
                    <HoldingsTab profile={profile} />
                  )}
                </>
              ) : (
                <div className="py-12 text-center text-sm text-zinc-600">
                  No profile data available for this politician yet.
                </div>
              )}
            </div>
          </div>
        </div>
      </div>

      {showCreateSignal && session && memberId ? (
        <CreateSignalModal
          session={session}
          initialKind="politician"
          initialQuery={headerName}
          initialActor={{
            actorType: 'politician',
            actorName: headerName,
            actorKey: memberId,
            subtitle: headerSummary
              ? [headerSummary.party || 'Congress', headerSummary.state, headerSummary.chamber].filter(Boolean).join(' • ')
              : null,
          }}
          lockActorContext
          zIndex={90}
          onCreated={() => {
            setSignalMessage(`Signals enabled for ${headerName}.`);
          }}
          onClose={() => setShowCreateSignal(false)}
        />
      ) : null}
    </>
  );
}
