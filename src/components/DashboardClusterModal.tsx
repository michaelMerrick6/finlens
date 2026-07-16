'use client';

import Image, { type ImageLoaderProps } from 'next/image';
import { useEffect, useRef, useState } from 'react';
import { useRouter } from 'next/navigation';
import { createPortal } from 'react-dom';
import { ExternalLink, Loader2, Shield, UserRound, X } from 'lucide-react';

import OptionTradeBadge from '@/components/OptionTradeBadge';
import PoliticianHeadshot from '@/components/PoliticianHeadshot';
import {
  clusterCategoryLabel,
  clusterEvidenceItems,
  clusterHeadline,
  clusterReason,
} from '@/lib/cluster-presentation';
import { getTickerLogoUrl } from '@/lib/company-logos';
import type { DashboardClusterDetail, DashboardClusterTransaction } from '@/lib/dashboard-cluster-types';
import { formatCalendarDate } from '@/lib/date-format';
import { parsePoliticianOptionDetails } from '@/lib/politician-option-trades';

type ClusterPreview = {
  id: string;
  ticker: string;
  title: string;
  summary: string;
  ruleLabel: string;
  actorPreview: string | null;
  actorCount: number;
  amountLabel: string | null;
  amountFloor?: number;
  sourceLabel: string;
  publishedAt: string | null;
  direction: 'buy' | 'sell' | null;
  ruleKey?: string;
  sourceCounts?: {
    congress: number;
    insiders: number;
    funds: number;
  };
  windowDays?: number | null;
};

type DashboardClusterModalProps = {
  cluster: ClusterPreview | null;
  detail: DashboardClusterDetail | null;
  loading: boolean;
  error: string;
  open: boolean;
  onClose: () => void;
};

const passthroughImageLoader = ({ src }: ImageLoaderProps) => src;

function TickerLogo({ ticker }: { ticker: string }) {
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  const logoUrl = getTickerLogoUrl(ticker, 56);
  const activeLogoUrl = logoUrl && failedUrl !== logoUrl ? logoUrl : null;
  const hue = ticker.split('').reduce((acc, ch) => acc + ch.charCodeAt(0), 0) % 360;

  if (activeLogoUrl) {
    return (
      <div className="h-11 w-11 shrink-0 overflow-hidden rounded-full border border-white/10 bg-black/30">
        <Image
          loader={passthroughImageLoader}
          unoptimized
          src={activeLogoUrl}
          alt={ticker}
          width={44}
          height={44}
          sizes="44px"
          className="h-full w-full object-contain p-0.5"
          onError={() => setFailedUrl(activeLogoUrl)}
        />
      </div>
    );
  }

  return (
    <div
      className="flex h-11 w-11 shrink-0 items-center justify-center rounded-full border border-white/10 text-[13px] font-bold text-white"
      style={{
        background: `linear-gradient(135deg, hsl(${hue},65%,45%), hsl(${(hue + 40) % 360},65%,38%))`,
      }}
    >
      {ticker.slice(0, 2)}
    </div>
  );
}

function formatDate(value: string | null | undefined) {
  if (!value) {
    return '—';
  }
  return formatCalendarDate(value, 'UTC');
}

function MonogramAvatar({ label, kind }: { label: string; kind: DashboardClusterTransaction['sourceType'] }) {
  if (kind === 'insider') {
    return (
      <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-full border border-emerald-400/20 bg-emerald-500/10 text-emerald-300 shadow-[0_0_18px_rgba(16,185,129,0.08)]">
        <UserRound className="h-4 w-4" />
      </div>
    );
  }

  const initials = label
    .split(/\s+/)
    .map((part) => part.charAt(0))
    .join('')
    .slice(0, 2)
    .toUpperCase();

  const tone = kind === 'fund' ? 'from-emerald-500/80 to-emerald-700/80' : 'from-zinc-500/80 to-zinc-700/80';

  return (
    <div className={`flex h-9 w-9 shrink-0 items-center justify-center rounded-full border border-white/10 bg-gradient-to-br ${tone} text-[11px] font-semibold text-white`}>
      {initials || <Shield className="h-4 w-4" />}
    </div>
  );
}

function ClusterActorAvatar({ transaction }: { transaction: DashboardClusterTransaction }) {
  if (transaction.sourceType === 'politician' && transaction.memberId) {
    return (
      <PoliticianHeadshot
        memberId={transaction.memberId}
        name={transaction.actorName}
        party={transaction.party}
        size={36}
      />
    );
  }

  return <MonogramAvatar label={transaction.actorName} kind={transaction.sourceType} />;
}

function optionDetailsForTransaction(transaction: DashboardClusterTransaction) {
  if (transaction.sourceType !== 'politician') {
    return null;
  }

  return parsePoliticianOptionDetails({
    asset_name: transaction.assetName,
    asset_type: transaction.assetType,
  });
}

function uniqueActorCount(transactions: DashboardClusterTransaction[]) {
  const actors = new Set(
    transactions.map((transaction) => transaction.memberId || transaction.actorName.trim().toLowerCase()).filter(Boolean),
  );
  return actors.size;
}

export default function DashboardClusterModal({
  cluster,
  detail,
  loading,
  error,
  open,
  onClose,
}: DashboardClusterModalProps) {
  const router = useRouter();
  const panelRef = useRef<HTMLDivElement>(null);
  const [isVisible, setIsVisible] = useState(false);

  useEffect(() => {
    if (!open) {
      const frame = requestAnimationFrame(() => setIsVisible(false));
      return () => cancelAnimationFrame(frame);
    }

    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';

    // Trigger animation after the portal is mounted.
    let innerFrame = 0;
    const outerFrame = requestAnimationFrame(() => {
      innerFrame = requestAnimationFrame(() => {
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
      cancelAnimationFrame(outerFrame);
      cancelAnimationFrame(innerFrame);
      document.body.style.overflow = previousOverflow;
      window.removeEventListener('keydown', onKeyDown);
    };
  }, [open, onClose]);

  if (!open || !cluster || typeof document === 'undefined') {
    return null;
  }

  const transactions = detail?.transactions || [];
  const resolvedActorCount = transactions.length ? uniqueActorCount(transactions) : cluster.actorCount;
  const isBuy = cluster.direction !== 'sell';
  const inferredRuleKey = cluster.ruleKey || (
    cluster.ruleLabel.toLowerCase().includes('cross')
      ? 'cross_source_accumulation'
      : cluster.ruleLabel.toLowerCase().includes('insider')
        ? 'insider_cluster'
        : 'congress_cluster'
  );
  const presentationCluster = {
    ...cluster,
    amountFloor: cluster.amountFloor || 0,
    ruleKey: inferredRuleKey,
    sourceCounts: cluster.sourceCounts || (
      inferredRuleKey === 'insider_cluster'
        ? { congress: 0, insiders: cluster.actorCount, funds: 0 }
        : inferredRuleKey === 'congress_cluster'
          ? { congress: cluster.actorCount, insiders: 0, funds: 0 }
          : { congress: 0, insiders: 0, funds: 0 }
    ),
    windowDays: cluster.windowDays ?? null,
  };
  const evidence = clusterEvidenceItems(presentationCluster);

  return createPortal(
    <div
      className="fixed inset-0 z-[200] flex items-center justify-center overflow-hidden px-4 py-6 backdrop-blur-sm sm:py-8"
      style={{
        backgroundColor: isVisible ? 'rgba(0,0,0,0.68)' : 'rgba(0,0,0,0)',
        transition: 'background-color 0.28s cubic-bezier(0.16,1,0.3,1)',
      }}
    >
      <style>{`
        @keyframes clusterTransactionIn {
          from { opacity: 0; transform: translateY(8px); }
          to { opacity: 1; transform: translateY(0); }
        }
      `}</style>
      <button type="button" aria-label="Close cluster detail" onClick={onClose} className="absolute inset-0" />

      <div
        ref={panelRef}
        role="dialog"
        aria-modal="true"
        className="cluster-detail-panel relative z-[201] max-h-[calc(100dvh-3rem)] w-full max-w-[640px] overflow-y-auto overscroll-contain rounded-2xl pb-3 sm:max-h-[calc(100dvh-4rem)]"
        style={{
          opacity: isVisible ? 1 : 0,
          transform: isVisible ? 'translateY(0) scale(1)' : 'translateY(18px) scale(0.975)',
          transition: 'opacity 0.28s cubic-bezier(0.16,1,0.3,1), transform 0.28s cubic-bezier(0.16,1,0.3,1)',
        }}
      >
        {/* Header card */}
        <div
          className="overflow-hidden rounded-2xl border border-white/[0.08]"
          style={{
            background: 'linear-gradient(180deg, rgba(16,22,30,0.98) 0%, rgba(10,14,20,0.98) 100%)',
            boxShadow: '0 24px 80px rgba(0,0,0,0.5), 0 0 0 1px rgba(255,255,255,0.03) inset',
          }}
        >
          {/* Accent strip at top */}
          <div
            className="h-[2px] w-full"
            style={{
              background: isBuy
                ? 'linear-gradient(90deg, transparent, #10b981, #34d399, transparent)'
                : 'linear-gradient(90deg, transparent, #ef4444, #f87171, transparent)',
            }}
          />

          {/* Header section */}
          <div className="px-5 pb-4 pt-5 sm:px-6">
            <div className="flex items-start justify-between gap-4">
              <div className="flex items-start gap-3.5 min-w-0">
                <TickerLogo ticker={cluster.ticker} />
                <div className="min-w-0 pt-0.5">
                  <div className="flex items-center gap-2">
                    <span className="text-[13px] font-bold tracking-[0.12em] text-white/90">{cluster.ticker}</span>
                    <span
                      className={`inline-flex items-center rounded-md px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider ${
                        isBuy
                          ? 'bg-emerald-500/[0.12] text-emerald-400'
                          : 'bg-red-500/[0.12] text-red-400'
                      }`}
                    >
                      {clusterCategoryLabel(presentationCluster)}
                    </span>
                  </div>
                  <h3 className="mt-1.5 text-[17px] font-semibold leading-snug tracking-[-0.01em] text-white">{clusterHeadline(presentationCluster)}</h3>
                </div>
              </div>

              <button
                type="button"
                onClick={onClose}
                className="mt-0.5 inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-lg text-zinc-500 transition hover:bg-white/[0.06] hover:text-zinc-300"
              >
                <X className="h-4 w-4" />
              </button>
            </div>

            {/* Meta row */}
            <div className="mt-3.5 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-zinc-500">
              {cluster.amountLabel ? (
                <span className="font-semibold text-white/80">Tracked floor {cluster.amountLabel}</span>
              ) : null}
              <span>{resolvedActorCount} actor{resolvedActorCount === 1 ? '' : 's'}</span>
              <span>{transactions.length || '—'} transaction{transactions.length === 1 ? '' : 's'}</span>
              <span>{formatDate(cluster.publishedAt)}</span>
            </div>

            <div className="mt-4 rounded-xl border border-white/[0.055] bg-white/[0.018] px-3.5 py-3">
              <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-zinc-600">Why it matters</div>
              <p className="mt-1.5 text-xs leading-5 text-zinc-400">{clusterReason(presentationCluster)}</p>
              <div className="mt-2.5 flex flex-wrap gap-1.5">
                {evidence.map((item) => (
                  <span
                    key={item.key}
                    className="rounded-md border border-white/[0.065] bg-white/[0.025] px-2 py-1 text-[10px] font-medium text-zinc-400"
                  >
                    {item.label}
                  </span>
                ))}
              </div>
            </div>
          </div>

          {/* Divider */}
          <div className="mx-5 border-t border-white/[0.06] sm:mx-6" />

          {/* Transactions section */}
          <div className="px-5 pb-5 pt-4 sm:px-6">
            <div className="mb-3 text-[10px] font-semibold uppercase tracking-[0.2em] text-zinc-600">
              Supporting filings
            </div>

            {loading ? (
              <div className="space-y-2 py-1">
                <div className="mb-3 flex items-center gap-2.5 text-xs text-zinc-500">
                  <Loader2 className="h-3.5 w-3.5 animate-spin text-zinc-600" />
                  Loading cluster details…
                </div>
                {[0, 1, 2].map((item) => (
                  <div
                    key={item}
                    className="flex animate-pulse items-center gap-3 rounded-xl bg-white/[0.018] px-3 py-2.5"
                  >
                    <div className="h-9 w-9 rounded-full bg-white/[0.055]" />
                    <div className="min-w-0 flex-1 space-y-2">
                      <div className="h-3 w-40 rounded-full bg-white/[0.06]" />
                      <div className="h-2.5 w-56 rounded-full bg-white/[0.035]" />
                    </div>
                    <div className="h-3 w-16 rounded-full bg-white/[0.05]" />
                  </div>
                ))}
              </div>
            ) : error ? (
              <div className="rounded-xl bg-red-500/[0.06] px-4 py-3 text-sm text-red-400/90">
                {error}
              </div>
            ) : !transactions.length ? (
              <div className="py-8 text-center text-xs text-zinc-600">
                No transactions resolved yet.
              </div>
            ) : (
              <div className="space-y-0.5">
                {transactions.map((transaction, index) => {
                  const optionDetails = optionDetailsForTransaction(transaction);

                  return (
                    <div
                      key={transaction.id}
                      className="group flex items-center gap-3 rounded-xl px-3 py-2.5 transition hover:bg-white/[0.025]"
                      style={{
                        animation: 'clusterTransactionIn 260ms cubic-bezier(0.16,1,0.3,1) both',
                        animationDelay: `${Math.min(index * 28, 220)}ms`,
                      }}
                    >
                      <ClusterActorAvatar transaction={transaction} />

                      <div className="min-w-0 flex-1">
                        <div className="flex flex-wrap items-center gap-1.5">
                          {transaction.sourceType === 'politician' && transaction.memberId ? (
                            <button
                              type="button"
                              onClick={(e) => {
                                e.stopPropagation();
                                onClose();
                                router.push(`/politicians?profile=${encodeURIComponent(transaction.memberId!)}`);
                              }}
                              className="text-sm font-medium text-white transition hover:text-emerald-400"
                            >
                              {transaction.actorName}
                            </button>
                          ) : (
                            <span className="text-sm font-medium text-white">{transaction.actorName}</span>
                          )}
                          <span
                            className={`inline-flex items-center rounded-md px-1.5 py-[1px] text-[9px] font-semibold uppercase tracking-wider ${
                              transaction.transactionTypeLabel.toLowerCase().includes('buy') || transaction.transactionTypeLabel.toLowerCase().includes('purchase')
                                ? 'bg-emerald-500/[0.1] text-emerald-400/90'
                                : transaction.transactionTypeLabel.toLowerCase().includes('sell') || transaction.transactionTypeLabel.toLowerCase().includes('sale')
                                  ? 'bg-red-500/[0.1] text-red-400/90'
                                  : 'bg-white/[0.04] text-zinc-400'
                            }`}
                          >
                            {transaction.transactionTypeLabel}
                          </span>
                          {optionDetails ? (
                            <OptionTradeBadge
                              label={optionDetails.badgeLabel}
                              tooltip={optionDetails.tooltip}
                              className="inline-flex items-center rounded-md bg-orange-500/[0.08] px-1.5 py-[1px] text-[9px] font-semibold text-orange-400/90"
                            />
                          ) : null}
                        </div>

                        <div className="mt-0.5 flex flex-wrap items-center gap-x-1.5 text-[11px] text-zinc-600">
                          {transaction.actorSubtitle ? <span>{transaction.actorSubtitle}</span> : null}
                          {transaction.transactionDate ? (
                            <>
                              {transaction.actorSubtitle ? <span className="text-white/10">·</span> : null}
                              <span>{formatDate(transaction.transactionDate)}</span>
                            </>
                          ) : null}
                        </div>
                      </div>

                      <div className="shrink-0 text-right">
                        <div className="text-sm font-semibold tabular-nums text-white/90">{transaction.amountLabel || '—'}</div>
                        {transaction.sourceUrl ? (
                          <a
                            href={transaction.sourceUrl}
                            target="_blank"
                            rel="noreferrer"
                            className="mt-0.5 inline-flex items-center gap-1 text-[10px] text-zinc-600 transition hover:text-zinc-400"
                          >
                            Source <ExternalLink className="h-2.5 w-2.5" />
                          </a>
                        ) : null}
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>,
    document.body,
  );
}
