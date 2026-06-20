'use client';

import { useMemo, useState } from 'react';
import Link from 'next/link';
import {
  ArrowUpRight,
  Building2,
  Landmark,
  ShieldAlert,
  TrendingDown,
  TrendingUp,
} from 'lucide-react';

import PoliticianHeadshot from '@/components/PoliticianHeadshot';
import InsiderTransactionsModal from '@/components/InsiderTransactionsModal';
import { formatCalendarDate } from '@/lib/date-format';
import { buildInsiderPositionLabel } from '@/lib/insider-position-label';
import type {
  TickerFundHolder,
  TickerFundSummary,
  TickerInsiderHolding,
  TickerInsightTone,
  TickerInsiderTransaction,
  TickerInsiderWindow,
  TickerIntelligencePayload,
  TickerPoliticianHolder,
  TickerPoliticianTransaction,
} from '@/lib/ticker-intelligence-types';

type TickerIntelligencePageProps = TickerIntelligencePayload;

type HolderTab = 'politicians' | 'insiders' | 'hedge-funds';
type InsiderViewMode = 'feed' | 'holdings';

function formatCompactCurrency(value: number | null | undefined) {
  const amount = Number(value || 0);
  if (!Number.isFinite(amount) || amount === 0) {
    return '$0';
  }
  const abs = Math.abs(amount);
  const prefix = amount < 0 ? '-$' : '$';
  if (abs >= 1_000_000_000) {
    return `${prefix}${(abs / 1_000_000_000).toFixed(1)}B`;
  }
  if (abs >= 1_000_000) {
    return `${prefix}${(abs / 1_000_000).toFixed(1)}M`;
  }
  if (abs >= 1_000) {
    return `${prefix}${(abs / 1_000).toFixed(0)}K`;
  }
  return `${prefix}${Math.round(abs).toLocaleString()}`;
}

function formatCompactNumber(value: number | null | undefined) {
  const amount = Number(value || 0);
  if (!Number.isFinite(amount) || amount <= 0) {
    return '0';
  }
  if (amount >= 1_000_000_000) {
    return `${(amount / 1_000_000_000).toFixed(1)}B`;
  }
  if (amount >= 1_000_000) {
    return `${(amount / 1_000_000).toFixed(1)}M`;
  }
  if (amount >= 1_000) {
    return `${(amount / 1_000).toFixed(1)}K`;
  }
  return Math.round(amount).toLocaleString();
}

function formatPrice(value: number | null | undefined) {
  const amount = Number(value);
  if (!Number.isFinite(amount) || amount <= 0) {
    return 'N/A';
  }
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: amount >= 100 ? 2 : 4,
  }).format(amount);
}

function formatTransactionLabel(value: string) {
  const normalized = value.trim().toLowerCase();
  if (normalized === 'buy' || normalized === 'purchase') return 'Buy';
  if (normalized === 'sell' || normalized === 'sale') return 'Sell';
  return value || 'Other';
}

function normalizeDateValue(value: string | null | undefined) {
  return String(value || '').trim();
}

function defaultInsiderWindowKey(windows: TickerInsiderWindow[]) {
  return windows.find((item) => item.key === 'all')?.key || windows[0]?.key || '';
}

function earliestInsiderCoverageDate(transactions: TickerInsiderTransaction[]) {
  return transactions.reduce<string | null>((earliest, trade) => {
    const dateValue = normalizeDateValue(trade.publishedDate || trade.transactionDate);
    if (!dateValue) {
      return earliest;
    }
    return !earliest || dateValue < earliest ? dateValue : earliest;
  }, null);
}

function tonePresentation(tone: TickerInsightTone) {
  if (tone === 'bullish') {
    return {
      label: 'Bullish',
      pill: 'border-emerald-500/20 bg-emerald-500/10 text-emerald-300',
      text: 'text-emerald-300',
      ring: '#10b981',
      icon: TrendingUp,
    };
  }
  if (tone === 'bearish') {
    return {
      label: 'Bearish',
      pill: 'border-red-500/20 bg-red-500/10 text-red-300',
      text: 'text-red-300',
      ring: '#ef4444',
      icon: TrendingDown,
    };
  }
  return {
    label: 'Neutral',
    pill: 'border-white/10 bg-white/[0.05] text-zinc-300',
    text: 'text-zinc-300',
    ring: '#a1a1aa',
    icon: TrendingUp,
  };
}

function SectionHeader({
  eyebrow,
  title,
  detail,
}: {
  eyebrow: string;
  title: string;
  detail: string;
}) {
  return (
    <div>
      <div className="text-[11px] font-semibold uppercase tracking-[0.2em] text-zinc-600">{eyebrow}</div>
      <h2 className="mt-3 text-3xl font-semibold tracking-[-0.03em] text-white">{title}</h2>
      <p className="mt-2 max-w-3xl text-sm leading-6 text-zinc-400">{detail}</p>
    </div>
  );
}

function EmptyState({ message }: { message: string }) {
  return (
    <div className="rounded-[1.75rem] border border-dashed border-white/[0.08] bg-white/[0.02] px-5 py-10 text-center text-sm text-zinc-500">
      {message}
    </div>
  );
}

function TopSummaryCard({
  label,
  value,
  detail,
}: {
  label: string;
  value: string;
  detail: string;
}) {
  return (
    <div className="rounded-[1.5rem] border border-white/[0.08] bg-white/[0.03] p-4">
      <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-zinc-600">{label}</div>
      <div className="mt-2 text-2xl font-semibold text-white">{value}</div>
      <div className="mt-1 text-sm text-zinc-500">{detail}</div>
    </div>
  );
}

function PoliticianHolderTable({ holders }: { holders: TickerPoliticianHolder[] }) {
  if (!holders.length) {
    return <EmptyState message="No politician holders estimated from the filed trades for this stock yet." />;
  }

  return (
    <div className="overflow-x-auto">
      <table className="min-w-full border-collapse text-left">
        <thead>
          <tr className="border-b border-white/10 text-xs uppercase tracking-[0.16em] text-zinc-500">
            <th className="px-0 py-3 font-medium">Holder</th>
            <th className="px-4 py-3 font-medium">Chamber</th>
            <th className="px-4 py-3 font-medium">Min</th>
            <th className="px-4 py-3 font-medium">Mid</th>
            <th className="px-4 py-3 font-medium">Max</th>
            <th className="px-4 py-3 font-medium">Last Activity</th>
          </tr>
        </thead>
        <tbody>
          {holders.map((holder) => {
            return (
              <tr key={holder.key} className="border-b border-white/[0.06] align-top last:border-0">
                <td className="px-0 py-4">
                  <div className="flex items-center gap-3">
                    <PoliticianHeadshot
                      memberId={holder.memberId}
                      name={holder.name}
                      party={holder.party}
                      size={40}
                    />
                    <div>
                      <div className="text-sm font-medium text-white">{holder.name}</div>
                      <div className="mt-1 text-xs text-zinc-500">{holder.tradeCount} filed trades touching this stock</div>
                    </div>
                  </div>
                </td>
                <td className="px-4 py-4 text-sm text-zinc-300">{holder.chamber || 'Unknown'}</td>
                <td className="px-4 py-4 text-sm text-zinc-300">{formatCompactCurrency(holder.minValue)}</td>
                <td className="px-4 py-4 text-sm font-medium text-white">{formatCompactCurrency(holder.midValue)}</td>
                <td className="px-4 py-4 text-sm text-zinc-300">{formatCompactCurrency(holder.maxValue)}</td>
                <td className="px-4 py-4 text-sm text-zinc-300">{formatCalendarDate(holder.lastTradeDate)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function PoliticianTransactionsTable({
  transactions,
}: {
  transactions: TickerPoliticianTransaction[];
}) {
  if (!transactions.length) {
    return <EmptyState message="No recent politician transactions found for this stock." />;
  }

  return (
    <div className="overflow-x-auto">
      <table className="min-w-full border-collapse text-left">
        <thead>
          <tr className="border-b border-white/10 text-xs uppercase tracking-[0.16em] text-zinc-500">
            <th className="px-0 py-3 font-medium">Name</th>
            <th className="px-4 py-3 font-medium">Type</th>
            <th className="px-4 py-3 font-medium">Traded</th>
            <th className="px-4 py-3 font-medium">Filed</th>
            <th className="px-4 py-3 font-medium">Amount</th>
          </tr>
        </thead>
        <tbody>
          {transactions.map((trade) => {
            const label = formatTransactionLabel(trade.transactionType);
            const toneClass =
              label === 'Buy'
                ? 'border-emerald-500/20 bg-emerald-500/10 text-emerald-300'
                : label === 'Sell'
                  ? 'border-red-500/20 bg-red-500/10 text-red-300'
                  : 'border-white/10 bg-white/[0.05] text-zinc-300';
            return (
              <tr key={trade.id} className="border-b border-white/[0.06] align-top last:border-0">
                <td className="px-0 py-4">
                  <div className="flex items-center gap-3">
                    <PoliticianHeadshot
                      memberId={trade.memberId}
                      name={trade.name}
                      party={trade.party}
                      size={36}
                    />
                    <div>
                      <div className="text-sm font-medium text-white">{trade.name}</div>
                      <div className="mt-1 text-xs text-zinc-500">{trade.chamber || 'Unknown chamber'}</div>
                    </div>
                  </div>
                </td>
                <td className="px-4 py-4">
                  <span className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-medium ${toneClass}`}>
                    {label}
                  </span>
                </td>
                <td className="px-4 py-4 text-sm text-zinc-300">{formatCalendarDate(trade.transactionDate)}</td>
                <td className="px-4 py-4 text-sm text-zinc-300">{formatCalendarDate(trade.publishedDate)}</td>
                <td className="px-4 py-4 text-sm text-zinc-300">
                  {trade.sourceUrl ? (
                    <Link
                      href={trade.sourceUrl}
                      target="_blank"
                      rel="noreferrer"
                      className="inline-flex items-center gap-1.5 hover:text-white"
                    >
                      {trade.amountRange || 'Undisclosed'}
                      <ArrowUpRight className="h-3.5 w-3.5" />
                    </Link>
                  ) : (
                    trade.amountRange || 'Undisclosed'
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function InsiderSentimentPanel({
  windows,
  transactions,
  holdings,
}: {
  windows: TickerInsiderWindow[];
  transactions: TickerInsiderTransaction[];
  holdings: TickerInsiderHolding[];
}) {
  const [selectedWindowKey, setSelectedWindowKey] = useState(defaultInsiderWindowKey(windows));
  const [viewMode, setViewMode] = useState<InsiderViewMode>('feed');
  const [selectedInsiderKey, setSelectedInsiderKey] = useState<string | null>(null);
  const [visibleCount, setVisibleCount] = useState(16);
  const fallbackWindowKey = defaultInsiderWindowKey(windows);
  const windowData =
    windows.find((item) => item.key === selectedWindowKey) ||
    windows.find((item) => item.key === fallbackWindowKey) ||
    windows[0] ||
    null;
  const filteredTransactions = useMemo(() => {
    if (!windowData) {
      return [];
    }
    if (typeof windowData.days !== 'number') {
      return transactions;
    }
    const todayIso = new Date().toISOString().slice(0, 10);
    const cutoff = new Date(`${todayIso}T12:00:00Z`);
    cutoff.setUTCDate(cutoff.getUTCDate() - windowData.days);
    const cutoffIso = cutoff.toISOString().slice(0, 10);
    return transactions.filter((trade) => {
      const dateValue = String(trade.transactionDate || trade.publishedDate || '').trim();
      return Boolean(dateValue && dateValue >= cutoffIso);
    });
  }, [transactions, windowData]);
  const coverageStartDate = useMemo(() => earliestInsiderCoverageDate(filteredTransactions), [filteredTransactions]);
  const selectedHolding = useMemo(
    () => holdings.find((holding) => holding.key === selectedInsiderKey) || null,
    [holdings, selectedInsiderKey],
  );
  const selectedTransactions = useMemo(
    () => (selectedInsiderKey ? transactions.filter((trade) => trade.identityKey === selectedInsiderKey) : []),
    [transactions, selectedInsiderKey],
  );
  const selectedInsiderName = selectedHolding?.filerName || selectedTransactions[0]?.filerName || '';
  const selectedInsiderRelation = selectedHolding?.filerRelation || selectedTransactions[0]?.filerRelation || null;
  const visibleTransactions = filteredTransactions.slice(0, visibleCount);
  const hasMoreTransactions = visibleCount < filteredTransactions.length;

  if (!windowData) {
    return (
      <div className="space-y-6">
        <EmptyState message="No insider filings available for this stock yet." />
      </div>
    );
  }

  const tone = tonePresentation(windowData.tone);
  const Icon = tone.icon;
  const buyDegrees = Math.round(windowData.buyRatio * 360);
  const isAllWindow = windowData.days === null;
  const sentimentBadgeLabel =
    isAllWindow && coverageStartDate
      ? `All since ${formatCalendarDate(coverageStartDate)}`
      : `${windowData.label} sentiment`;
  const transactionPanelTitle = isAllWindow ? 'Insider transactions' : 'Recent insider transactions';
  const transactionPanelDetail =
    isAllWindow && coverageStartDate
      ? `${windowData.transactionCount} insider filings since ${formatCalendarDate(coverageStartDate)}.`
      : `${windowData.transactionCount} insider filings matched in the selected window.`;
  const holdingsPanelDetail = `${holdings.length} insiders with holdings derived from their latest Form 4 on record. Sorted from largest to smallest position.`;
  const ringStyle = {
    background: `conic-gradient(#10b981 0 ${buyDegrees}deg, #ef4444 ${buyDegrees}deg 360deg)`,
  };

  return (
    <div className="space-y-6">
      <div className="rounded-[1.75rem] border border-white/[0.08] bg-white/[0.03] p-5">
        <div className="flex flex-wrap items-center gap-2">
          {windows.map((item) => (
            <button
              key={item.key}
              type="button"
              onClick={() => {
                setSelectedWindowKey(item.key);
                setVisibleCount(16);
              }}
              className={`rounded-full border px-3 py-1.5 text-xs font-medium transition ${
                item.key === windowData.key
                  ? 'border-[#10b981]/25 bg-[#10b981]/12 text-[#7ee7c4]'
                  : 'border-white/[0.08] bg-white/[0.03] text-zinc-500 hover:text-white'
              }`}
            >
              {item.label}
            </button>
          ))}
        </div>

        <div className="mt-6 grid gap-6 lg:grid-cols-[220px_minmax(0,1fr)] lg:items-center">
          <div className="flex flex-col items-center gap-4">
            <div className="relative h-40 w-40 rounded-full p-3" style={ringStyle}>
              <div className="flex h-full w-full flex-col items-center justify-center rounded-full bg-[#070707]">
                <Icon className={`h-6 w-6 ${tone.text}`} />
                <div className={`mt-2 text-lg font-semibold ${tone.text}`}>{tone.label}</div>
                <div className="mt-1 text-xs text-zinc-500">{Math.round(windowData.buyRatio * 100)}% buy-weighted</div>
              </div>
            </div>
            <span className={`inline-flex rounded-full border px-3 py-1 text-xs font-medium ${tone.pill}`}>
              {sentimentBadgeLabel}
            </span>
          </div>

          <div className="grid gap-4 sm:grid-cols-3">
            <TopSummaryCard
              label="Buying"
              value={formatCompactCurrency(windowData.buyValue)}
              detail={`${windowData.buyCount} insider buys`}
            />
            <TopSummaryCard
              label="Selling"
              value={formatCompactCurrency(windowData.sellValue)}
              detail={`${windowData.sellCount} insider sells`}
            />
            <TopSummaryCard
              label="Net"
              value={formatCompactCurrency(windowData.netValue)}
              detail={
                windowData.netValue > 0
                  ? 'Net insider accumulation'
                  : windowData.netValue < 0
                    ? 'Net insider distribution'
                    : 'Balanced insider flow'
              }
            />
          </div>
        </div>
      </div>

      <div className="rounded-[1.75rem] border border-white/[0.08] bg-white/[0.03] p-5">
        <div className="mb-4">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div>
              <div className="text-sm font-semibold text-white">{viewMode === 'feed' ? transactionPanelTitle : 'Insider holdings'}</div>
              <div className="mt-1 text-sm text-zinc-500">{viewMode === 'feed' ? transactionPanelDetail : holdingsPanelDetail}</div>
            </div>
            <div className="flex flex-wrap gap-2">
              {([
                { key: 'feed' as const, label: 'Feed' },
                { key: 'holdings' as const, label: 'Insider Holdings' },
              ]).map((item) => (
                <button
                  key={item.key}
                  type="button"
                  onClick={() => setViewMode(item.key)}
                  className={`rounded-full border px-3 py-1.5 text-xs font-medium transition ${
                    item.key === viewMode
                      ? 'border-[#10b981]/25 bg-[#10b981]/12 text-[#7ee7c4]'
                      : 'border-white/[0.08] bg-white/[0.03] text-zinc-500 hover:text-white'
                  }`}
                >
                  {item.label}
                </button>
              ))}
            </div>
          </div>
        </div>

        {viewMode === 'feed' ? filteredTransactions.length ? (
          <>
            <div className="overflow-x-auto">
              <table className="min-w-full border-collapse text-left">
                <thead>
                  <tr className="border-b border-white/10 text-xs uppercase tracking-[0.16em] text-zinc-500">
                    <th className="px-0 py-3 font-medium">Insider</th>
                    <th className="px-4 py-3 font-medium">Role</th>
                    <th className="px-4 py-3 font-medium">Type</th>
                    <th className="px-4 py-3 font-medium">Value</th>
                    <th className="px-4 py-3 font-medium">Traded</th>
                  </tr>
                </thead>
                <tbody>
                  {visibleTransactions.map((trade) => {
                    const toneClass =
                      trade.direction === 'buy'
                        ? 'border-emerald-500/20 bg-emerald-500/10 text-emerald-300'
                        : trade.direction === 'sell'
                          ? 'border-red-500/20 bg-red-500/10 text-red-300'
                          : 'border-white/10 bg-white/[0.05] text-zinc-300';
                    const displayValue = trade.direction === 'sell' ? -trade.value : trade.value;
                    const positionLabel = buildInsiderPositionLabel(trade);

                    return (
                      <tr key={trade.id} className="border-b border-white/[0.06] align-top last:border-0">
                        <td className="px-0 py-4 text-sm font-medium text-white">
                          <button
                            type="button"
                            onClick={() => setSelectedInsiderKey(trade.identityKey)}
                            className="transition hover:text-[#7ee7c4]"
                          >
                            {trade.filerName}
                          </button>
                        </td>
                        <td className="px-4 py-4 text-sm text-zinc-300">{trade.filerRelation || 'Insider'}</td>
                        <td className="px-4 py-4">
                          <span className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-medium ${toneClass}`}>
                            {trade.direction === 'buy' ? 'Buy' : trade.direction === 'sell' ? 'Sell' : trade.transactionCode || 'Other'}
                          </span>
                        </td>
                        <td className="px-4 py-4 text-sm text-zinc-300">
                          <div>
                            {trade.sourceUrl ? (
                              <Link
                                href={trade.sourceUrl}
                                target="_blank"
                                rel="noreferrer"
                                className="inline-flex items-center gap-1.5 hover:text-white"
                              >
                                {formatCompactCurrency(displayValue)}
                                <ArrowUpRight className="h-3.5 w-3.5" />
                              </Link>
                            ) : (
                              formatCompactCurrency(displayValue)
                            )}
                            {positionLabel ? (
                              <div className="mt-1 text-[11px] leading-4 text-zinc-500">
                                {positionLabel}
                              </div>
                            ) : null}
                          </div>
                        </td>
                        <td className="px-4 py-4 text-sm text-zinc-300">{formatCalendarDate(trade.transactionDate || trade.publishedDate)}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
            {hasMoreTransactions ? (
              <div className="mt-4 flex justify-center">
                <button
                  type="button"
                  onClick={() => setVisibleCount((count) => count + 16)}
                  className="rounded-xl border border-white/[0.08] bg-white/[0.03] px-4 py-2 text-sm font-medium text-zinc-300 transition hover:border-white/[0.14] hover:text-white"
                >
                  Show more insider filings
                </button>
              </div>
            ) : null}
          </>
        ) : (
          <EmptyState message="No insider transactions found in this window." />
        ) : holdings.length ? (
          <div className="overflow-x-auto">
            <table className="min-w-full border-collapse text-left">
              <thead>
                <tr className="border-b border-white/10 text-xs uppercase tracking-[0.16em] text-zinc-500">
                  <th className="px-0 py-3 font-medium">Insider</th>
                  <th className="px-4 py-3 font-medium">Role</th>
                  <th className="px-4 py-3 font-medium">Shares Held</th>
                  <th className="px-4 py-3 font-medium">Est. Value</th>
                  <th className="px-4 py-3 font-medium">Last Filing</th>
                </tr>
              </thead>
              <tbody>
                {holdings.map((holding) => {
                  const holdingToneClass =
                    holding.lastDirection === 'buy'
                      ? 'border-emerald-500/20 bg-emerald-500/10 text-emerald-300'
                      : holding.lastDirection === 'sell'
                        ? 'border-red-500/20 bg-red-500/10 text-red-300'
                        : 'border-white/10 bg-white/[0.05] text-zinc-300';
                  const holdingChangeLabel =
                    holding.holdingChangePct !== null
                      ? `${holding.lastDirection === 'buy' ? 'Added' : holding.lastDirection === 'sell' ? 'Trimmed' : 'Changed'} ${Math.round(holding.holdingChangePct * 100)}%`
                      : null;

                  return (
                    <tr key={holding.key} className="border-b border-white/[0.06] align-top last:border-0">
                      <td className="px-0 py-4 text-sm font-medium text-white">
                        <button
                          type="button"
                          onClick={() => setSelectedInsiderKey(holding.key)}
                          className="transition hover:text-[#7ee7c4]"
                        >
                          {holding.filerName}
                        </button>
                      </td>
                      <td className="px-4 py-4 text-sm text-zinc-300">{holding.filerRelation || 'Insider'}</td>
                      <td className="px-4 py-4 text-sm font-medium text-white">{formatCompactNumber(holding.sharesHeld)}</td>
                      <td className="px-4 py-4 text-sm text-zinc-300">
                        {holding.estimatedValue !== null ? formatCompactCurrency(holding.estimatedValue) : 'N/A'}
                      </td>
                      <td className="px-4 py-4 text-sm text-zinc-300">
                        <div>
                          {holding.sourceUrl ? (
                            <Link
                              href={holding.sourceUrl}
                              target="_blank"
                              rel="noreferrer"
                              className="inline-flex items-center gap-1.5 hover:text-white"
                            >
                              {formatCalendarDate(holding.lastTransactionDate || holding.publishedDate)}
                              <ArrowUpRight className="h-3.5 w-3.5" />
                            </Link>
                          ) : (
                            formatCalendarDate(holding.lastTransactionDate || holding.publishedDate)
                          )}
                          <div className="mt-1">
                            <span className={`inline-flex rounded-full border px-2 py-0.5 text-[10px] font-medium ${holdingToneClass}`}>
                              {holding.lastDirection === 'buy' ? 'Last filing buy' : holding.lastDirection === 'sell' ? 'Last filing sell' : 'Latest filing'}
                            </span>
                          </div>
                          {holdingChangeLabel ? (
                            <div className="mt-1 text-[11px] leading-4 text-zinc-500">{holdingChangeLabel}</div>
                          ) : null}
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : (
          <EmptyState message="No insider holdings could be derived from the latest Form 4 filings for this stock yet." />
        )}
      </div>

      <InsiderTransactionsModal
        open={Boolean(selectedInsiderKey && selectedTransactions.length)}
        insiderName={selectedInsiderName}
        insiderRelation={selectedInsiderRelation}
        holding={selectedHolding}
        transactions={selectedTransactions}
        onClose={() => setSelectedInsiderKey(null)}
      />
    </div>
  );
}

function HedgeFundTable({
  holders,
  summary,
}: {
  holders: TickerFundHolder[];
  summary: TickerFundSummary;
}) {
  return (
    <div className="space-y-6">
      <div className="grid gap-4 sm:grid-cols-3">
        <TopSummaryCard label="Increasing" value={String(summary.increased)} detail="Funds adding to positions" />
        <TopSummaryCard label="Decreasing" value={String(summary.decreased)} detail="Funds trimming exposure" />
        <TopSummaryCard label="Neutral" value={String(summary.neutral)} detail="Funds holding roughly flat" />
      </div>

      <div className="rounded-[1.75rem] border border-white/[0.08] bg-white/[0.03] p-5">
        <div className="mb-4">
          <div className="text-sm font-semibold text-white">Current hedge-fund holders</div>
          <div className="mt-1 text-sm text-zinc-500">Latest reported holders in your tracked 13F universe.</div>
        </div>

        {holders.length ? (
          <div className="overflow-x-auto">
            <table className="min-w-full border-collapse text-left">
              <thead>
                <tr className="border-b border-white/10 text-xs uppercase tracking-[0.16em] text-zinc-500">
                  <th className="px-0 py-3 font-medium">Fund</th>
                  <th className="px-4 py-3 font-medium">Value</th>
                  <th className="px-4 py-3 font-medium">Shares</th>
                  <th className="px-4 py-3 font-medium">Positioning</th>
                  <th className="px-4 py-3 font-medium">Report Period</th>
                </tr>
              </thead>
              <tbody>
                {holders.map((holder) => {
                  const toneClass =
                    holder.changeKind === 'new' || holder.changeKind === 'increase'
                      ? 'border-emerald-500/20 bg-emerald-500/10 text-emerald-300'
                      : holder.changeKind === 'decrease'
                        ? 'border-red-500/20 bg-red-500/10 text-red-300'
                        : 'border-white/10 bg-white/[0.05] text-zinc-300';

                  return (
                    <tr key={holder.key} className="border-b border-white/[0.06] align-top last:border-0">
                      <td className="px-0 py-4 text-sm font-medium text-white">{holder.fundName}</td>
                      <td className="px-4 py-4 text-sm text-zinc-300">
                        {holder.sourceUrl ? (
                          <Link
                            href={holder.sourceUrl}
                            target="_blank"
                            rel="noreferrer"
                            className="inline-flex items-center gap-1.5 hover:text-white"
                          >
                            {formatCompactCurrency(holder.valueHeld)}
                            <ArrowUpRight className="h-3.5 w-3.5" />
                          </Link>
                        ) : (
                          formatCompactCurrency(holder.valueHeld)
                        )}
                      </td>
                      <td className="px-4 py-4 text-sm text-zinc-300">{formatCompactNumber(holder.sharesHeld)}</td>
                      <td className="px-4 py-4">
                        <span className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-medium ${toneClass}`}>
                          {holder.changeLabel}
                        </span>
                      </td>
                      <td className="px-4 py-4 text-sm text-zinc-300">{formatCalendarDate(holder.reportPeriod || holder.publishedDate)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : (
          <EmptyState message="No active hedge-fund holders found for this stock in the tracked 13F universe." />
        )}
      </div>
    </div>
  );
}

export default function TickerIntelligencePage({
  overview,
  politicianHolders,
  politicianTransactions,
  insiderWindows,
  insiderTransactions,
  insiderHoldings,
  hedgeFundHolders,
  hedgeFundSummary,
}: TickerIntelligencePageProps) {
  const [activeTab, setActiveTab] = useState<HolderTab>('politicians');

  const tabConfig = [
    {
      key: 'politicians' as const,
      label: 'Politicians',
      icon: Landmark,
      count: overview.politicianHolderCount,
      detail: 'Current holders and recent congressional transactions',
    },
    {
      key: 'insiders' as const,
      label: 'Insiders',
      icon: ShieldAlert,
      count: overview.insiderTransactionCount,
      detail: 'Sentiment over time and recent insider activity',
    },
    {
      key: 'hedge-funds' as const,
      label: 'Hedge Funds',
      icon: Building2,
      count: overview.hedgeFundHolderCount,
      detail: 'Latest tracked 13F holders and positioning changes',
    },
  ];

  return (
    <div className="mx-auto w-full max-w-[1580px] px-4 py-8 sm:px-6 lg:px-8">
      <div className="space-y-8">
        <section className="glass-panel rounded-[2rem] p-6 md:p-8">
          <div className="flex flex-col gap-6 xl:flex-row xl:items-end xl:justify-between">
            <SectionHeader
              eyebrow="Stock Workspace"
              title={`${overview.companyName}`}
              detail="Live, filing-backed intelligence for this stock across Congress, insiders, and tracked hedge funds."
            />

            <div className="grid gap-3 sm:grid-cols-2 xl:min-w-[420px]">
              <TopSummaryCard
                label="Symbol"
                value={overview.symbol}
                detail={
                  overview.currentPrice
                    ? `${formatPrice(overview.currentPrice)}${overview.priceAsOf ? ` · as of ${formatCalendarDate(overview.priceAsOf)}` : ''}`
                    : 'Price unavailable right now'
                }
              />
              <TopSummaryCard
                label="Coverage"
                value={String(overview.sourceCount)}
                detail="Congress, insiders, and 13F signal overlap"
              />
            </div>
          </div>

          <div className="mt-6 flex flex-wrap items-center gap-2">
            {overview.sector ? (
              <span className="rounded-full border border-blue-500/20 bg-blue-500/10 px-3 py-1 text-sm text-blue-300">
                {overview.sector}
              </span>
            ) : null}
            {overview.industry ? (
              <span className="rounded-full border border-white/10 bg-white/[0.05] px-3 py-1 text-sm text-zinc-300">
                {overview.industry}
              </span>
            ) : null}
            {overview.latestActivityDate ? (
              <span className="rounded-full border border-white/10 bg-white/[0.05] px-3 py-1 text-sm text-zinc-400">
                Latest filing {formatCalendarDate(overview.latestActivityDate)}
              </span>
            ) : null}
          </div>
        </section>

        <div className="grid gap-8 xl:grid-cols-[minmax(0,1.4fr)_340px]">
          <section className="space-y-6">
            <div className="glass-panel rounded-[2rem] p-4">
              <div className="flex flex-col gap-3 lg:flex-row">
                {tabConfig.map((tab) => {
                  const Icon = tab.icon;
                  const isActive = activeTab === tab.key;
                  return (
                    <button
                      key={tab.key}
                      type="button"
                      onClick={() => setActiveTab(tab.key)}
                      className={`flex flex-1 items-start gap-3 rounded-[1.5rem] border px-4 py-4 text-left transition ${
                        isActive
                          ? 'border-[#10b981]/22 bg-[#10b981]/10'
                          : 'border-white/[0.06] bg-white/[0.02] hover:border-white/[0.12] hover:bg-white/[0.04]'
                      }`}
                    >
                      <div
                        className={`flex h-11 w-11 shrink-0 items-center justify-center rounded-2xl ${
                          isActive ? 'bg-[#10b981]/15 text-[#7ee7c4]' : 'bg-white/[0.05] text-zinc-500'
                        }`}
                      >
                        <Icon className="h-5 w-5" />
                      </div>
                      <div className="min-w-0">
                        <div className={`text-sm font-semibold ${isActive ? 'text-white' : 'text-zinc-300'}`}>{tab.label}</div>
                        <div className="mt-1 text-xs text-zinc-500">{tab.detail}</div>
                        <div className="mt-2 text-lg font-semibold text-white">{tab.count}</div>
                      </div>
                    </button>
                  );
                })}
              </div>
            </div>

            {activeTab === 'politicians' ? (
              <div className="space-y-6">
                <div className="rounded-[1.75rem] border border-white/[0.08] bg-white/[0.03] p-5">
                  <div className="mb-4">
                    <div className="text-sm font-semibold text-white">Current holders</div>
                    <div className="mt-1 text-sm text-zinc-500">
                      Estimated live exposure derived from the full history of filed congressional trades for this stock.
                    </div>
                  </div>
                  <PoliticianHolderTable holders={politicianHolders} />
                </div>

                <div className="rounded-[1.75rem] border border-white/[0.08] bg-white/[0.03] p-5">
                  <div className="mb-4">
                    <div className="text-sm font-semibold text-white">Recent transactions</div>
                    <div className="mt-1 text-sm text-zinc-500">Latest politician transactions filed for this stock.</div>
                  </div>
                  <PoliticianTransactionsTable transactions={politicianTransactions} />
                </div>
              </div>
            ) : null}

            {activeTab === 'insiders' ? (
              <InsiderSentimentPanel
                key={overview.symbol}
                windows={insiderWindows}
                transactions={insiderTransactions}
                holdings={insiderHoldings}
              />
            ) : null}

            {activeTab === 'hedge-funds' ? (
              <HedgeFundTable holders={hedgeFundHolders} summary={hedgeFundSummary} />
            ) : null}
          </section>

          <aside className="space-y-4">
            <div className="glass-panel rounded-[2rem] p-5">
              <div className="text-sm font-semibold text-white">Coverage snapshot</div>
              <div className="mt-4 grid gap-3">
                <TopSummaryCard
                  label="Politician Holders"
                  value={String(overview.politicianHolderCount)}
                  detail={`${overview.politicianTransactionCount} total filed transactions`}
                />
                <TopSummaryCard
                  label="Insider Filings"
                  value={String(overview.insiderTransactionCount)}
                  detail="Recent insider transactions for this stock"
                />
                <TopSummaryCard
                  label="Fund Holders"
                  value={String(overview.hedgeFundHolderCount)}
                  detail="Latest tracked hedge funds still holding the name"
                />
              </div>
            </div>

            <div className="glass-panel rounded-[2rem] p-5">
              <div className="text-sm font-semibold text-white">What this page answers</div>
              <div className="mt-4 space-y-3 text-sm text-zinc-400">
                <div>Which politicians still appear to hold this stock, and what are their estimated ranges?</div>
                <div>Whether insider flow is net bullish or bearish over different trailing windows.</div>
                <div>Which hedge funds currently hold the name and whether they are increasing or reducing exposure.</div>
              </div>
            </div>
          </aside>
        </div>
      </div>
    </div>
  );
}
