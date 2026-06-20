'use client';

import { useEffect } from 'react';
import Link from 'next/link';
import { ArrowUpRight, X } from 'lucide-react';

import { formatCalendarDate } from '@/lib/date-format';
import { buildInsiderPositionLabel } from '@/lib/insider-position-label';
import type { TickerInsiderHolding, TickerInsiderTransaction } from '@/lib/ticker-intelligence-types';

type InsiderTransactionsModalProps = {
  open: boolean;
  insiderName: string;
  insiderRelation: string | null;
  holding: TickerInsiderHolding | null;
  transactions: TickerInsiderTransaction[];
  onClose: () => void;
};

function formatCompactNumber(value: number | null | undefined) {
  const amount = Number(value || 0);
  if (!Number.isFinite(amount)) {
    return 'N/A';
  }
  if (amount >= 1_000_000_000) return `${(amount / 1_000_000_000).toFixed(1)}B`;
  if (amount >= 1_000_000) return `${(amount / 1_000_000).toFixed(1)}M`;
  if (amount >= 1_000) return `${(amount / 1_000).toFixed(1)}K`;
  return amount.toLocaleString(undefined, {
    maximumFractionDigits: amount % 1 === 0 ? 0 : 2,
  });
}

function formatCompactCurrency(value: number | null | undefined) {
  const amount = Number(value || 0);
  if (!Number.isFinite(amount) || amount <= 0) {
    return 'N/A';
  }
  if (amount >= 1_000_000_000) return `$${(amount / 1_000_000_000).toFixed(1)}B`;
  if (amount >= 1_000_000) return `$${(amount / 1_000_000).toFixed(1)}M`;
  if (amount >= 1_000) return `$${(amount / 1_000).toFixed(1)}K`;
  return `$${Math.round(amount).toLocaleString()}`;
}

export default function InsiderTransactionsModal({
  open,
  insiderName,
  insiderRelation,
  holding,
  transactions,
  onClose,
}: InsiderTransactionsModalProps) {
  useEffect(() => {
    if (!open) {
      return;
    }

    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        onClose();
      }
    };

    window.addEventListener('keydown', onKeyDown);
    return () => {
      document.body.style.overflow = previousOverflow;
      window.removeEventListener('keydown', onKeyDown);
    };
  }, [open, onClose]);

  if (!open) {
    return null;
  }

  return (
    <div className="fixed inset-0 z-[90] flex items-center justify-center bg-black/60 px-4 py-6 backdrop-blur-sm">
      <button type="button" aria-label="Close insider transactions" onClick={onClose} className="absolute inset-0" />

      <div className="relative z-[91] flex max-h-[84vh] w-full max-w-[980px] flex-col overflow-hidden rounded-[28px] border border-white/10 bg-[#0b0f14]/96 shadow-[0_28px_100px_rgba(0,0,0,0.55)]">
        <div className="border-b border-white/8 px-5 py-5 sm:px-6">
          <div className="flex items-start justify-between gap-4">
            <div className="min-w-0">
              <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-zinc-600">Insider Transactions</div>
              <h3 className="mt-2 text-2xl font-semibold tracking-[-0.03em] text-white">{insiderName}</h3>
              <div className="mt-2 flex flex-wrap items-center gap-2 text-sm text-zinc-500">
                <span>{insiderRelation || 'Insider'}</span>
                <span className="text-white/15">·</span>
                <span>{transactions.length} transaction{transactions.length === 1 ? '' : 's'} for this stock</span>
                {holding ? (
                  <>
                    <span className="text-white/15">·</span>
                    <span>{formatCompactNumber(holding.sharesHeld)} shares currently shown</span>
                  </>
                ) : null}
              </div>
            </div>

            <div className="flex items-start gap-3">
              {holding ? (
                <div className="hidden text-right sm:block">
                  <div className="text-sm font-medium text-white">{formatCompactCurrency(holding.estimatedValue)}</div>
                  <div className="mt-1 text-xs text-zinc-500">Estimated current value</div>
                </div>
              ) : null}
              <button
                type="button"
                onClick={onClose}
                className="inline-flex h-10 w-10 items-center justify-center rounded-2xl border border-white/10 bg-white/[0.03] text-zinc-300 transition hover:bg-white/[0.08] hover:text-white"
              >
                <X className="h-4 w-4" />
              </button>
            </div>
          </div>
        </div>

        <div className="overflow-y-auto px-4 py-4 sm:px-6">
          {!transactions.length ? (
            <div className="rounded-2xl border border-white/8 bg-white/[0.02] px-4 py-6 text-sm text-zinc-500">
              No insider transactions were found for this person on this stock.
            </div>
          ) : (
            <div className="overflow-hidden rounded-2xl border border-white/8 bg-white/[0.02]">
              <div className="overflow-x-auto">
                <table className="min-w-full border-collapse text-left">
                  <thead className="sticky top-0 z-[1] bg-[#09090b]">
                    <tr className="border-b border-white/10 text-xs uppercase tracking-[0.16em] text-zinc-500">
                      <th className="px-4 py-3 font-medium">Traded</th>
                      <th className="px-4 py-3 font-medium">Type</th>
                      <th className="px-4 py-3 font-medium">Shares</th>
                      <th className="px-4 py-3 font-medium">Value</th>
                      <th className="px-4 py-3 font-medium">Context</th>
                    </tr>
                  </thead>
                  <tbody>
                    {transactions.map((trade) => {
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
                          <td className="px-4 py-3.5 text-sm text-zinc-300">
                            {formatCalendarDate(trade.transactionDate || trade.publishedDate)}
                          </td>
                          <td className="px-4 py-3.5">
                            <span className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-medium ${toneClass}`}>
                              {trade.direction === 'buy' ? 'Buy' : trade.direction === 'sell' ? 'Sell' : trade.transactionCode || 'Other'}
                            </span>
                          </td>
                          <td className="px-4 py-3.5 text-sm text-zinc-300">{formatCompactNumber(trade.amount)}</td>
                          <td className="px-4 py-3.5 text-sm text-zinc-300">
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
                          </td>
                          <td className="px-4 py-3.5 text-sm text-zinc-500">{positionLabel || 'No holdings context resolved for this filing.'}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
