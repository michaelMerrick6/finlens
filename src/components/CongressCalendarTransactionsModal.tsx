'use client';

import Link from 'next/link';
import { createPortal } from 'react-dom';
import { useEffect, useRef } from 'react';
import { ArrowUpRight, ExternalLink, X } from 'lucide-react';

import PoliticianHeadshot from '@/components/PoliticianHeadshot';
import type {
  CongressCalendarCompany,
  CongressCalendarTransactionsData,
} from '@/lib/congress-cluster-calendar-types';
import { formatCalendarDate } from '@/lib/date-format';
import { formatCompactCurrency } from '@/lib/hedge-funds';

function minimumLabel(value: number) {
  if (!Number.isFinite(value) || value <= 0) return '$0';
  const compact = formatCompactCurrency(value);
  return compact ? `${compact}+` : '$0';
}

function TransactionSkeleton() {
  return (
    <div className="space-y-2 p-4 sm:p-5">
      {Array.from({ length: 3 }).map((_, index) => (
        <div key={index} className="flex animate-pulse items-center gap-3 rounded-xl bg-white/[0.018] p-3">
          <div className="h-10 w-10 rounded-full bg-white/[0.06]" />
          <div className="min-w-0 flex-1 space-y-2">
            <div className="h-3 w-36 rounded-full bg-white/[0.07]" />
            <div className="h-2.5 w-48 max-w-full rounded-full bg-white/[0.04]" />
          </div>
        </div>
      ))}
    </div>
  );
}

export default function CongressCalendarTransactionsModal({
  selection,
  data,
  loading,
  error,
  onClose,
}: {
  selection: { date: string; company: CongressCalendarCompany } | null;
  data: CongressCalendarTransactionsData | null;
  loading: boolean;
  error: string;
  onClose: () => void;
}) {
  const closeButtonRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    if (!selection) return;
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    closeButtonRef.current?.focus();
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', onKeyDown);
    return () => {
      document.body.style.overflow = previousOverflow;
      window.removeEventListener('keydown', onKeyDown);
    };
  }, [onClose, selection]);

  if (!selection || typeof document === 'undefined') return null;
  const { company, date } = selection;

  return createPortal(
    <div className="fixed inset-0 z-[200] flex items-end justify-center bg-black/70 p-2 backdrop-blur-sm sm:items-center sm:p-6">
      <button type="button" aria-label="Close calendar transactions" onClick={onClose} className="absolute inset-0" />
      <section
        role="dialog"
        aria-modal="true"
        aria-labelledby="calendar-transactions-title"
        className="relative z-[201] flex max-h-[90dvh] w-full max-w-[680px] flex-col overflow-hidden rounded-2xl border border-white/[0.09] bg-[#0b0e12] shadow-[0_28px_100px_rgba(0,0,0,0.65)]"
      >
        <header className="border-b border-white/[0.07] px-4 pb-4 pt-4 sm:px-5 sm:pt-5">
          <div className="flex items-start justify-between gap-4">
            <div>
              <div className="text-[10px] font-semibold uppercase tracking-[0.17em] text-emerald-300/75">
                Congressional purchases · traded {formatCalendarDate(date, 'UTC')}
              </div>
              <h2 id="calendar-transactions-title" className="mt-1.5 text-xl font-semibold tracking-tight text-white">
                {company.ticker} purchases
              </h2>
              <p className="mt-1 text-sm text-zinc-500">{company.companyName || `${company.ticker} stock purchases`}</p>
            </div>
            <button
              ref={closeButtonRef}
              type="button"
              aria-label="Close"
              onClick={onClose}
              className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl border border-white/[0.08] bg-white/[0.025] text-zinc-500 transition hover:bg-white/[0.06] hover:text-white"
            >
              <X className="h-4 w-4" />
            </button>
          </div>

          <div className="mt-4 grid grid-cols-3 divide-x divide-white/[0.06] overflow-hidden rounded-xl border border-white/[0.07] bg-white/[0.018]">
            <div className="px-3 py-2.5">
              <div className="text-sm font-semibold tabular-nums text-emerald-200">{minimumLabel(company.amountFloor)}</div>
              <div className="mt-0.5 text-[9px] uppercase tracking-[0.1em] text-zinc-600">Minimum</div>
            </div>
            <div className="px-3 py-2.5">
              <div className="text-sm font-semibold tabular-nums text-white">{company.actorCount}</div>
              <div className="mt-0.5 text-[9px] uppercase tracking-[0.1em] text-zinc-600">Lawmakers</div>
            </div>
            <div className="px-3 py-2.5">
              <div className="text-sm font-semibold tabular-nums text-white">{company.tradeCount}</div>
              <div className="mt-0.5 text-[9px] uppercase tracking-[0.1em] text-zinc-600">Purchases</div>
            </div>
          </div>
        </header>

        <div className="min-h-0 flex-1 overflow-y-auto overscroll-contain">
          {loading ? (
            <TransactionSkeleton />
          ) : error ? (
            <div role="alert" className="m-4 rounded-xl border border-red-400/15 bg-red-400/[0.06] px-4 py-3 text-sm text-red-200 sm:m-5">
              {error}
            </div>
          ) : !data?.transactions.length ? (
            <div className="px-5 py-12 text-center text-sm text-zinc-500">No matching filings were found.</div>
          ) : (
            <div className="divide-y divide-white/[0.055]">
              {data.transactions.map((transaction) => (
                <article key={transaction.id} className="flex items-start gap-3 px-4 py-3.5 sm:px-5">
                  <PoliticianHeadshot memberId={transaction.memberId} name={transaction.politicianName} party={transaction.party} size={40} />
                  <div className="min-w-0 flex-1">
                    <div className="truncate text-sm font-medium text-white">{transaction.politicianName}</div>
                    <div className="mt-0.5 text-[11px] text-zinc-500">
                      {transaction.chamber || 'Congress'} · disclosed {formatCalendarDate(transaction.publishedDate, 'UTC')}
                    </div>
                    {transaction.assetName ? <div className="mt-1 truncate text-[11px] text-zinc-700">{transaction.assetName}</div> : null}
                  </div>
                  <div className="shrink-0 text-right">
                    <div className="text-sm font-semibold tabular-nums text-white">{transaction.amountRange || 'Not reported'}</div>
                    {transaction.sourceUrl ? (
                      <a href={transaction.sourceUrl} target="_blank" rel="noreferrer" className="mt-1 inline-flex items-center gap-1 text-[10px] text-zinc-600 transition hover:text-zinc-300">
                        Filing <ExternalLink className="h-2.5 w-2.5" />
                      </a>
                    ) : null}
                  </div>
                </article>
              ))}
            </div>
          )}
        </div>

        <footer className="flex items-center justify-between gap-4 border-t border-white/[0.07] px-4 py-3 sm:px-5">
          <p className="text-[10px] leading-4 text-zinc-700">Amounts are the ranges reported in public filings.</p>
          <Link href={`/dashboard?ticker=${encodeURIComponent(company.ticker)}`} className="inline-flex shrink-0 items-center gap-1 text-xs font-medium text-zinc-400 transition hover:text-white">
            Stock overview <ArrowUpRight className="h-3.5 w-3.5" />
          </Link>
        </footer>
      </section>
    </div>,
    document.body,
  );
}
