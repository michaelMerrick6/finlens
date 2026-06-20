import type { Metadata } from 'next';
import Link from 'next/link';
import { ArrowLeft, Building2, CalendarClock, TrendingUp, Wallet } from 'lucide-react';
import { notFound } from 'next/navigation';

import HedgeFundFollowButton from '@/components/HedgeFundFollowButton';
import { HedgeFundQuarterExplorer } from '@/components/HedgeFundQuarterExplorer';
import { formatCalendarDate } from '@/lib/date-format';
import { buildFundQuarterSnapshots, formatCompactCurrency } from '@/lib/hedge-funds';
import { getCachedFundHoldings } from '@/lib/public-data';

export const dynamic = 'force-dynamic';

function decodeFundName(param: string) {
  try {
    return decodeURIComponent(param);
  } catch {
    return param;
  }
}

export async function generateMetadata({ params }: { params: Promise<{ fundName: string }> }): Promise<Metadata> {
  const { fundName } = await params;
  const decoded = decodeFundName(fundName);
  return {
    title: `${decoded} — Vail Hedge Funds`,
    description: `Current holdings and quarter-over-quarter 13F changes for ${decoded}.`,
  };
}

export default async function HedgeFundDetailPage({ params }: { params: Promise<{ fundName: string }> }) {
  const { fundName } = await params;
  const decodedFundName = decodeFundName(fundName);
  const rows = await getCachedFundHoldings(decodedFundName);

  if (!rows.length) {
    notFound();
  }

  const periods = buildFundQuarterSnapshots(rows);
  const latest = periods[0];
  const prevPeriod = periods.length >= 2 ? periods[1] : null;
  const valueChange = latest && prevPeriod
    ? latest.totalValue - prevPeriod.totalValue
    : null;
  const valueChangePct = valueChange != null && prevPeriod && prevPeriod.totalValue > 0
    ? (valueChange / prevPeriod.totalValue) * 100
    : null;

  return (
    <div className="mx-auto max-w-5xl px-4 py-6 sm:px-6 lg:px-8">
      {/* Breadcrumb */}
      <Link href="/hedge-funds" className="inline-flex items-center gap-1.5 text-xs text-zinc-500 transition hover:text-white">
        <ArrowLeft className="h-3 w-3" />
        Back to directory
      </Link>

      {/* Header */}
      <div className="mt-4 flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
        <div className="min-w-0">
          <div className="inline-flex items-center gap-2 rounded-full border border-emerald-500/20 bg-emerald-500/10 px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.2em] text-emerald-400">
            <Building2 className="h-3 w-3" />
            13F-HR Detail
          </div>
          <h1 className="mt-3 text-2xl font-semibold tracking-tight text-white sm:text-3xl">{decodedFundName}</h1>
          <p className="mt-1 text-sm text-zinc-500">
            Holdings and quarter-over-quarter position changes from tracked 13F filings.
          </p>
        </div>
        <HedgeFundFollowButton fundName={decodedFundName} />
      </div>

      {/* Stats */}
      {latest && (
        <div className="mt-5 grid grid-cols-2 gap-3 sm:grid-cols-4">
          <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-4">
            <div className="flex items-center gap-2 text-[10px] font-semibold uppercase tracking-[0.15em] text-zinc-600">
              <Wallet className="h-3 w-3" />
              Portfolio Value
            </div>
            <div className="mt-2 text-xl font-semibold text-white">{formatCompactCurrency(latest.totalValue)}</div>
            {valueChangePct != null && (
              <div className={`mt-0.5 text-xs font-medium ${valueChangePct >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                {valueChangePct >= 0 ? '+' : ''}{valueChangePct.toFixed(1)}% QoQ
              </div>
            )}
          </div>
          <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-4">
            <div className="flex items-center gap-2 text-[10px] font-semibold uppercase tracking-[0.15em] text-zinc-600">
              <TrendingUp className="h-3 w-3" />
              Holdings
            </div>
            <div className="mt-2 text-xl font-semibold text-white">{latest.holdingCount.toLocaleString()}</div>
            <div className="mt-0.5 text-xs text-zinc-600">{latest.quarterLabel}</div>
          </div>
          <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-4">
            <div className="flex items-center gap-2 text-[10px] font-semibold uppercase tracking-[0.15em] text-zinc-600">
              <CalendarClock className="h-3 w-3" />
              Last Filed
            </div>
            <div className="mt-2 text-lg font-semibold text-white">{formatCalendarDate(latest.publishedDate)}</div>
          </div>
          <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-4">
            <div className="flex items-center gap-2 text-[10px] font-semibold uppercase tracking-[0.15em] text-zinc-600">
              <CalendarClock className="h-3 w-3" />
              Next Expected
            </div>
            <div className="mt-2 text-lg font-semibold text-white">{formatCalendarDate(latest.nextExpectedFiling)}</div>
          </div>
        </div>
      )}

      {/* Quarter Explorer */}
      <div className="mt-6">
        <HedgeFundQuarterExplorer periods={periods} />
      </div>
    </div>
  );
}
