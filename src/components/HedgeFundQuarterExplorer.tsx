'use client';

import Image, { type ImageLoaderProps } from 'next/image';
import Link from 'next/link';
import { useMemo, useState } from 'react';
import {
  ArrowDown,
  ArrowUp,
  ArrowUpRight,
  CircleHelp,
  CirclePlus,
  LogOut as ExitIcon,
  Minus,
  Search,
} from 'lucide-react';

import { getTickerLogoUrl } from '@/lib/company-logos';
import { formatFundChangeLabel, getFundChangeKind } from '@/lib/fund-holdings';
import { formatCompactCurrency, formatShareCount, type FundQuarterSnapshot } from '@/lib/hedge-funds';

// ─── Change Kind UI ────────────────────────────────────────────────────────────

type ChangeKind = ReturnType<typeof getFundChangeKind>;

function ChangeKindBadge({ kind }: { kind: ChangeKind }) {
  const config: Record<string, { label: string; icon: React.ElementType; colors: string }> = {
    new: { label: 'New position', icon: CirclePlus, colors: 'text-emerald-400 bg-emerald-400/10 border-emerald-400/20' },
    increase: { label: 'Increase from last quarter', icon: ArrowUp, colors: 'text-emerald-400 bg-emerald-400/10 border-emerald-400/20' },
    decrease: { label: 'Decrease from last quarter', icon: ArrowDown, colors: 'text-red-400 bg-red-400/10 border-red-400/20' },
    exit: { label: 'Exit', icon: ExitIcon, colors: 'text-red-400 bg-red-400/10 border-red-400/20' },
    hold: { label: 'Flat', icon: Minus, colors: 'text-zinc-400 bg-white/5 border-white/10' },
    unknown: { label: '—', icon: Minus, colors: 'text-zinc-600 bg-white/5 border-white/10' },
  };

  const c = config[kind] ?? config.unknown;
  const Icon = c.icon;

  return (
    <span className={`inline-flex items-center gap-1 whitespace-nowrap rounded-md border px-1.5 py-0.5 text-[10px] font-semibold ${c.colors}`}>
      <Icon className="h-2.5 w-2.5" />
      {c.label}
    </span>
  );
}

function changeTextColor(kind: ChangeKind) {
  if (kind === 'new' || kind === 'increase') {
    return 'text-emerald-400';
  }
  if (kind === 'decrease' || kind === 'exit') {
    return 'text-red-400';
  }
  return 'text-zinc-400';
}

// ─── Tab Switch ────────────────────────────────────────────────────────────────

type ViewTab = 'holdings' | 'changes';
const INITIAL_RENDER_COUNT = 20;
const PAGE_ROW_LIMIT = 100;

const passthroughImageLoader = ({ src }: ImageLoaderProps) => src;

function TickerLogo({ ticker }: { ticker: string | null | undefined }) {
  const normalizedTicker = String(ticker || '').trim().toUpperCase();
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  const logoUrl = normalizedTicker ? getTickerLogoUrl(normalizedTicker, 40) : null;
  const activeLogoUrl = logoUrl && failedUrl !== logoUrl ? logoUrl : null;
  const hue = normalizedTicker.split('').reduce((acc, ch) => acc + ch.charCodeAt(0), 0) % 360;

  if (activeLogoUrl) {
    return (
      <div className="h-7 w-7 shrink-0 overflow-hidden rounded-lg border border-white/10 bg-black/30">
        <Image
          loader={passthroughImageLoader}
          unoptimized
          src={activeLogoUrl}
          alt={normalizedTicker}
          width={28}
          height={28}
          sizes="28px"
          className="h-full w-full object-contain p-0.5"
          onError={() => setFailedUrl(activeLogoUrl)}
        />
      </div>
    );
  }

  return (
    <div
      className="flex h-7 w-7 shrink-0 items-center justify-center rounded-lg text-[9px] font-bold text-white"
      style={{
        background: `linear-gradient(135deg, hsl(${hue},65%,45%), hsl(${(hue + 40) % 360},65%,38%))`,
      }}
    >
      {(normalizedTicker || '?').slice(0, 2)}
    </div>
  );
}

function QoqHeader({ align = 'left' }: { align?: 'left' | 'right' }) {
  return (
    <div className={`group relative inline-flex items-center gap-1 ${align === 'right' ? 'justify-end' : ''}`}>
      <span>Position Δ</span>
      <CircleHelp className="h-3 w-3 text-zinc-700 transition group-hover:text-zinc-400" />
      <div
        className={`pointer-events-none absolute top-full z-10 mt-2 hidden w-56 rounded-xl border border-white/[0.08] bg-[#0f1115] px-3 py-2 text-[11px] normal-case leading-relaxed tracking-normal text-zinc-300 shadow-2xl group-hover:block ${
          align === 'right' ? 'right-0' : 'left-0'
        }`}
      >
        Share-count change versus the previous 13F quarter, not stock-price performance.
      </div>
    </div>
  );
}

// ─── Quarter Explorer ──────────────────────────────────────────────────────────

export function HedgeFundQuarterExplorer({ periods }: { periods: FundQuarterSnapshot[] }) {
  const [selectedPeriod, setSelectedPeriod] = useState<string>(periods[0]?.reportPeriod || '');
  const [activeView, setActiveView] = useState<ViewTab>('holdings');
  const [holdingSearch, setHoldingSearch] = useState('');
  const [currentPage, setCurrentPage] = useState(1);
  const [visibleCount, setVisibleCount] = useState(INITIAL_RENDER_COUNT);

  const selected = periods.find((p) => p.reportPeriod === selectedPeriod) || periods[0];

  const filteredHoldings = useMemo(() => {
    if (!selected) return [];
    const q = holdingSearch.trim().toLowerCase();
    const list = activeView === 'holdings' ? selected.holdings : selected.changes;
    if (!q) return list;
    return list.filter((h) =>
      String(h.ticker || '').toLowerCase().includes(q)
    );
  }, [selected, holdingSearch, activeView]);

  const pageStart = (currentPage - 1) * PAGE_ROW_LIMIT;
  const pageCapacity = Math.min(PAGE_ROW_LIMIT, Math.max(filteredHoldings.length - pageStart, 0));
  const pagedHoldings = filteredHoldings.slice(pageStart, pageStart + Math.min(visibleCount, pageCapacity));
  const showingStart = filteredHoldings.length ? pageStart + 1 : 0;
  const showingEnd = filteredHoldings.length ? pageStart + pagedHoldings.length : 0;
  const canLoadMore = visibleCount < pageCapacity;
  const canPrevPage = currentPage > 1;
  const canNextPage = pageStart + pageCapacity < filteredHoldings.length;

  if (!selected) return null;

  return (
    <div className="space-y-4">
      {/* Quarter selector */}
      <div className="flex flex-wrap items-center gap-2">
        <span className="mr-1 text-[10px] font-semibold uppercase tracking-[0.15em] text-zinc-600">Quarter</span>
        {periods.map((p) => (
          <button
            key={p.reportPeriod}
            onClick={() => {
              setSelectedPeriod(p.reportPeriod);
              setHoldingSearch('');
              setCurrentPage(1);
              setVisibleCount(INITIAL_RENDER_COUNT);
            }}
            className={`rounded-lg border px-3 py-1.5 text-xs font-medium transition ${
              selected.reportPeriod === p.reportPeriod
                ? 'border-[#10b981]/30 bg-[#10b981]/10 text-[#10b981]'
                : 'border-white/[0.06] bg-white/[0.02] text-zinc-400 hover:border-white/[0.12] hover:text-white'
            }`}
          >
            {p.quarterLabel}
          </button>
        ))}
      </div>

      {/* View toggle + search */}
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div className="flex gap-0 rounded-xl border border-white/[0.06] bg-white/[0.02] p-0.5">
            {(['holdings', 'changes'] as const).map((tab) => (
            <button
              key={tab}
              onClick={() => {
                setActiveView(tab);
                setHoldingSearch('');
                setCurrentPage(1);
                setVisibleCount(INITIAL_RENDER_COUNT);
              }}
              className={`rounded-[10px] px-4 py-1.5 text-xs font-medium transition ${
                activeView === tab
                  ? 'bg-white/10 text-white'
                  : 'text-zinc-500 hover:text-zinc-300'
              }`}
            >
              {tab === 'holdings' ? `Holdings (${selected.holdingCount})` : `Changes (${selected.changes.length})`}
            </button>
          ))}
        </div>

        <label className="relative block w-full sm:w-56">
          <Search className="pointer-events-none absolute left-2.5 top-1/2 h-3 w-3 -translate-y-1/2 text-zinc-600" />
          <input
            value={holdingSearch}
            onChange={(e) => {
              setHoldingSearch(e.target.value);
              setCurrentPage(1);
              setVisibleCount(INITIAL_RENDER_COUNT);
            }}
            placeholder="Filter tickers…"
            className="h-8 w-full rounded-lg border border-white/[0.06] bg-white/[0.02] pl-7 pr-3 text-xs text-white outline-none transition placeholder:text-zinc-600 focus:border-[#10b981]/30"
          />
        </label>
      </div>

      {/* Table */}
      <div className="overflow-hidden rounded-2xl border border-white/[0.06] bg-white/[0.02]">
        {activeView === 'holdings' ? (
          <>
            <div className="grid grid-cols-[1fr_0.8fr_0.8fr_0.7fr] gap-2 border-b border-white/[0.06] bg-white/[0.01] px-4 py-2.5 text-[10px] font-semibold uppercase tracking-[0.15em] text-zinc-600">
              <span>Ticker</span>
              <span className="text-right">Value Held</span>
              <span className="text-right">Shares</span>
              <div className="text-right">
                <QoqHeader align="right" />
              </div>
            </div>
            <div className="divide-y divide-white/[0.04]">
              {pagedHoldings.map((h) => {
                const kind = getFundChangeKind(h);
                return (
                  <Link
                    key={`${selected.reportPeriod}-${h.ticker}`}
                    href={h.ticker ? `/?q=${h.ticker}` : '#'}
                    className="grid grid-cols-[1fr_0.8fr_0.8fr_0.7fr] items-center gap-2 px-4 py-3 transition hover:bg-white/[0.03]"
                  >
                    <div className="flex items-center gap-2">
                      <TickerLogo ticker={h.ticker} />
                      <span className="text-sm font-medium text-white">{h.ticker || 'N/A'}</span>
                      <ArrowUpRight className="h-3 w-3 text-zinc-700" />
                    </div>
                    <div className="text-right text-xs text-zinc-300">{formatCompactCurrency(h.value_held)}</div>
                    <div className="text-right text-xs text-zinc-500">{formatShareCount(h.shares_held)}</div>
                    <div className={`text-right text-xs font-medium ${changeTextColor(kind)}`}>
                      {formatFundChangeLabel(h)}
                    </div>
                  </Link>
                );
              })}
              {pagedHoldings.length === 0 && (
                <div className="px-4 py-12 text-center text-sm text-zinc-600">
                  {holdingSearch ? 'No tickers match that filter.' : 'No positive-share holdings reported.'}
                </div>
              )}
            </div>
          </>
        ) : (
          <>
            <div className="grid grid-cols-[1fr_1fr_0.6fr_0.6fr_0.6fr] gap-2 border-b border-white/[0.06] bg-white/[0.01] px-4 py-2.5 text-[10px] font-semibold uppercase tracking-[0.15em] text-zinc-600">
              <span>Ticker</span>
              <span>Action</span>
              <div className="text-right">
                <QoqHeader align="right" />
              </div>
              <span className="text-right">Shares</span>
              <span className="text-right">Value</span>
            </div>
            <div className="divide-y divide-white/[0.04]">
              {pagedHoldings.map((h) => {
                const kind = getFundChangeKind(h);
                return (
                  <Link
                    key={`${selected.reportPeriod}-change-${h.ticker}`}
                    href={h.ticker ? `/?q=${h.ticker}` : '#'}
                    className="grid grid-cols-[1fr_1fr_0.6fr_0.6fr_0.6fr] items-center gap-2 px-4 py-3 transition hover:bg-white/[0.03]"
                  >
                    <div className="flex items-center gap-2">
                      <TickerLogo ticker={h.ticker} />
                      <span className="text-sm font-medium text-white">{h.ticker || 'N/A'}</span>
                    </div>
                    <div><ChangeKindBadge kind={kind} /></div>
                    <div className={`text-right text-xs font-medium ${changeTextColor(kind)}`}>
                      {formatFundChangeLabel(h)}
                    </div>
                    <div className="text-right text-xs text-zinc-500">{formatShareCount(h.shares_held)}</div>
                    <div className="text-right text-xs text-zinc-300">{formatCompactCurrency(h.value_held)}</div>
                  </Link>
                );
              })}
              {pagedHoldings.length === 0 && (
                <div className="px-4 py-12 text-center text-sm text-zinc-600">
                  {holdingSearch ? 'No tickers match that filter.' : 'No position changes available.'}
                </div>
              )}
            </div>
          </>
        )}

        {filteredHoldings.length > 0 && (
          <div className="flex flex-col gap-3 border-t border-white/[0.06] px-4 py-3 sm:flex-row sm:items-center sm:justify-between">
            <div className="text-xs text-zinc-500">
              Showing {showingStart.toLocaleString()}–{showingEnd.toLocaleString()} of {filteredHoldings.length.toLocaleString()}
            </div>

            <div className="flex flex-wrap items-center gap-2">
              {canLoadMore && (
                <button
                  type="button"
                  onClick={() => setVisibleCount((count) => Math.min(count + 20, PAGE_ROW_LIMIT))}
                  className="rounded-lg border border-white/[0.08] bg-white/[0.03] px-3 py-1.5 text-xs font-medium text-zinc-300 transition hover:border-white/[0.14] hover:text-white"
                >
                  Load 20 more
                </button>
              )}
              <button
                type="button"
                disabled={!canPrevPage}
                onClick={() => {
                  setCurrentPage((page) => Math.max(page - 1, 1));
                  setVisibleCount(INITIAL_RENDER_COUNT);
                }}
                className="rounded-lg border border-white/[0.08] bg-white/[0.03] px-3 py-1.5 text-xs font-medium text-zinc-300 transition hover:border-white/[0.14] hover:text-white disabled:cursor-not-allowed disabled:opacity-40"
              >
                Previous page
              </button>
              <button
                type="button"
                disabled={!canNextPage}
                onClick={() => {
                  setCurrentPage((page) => page + 1);
                  setVisibleCount(INITIAL_RENDER_COUNT);
                }}
                className="rounded-lg border border-white/[0.08] bg-white/[0.03] px-3 py-1.5 text-xs font-medium text-zinc-300 transition hover:border-white/[0.14] hover:text-white disabled:cursor-not-allowed disabled:opacity-40"
              >
                Next page
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
