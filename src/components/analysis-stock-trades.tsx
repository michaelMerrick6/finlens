'use client';

import { useEffect, useState } from 'react';
import type { AnalysisBasis, AnalysisPeriod, AnalysisTradePage } from '@/lib/analysis-overview';
import type { AnalysisTrade } from '@/lib/congress-analysis';
import { readPublicPage, storePublicPage } from '@/lib/public-page-cache';
import { dateLabel } from '@/lib/ui-format';
import { Avatar } from './identity-images';
import { LoadingRegion, Skeleton } from './loading-state';

export function AnalysisStockTrades({ ticker, period, basis, instrument, end, onSelect }: {
  ticker: string; period: AnalysisPeriod; basis: AnalysisBasis; instrument: string; end: string;
  onSelect: (trade: AnalysisTrade) => void;
}) {
  const [offset, setOffset] = useState(0);
  const [revision, setRevision] = useState(0);
  const key = `analysis-trades:${period}:${basis}:${instrument}:${end}:${ticker}:${offset}`;
  const [trades, setTrades] = useState<AnalysisTrade[]>(() => readPublicPage<AnalysisTradePage>(key)?.trades || []);
  const [next, setNext] = useState<number | null>(null);
  const [busy, setBusy] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    const controller = new AbortController();
    async function load() {
      const saved = readPublicPage<AnalysisTradePage>(key);
      if (saved) return saved;
      const response = await fetch(`/api/analysis/trades?${new URLSearchParams({ ticker, period, basis, instrument, end, offset: String(offset) })}`, { signal: controller.signal });
      const result = await response.json();
      if (!response.ok) throw new Error(result.error || 'Trade details are temporarily unavailable.');
      if (!controller.signal.aborted) storePublicPage(key, result);
      return result as AnalysisTradePage;
    }
    load().then(result => {
      if (controller.signal.aborted) return;
      setTrades(current => offset === 0 ? result.trades : [...current, ...result.trades]);
      setNext(result.nextOffset);
    }).catch(error => { if (!controller.signal.aborted) setError(error.message); })
      .finally(() => { if (!controller.signal.aborted) setBusy(false); });
    return () => controller.abort();
  }, [ticker, period, basis, instrument, end, offset, revision, key]);

  return <div aria-busy={busy}>
    {busy && !trades.length && <LoadingRegion label={`Loading ${ticker} trades…`}>
      {Array.from({ length: 4 }, (_, index) => <div className="analysis-trade" key={index}>
        <span className="analysis-person"><Skeleton className="skeleton-avatar"/><Skeleton className="skeleton-name"/></span>
        <Skeleton className="skeleton-badge"/><Skeleton className="skeleton-line"/><Skeleton className="skeleton-line"/><Skeleton className="skeleton-line"/>
      </div>)}
    </LoadingRegion>}
    {!!trades.length && <>
      <div className="analysis-trade-heading" aria-hidden="true"><span>Politician</span><span>Activity</span><span>Disclosed amount</span><span>Transaction date</span><span>Source</span></div>
      {trades.map(trade => <button key={trade.id} className="analysis-trade" onClick={() => onSelect(trade)}>
        <span className="analysis-person"><Avatar name={trade.politician_name || 'Unknown'} memberId={trade.member_id}/><span>{trade.politician_name}</span></span>
        <span className={`analysis-direction ${trade.transaction_type === 'buy' ? 'purchase' : 'sale'}`}>{trade.transaction_type === 'buy' ? 'Purchase' : 'Sale'}</span>
        <span>{trade.amount_range || 'Amount unavailable'}</span><span className="analysis-dates">{dateLabel(trade.transaction_date)}<small>Disclosed {dateLabel(trade.published_date)}</small></span><span>View filing details →</span>
      </button>)}
    </>}
    {error && <div className="empty-state" role="alert"><p>{error}</p><button className="button secondary small" onClick={() => { setError(''); setBusy(true); setRevision(value => value + 1); }}>Try again</button></div>}
    {!busy && !error && !trades.length && <p>No eligible trades in this period.</p>}
    {!error && next !== null && <div className="load-more"><button className="button secondary small" disabled={busy} onClick={() => { setBusy(true); setOffset(next); }}>{busy ? 'Loading trades…' : 'Show more trades'}</button></div>}
  </div>;
}
