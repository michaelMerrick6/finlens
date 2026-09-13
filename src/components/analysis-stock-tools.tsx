"use client";
import Link from 'next/link';
import { useState, type CSSProperties } from 'react';
import { prepareTrackingChime } from '@/lib/tracking-chime';
import { useAccount } from './account-provider';

export function AnalysisStockTools({ ticker }: { ticker: string }) {
  const { account, session, loading, openSignIn, mutate } = useAccount();
  const [burst, setBurst] = useState(0);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const followed = account?.follows.tickers.find(item => item.ticker === ticker);
  return <div className="analysis-stock-tools">
    <div><strong>Follow {ticker}</strong><p className="fine-print">{followed ? 'Saved to your tracking list.' : 'Keep this stock in your tracking list.'} Email delivery follows your account settings.</p></div>
    <div className="analysis-stock-actions">
      <div className="track-control">
      {burst > 0 && <span className="track-confetti" key={burst} aria-hidden="true" onAnimationEnd={() => setBurst(0)}>
        {Array.from({ length: 10 }, (_, i) => {
          const angle = i / 10 * Math.PI * 2;
          const distance = 32 + i % 3 * 9;
          return <i key={i} style={{
            '--confetti-x': `${Math.cos(angle) * distance}px`,
            '--confetti-y': `${Math.sin(angle) * distance - 16}px`,
            '--confetti-spin': `${(i % 2 ? 1 : -1) * (60 + i * 12)}deg`,
            background: ['#486d59', '#9eaf83', '#d4b66b', '#b9c9b0'][i % 4],
            borderRadius: i % 3 === 0 ? '50%' : '1px',
          } as CSSProperties} />;
        })}
      </span>}
      <button className={`button ${followed ? 'secondary' : 'primary'}`} disabled={busy || loading} onClick={async () => {
        if (!session) { openSignIn(); return; }
        const chime = followed ? null : prepareTrackingChime();
        setBusy(true); setError('');
        try {
          await mutate('/api/account/follows', followed ? { kind: 'ticker', id: followed.id } : { kind: 'ticker', ticker, alertMode: 'activity' }, followed ? 'DELETE' : 'POST');
          chime?.play();
          if (!followed && !window.matchMedia('(prefers-reduced-motion: reduce)').matches) setBurst(n => n + 1);
        } catch (e) { chime?.cancel(); setError(e instanceof Error ? e.message : 'Could not update tracking.'); }
        finally { setBusy(false); }
      }}>{busy ? 'Saving…' : followed ? `Untrack ${ticker}` : `Track ${ticker}`}</button>
      </div>
      <Link className="text-link" href="/tracking">Manage alerts →</Link>
    </div>
    {error && <p className="error" role="alert">{error}</p>}
  </div>;
}
