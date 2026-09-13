"use client";
import { useEffect, useRef, useState, type CSSProperties } from 'react';
import type { AccountFollowSuggestion, AccountTickerSuggestion } from '@/lib/account-types';
import { prepareTrackingChime } from '@/lib/tracking-chime';
import { useAccount } from './account-provider';
import { Modal } from './modal';
import { Avatar } from './disclosure-feed';
import { CompanyLogo } from './identity-images';

type Result = { key: string; name: string; subtitle: string; kind: 'actor' | 'ticker' };
const starters: Record<'actor' | 'ticker', Result[]> = {
  actor: [
    { key: 'p000197', name: 'Nancy Pelosi', subtitle: 'Politician', kind: 'actor' },
    { key: 'k000389', name: 'Ro Khanna', subtitle: 'Politician', kind: 'actor' },
    { key: 'm001157', name: 'Michael McCaul', subtitle: 'Politician', kind: 'actor' },
  ],
  ticker: [
    { key: 'NVDA', name: 'NVDA', subtitle: 'NVIDIA', kind: 'ticker' },
    { key: 'AAPL', name: 'AAPL', subtitle: 'Apple', kind: 'ticker' },
    { key: 'MSFT', name: 'MSFT', subtitle: 'Microsoft', kind: 'ticker' },
    { key: 'AMZN', name: 'AMZN', subtitle: 'Amazon', kind: 'ticker' },
  ],
};
export function AddTracking({ onClose }: { onClose: () => void }) {
  const { account, session, mutate } = useAccount();
  const [kind, setKind] = useState<'actor' | 'ticker'>('actor');
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<Result[]>([]);
  const [searching, setSearching] = useState(false);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState('');
  const [added, setAdded] = useState('');
  const [burst, setBurst] = useState('');
  const input = useRef<HTMLInputElement>(null);
  const saving = useRef(false);
  useEffect(() => { input.current?.focus(); }, []);
  useEffect(() => {
    const controller = new AbortController();
    const timer = setTimeout(async () => {
      if (query.trim().length < 2 || !session) { setResults([]); setSearching(false); return; }
      setSearching(true);
      try {
        const url = kind === 'actor' ? '/api/account/follow-search?actorType=politician&query=' : '/api/account/ticker-search?query=';
        const response = await fetch(url + encodeURIComponent(query.trim()), { signal: controller.signal, headers: { Authorization: `Bearer ${session.access_token}` } });
        const data = await response.json();
        if (!response.ok || !data.ok) throw new Error(data.error || 'Search could not load. Please try again.');
        if (controller.signal.aborted) return;
        setResults(kind === 'actor' ? data.suggestions.map((s: AccountFollowSuggestion) => ({ key: s.actorKey, name: s.actorName, subtitle: s.subtitle || 'Politician', kind })) : data.suggestions.map((s: AccountTickerSuggestion) => ({ key: s.ticker, name: s.ticker, subtitle: s.companyName, kind })));
      } catch (e) { if (!controller.signal.aborted) setError(e instanceof Error ? e.message : 'Search could not load.'); }
      finally { if (!controller.signal.aborted) setSearching(false); }
    }, 200);
    return () => { clearTimeout(timer); controller.abort(); };
  }, [query, kind, session]);
  if (!account) return null;
  const displayed = query.trim() ? results : starters[kind];
  const full = account.followCount >= account.followLimit;
  return <Modal animated title="Add to tracking" onClose={onClose}>
    <div className="add-tracking">
      <h2>Make it your list.</h2>
      <div className="segmented" aria-label="Search category">
        {(['actor', 'ticker'] as const).map(value => <button key={value} aria-pressed={kind === value} className={kind === value ? 'selected' : ''} onClick={() => { setKind(value); setQuery(''); setResults([]); setError(''); setSearching(false); input.current?.focus(); }}>{value === 'actor' ? 'Politicians' : 'Stocks'}</button>)}
      </div>
      <label className="add-search">{kind === 'actor' ? 'Search politicians' : 'Search stocks'}
        <input ref={input} value={query} placeholder={kind === 'actor' ? 'Name, e.g. Nancy Pelosi' : 'Ticker or company name'} onChange={e => { setQuery(e.target.value); setResults([]); setError(''); setSearching(e.target.value.trim().length >= 2); }} />
      </label>
      {!query.trim() && <small className="fine-print">A few to explore</small>}
      <div className="add-results" aria-busy={searching}>
        {searching ? <p className="muted" role="status">Searching…</p> : displayed.length ? displayed.map(item => {
          const tracked = item.kind === 'ticker' ? account.follows.tickers.some(t => t.ticker === item.key) : account.follows.actors.some(a => a.actorType === 'politician' && (a.actorKey.toLowerCase() === item.key.toLowerCase() || String(a.metadata.member_id || '').toLowerCase() === item.key.toLowerCase()));
          return <div className="add-result" key={item.key}>
            {item.kind === 'actor' ? <Avatar name={item.name} memberId={item.key.toUpperCase()} /> : <CompanyLogo ticker={item.key} />}
            <div className="add-result-name"><strong>{item.name}</strong><small>{item.subtitle}</small></div>
            <div className="add-result-action">
              {burst === item.key && <span className="track-confetti" aria-hidden="true" onAnimationEnd={() => setBurst('')}>{Array.from({ length: 10 }, (_, i) => <i key={i} style={{ '--confetti-x': `${Math.cos(i / 10 * Math.PI * 2) * 40}px`, '--confetti-y': `${Math.sin(i / 10 * Math.PI * 2) * 40 - 16}px`, '--confetti-spin': `${i * 30}deg`, background: ['#486d59', '#9eaf83', '#d4b66b'][i % 3] } as CSSProperties} />)}</span>}
              <button className={`button ${tracked ? 'secondary' : 'primary'}`} aria-label={tracked ? `Tracking ${item.name}` : `Track ${item.name}`} disabled={tracked || !!busy || full} onClick={async () => {
                if (saving.current) return;
                saving.current = true;
                const chime = prepareTrackingChime();
                setBusy(item.key); setError('');
                try {
                  await mutate('/api/account/follows', item.kind === 'ticker' ? { kind: 'ticker', ticker: item.key, alertMode: 'activity' } : { kind: 'actor', actorType: 'politician', actorName: item.name, actorKey: item.key, alertMode: 'activity' });
                  chime.play(); setAdded(`${item.name} added to your list.`);
                  if (!window.matchMedia('(prefers-reduced-motion: reduce)').matches) setBurst(item.key);
                } catch (e) { chime.cancel(); setError(e instanceof Error ? e.message : 'Could not add to tracking.'); }
                finally { saving.current = false; setBusy(''); }
              }}>{busy === item.key ? 'Saving…' : tracked ? '✓ Tracking' : 'Track'}</button>
            </div>
          </div>;
        }) : <p className="muted">{query.trim().length < 2 ? 'Type at least two characters to find someone or a stock.' : error ? 'Try another search or reopen this panel.' : 'No matches. Try another name or ticker.'}</p>}
      </div>
      {error && <p className="error" role="alert">{error}</p>}
      <p className="add-status" role="status">{added}</p>
      <footer className="add-footer"><div><small>{account.followCount} of {account.followLimit} tracking slots used</small><small>{full ? 'Your list is full. Remove an item to add another.' : account.subscriptions.email.active ? 'Email alerts are on.' : 'Saved to your list · Emails off.'}</small></div><button className="button secondary" data-modal-close>Done</button></footer>
    </div>
  </Modal>;
}
