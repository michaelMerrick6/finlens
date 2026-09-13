"use client";
import { useEffect, useState } from 'react';
import { useAccount } from './account-provider';
export function BriefSignup() {
  const { session, loading, openSignIn } = useAccount();
  const [enabled, setEnabled] = useState(false);
  const [busy, setBusy] = useState(false);
  const [ready, setReady] = useState(false);
  const [error, setError] = useState('');
  useEffect(() => {
    if (!session) return;
    const c = new AbortController();
    fetch('/api/account/sunday-brief', { signal: c.signal, headers: { Authorization: `Bearer ${session.access_token}` } }).then(async r => {
      const data = await r.json(); if (!r.ok) throw Error(data.error || 'Could not load your preference.');
      if (!c.signal.aborted) { setEnabled(data.enabled); setReady(true); }
    }).catch(e => { if (!c.signal.aborted) setError(e.message); });
    return () => c.abort();
  }, [session]);
  return <div className="brief-signup">
    <div><strong>{enabled && session ? 'You’re on the list.' : 'A little perspective for your Sunday.'}</strong><p>{enabled && session ? `We’ll send the first edition to ${session.user.email} when it’s ready.` : 'Subscribe for the first edition. Free, with a separate preference from trade alerts.'}</p></div>
    <button className="button primary" disabled={loading || busy || (!!session && !ready)} onClick={async () => {
      if (!session) { openSignIn(); return; }
      setBusy(true); setError('');
      try {
        const r = await fetch('/api/account/sunday-brief', { method: 'POST', headers: { Authorization: `Bearer ${session.access_token}`, 'Content-Type': 'application/json' }, body: JSON.stringify({ enabled: !enabled }) });
        const data = await r.json(); if (!r.ok) throw Error(data.error || 'Could not save your preference.'); setEnabled(data.enabled);
      } catch (e) { setError(e instanceof Error ? e.message : 'Please try again.'); } finally { setBusy(false); }
    }}>{busy ? 'Saving…' : enabled && session ? 'Unsubscribe' : session ? 'Get Sunday Brief' : 'Sign in to subscribe'}</button>
    {error && <p role="alert" className="error">{error}</p>}
  </div>;
}
