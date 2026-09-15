"use client";
import { useEffect, useState, type CSSProperties } from 'react';
import { prepareTrackingChime } from '@/lib/tracking-chime';
import { Modal } from './modal';
import { useAccount } from './account-provider';
export function BriefSignup() {
  const { session, loading, openSignIn } = useAccount();
  const [enabled, setEnabled] = useState(false);
  const [confirmedEmail, setConfirmedEmail] = useState<string | null>(null);
  const [burst, setBurst] = useState(false);
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
  return <><div className="brief-signup">
    <div><strong>{enabled && session ? 'You’re on the list.' : 'Get the Sunday Brief.'}</strong><p>{enabled && session ? `Sunday Brief will arrive at ${session.user.email} every Sunday.` : 'Delivered to your inbox. Newsletter preferences are separate from trade alerts.'}</p></div>
    <span style={{position:"relative",display:"inline-block"}}>

    <button className="button primary" disabled={loading || busy || (!!session && !ready)} onClick={async () => {
      if (!session) { openSignIn(); return; }
      const chime = enabled ? null : prepareTrackingChime();
      setBusy(true); setError('');
      try {
        const r = await fetch('/api/account/sunday-brief', { method: 'POST', headers: { Authorization: `Bearer ${session.access_token}`, 'Content-Type': 'application/json' }, body: JSON.stringify({ enabled: !enabled }) });
        const data = await r.json(); if (!r.ok) throw Error(data.error || 'Could not save your preference.'); setEnabled(data.enabled);
        if (!enabled && data.enabled) { setConfirmedEmail(session.user.email || "your account email"); setBurst(true); chime?.play(); } else { chime?.cancel(); }
      } catch (e) { chime?.cancel(); setError(e instanceof Error ? e.message : 'Please try again.'); } finally { setBusy(false); }
    }}>{busy ? 'Saving…' : enabled && session ? 'Unsubscribe' : session ? 'Get Sunday Brief' : 'Sign in to subscribe'}</button>
    </span>
    {error && <p role="alert" className="error">{error}</p>}
  </div>
  {confirmedEmail && <Modal title="Sunday Brief" animated onClose={() => { setConfirmedEmail(null); setBurst(false); }}>
    <div className="brief-confirmation">
    {burst && <span className="track-confetti" aria-hidden="true" onAnimationEnd={() => setBurst(false)}>{Array.from({length:10},(_,i)=><i key={i} style={{"--confetti-x":`${Math.cos(i/10*Math.PI*2)*40}px`,"--confetti-y":`${Math.sin(i/10*Math.PI*2)*40-16}px`,"--confetti-spin":`${i*30}deg`,background:["#486d59","#9eaf83","#d4b66b"][i%3]} as CSSProperties}/>)}</span>}
      <h2>You’re on the list.</h2>
      <p>Sunday Brief will arrive at <strong>{confirmedEmail}</strong> every Sunday.</p>
      <p>A short look at congressional trades and the stories behind them.</p>
      <p className="fine-print">Your trade alerts haven’t changed. You can unsubscribe from the Sunday Brief page.</p>
      <button className="button primary" data-modal-close>Got it</button>
    </div>
  </Modal>}
  </>;
}
