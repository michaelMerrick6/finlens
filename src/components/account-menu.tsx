"use client";
import Link from 'next/link';
import { useEffect, useRef, useState } from 'react';
import { useAccount } from './account-provider';
export function AccountMenu() {
  const { session, account, signOut } = useAccount();
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const root = useRef<HTMLDivElement>(null);
  const trigger = useRef<HTMLButtonElement>(null);
  useEffect(() => {
    if (!open) return;
    const outside = (e: PointerEvent) => { if (!root.current?.contains(e.target as Node)) setOpen(false); };
    const escape = (e: KeyboardEvent) => { if (e.key === 'Escape') { setOpen(false); trigger.current?.focus(); } };
    document.addEventListener('pointerdown', outside);
    document.addEventListener('keydown', escape);
    return () => { document.removeEventListener('pointerdown', outside); document.removeEventListener('keydown', escape); };
  }, [open]);
  if (!session) return null;
  return <div className="account-menu" ref={root} onBlur={e => { if (!e.currentTarget.contains(e.relatedTarget)) setOpen(false); }}>
    <button ref={trigger} className="button secondary small" aria-expanded={open} aria-controls="account-panel" onClick={() => setOpen(value => !value)}>Account <span aria-hidden="true">⌄</span></button>
    {open && <div id="account-panel" className="account-panel" aria-label="Your account">
      <span className="eyebrow">YOUR ACCOUNT</span>
      <strong className="account-email">{session.user.email || 'Signed in'}</strong>
      {account && <p>{account.followCount} of {account.followLimit} tracking slots used<br/>Email alerts {account.subscriptions.email.active ? 'on' : 'off'}</p>}
      <Link href="/tracking" onClick={() => setOpen(false)}>Tracking & alerts <span aria-hidden="true">→</span></Link>
      <button disabled={busy} onClick={async () => { setBusy(true); setError(''); try { await signOut(); setOpen(false); } catch { setError('Could not sign out. Please try again.'); } finally { setBusy(false); } }}>{busy ? 'Signing out…' : 'Sign out'}</button>
      {error && <p className="error" role="alert">{error}</p>}
    </div>}
  </div>;
}
