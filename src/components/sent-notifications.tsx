"use client";

import { useEffect, useState } from 'react';
import { useAccount } from './account-provider';
import { Icon } from './icon';

type SentEmail = { id: string; kind: 'daily' | 'individual'; subject: string; sent_at: string; item_count: number; destination: string };
type SentItem = { eventId: string; title: string; actor?: string; ticker?: string; asset?: string; activity?: string; amount?: string; tradedAt?: string; filedAt?: string; summary?: string; sourceUrl?: string };
type SentDetail = { items: SentItem[]; text_body: string | null };

function sentTime(value: string) {
  return new Intl.DateTimeFormat(undefined, { dateStyle: 'medium', timeStyle: 'short' }).format(new Date(value));
}
function sourceUrl(value?: string) {
  try { const url = new URL(value || ''); return ['http:', 'https:'].includes(url.protocol) ? url.href : null; } catch { return null; }
}
function SentCard({ email, token }: { email: SentEmail; token: string }) {
  const [open, setOpen] = useState(false);
  const [detail, setDetail] = useState<SentDetail | null>(null);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  useEffect(() => {
    if (!open || detail) return;
    const controller = new AbortController();
    fetch(`/api/account/notifications?id=${encodeURIComponent(email.id)}`, { headers: { Authorization: `Bearer ${token}` }, signal: controller.signal })
      .then(async response => { if (!response.ok) throw new Error('Could not load this email. Close it and try again.'); return response.json(); })
      .then(data => setDetail(data.notification))
      .catch(e => { if (!controller.signal.aborted) setError(e.message); })
      .finally(() => { if (!controller.signal.aborted) setBusy(false); });
    return () => controller.abort();
  }, [open, detail, email.id, token]);
  return <article className="sent-email">
    <button className="sent-email-heading" aria-expanded={open} aria-controls={`sent-${email.id}`} onClick={() => { setOpen(!open); setError(''); if (!open && !detail) setBusy(true); }}>
      <span className="outlined-icon"><Icon name="bell" size={20} /></span>
      <span className="sent-email-title"><strong>{email.subject}</strong><small>{email.kind === 'daily' ? `${email.item_count} updates · Daily roundup` : 'Earlier individual email'} · Sent to {email.destination}</small></span>
      <span className="sent-email-time"><span className="sent-badge">Sent</span><time dateTime={email.sent_at}>{sentTime(email.sent_at)}</time></span>
      <span aria-hidden="true">{open ? '−' : '+'}</span>
    </button>
    {open && <div id={`sent-${email.id}`} className="sent-email-content">
      {busy && <p role="status">Loading email…</p>}
      {error && <p className="error" role="alert">{error}</p>}
      {detail?.items.map((item, index) => <div className="sent-email-item" key={`${item.eventId}-${index}`}>
        <strong>{[item.actor, item.activity, item.asset || item.ticker, item.amount].filter(Boolean).join(' · ') || item.title}</strong>
        {item.summary && <p>{item.summary}</p>}
        <small>{[item.tradedAt && `Traded ${item.tradedAt.slice(0, 10)}`, item.filedAt && `Filed ${item.filedAt.slice(0, 10)}`].filter(Boolean).join(' · ')}</small>
        {sourceUrl(item.sourceUrl) && <a href={sourceUrl(item.sourceUrl)!} target="_blank" rel="noreferrer">Source filing ↗</a>}
      </div>)}
      {detail?.text_body && <details className="sent-email-original"><summary>View full email text</summary><pre>{detail.text_body}</pre></details>}
    </div>}
  </article>;
}

export function SentNotifications() {
  const { session } = useAccount();
  const token = session?.access_token;
  const [emails, setEmails] = useState<SentEmail[]>([]);
  const [offset, setOffset] = useState(0);
  const [hasMore, setHasMore] = useState(false);
  const [busy, setBusy] = useState(true);
  const [error, setError] = useState('');
  const [revision, setRevision] = useState(0);
  const zone = Intl.DateTimeFormat().resolvedOptions().timeZone.replaceAll('_', ' ');
  useEffect(() => {
    if (!token) return;
    const controller = new AbortController();
    fetch(`/api/account/notifications?offset=${offset}`, { headers: { Authorization: `Bearer ${token}` }, signal: controller.signal })
      .then(async response => { if (!response.ok) throw new Error('Your sent emails could not load. Please try again.'); return response.json(); })
      .then(data => { setEmails(previous => offset ? [...new Map([...previous, ...data.notifications].map((email: SentEmail) => [email.id, email])).values()] : data.notifications); setHasMore(data.hasMore); })
      .catch(e => { if (!controller.signal.aborted) setError(e.message); })
      .finally(() => { if (!controller.signal.aborted) setBusy(false); });
    return () => controller.abort();
  }, [token, offset, revision]);
  return <section className="sent-notifications" aria-label="Sent emails">
    <div className="sent-intro"><div><span className="eyebrow">YOUR EMAIL HISTORY</span><h2>Sent to you.</h2><p>One daily roundup across your tracked people and stocks. Expand an email to see what it included.</p></div><small>Times shown in {zone}</small></div>
    {emails.map(email => <SentCard key={email.id} email={email} token={token || ''} />)}
    {busy && <p role="status">Loading sent emails…</p>}
    {error && <div role="alert"><p className="error">{error}</p><button className="button secondary" onClick={() => { setBusy(true); setError(''); setRevision(revision + 1); }}>Try again</button></div>}
    {!busy && !error && !emails.length && <div className="empty-tracking"><Icon name="bell" size={28} /><h3>No emails sent yet.</h3><p>When email alerts are enabled and there’s new activity, your daily roundup will appear here after it’s sent.</p></div>}
    {!busy && !error && hasMore && <button className="button secondary" onClick={() => { setBusy(true); setOffset(emails.length); }}>Load older emails</button>}
  </section>;
}
