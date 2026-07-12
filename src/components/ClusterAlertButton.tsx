'use client';

import { type CSSProperties, useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import type { Session } from '@supabase/supabase-js';
import { BellPlus, Check, CheckCircle2, Sparkles, X } from 'lucide-react';

import { supabase } from '@/lib/supabase';

type ClusterAlertChannel = 'email' | 'sms';

const CLUSTER_RULES = [
  ['Congress', '2+ members'],
  ['Insiders', '2+ insiders'],
  ['Cross-source', 'Congress, insiders, or funds align'],
  ['Window', '10-45 days by source'],
  ['Daily cap', '5 cluster events per UTC day'],
] as const;

const CONFETTI = [
  { x: '-88px', y: '-58px', r: '-42deg', c: '#34d399', d: '0ms' },
  { x: '-54px', y: '-88px', r: '38deg', c: '#60a5fa', d: '40ms' },
  { x: '-18px', y: '-78px', r: '-70deg', c: '#fbbf24', d: '80ms' },
  { x: '34px', y: '-86px', r: '54deg', c: '#f472b6', d: '30ms' },
  { x: '78px', y: '-50px', r: '92deg', c: '#22d3ee', d: '70ms' },
  { x: '-82px', y: '4px', r: '120deg', c: '#a78bfa', d: '110ms' },
  { x: '-40px', y: '38px', r: '-112deg', c: '#10b981', d: '60ms' },
  { x: '42px', y: '40px', r: '124deg', c: '#f87171', d: '100ms' },
  { x: '88px', y: '6px', r: '-96deg', c: '#38bdf8', d: '130ms' },
] as const;

export default function ClusterAlertButton() {
  const router = useRouter();
  const [session, setSession] = useState<Session | null>(null);
  const [enabled, setEnabled] = useState(false);
  const [selectedChannels, setSelectedChannels] = useState<ClusterAlertChannel[]>([]);
  const [modalOpen, setModalOpen] = useState(false);
  const [saving, setSaving] = useState(false);
  const [added, setAdded] = useState(false);
  const [error, setError] = useState('');
  const [hasClusterAccess, setHasClusterAccess] = useState(false);

  useEffect(() => {
    let mounted = true;

    supabase.auth.getSession().then(({ data }) => {
      if (mounted) {
        setSession(data.session);
      }
    });

    const { data } = supabase.auth.onAuthStateChange((_event, nextSession) => {
      setSession(nextSession);
    });

    return () => {
      mounted = false;
      data.subscription.unsubscribe();
    };
  }, []);

  useEffect(() => {
    if (!session) {
      setEnabled(false);
      setSelectedChannels([]);
      setHasClusterAccess(false);
      return;
    }

    fetch('/api/account/cluster-alerts', {
      headers: { Authorization: `Bearer ${session.access_token}` },
    })
      .then((response) => response.json())
      .then((payload: { enabled?: boolean; channels?: ClusterAlertChannel[]; hasClusterAccess?: boolean }) => {
        setEnabled(Boolean(payload.enabled));
        setSelectedChannels(payload.channels || []);
        setHasClusterAccess(Boolean(payload.hasClusterAccess));
      })
      .catch(() => {
        setEnabled(false);
        setSelectedChannels([]);
        setHasClusterAccess(false);
      });
  }, [session]);

  useEffect(() => {
    if (!modalOpen) {
      return;
    }

    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape' && !saving) {
        setModalOpen(false);
      }
    };

    window.addEventListener('keydown', onKeyDown);
    return () => {
      document.body.style.overflow = previousOverflow;
      window.removeEventListener('keydown', onKeyDown);
    };
  }, [modalOpen, saving]);

  function closeModal() {
    if (!saving) {
      setModalOpen(false);
    }
  }

  async function handleConfirm() {
    if (!session) {
      setModalOpen(false);
      router.push('/auth?mode=signup');
      return;
    }

    if (!hasClusterAccess) {
      setModalOpen(false);
      router.push('/pricing');
      return;
    }

    setSaving(true);
    setAdded(false);
    setError('');
    try {
      const response = await fetch('/api/account/cluster-alerts', {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${session.access_token}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ enabled: !enabled, channels: selectedChannels }),
      });
      const payload = (await response.json()) as {
        enabled?: boolean;
        deliveryReady?: boolean;
        channels?: ClusterAlertChannel[];
        code?: string;
        error?: string;
      };
      if (response.ok) {
        setEnabled(Boolean(payload.enabled));
        setSelectedChannels(payload.channels || []);
        if (payload.enabled) {
          setAdded(true);
          window.setTimeout(() => {
            setModalOpen(false);
            setAdded(false);
            router.push('/alerts');
          }, 1150);
        } else {
          setModalOpen(false);
        }
      } else {
        if (payload.code === 'PRO_REQUIRED') {
          setModalOpen(false);
          router.push('/pricing');
          return;
        }
        setError(payload.error || 'Could not update cluster alerts right now.');
      }
    } catch {
      setError('Could not update cluster alerts right now.');
    } finally {
      setSaving(false);
    }
  }

  const actionLabel = enabled ? 'Unfollow clusters' : 'Follow clusters';

  return (
    <>
      <button
        type="button"
        onClick={() => {
          setError('');
          setAdded(false);
          setModalOpen(true);
        }}
        aria-haspopup="dialog"
        aria-expanded={modalOpen}
        className="inline-flex items-center gap-2 rounded-lg border border-emerald-500/20 bg-emerald-500/[0.08] px-3.5 py-2 text-xs font-semibold text-emerald-300 transition hover:border-emerald-400/35 hover:bg-emerald-500/[0.14] hover:text-emerald-200"
      >
        {enabled ? <Check className="h-3.5 w-3.5" /> : <BellPlus className="h-3.5 w-3.5" />}
        {enabled ? 'Following clusters' : 'Follow clusters'}
      </button>

      {modalOpen ? (
        <div className="fixed inset-0 z-[90] flex items-start justify-center px-4 pt-[16vh]">
          <button
            type="button"
            aria-label="Close cluster alerts"
            onClick={closeModal}
            className="absolute inset-0 bg-black/65 backdrop-blur-sm"
          />

          <div
            role="dialog"
            aria-modal="true"
            aria-labelledby="cluster-alert-title"
            className="relative z-[91] w-full max-w-[420px] overflow-hidden rounded-3xl border border-white/[0.08] bg-[#101010] shadow-2xl shadow-black/60"
          >
            <div className="flex items-center justify-between px-5 pb-1 pt-4">
              <div className="inline-flex items-center gap-2 rounded-full border border-emerald-500/15 bg-emerald-500/[0.06] px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.16em] text-emerald-300">
                <Sparkles className="h-3 w-3" />
                Clusters
              </div>
              <button
                type="button"
                aria-label="Close cluster alerts"
                onClick={closeModal}
                className="flex h-7 w-7 items-center justify-center rounded-md text-zinc-500 transition hover:bg-white/5 hover:text-white"
              >
                <X className="h-4 w-4" />
              </button>
            </div>

            <div className="px-5 pb-5 pt-3">
              {added ? (
                <div className="relative overflow-hidden py-7 text-center">
                  <div className="pointer-events-none absolute inset-0">
                    {CONFETTI.map((piece, index) => (
                      <span
                        key={`${piece.x}-${piece.y}-${index}`}
                        className="cluster-confetti-piece"
                        style={
                          {
                            '--x': piece.x,
                            '--y': piece.y,
                            '--r': piece.r,
                            '--c': piece.c,
                            '--d': piece.d,
                          } as CSSProperties
                        }
                      />
                    ))}
                  </div>
                  <div className="relative mx-auto flex h-12 w-12 items-center justify-center rounded-full border border-emerald-400/30 bg-emerald-400/10 text-emerald-300 animate-[pulse_700ms_ease-out_1]">
                    <CheckCircle2 className="h-6 w-6" />
                  </div>
                  <h2 id="cluster-alert-title" className="relative mt-4 text-xl font-semibold tracking-tight text-white">
                    Following clusters.
                  </h2>
                  <p className="relative mt-2 text-sm text-zinc-500">Opening your Alerts page.</p>
                </div>
              ) : (
                <>
                  <h2 id="cluster-alert-title" className="text-2xl font-semibold tracking-tight text-white">
                    {!hasClusterAccess && session ? 'Clusters are Pro.' : enabled ? 'You are following clusters.' : 'Follow clusters.'}
                  </h2>
                  <p className="mt-2 text-sm leading-6 text-zinc-400">
                    {!hasClusterAccess && session
                      ? 'Upgrade to Pro to follow the cluster feed and receive cluster alerts.'
                      : 'A cluster is grouped movement around one stock from multiple politicians, insiders, or hedge funds inside a short window. Follow the feed for up to five cluster events per UTC day.'}
                  </p>

                  <div className="mt-5 rounded-2xl border border-white/[0.07] bg-white/[0.025] p-3.5">
                    <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-zinc-600">Detection rules</div>
                    <div className="mt-3 grid grid-cols-2 gap-2">
                      {CLUSTER_RULES.map(([label, value]) => (
                        <div key={label} className="rounded-xl border border-white/[0.06] bg-black/20 px-3 py-2.5">
                          <div className="text-[11px] font-semibold text-zinc-200">{label}</div>
                          <div className="mt-1 text-[11px] leading-4 text-zinc-500">{value}</div>
                        </div>
                      ))}
                    </div>
                  </div>

                  {error ? (
                    <div className="mt-3 rounded-lg border border-red-500/20 bg-red-500/5 px-3 py-2 text-xs text-red-300">
                      {error}
                    </div>
                  ) : null}
                </>
              )}
            </div>

            {!added ? (
              <div className="flex items-center justify-end gap-3 border-t border-white/[0.06] px-5 py-4">
                <button
                  type="button"
                  onClick={closeModal}
                  className="rounded-lg px-3 py-2 text-sm text-zinc-500 transition hover:text-white"
                >
                  Not now
                </button>
                <button
                  type="button"
                  onClick={handleConfirm}
                  disabled={saving}
                  className={`rounded-xl px-4 py-2.5 text-sm font-semibold transition disabled:cursor-wait disabled:opacity-60 ${
                    enabled
                      ? 'border border-white/[0.08] bg-white/[0.04] text-zinc-300 hover:bg-white/[0.08] hover:text-white'
                      : 'bg-white text-black hover:bg-emerald-200'
                  }`}
                >
                  {saving ? 'Saving...' : !hasClusterAccess && session ? 'View Pro' : actionLabel}
                </button>
              </div>
            ) : null}
          </div>
        </div>
      ) : null}
    </>
  );
}
