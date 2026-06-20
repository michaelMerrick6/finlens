'use client';

import { useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import type { Session } from '@supabase/supabase-js';
import { BellPlus, Check } from 'lucide-react';

import { CreateSignalModal } from '@/components/CreateSignalModal';
import type { AccountState } from '@/lib/account-types';
import { supabase } from '@/lib/supabase';

function localActorKey(value: string) {
  return value
    .normalize('NFKD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '');
}

function isFollowingFund(state: AccountState | null, fundName: string) {
  const target = localActorKey(fundName);
  if (!target) return false;

  return Boolean(
    state?.follows.actors.some(
      (follow) =>
        follow.actorType === 'fund' &&
        (localActorKey(follow.actorName) === target || localActorKey(follow.actorKey) === target),
    ),
  );
}

export default function HedgeFundFollowButton({ fundName }: { fundName: string }) {
  const router = useRouter();
  const [session, setSession] = useState<Session | null>(null);
  const [accountState, setAccountState] = useState<AccountState | null>(null);
  const [modalOpen, setModalOpen] = useState(false);

  useEffect(() => {
    let cancelled = false;

    async function loadSessionAndState() {
      const { data } = await supabase.auth.getSession();
      if (cancelled) return;

      const nextSession = data.session || null;
      setSession(nextSession);
      if (!nextSession) return;

      try {
        const response = await fetch('/api/account/state?history=0&preview=0', {
          headers: { Authorization: `Bearer ${nextSession.access_token}` },
        });
        const payload = (await response.json()) as { ok?: boolean; state?: AccountState };
        if (!cancelled && response.ok && payload.ok && payload.state) {
          setAccountState(payload.state);
        }
      } catch {
        if (!cancelled) setAccountState(null);
      }
    }

    loadSessionAndState();
    const { data: subscription } = supabase.auth.onAuthStateChange((_event, nextSession) => {
      setSession(nextSession);
      if (!nextSession) setAccountState(null);
    });

    return () => {
      cancelled = true;
      subscription.subscription.unsubscribe();
    };
  }, []);

  const following = isFollowingFund(accountState, fundName);

  function handleClick() {
    if (!session) {
      router.push('/auth?mode=signup');
      return;
    }

    setModalOpen(true);
  }

  return (
    <>
      <button
        type="button"
        onClick={handleClick}
        className="inline-flex w-fit items-center gap-2 rounded-lg border border-emerald-500/20 bg-emerald-500/[0.08] px-3.5 py-2 text-xs font-semibold text-emerald-300 transition hover:border-emerald-400/35 hover:bg-emerald-500/[0.14] hover:text-emerald-200"
      >
        {following ? <Check className="h-3.5 w-3.5" /> : <BellPlus className="h-3.5 w-3.5" />}
        {following ? 'Following fund' : 'Follow fund'}
      </button>

      {modalOpen && session ? (
        <CreateSignalModal
          session={session}
          initialKind="fund"
          initialQuery={fundName}
          onCreated={(state) => {
            setAccountState(state);
            setModalOpen(false);
          }}
          onClose={() => setModalOpen(false)}
          zIndex={90}
        />
      ) : null}
    </>
  );
}
