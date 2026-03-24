'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import type { Session } from '@supabase/supabase-js';

import { supabase } from '@/lib/supabase';

export function HeaderAccountLink() {
  const [session, setSession] = useState<Session | null>(null);

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

  return (
    <Link href={session ? '/alerts' : '/auth'} className="btn-primary text-sm px-4 py-2">
      {session ? 'Account' : 'Get Alerts'}
    </Link>
  );
}
