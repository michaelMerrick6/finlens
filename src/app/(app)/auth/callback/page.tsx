'use client';

import { useEffect } from 'react';
import { useRouter } from 'next/navigation';

import { supabase } from '@/lib/supabase';

export default function AuthCallbackPage() {
  const router = useRouter();

  useEffect(() => {
    // The Supabase JS client automatically detects the hash fragment
    // tokens from the implicit OAuth flow and stores them.
    // We just need to wait for the auth state to settle, then redirect.
    supabase.auth.onAuthStateChange((event, session) => {
      if (event === 'SIGNED_IN' && session) {
        router.replace('/');
      }
    });

    // Also check if session already exists (in case the event already fired)
    supabase.auth.getSession().then(({ data }) => {
      if (data.session) {
        router.replace('/');
      }
    });

    // Fallback: if nothing happens after 5s, send them back to auth
    const timeout = setTimeout(() => {
      router.replace('/auth?error=callback_timeout');
    }, 5000);

    return () => clearTimeout(timeout);
  }, [router]);

  return (
    <div className="flex min-h-[60vh] items-center justify-center">
      <div className="text-center">
        <div className="mx-auto h-8 w-8 animate-spin rounded-full border-2 border-white/20 border-t-emerald-400" />
        <p className="mt-4 text-sm text-zinc-500">Completing sign in...</p>
      </div>
    </div>
  );
}
