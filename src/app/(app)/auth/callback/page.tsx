'use client';

import { useEffect } from 'react';
import { useRouter } from 'next/navigation';

import { supabase } from '@/lib/supabase';

export default function AuthCallbackPage() {
  const router = useRouter();

  useEffect(() => {
    let mounted = true;
    let timeout: ReturnType<typeof setTimeout> | null = null;

    const finish = () => {
      if (mounted) {
        router.replace('/dashboard');
      }
    };

    const fail = (message: string) => {
      if (!mounted) return;
      router.replace(`/auth?error=${encodeURIComponent(message)}`);
    };

    const { data } = supabase.auth.onAuthStateChange((event, session) => {
      if (event === 'SIGNED_IN' && session) {
        finish();
      }
    });

    const completeOAuth = async () => {
      const url = new URL(window.location.href);
      const oauthError = url.searchParams.get('error_description') || url.searchParams.get('error');
      if (oauthError) {
        fail(oauthError);
        return;
      }

      const code = url.searchParams.get('code');
      if (code) {
        const { error } = await supabase.auth.exchangeCodeForSession(code);
        if (error) {
          fail(error.message);
          return;
        }
        finish();
        return;
      }

      const { data: sessionData, error } = await supabase.auth.getSession();
      if (error) {
        fail(error.message);
        return;
      }
      if (sessionData.session) {
        finish();
        return;
      }

      timeout = setTimeout(() => {
        fail('callback_timeout');
      }, 5000);
    };

    void completeOAuth();

    return () => {
      mounted = false;
      if (timeout) clearTimeout(timeout);
      data.subscription.unsubscribe();
    };
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
