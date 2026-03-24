'use client';

import Link from 'next/link';
import { type FormEvent, useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import type { Session } from '@supabase/supabase-js';
import { AlertTriangle, KeyRound, ShieldCheck, UserPlus } from 'lucide-react';

import { supabase } from '@/lib/supabase';

type AuthMode = 'signin' | 'signup';

export function AuthPanel() {
  const router = useRouter();
  const [mode, setMode] = useState<AuthMode>('signin');
  const [session, setSession] = useState<Session | null>(null);
  const [displayName, setDisplayName] = useState('');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState('');
  const [error, setError] = useState('');

  useEffect(() => {
    let mounted = true;

    supabase.auth.getSession().then(({ data }) => {
      if (mounted) {
        setSession(data.session);
        if (data.session) {
          router.replace('/alerts');
        }
      }
    });

    const { data } = supabase.auth.onAuthStateChange((_event, nextSession) => {
      setSession(nextSession);
      if (nextSession) {
        router.replace('/alerts');
      }
    });

    return () => {
      mounted = false;
      data.subscription.unsubscribe();
    };
  }, [router]);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setBusy(true);
    setError('');
    setMessage('');

    try {
      if (mode === 'signup') {
        const response = await supabase.auth.signUp({
          email: email.trim(),
          password,
          options: {
            data: {
              display_name: displayName.trim() || undefined,
            },
          },
        });

        if (response.error) {
          throw response.error;
        }

        if (response.data.session) {
          router.replace('/alerts');
          return;
        }

        setMessage('Account created. If email confirmation is enabled in Supabase, confirm your email and then sign in.');
      } else {
        const response = await supabase.auth.signInWithPassword({
          email: email.trim(),
          password,
        });

        if (response.error) {
          throw response.error;
        }

        router.replace('/alerts');
      }
    } catch (value) {
      setError(value instanceof Error ? value.message : 'Authentication failed.');
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="mx-auto max-w-5xl">
      <div className="grid gap-6 lg:grid-cols-[1.15fr_0.85fr]">
        <div className="glass-panel rounded-3xl p-8">
          <div className="mb-6 inline-flex items-center gap-2 rounded-full border border-blue-500/20 bg-blue-500/10 px-3 py-1 text-xs uppercase tracking-[0.22em] text-blue-300">
            <ShieldCheck className="h-3.5 w-3.5" />
            Private Alerts
          </div>

          <h1 className="text-4xl font-semibold tracking-tight text-white">Sign in to manage your follows.</h1>
          <p className="mt-4 max-w-2xl text-sm leading-6 text-[var(--text-secondary)]">
            Connect your account to email and Telegram, track up to your follow limit, and manage the alerts Vail sends for
            stocks, politicians, and insiders.
          </p>

          <div className="mt-8 grid gap-4 md:grid-cols-3">
            <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
              <div className="text-sm font-medium text-white">Stock follows</div>
              <div className="mt-2 text-sm text-zinc-400">Track a ticker and get activity or unusual alerts when insiders or politicians trade it.</div>
            </div>
            <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
              <div className="text-sm font-medium text-white">Person follows</div>
              <div className="mt-2 text-sm text-zinc-400">Follow a specific politician or insider and collapse noisy filings into one usable summary.</div>
            </div>
            <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
              <div className="text-sm font-medium text-white">Private delivery</div>
              <div className="mt-2 text-sm text-zinc-400">Use Telegram for speed and email for a persistent record of what Vail sent you.</div>
            </div>
          </div>
        </div>

        <div className="glass-panel rounded-3xl p-6 md:p-8">
          <div className="flex items-center justify-between gap-3">
            <div>
              <div className="text-sm uppercase tracking-[0.18em] text-zinc-500">Account Access</div>
              <div className="mt-2 text-2xl font-semibold text-white">{mode === 'signin' ? 'Welcome back' : 'Create account'}</div>
            </div>

            <div className="inline-flex rounded-full border border-white/10 bg-white/5 p-1 text-sm">
              <button
                type="button"
                onClick={() => {
                  setMode('signin');
                  setError('');
                  setMessage('');
                }}
                className={`rounded-full px-3 py-1.5 transition ${mode === 'signin' ? 'bg-white/15 text-white' : 'text-zinc-400 hover:text-white'}`}
              >
                Sign in
              </button>
              <button
                type="button"
                onClick={() => {
                  setMode('signup');
                  setError('');
                  setMessage('');
                }}
                className={`rounded-full px-3 py-1.5 transition ${mode === 'signup' ? 'bg-white/15 text-white' : 'text-zinc-400 hover:text-white'}`}
              >
                Sign up
              </button>
            </div>
          </div>

          <form className="mt-8 space-y-4" onSubmit={handleSubmit}>
            {mode === 'signup' ? (
              <div>
                <label className="mb-2 block text-sm text-zinc-300">Display name</label>
                <input
                  type="text"
                  value={displayName}
                  onChange={(event) => setDisplayName(event.target.value)}
                  className="w-full rounded-2xl border border-white/10 bg-[#0b1020] px-4 py-3 text-sm text-zinc-100 outline-none transition focus:border-blue-400/40"
                  placeholder="Mike"
                />
              </div>
            ) : null}

            <div>
              <label className="mb-2 block text-sm text-zinc-300">Email</label>
              <input
                type="email"
                value={email}
                onChange={(event) => setEmail(event.target.value)}
                className="w-full rounded-2xl border border-white/10 bg-[#0b1020] px-4 py-3 text-sm text-zinc-100 outline-none transition focus:border-blue-400/40"
                placeholder="you@vail.finance"
                required
              />
            </div>

            <div>
              <label className="mb-2 block text-sm text-zinc-300">Password</label>
              <input
                type="password"
                value={password}
                onChange={(event) => setPassword(event.target.value)}
                className="w-full rounded-2xl border border-white/10 bg-[#0b1020] px-4 py-3 text-sm text-zinc-100 outline-none transition focus:border-blue-400/40"
                placeholder="••••••••"
                minLength={8}
                required
              />
            </div>

            {message ? (
              <div className="rounded-2xl border border-emerald-500/20 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-200">
                {message}
              </div>
            ) : null}

            {error ? (
              <div className="flex items-start gap-3 rounded-2xl border border-red-500/20 bg-red-500/10 px-4 py-3 text-sm text-red-200">
                <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
                <span>{error}</span>
              </div>
            ) : null}

            <button
              type="submit"
              disabled={busy}
              className="inline-flex w-full items-center justify-center gap-2 rounded-2xl bg-blue-500 px-4 py-3 text-sm font-medium text-white transition hover:bg-blue-400 disabled:cursor-not-allowed disabled:opacity-60"
            >
              {mode === 'signin' ? <KeyRound className="h-4 w-4" /> : <UserPlus className="h-4 w-4" />}
              {busy ? 'Working...' : mode === 'signin' ? 'Sign in' : 'Create account'}
            </button>
          </form>

          <div className="mt-6 text-sm text-zinc-400">
            By continuing you’re creating your private Vail alert workspace. Ops tools stay internal at{' '}
            <Link href="/ops" className="text-blue-300 hover:text-blue-200">
              /ops
            </Link>
            .
          </div>

          {session ? (
            <div className="mt-4 rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-sm text-zinc-300">
              You are already signed in.{' '}
              <Link href="/alerts" className="text-blue-300 hover:text-blue-200">
                Go to your alert workspace
              </Link>
              .
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}
