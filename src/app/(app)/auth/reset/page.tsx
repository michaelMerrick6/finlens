'use client';

import Link from 'next/link';
import { useEffect, useState, type FormEvent } from 'react';
import { supabase } from '@/lib/supabase';

export default function PasswordResetPage() {
  const [ready, setReady] = useState(false);
  const [hasSession, setHasSession] = useState(false);
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [confirmation, setConfirmation] = useState('');
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState('');
  const [error, setError] = useState('');
  const [updated, setUpdated] = useState(false);

  useEffect(() => {
    let mounted = true;
    supabase.auth.getSession().then(({ data, error }) => {
      if (!mounted) return;
      setHasSession(Boolean(data.session));
      if (error) setError('Your reset link could not be verified. Request a new link below.');
      setReady(true);
    }).catch(() => {
      if (!mounted) return;
      setError('Your reset link could not be verified. Request a new link below.');
      setReady(true);
    });
    const { data } = supabase.auth.onAuthStateChange((_event, session) => {
      if (mounted) { setHasSession(Boolean(session)); setReady(true); }
    });
    return () => { mounted = false; data.subscription.unsubscribe(); };
  }, []);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError('');
    setMessage('');
    if (hasSession && password !== confirmation) {
      setError('Passwords do not match.');
      return;
    }
    setBusy(true);
    try {
      if (hasSession) {
        const { error } = await supabase.auth.updateUser({ password });
        if (error) throw error;
        setPassword('');
        setConfirmation('');
        setUpdated(true);
        setMessage('Your password has been updated.');
      } else {
        const origin = process.env.NEXT_PUBLIC_APP_URL?.trim().replace(/\/+$/, '') || window.location.origin;
        const { error } = await supabase.auth.resetPasswordForEmail(email.trim(), {
          redirectTo: `${origin}/auth/callback?recovery=1`,
        });
        if (error) throw error;
        setMessage('If an account exists for this email, you will receive a password reset link. Check your inbox and spam folder.');
      }
    } catch (value) {
      setError(value instanceof Error ? value.message : 'Could not reset your password. Please try again.');
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="mx-auto max-w-md px-4 py-10">
      <section className="rounded-2xl border border-white/10 bg-white/[0.03] p-6">
        <h1 className="text-2xl font-semibold text-white">{hasSession ? 'Choose a new password' : 'Reset your password'}</h1>
        <p className="mt-3 text-sm leading-6 text-zinc-400">{hasSession ? 'Use at least 8 characters for your new password.' : 'Enter your account email and we’ll send you a reset link.'}</p>
        {!ready ? <p role="status" className="mt-6 text-sm text-zinc-300">Checking your session…</p> : !updated ? (
          <form onSubmit={submit} className="mt-6 space-y-4">
            {hasSession ? <>
              <label className="block text-sm text-zinc-200">New password
                <input type="password" autoComplete="new-password" required minLength={8} value={password} onChange={event => setPassword(event.target.value)} className="mt-2 w-full rounded-xl border border-white/15 bg-black/30 p-3 text-white" />
              </label>
              <label className="block text-sm text-zinc-200">Confirm new password
                <input type="password" autoComplete="new-password" required minLength={8} value={confirmation} onChange={event => setConfirmation(event.target.value)} className="mt-2 w-full rounded-xl border border-white/15 bg-black/30 p-3 text-white" />
              </label>
            </> : <label className="block text-sm text-zinc-200">Email
              <input type="email" autoComplete="email" required value={email} onChange={event => setEmail(event.target.value)} className="mt-2 w-full rounded-xl border border-white/15 bg-black/30 p-3 text-white" />
            </label>}
            <button disabled={busy} className="min-h-11 w-full rounded-xl bg-emerald-300 px-4 py-3 text-sm font-semibold text-black disabled:opacity-50">{busy ? 'Please wait…' : hasSession ? 'Update password' : 'Send reset link'}</button>
          </form>
        ) : null}
        {error ? <p role="alert" className="mt-4 text-sm text-red-300">{error}</p> : null}
        {message ? <p role="status" className="mt-4 text-sm leading-6 text-emerald-300">{message}</p> : null}
        <Link href={updated ? '/dashboard' : '/auth?mode=signin'} className="mt-6 inline-flex min-h-11 items-center text-sm text-zinc-300 underline">{updated ? 'Continue to dashboard' : 'Back to sign in'}</Link>
      </section>
    </div>
  );
}
