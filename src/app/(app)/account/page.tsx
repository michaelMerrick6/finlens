'use client';

import Image, { type ImageLoaderProps } from 'next/image';
import Link from 'next/link';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { useRouter } from 'next/navigation';
import type { Session } from '@supabase/supabase-js';
import {
  AlertTriangle,
  ArrowRight,
  Bell,
  Building2,
  CreditCard,
  ExternalLink,
  Landmark,
  LogOut,
  Mail,
  MessageSquare,
  Shield,
  ShieldAlert,
  TrendingUp,
} from 'lucide-react';

import type { AccountState, AlertMode } from '@/lib/account-types';
import { formatCalendarDate, formatDateTimeValue } from '@/lib/date-format';
import { supabase } from '@/lib/supabase';

type AccountApiPayload = {
  ok?: boolean;
  error?: string;
  state?: AccountState;
  url?: string;
};

type CombinedFollow = {
  id: string;
  type: 'ticker' | 'politician' | 'insider' | 'fund';
  label: string;
  sublabel: string;
  alertMode: AlertMode;
  createdAt: string;
};

const passthroughImageLoader = ({ src }: ImageLoaderProps) => src;

function formatBillingStatusLabel(value: string | null | undefined) {
  const normalized = String(value || 'free').trim().toLowerCase();
  return normalized
    .split('_')
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function alertModeLabel(value: AlertMode) {
  if (value === 'activity') return 'All activity';
  if (value === 'unusual') return 'Unusual only';
  return 'Custom';
}

function channelLabel(value: string) {
  if (value === 'email') return 'Email';
  if (value === 'sms') return 'Text';
  return value;
}

function signalStatusTone(status: string) {
  if (status === 'sent') {
    return 'border-emerald-500/20 bg-emerald-500/10 text-emerald-300';
  }
  if (status === 'failed') {
    return 'border-red-500/20 bg-red-500/10 text-red-300';
  }
  return 'border-amber-500/20 bg-amber-500/10 text-amber-300';
}

function metricDetailTone(isPositive: boolean) {
  return isPositive ? 'text-emerald-300' : 'text-zinc-500';
}

function MetricCard({
  eyebrow,
  title,
  detail,
  icon: Icon,
  iconClassName,
}: {
  eyebrow: string;
  title: string;
  detail: string;
  icon: React.ElementType;
  iconClassName: string;
}) {
  return (
    <div className="glass-panel rounded-2xl p-5">
      <div className="flex items-start justify-between gap-3">
        <div>
          <div className="text-[10px] uppercase tracking-[0.18em] text-zinc-600">{eyebrow}</div>
          <div className="mt-2 text-xl font-semibold text-white">{title}</div>
          <div className="mt-1 text-sm text-zinc-500">{detail}</div>
        </div>
        <div className={`flex h-10 w-10 shrink-0 items-center justify-center rounded-xl ${iconClassName}`}>
          <Icon className="h-4 w-4" />
        </div>
      </div>
    </div>
  );
}

export default function AccountPage() {
  const router = useRouter();
  const [session, setSession] = useState<Session | null>(null);
  const [sessionLoading, setSessionLoading] = useState(true);
  const [accountState, setAccountState] = useState<AccountState | null>(null);
  const [accountLoading, setAccountLoading] = useState(false);
  const [billingAction, setBillingAction] = useState<'portal' | null>(null);
  const [signingOut, setSigningOut] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    let mounted = true;

    supabase.auth.getSession().then(({ data }) => {
      if (!mounted) {
        return;
      }
      setSession(data.session);
      setSessionLoading(false);
      if (!data.session) {
        router.replace('/auth');
      }
    });

    const { data } = supabase.auth.onAuthStateChange((_event, nextSession) => {
      setSession(nextSession);
      setSessionLoading(false);
      if (!nextSession) {
        router.replace('/');
      }
    });

    return () => {
      mounted = false;
      data.subscription.unsubscribe();
    };
  }, [router]);

  const authedRequest = useCallback(async (path: string, init?: RequestInit) => {
    const accessToken = session?.access_token;
    if (!accessToken) {
      throw new Error('Your session has expired. Sign in again.');
    }

    const headers = new Headers(init?.headers);
    headers.set('Authorization', `Bearer ${accessToken}`);
    if (init?.body) {
      headers.set('Content-Type', 'application/json');
    }

    const response = await fetch(path, {
      ...init,
      headers,
    });

    const payload = (await response.json()) as AccountApiPayload;
    if (!response.ok || payload.ok === false) {
      throw new Error(payload.error || 'Account request failed.');
    }

    return payload;
  }, [session]);

  useEffect(() => {
    if (!session) {
      return;
    }

    let active = true;

    async function loadAccountState() {
      setAccountLoading(true);
      setError('');

      try {
        const payload = await authedRequest('/api/account/state');
        if (active) {
          setAccountState(payload.state || null);
        }
      } catch (value) {
        if (active) {
          setError(value instanceof Error ? value.message : 'Failed to load account details.');
        }
      } finally {
        if (active) {
          setAccountLoading(false);
        }
      }
    }

    loadAccountState();

    return () => {
      active = false;
    };
  }, [session, authedRequest]);

  async function handleBillingPortal() {
    if (!accountState?.billing.portalReady) {
      setError('Billing portal is not configured yet.');
      return;
    }

    setBillingAction('portal');
    setError('');
    try {
      const payload = await authedRequest('/api/account/billing/portal', {
        method: 'POST',
      });
      if (!payload.url) {
        throw new Error('Missing billing portal URL.');
      }
      window.location.href = payload.url;
    } catch (value) {
      setError(value instanceof Error ? value.message : 'Failed to open billing portal.');
      setBillingAction(null);
    }
  }

  async function handleSignOut() {
    setSigningOut(true);
    await supabase.auth.signOut();
    router.replace('/');
  }

  const user = session?.user || null;
  const displayName =
    accountState?.profile.displayName ||
    user?.user_metadata?.display_name ||
    user?.user_metadata?.full_name ||
    user?.email?.split('@')[0] ||
    'User';
  const avatarUrl = user?.user_metadata?.avatar_url;
  const billing = accountState?.billing || null;
  const emailState = accountState?.subscriptions.email;
  const smsState = accountState?.subscriptions.sms;
  const activeChannels =
    Number(Boolean(accountState?.profile.emailEnabled && emailState?.destination)) +
    Number(Boolean(accountState?.profile.textEnabled && smsState?.destination));
  const latestHistory = accountState?.history[0] || null;

  const combinedFollows = useMemo<CombinedFollow[]>(() => {
    if (!accountState) {
      return [];
    }

    return [
      ...accountState.follows.tickers.map((follow) => ({
        id: follow.id,
        type: 'ticker' as const,
        label: follow.ticker,
        sublabel: 'Stock signal',
        alertMode: follow.alertMode,
        createdAt: follow.createdAt,
      })),
      ...accountState.follows.actors.map((follow) => ({
        id: follow.id,
        type: follow.actorType,
        label: follow.actorName,
        sublabel:
          follow.actorType === 'politician'
            ? 'Congress member'
            : follow.actorType === 'fund'
              ? 'Hedge fund'
              : 'Corporate insider',
        alertMode: follow.alertMode,
        createdAt: follow.createdAt,
      })),
    ].sort((left, right) => {
      return new Date(right.createdAt).getTime() - new Date(left.createdAt).getTime();
    });
  }, [accountState]);

  if (sessionLoading) {
    return (
      <div className="flex min-h-[60vh] items-center justify-center">
        <div className="h-8 w-8 animate-spin rounded-full border-2 border-white/20 border-t-emerald-400" />
      </div>
    );
  }

  if (!session || !user) {
    return null;
  }

  return (
    <div className="mx-auto max-w-6xl px-4 py-12 sm:px-6">
      <div className="glass-panel rounded-3xl p-8">
        <div className="flex flex-col gap-6 lg:flex-row lg:items-start lg:justify-between">
          <div className="flex items-start gap-5">
            {avatarUrl ? (
              <Image
                loader={passthroughImageLoader}
                unoptimized
                src={avatarUrl}
                alt=""
                width={72}
                height={72}
                sizes="72px"
                className="h-[72px] w-[72px] rounded-2xl"
                referrerPolicy="no-referrer"
              />
            ) : (
              <div className="flex h-[72px] w-[72px] items-center justify-center rounded-2xl bg-gradient-to-br from-emerald-500 to-emerald-700 text-3xl font-bold text-white">
                {displayName.charAt(0).toUpperCase()}
              </div>
            )}

            <div className="min-w-0">
              <div className="text-[10px] uppercase tracking-[0.18em] text-zinc-600">Account Overview</div>
              <h1 className="mt-2 truncate text-3xl font-semibold tracking-tight text-white">{displayName}</h1>
              <p className="mt-1 truncate text-sm text-zinc-500">{user.email}</p>
              {accountState ? (
                <p className="mt-2 text-sm text-zinc-600">
                  Workspace {accountState.watchlist.name} • {accountState.followCount} of {accountState.followLimit} follows used
                </p>
              ) : null}
              <div className="mt-4 flex flex-wrap items-center gap-2">
                <span
                  className={`inline-flex items-center gap-1.5 rounded-full px-3 py-1 text-xs font-medium ${
                    billing?.planKey === 'pro'
                      ? 'border border-emerald-500/30 bg-emerald-500/15 text-emerald-300'
                      : 'border border-white/10 bg-white/5 text-zinc-400'
                  }`}
                >
                  <Shield className="h-3 w-3" />
                  {billing?.planName || 'Free'}
                </span>
                {billing ? (
                  <span className="rounded-full border border-white/10 bg-white/[0.03] px-3 py-1 text-xs text-zinc-400">
                    {formatBillingStatusLabel(billing.status)}
                  </span>
                ) : null}
                {accountState ? (
                  <span className="rounded-full border border-white/10 bg-white/[0.03] px-3 py-1 text-xs text-zinc-400">
                    {accountState.followCount} / {accountState.followLimit} follows
                  </span>
                ) : null}
              </div>
            </div>
          </div>

          <div className="flex flex-col gap-3 sm:flex-row">
            <Link
              href="/signals"
              className="inline-flex items-center justify-center gap-2 rounded-2xl border border-white/10 bg-white/[0.04] px-5 py-3 text-sm font-medium text-white transition hover:bg-white/[0.08]"
            >
              <Bell className="h-4 w-4" />
              Manage Signals
            </Link>
            {billing?.planKey === 'pro' ? (
              <button
                type="button"
                onClick={handleBillingPortal}
                disabled={billingAction !== null}
                className="inline-flex items-center justify-center gap-2 rounded-2xl bg-emerald-600 px-5 py-3 text-sm font-medium text-white transition hover:bg-emerald-500 disabled:cursor-not-allowed disabled:opacity-60"
              >
                <CreditCard className="h-4 w-4" />
                {billingAction === 'portal' ? 'Opening billing...' : 'Manage Billing'}
              </button>
            ) : (
              <Link
                href="/pricing"
                className="inline-flex items-center justify-center gap-2 rounded-2xl bg-emerald-600 px-5 py-3 text-sm font-medium text-white transition hover:bg-emerald-500 disabled:cursor-not-allowed disabled:opacity-60"
              >
                <CreditCard className="h-4 w-4" />
                Upgrade to Pro
              </Link>
            )}
          </div>
        </div>
      </div>

      {error ? (
        <div className="mt-6 flex items-center gap-2 rounded-2xl border border-red-500/20 bg-red-500/5 px-4 py-3 text-sm text-red-300">
          <AlertTriangle className="h-4 w-4 shrink-0" />
          <span>{error}</span>
        </div>
      ) : null}

      {accountLoading && !accountState ? (
        <div className="mt-6 flex min-h-[240px] items-center justify-center rounded-3xl border border-white/[0.06] bg-white/[0.02]">
          <div className="h-8 w-8 animate-spin rounded-full border-2 border-white/20 border-t-emerald-400" />
        </div>
      ) : accountState ? (
        <>
          <div className="mt-6 grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
            <MetricCard
              eyebrow="Membership"
              title={billing?.planName || 'Free'}
              detail={
                billing?.planKey === 'pro' && billing.currentPeriodEnd
                  ? billing.cancelAtPeriodEnd
                    ? `Access ends ${formatCalendarDate(billing.currentPeriodEnd)}`
                    : `Renews ${formatCalendarDate(billing.currentPeriodEnd)}`
                  : 'Free tier account'
              }
              icon={Shield}
              iconClassName="bg-emerald-500/10 text-emerald-400"
            />
            <MetricCard
              eyebrow="Delivery"
              title={`${activeChannels} active`}
              detail={
                activeChannels > 0
                  ? `${accountState.profile.emailEnabled ? 'Email' : ''}${accountState.profile.emailEnabled && accountState.profile.textEnabled ? ' + ' : ''}${accountState.profile.textEnabled ? 'Text' : ''}`
                  : 'No live channels connected'
              }
              icon={Mail}
              iconClassName="bg-blue-500/10 text-blue-400"
            />
            <MetricCard
              eyebrow="Follows"
              title={`${accountState.followCount} / ${accountState.followLimit}`}
              detail={`${Math.max(accountState.followLimit - accountState.followCount, 0)} slots remaining`}
              icon={TrendingUp}
              iconClassName="bg-amber-500/10 text-amber-400"
            />
            <MetricCard
              eyebrow="Recent Signal"
              title={latestHistory ? channelLabel(latestHistory.channel) : 'No activity yet'}
              detail={latestHistory ? formatDateTimeValue(latestHistory.sentAt || latestHistory.queuedAt) : 'Deliveries will appear here once signals fire'}
              icon={Bell}
              iconClassName="bg-violet-500/10 text-violet-400"
            />
          </div>

          <div className="mt-6 grid gap-4 xl:grid-cols-[1.15fr,0.85fr]">
            <section className="glass-panel rounded-3xl p-6">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <div className="text-[10px] uppercase tracking-[0.18em] text-zinc-600">Plan & Billing</div>
                  <h2 className="mt-2 text-xl font-semibold text-white">Membership status</h2>
                  <p className="mt-1 text-sm text-zinc-500">
                    {billing?.planKey === 'pro'
                      ? 'Your account is on the paid plan with expanded follows and multi-channel delivery.'
                      : 'Upgrade when you want more follows, text delivery, and higher-capacity signal tracking.'}
                  </p>
                </div>
                <span
                  className={`rounded-full border px-3 py-1 text-xs ${
                    billing?.planKey === 'pro'
                      ? 'border-emerald-500/20 bg-emerald-500/10 text-emerald-300'
                      : 'border-white/10 bg-white/[0.03] text-zinc-400'
                  }`}
                >
                  {billing?.planName || 'Free'}
                </span>
              </div>

              <div className="mt-5 grid gap-3 sm:grid-cols-2">
                <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-4">
                  <div className="text-[10px] uppercase tracking-[0.16em] text-zinc-600">Billing status</div>
                  <div className="mt-2 text-lg font-semibold text-white">{formatBillingStatusLabel(billing?.status)}</div>
                  <div className="mt-1 text-sm text-zinc-500">
                    {billing?.planKey === 'pro'
                      ? billing?.currentPeriodEnd
                        ? billing.cancelAtPeriodEnd
                          ? `Cancels at period end on ${formatCalendarDate(billing.currentPeriodEnd)}.`
                          : `Next renewal on ${formatCalendarDate(billing.currentPeriodEnd)}.`
                        : 'Paid plan is active.'
                      : `Free plan supports ${billing?.freeFollowLimit || accountState.followLimit} follows.`}
                  </div>
                </div>
                <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-4">
                  <div className="text-[10px] uppercase tracking-[0.16em] text-zinc-600">Capacity</div>
                  <div className="mt-2 text-lg font-semibold text-white">
                    {accountState.followCount} of {accountState.followLimit} used
                  </div>
                  <div className={`mt-1 text-sm ${metricDetailTone(accountState.followCount < accountState.followLimit)}`}>
                    {Math.max(accountState.followLimit - accountState.followCount, 0)} follows still available on this plan.
                  </div>
                </div>
              </div>

              <div className="mt-5 flex flex-wrap gap-3">
                <Link
                  href="/signals"
                  className="inline-flex items-center gap-2 rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-2.5 text-sm font-medium text-white transition hover:bg-white/[0.08]"
                >
                  Open Signals
                  <ArrowRight className="h-4 w-4" />
                </Link>
                {billing?.planKey === 'pro' ? (
                  <button
                    type="button"
                    onClick={handleBillingPortal}
                    disabled={billingAction !== null}
                    className="inline-flex items-center gap-2 rounded-2xl border border-emerald-500/20 bg-emerald-500/10 px-4 py-2.5 text-sm font-medium text-emerald-300 transition hover:bg-emerald-500/15 disabled:opacity-60"
                  >
                    <CreditCard className="h-4 w-4" />
                    Manage billing
                  </button>
                ) : (
                  <Link
                    href="/pricing"
                    className="inline-flex items-center gap-2 rounded-2xl border border-emerald-500/20 bg-emerald-500/10 px-4 py-2.5 text-sm font-medium text-emerald-300 transition hover:bg-emerald-500/15 disabled:opacity-60"
                  >
                    <CreditCard className="h-4 w-4" />
                    Upgrade for 25 follows
                  </Link>
                )}
              </div>
            </section>

            <section className="glass-panel rounded-3xl p-6">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <div className="text-[10px] uppercase tracking-[0.18em] text-zinc-600">Delivery Channels</div>
                  <h2 className="mt-2 text-xl font-semibold text-white">Private delivery</h2>
                  <p className="mt-1 text-sm text-zinc-500">Email and text settings are managed from your Signals workspace.</p>
                </div>
                <Link
                  href="/signals"
                  className="inline-flex items-center gap-1.5 rounded-full border border-white/10 bg-white/[0.03] px-3 py-1 text-xs text-zinc-400 transition hover:bg-white/[0.06] hover:text-white"
                >
                  Manage
                  <ArrowRight className="h-3 w-3" />
                </Link>
              </div>

              <div className="mt-5 space-y-3">
                <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-4">
                  <div className="flex items-start justify-between gap-3">
                    <div className="flex items-center gap-3">
                      <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-blue-500/10 text-blue-400">
                        <Mail className="h-4 w-4" />
                      </div>
                      <div>
                        <div className="text-sm font-medium text-white">Email</div>
                        <div className="mt-1 text-sm text-zinc-500">
                          {emailState?.destination || accountState.profile.alertEmail || user.email || 'Not configured'}
                        </div>
                      </div>
                    </div>
                    <span
                      className={`rounded-full border px-2.5 py-1 text-[11px] ${
                        accountState.profile.emailEnabled && emailState?.destination
                          ? 'border-emerald-500/20 bg-emerald-500/10 text-emerald-300'
                          : 'border-white/10 bg-white/[0.03] text-zinc-500'
                      }`}
                    >
                      {accountState.profile.emailEnabled && emailState?.destination ? 'Active' : 'Off'}
                    </span>
                  </div>
                </div>

                <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-4">
                  <div className="flex items-start justify-between gap-3">
                    <div className="flex items-center gap-3">
                      <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-cyan-500/10 text-cyan-400">
                        <MessageSquare className="h-4 w-4" />
                      </div>
                      <div>
                        <div className="text-sm font-medium text-white">Text</div>
                        <div className="mt-1 text-sm text-zinc-500">
                          {smsState?.destination || 'Not configured'}
                        </div>
                      </div>
                    </div>
                    <span
                      className={`rounded-full border px-2.5 py-1 text-[11px] ${
                        accountState.profile.textEnabled && smsState?.destination
                          ? 'border-emerald-500/20 bg-emerald-500/10 text-emerald-300'
                          : 'border-white/10 bg-white/[0.03] text-zinc-500'
                      }`}
                    >
                      {accountState.profile.textEnabled && smsState?.destination ? 'Active' : 'Off'}
                    </span>
                  </div>
                </div>
              </div>
            </section>
          </div>

          <div className="mt-4 grid gap-4 xl:grid-cols-[1.05fr,0.95fr]">
            <section className="glass-panel rounded-3xl p-6">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <div className="text-[10px] uppercase tracking-[0.18em] text-zinc-600">Current Follows</div>
                  <h2 className="mt-2 text-xl font-semibold text-white">Active signals</h2>
                  <p className="mt-1 text-sm text-zinc-500">Your newest follows across stocks, politicians, and insiders.</p>
                </div>
                <Link
                  href="/signals"
                  className="inline-flex items-center gap-1.5 rounded-full border border-white/10 bg-white/[0.03] px-3 py-1 text-xs text-zinc-400 transition hover:bg-white/[0.06] hover:text-white"
                >
                  Manage
                  <ArrowRight className="h-3 w-3" />
                </Link>
              </div>

              {combinedFollows.length > 0 ? (
                <div className="mt-5 space-y-3">
                  {combinedFollows.slice(0, 8).map((follow) => {
                    const Icon =
                      follow.type === 'ticker'
                        ? TrendingUp
                        : follow.type === 'politician'
                          ? Landmark
                          : follow.type === 'fund'
                            ? Building2
                            : ShieldAlert;
                    const iconClassName =
                      follow.type === 'ticker'
                        ? 'bg-amber-500/10 text-amber-400'
                        : follow.type === 'politician'
                          ? 'bg-blue-500/10 text-blue-400'
                          : follow.type === 'fund'
                            ? 'bg-emerald-500/10 text-emerald-300'
                            : 'bg-violet-500/10 text-violet-400';

                    return (
                      <div
                        key={follow.id}
                        className="flex items-center justify-between gap-3 rounded-2xl border border-white/[0.06] bg-white/[0.02] px-4 py-3"
                      >
                        <div className="min-w-0 flex items-center gap-3">
                          <div className={`flex h-10 w-10 shrink-0 items-center justify-center rounded-xl ${iconClassName}`}>
                            <Icon className="h-4 w-4" />
                          </div>
                          <div className="min-w-0">
                            <div className="truncate text-sm font-medium text-white">{follow.label}</div>
                            <div className="mt-1 flex flex-wrap items-center gap-2 text-xs text-zinc-500">
                              <span>{follow.sublabel}</span>
                              <span className="text-white/10">•</span>
                              <span>{alertModeLabel(follow.alertMode)}</span>
                              <span className="text-white/10">•</span>
                              <span>Added {formatCalendarDate(follow.createdAt)}</span>
                            </div>
                          </div>
                        </div>
                        <span className="rounded-full border border-white/10 bg-white/[0.03] px-2.5 py-1 text-[11px] text-zinc-400">
                          {follow.type === 'ticker'
                            ? 'Stock'
                            : follow.type === 'politician'
                              ? 'Politician'
                              : follow.type === 'fund'
                                ? 'Fund'
                                : 'Insider'}
                        </span>
                      </div>
                    );
                  })}
                  {combinedFollows.length > 8 ? (
                    <div className="text-xs text-zinc-600">+{combinedFollows.length - 8} more follows in your Signals workspace.</div>
                  ) : null}
                </div>
              ) : (
                <div className="mt-5 rounded-2xl border border-dashed border-white/[0.08] bg-white/[0.01] px-4 py-8 text-sm text-zinc-500">
                  No follows yet. Open Signals to start tracking stocks, politicians, insiders, or hedge funds.
                </div>
              )}
            </section>

            <section className="glass-panel rounded-3xl p-6">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <div className="text-[10px] uppercase tracking-[0.18em] text-zinc-600">Recent Signal Activity</div>
                  <h2 className="mt-2 text-xl font-semibold text-white">Latest deliveries</h2>
                  <p className="mt-1 text-sm text-zinc-500">The most recent notifications sent through your private channels.</p>
                </div>
                <Link
                  href="/signals"
                  className="inline-flex items-center gap-1.5 rounded-full border border-white/10 bg-white/[0.03] px-3 py-1 text-xs text-zinc-400 transition hover:bg-white/[0.06] hover:text-white"
                >
                  Open
                  <ArrowRight className="h-3 w-3" />
                </Link>
              </div>

              {accountState.history.length > 0 ? (
                <div className="mt-5 space-y-3">
                  {accountState.history.slice(0, 5).map((item) => (
                    <div key={item.id} className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-4">
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <div className="truncate text-sm font-medium text-white">{item.title || 'Signal delivery'}</div>
                          <div className="mt-1 flex flex-wrap items-center gap-2 text-xs text-zinc-500">
                            <span>{channelLabel(item.channel)}</span>
                            <span className="text-white/10">•</span>
                            <span>{formatDateTimeValue(item.sentAt || item.queuedAt)}</span>
                            {item.ticker || item.actorName ? (
                              <>
                                <span className="text-white/10">•</span>
                                <span>{item.ticker || item.actorName}</span>
                              </>
                            ) : null}
                          </div>
                        </div>
                        <span className={`rounded-full border px-2.5 py-1 text-[11px] ${signalStatusTone(item.status)}`}>
                          {item.status}
                        </span>
                      </div>

                      {item.summary ? <p className="mt-3 text-sm leading-6 text-zinc-400">{item.summary}</p> : null}

                      <div className="mt-3 flex flex-wrap items-center gap-3 text-xs text-zinc-600">
                        {item.destination ? <span>{item.destination}</span> : null}
                        {item.publishedAt ? <span>Filed {formatDateTimeValue(item.publishedAt)}</span> : null}
                        {item.lastError ? <span className="text-red-400">{item.lastError}</span> : null}
                      </div>

                      {item.sourceUrl ? (
                        <a
                          href={item.sourceUrl}
                          target="_blank"
                          rel="noreferrer"
                          className="mt-3 inline-flex items-center gap-1.5 text-xs text-cyan-400 transition hover:text-cyan-300"
                        >
                          Open source filing
                          <ExternalLink className="h-3 w-3" />
                        </a>
                      ) : null}
                    </div>
                  ))}
                </div>
              ) : (
                <div className="mt-5 rounded-2xl border border-dashed border-white/[0.08] bg-white/[0.01] px-4 py-8 text-sm text-zinc-500">
                  No signal deliveries yet. Once your follows fire, the latest activity will appear here.
                </div>
              )}
            </section>
          </div>
        </>
      ) : null}

      <div className="mt-8">
        <button
          type="button"
          onClick={handleSignOut}
          disabled={signingOut}
          className="inline-flex items-center gap-2 rounded-2xl border border-white/[0.06] bg-white/[0.02] px-5 py-3 text-sm font-medium text-zinc-400 transition hover:border-red-500/20 hover:bg-red-500/[0.06] hover:text-red-300 disabled:opacity-50"
        >
          <LogOut className="h-4 w-4" />
          {signingOut ? 'Signing out...' : 'Sign out'}
        </button>
      </div>
    </div>
  );
}
