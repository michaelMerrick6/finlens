'use client';

import Link from 'next/link';
import { type FormEvent, useEffect, useMemo, useState } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import type { Session } from '@supabase/supabase-js';
import {
  AlertTriangle,
  Bell,
  CheckCircle2,
  ChevronRight,
  ExternalLink,
  History,
  LogOut,
  Mail,
  Plus,
  Radio,
  RefreshCw,
  Search,
  ShieldAlert,
  Trash2,
  UserRound,
} from 'lucide-react';

import type { AccountFollowSuggestion, AccountState, AccountTickerSuggestion, ActorType, AlertMode } from '@/lib/account-types';
import { supabase } from '@/lib/supabase';

type AccountApiResponse = {
  ok?: boolean;
  code?: string;
  error?: string;
  state?: AccountState;
  url?: string;
  result?: {
    sentChannels?: string[];
    skippedChannels?: string[];
  };
};

const ALERT_MODE_OPTIONS: AlertMode[] = ['activity', 'unusual', 'both'];
const FOLLOW_SEARCH_DEBOUNCE_MS = 180;

function formatDateTime(value: string | null | undefined) {
  if (!value) {
    return 'N/A';
  }

  return new Intl.DateTimeFormat('en-US', {
    dateStyle: 'medium',
    timeStyle: 'short',
    timeZone: 'America/Los_Angeles',
  }).format(new Date(value));
}

function channelLabel(value: string) {
  if (value === 'email') return 'Email';
  if (value === 'telegram') return 'Telegram';
  return value;
}

function destinationLabel(channel: string, destination: string | null) {
  if (!destination) {
    return 'Not configured';
  }
  if (channel === 'telegram' && /^-?\d+$/.test(destination)) {
    return 'Connected';
  }
  return destination;
}

export function AlertsWorkspace() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [session, setSession] = useState<Session | null>(null);
  const [sessionLoading, setSessionLoading] = useState(true);
  const [accountState, setAccountState] = useState<AccountState | null>(null);
  const [loadingState, setLoadingState] = useState(false);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const [message, setMessage] = useState('');
  const [migrationRequired, setMigrationRequired] = useState(false);
  const [emailInput, setEmailInput] = useState('');
  const [emailEnabled, setEmailEnabled] = useState(true);
  const [telegramUsername, setTelegramUsername] = useState('');
  const [telegramEnabled, setTelegramEnabled] = useState(false);
  const [tickerInput, setTickerInput] = useState('');
  const [tickerMode, setTickerMode] = useState<AlertMode>('unusual');
  const [tickerSuggestions, setTickerSuggestions] = useState<AccountTickerSuggestion[]>([]);
  const [tickerSearchBusy, setTickerSearchBusy] = useState(false);
  const [showTickerSuggestions, setShowTickerSuggestions] = useState(false);
  const [actorType, setActorType] = useState<ActorType>('politician');
  const [actorName, setActorName] = useState('');
  const [actorMode, setActorMode] = useState<AlertMode>('unusual');
  const [actorSuggestions, setActorSuggestions] = useState<AccountFollowSuggestion[]>([]);
  const [actorSearchBusy, setActorSearchBusy] = useState(false);
  const [showActorSuggestions, setShowActorSuggestions] = useState(false);
  const [selectedActorSuggestion, setSelectedActorSuggestion] = useState<AccountFollowSuggestion | null>(null);

  useEffect(() => {
    let mounted = true;

    supabase.auth.getSession().then(({ data }) => {
      if (!mounted) {
        return;
      }
      setSession(data.session);
      setSessionLoading(false);
    });

    const { data } = supabase.auth.onAuthStateChange((_event, nextSession) => {
      setSession(nextSession);
      setSessionLoading(false);
    });

    return () => {
      mounted = false;
      data.subscription.unsubscribe();
    };
  }, []);

  async function authedRequest(path: string, init?: RequestInit) {
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

    const payload = (await response.json()) as AccountApiResponse;
    if (!response.ok || !payload.ok) {
      if (payload.code === 'MIGRATION_REQUIRED') {
        setMigrationRequired(true);
      }
      throw new Error(payload.error || 'Account request failed.');
    }

    return payload;
  }

  async function loadState() {
    if (!session) {
      return;
    }

    setLoadingState(true);
    setError('');
    try {
      const payload = await authedRequest('/api/account/state');
      if (!payload.state) {
        throw new Error('Missing account state.');
      }
      setAccountState(payload.state);
      setMigrationRequired(false);
      setEmailInput(payload.state.profile.alertEmail || payload.state.user.email || '');
      setEmailEnabled(payload.state.profile.emailEnabled);
      setTelegramUsername(payload.state.profile.telegramUsername || '');
      setTelegramEnabled(payload.state.profile.telegramEnabled);
    } catch (value) {
      setError(value instanceof Error ? value.message : 'Failed to load your alert workspace.');
    } finally {
      setLoadingState(false);
    }
  }

  useEffect(() => {
    if (session) {
      loadState();
    } else if (!sessionLoading) {
      setAccountState(null);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [session, sessionLoading]);

  useEffect(() => {
    const billingState = searchParams.get('billing');
    if (!billingState) {
      return;
    }

    if (billingState === 'success') {
      setMessage('Checkout completed. Billing status will refresh as soon as Stripe confirms the subscription.');
    } else if (billingState === 'cancelled') {
      setMessage('Checkout was cancelled. You can return to billing any time.');
    } else if (billingState === 'portal') {
      setMessage('Returned from the billing portal.');
    }
  }, [searchParams]);

  useEffect(() => {
    if (!session) {
      setTickerSuggestions([]);
      setTickerSearchBusy(false);
      setActorSuggestions([]);
      setActorSearchBusy(false);
      return;
    }

    const tickerQuery = tickerInput.trim();
    if (tickerQuery.length < 2) {
      setTickerSuggestions([]);
      setTickerSearchBusy(false);
    } else {
      const tickerTimer = window.setTimeout(async () => {
        setTickerSearchBusy(true);
        try {
          const payload = await authedRequest(`/api/account/ticker-search?query=${encodeURIComponent(tickerQuery)}`);
          setTickerSuggestions((payload as { suggestions?: AccountTickerSuggestion[] }).suggestions || []);
        } catch {
          setTickerSuggestions([]);
        } finally {
          setTickerSearchBusy(false);
        }
      }, FOLLOW_SEARCH_DEBOUNCE_MS);

      return () => {
        window.clearTimeout(tickerTimer);
      };
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tickerInput, session]);

  useEffect(() => {
    if (!session) {
      setActorSuggestions([]);
      setActorSearchBusy(false);
      return;
    }

    const query = actorName.trim();
    if (query.length < 2) {
      setActorSuggestions([]);
      setActorSearchBusy(false);
      return;
    }

    const timer = window.setTimeout(async () => {
      setActorSearchBusy(true);
      try {
        const payload = await authedRequest(`/api/account/follow-search?actorType=${actorType}&query=${encodeURIComponent(query)}`);
        setActorSuggestions((payload as { suggestions?: AccountFollowSuggestion[] }).suggestions || []);
      } catch {
        setActorSuggestions([]);
      } finally {
        setActorSearchBusy(false);
      }
    }, FOLLOW_SEARCH_DEBOUNCE_MS);

    return () => {
      window.clearTimeout(timer);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [actorName, actorType, session]);

  const usageLabel = useMemo(() => {
    if (!accountState) {
      return '0 / 0 follows';
    }
    return `${accountState.followCount} / ${accountState.followLimit} follows`;
  }, [accountState]);

  const hasDeliveryChannel = Boolean(
    accountState && ((accountState.profile.emailEnabled && accountState.subscriptions.email.destination) || (accountState.profile.telegramEnabled && accountState.subscriptions.telegram.destination))
  );
  const hasStockFollow = Boolean(accountState?.follows.tickers.length);
  const hasPersonFollow = Boolean(accountState?.follows.actors.length);
  const hasTestAlert = Boolean(accountState?.history.some((item) => (item.title || '').toLowerCase().includes('vail test alert')));
  const isFreePlan = accountState?.billing.planKey === 'free';
  const remainingFollows = accountState ? Math.max(accountState.followLimit - accountState.followCount, 0) : 0;
  const onboardingSteps = accountState
    ? [
        {
          id: 'delivery',
          label: 'Connect at least one delivery channel',
          done: hasDeliveryChannel,
          detail: 'Turn on Telegram or email so Vail has somewhere to send live signals.',
        },
        {
          id: 'stock',
          label: 'Add your first stock follow',
          done: hasStockFollow,
          detail: 'Track a ticker like NVDA, PLTR, or IONQ.',
        },
        {
          id: 'person',
          label: 'Add your first person follow',
          done: hasPersonFollow,
          detail: 'Follow a politician or insider like Nancy Pelosi or Jensen Huang.',
        },
        {
          id: 'test',
          label: 'Send a test alert',
          done: hasTestAlert,
          detail: 'Confirm your private delivery path before relying on live signals.',
        },
      ]
    : [];

  async function handleEmailSave() {
    setSaving(true);
    setError('');
    setMessage('');
    try {
      const payload = await authedRequest('/api/account/delivery/email', {
        method: 'POST',
        body: JSON.stringify({
          alertEmail: emailInput,
          enabled: emailEnabled,
        }),
      });
      if (payload.state) {
        setAccountState(payload.state);
      }
      setMessage('Email delivery settings saved.');
    } catch (value) {
      setError(value instanceof Error ? value.message : 'Failed to save email delivery settings.');
    } finally {
      setSaving(false);
    }
  }

  async function handleTelegramSave() {
    setSaving(true);
    setError('');
    setMessage('');
    try {
      const payload = await authedRequest('/api/account/delivery/telegram', {
        method: 'POST',
        body: JSON.stringify({
          telegramUsername,
          enabled: telegramEnabled,
        }),
      });
      if (payload.state) {
        setAccountState(payload.state);
      }
      setMessage('Telegram delivery settings saved.');
    } catch (value) {
      setError(value instanceof Error ? value.message : 'Failed to save Telegram delivery settings.');
    } finally {
      setSaving(false);
    }
  }

  async function handleStartCheckout() {
    setSaving(true);
    setError('');
    setMessage('');
    try {
      const payload = await authedRequest('/api/account/billing/checkout', {
        method: 'POST',
      });
      const url = (payload as { url?: string }).url;
      if (!url) {
        throw new Error('Missing Stripe checkout URL.');
      }
      window.location.href = url;
    } catch (value) {
      setError(value instanceof Error ? value.message : 'Failed to open checkout.');
      setSaving(false);
    }
  }

  async function handleManageBilling() {
    setSaving(true);
    setError('');
    setMessage('');
    try {
      const payload = await authedRequest('/api/account/billing/portal', {
        method: 'POST',
      });
      const url = (payload as { url?: string }).url;
      if (!url) {
        throw new Error('Missing billing portal URL.');
      }
      window.location.href = url;
    } catch (value) {
      setError(value instanceof Error ? value.message : 'Failed to open billing portal.');
      setSaving(false);
    }
  }

  async function handleSendTestAlert() {
    setSaving(true);
    setError('');
    setMessage('');
    try {
      const payload = await authedRequest('/api/account/test-alert', {
        method: 'POST',
      });
      if (payload.state) {
        setAccountState(payload.state);
      }
      const sentChannels = payload.result?.sentChannels || [];
      setMessage(
        sentChannels.length
          ? `Test alert sent through ${sentChannels.join(' and ')}.`
          : 'Test alert request completed.'
      );
    } catch (value) {
      setError(value instanceof Error ? value.message : 'Failed to send a test alert.');
    } finally {
      setSaving(false);
    }
  }

  async function handleAddTicker(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSaving(true);
    setError('');
    setMessage('');
    try {
      const payload = await authedRequest('/api/account/follows', {
        method: 'POST',
        body: JSON.stringify({
          kind: 'ticker',
          ticker: tickerInput,
          alertMode: tickerMode,
        }),
      });
      if (payload.state) {
        setAccountState(payload.state);
      }
      setTickerInput('');
      setTickerSuggestions([]);
      setShowTickerSuggestions(false);
      setMessage('Ticker follow saved.');
    } catch (value) {
      setError(value instanceof Error ? value.message : 'Failed to add ticker follow.');
    } finally {
      setSaving(false);
    }
  }

  async function handleAddActor(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSaving(true);
    setError('');
    setMessage('');
    try {
      const payload = await authedRequest('/api/account/follows', {
        method: 'POST',
        body: JSON.stringify({
          kind: 'actor',
          actorType,
          actorName,
          actorKey: selectedActorSuggestion?.actorType === actorType ? selectedActorSuggestion.actorKey : null,
          alertMode: actorMode,
        }),
      });
      if (payload.state) {
        setAccountState(payload.state);
      }
      setActorName('');
      setActorSuggestions([]);
      setShowActorSuggestions(false);
      setSelectedActorSuggestion(null);
      setMessage(`${actorType === 'politician' ? 'Politician' : 'Insider'} follow saved.`);
    } catch (value) {
      setError(value instanceof Error ? value.message : 'Failed to add person follow.');
    } finally {
      setSaving(false);
    }
  }

  async function handleUpdateFollow(kind: 'ticker' | 'actor', id: string, alertMode: AlertMode) {
    setSaving(true);
    setError('');
    setMessage('');
    try {
      const payload = await authedRequest('/api/account/follows', {
        method: 'PATCH',
        body: JSON.stringify({ kind, id, alertMode }),
      });
      if (payload.state) {
        setAccountState(payload.state);
      }
      setMessage('Follow mode updated.');
    } catch (value) {
      setError(value instanceof Error ? value.message : 'Failed to update follow mode.');
    } finally {
      setSaving(false);
    }
  }

  async function handleDeleteFollow(kind: 'ticker' | 'actor', id: string) {
    setSaving(true);
    setError('');
    setMessage('');
    try {
      const payload = await authedRequest('/api/account/follows', {
        method: 'DELETE',
        body: JSON.stringify({ kind, id }),
      });
      if (payload.state) {
        setAccountState(payload.state);
      }
      setMessage('Follow removed.');
    } catch (value) {
      setError(value instanceof Error ? value.message : 'Failed to remove follow.');
    } finally {
      setSaving(false);
    }
  }

  async function signOut() {
    setSaving(true);
    await supabase.auth.signOut();
    setSaving(false);
    router.replace('/auth');
  }

  function applyActorSuggestion(suggestion: AccountFollowSuggestion) {
    setActorType(suggestion.actorType);
    setActorName(suggestion.actorName);
    setActorSuggestions([]);
    setShowActorSuggestions(false);
    setSelectedActorSuggestion(suggestion);
  }

  function applyTickerSuggestion(suggestion: AccountTickerSuggestion) {
    setTickerInput(suggestion.ticker);
    setTickerSuggestions([]);
    setShowTickerSuggestions(false);
  }

  if (sessionLoading) {
    return (
      <div className="glass-panel rounded-3xl p-8 text-sm text-zinc-400">
        Loading your Vail alert workspace...
      </div>
    );
  }

  if (!session) {
    return (
      <div className="mx-auto max-w-3xl space-y-6">
        <div className="glass-panel rounded-3xl p-8">
          <div className="mb-3 inline-flex items-center gap-2 rounded-full border border-blue-500/20 bg-blue-500/10 px-3 py-1 text-xs uppercase tracking-[0.22em] text-blue-300">
            <Bell className="h-3.5 w-3.5" />
            Sign In Required
          </div>
          <h1 className="text-3xl font-semibold text-white">Your private alert workspace lives behind auth.</h1>
          <p className="mt-4 max-w-2xl text-sm leading-6 text-[var(--text-secondary)]">
            Sign in to manage follows, choose activity vs unusual alerts, and connect Telegram or email for private delivery.
          </p>
          <div className="mt-6">
            <Link href="/auth" className="btn-primary text-sm px-4 py-2">
              Go To Auth
            </Link>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-8">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <div className="mb-3 inline-flex items-center gap-2 rounded-full border border-blue-500/20 bg-blue-500/10 px-3 py-1 text-xs uppercase tracking-[0.22em] text-blue-300">
            <Bell className="h-3.5 w-3.5" />
            Alert Workspace
          </div>
          <h1 className="text-3xl font-semibold text-white">Manage follows, delivery, and history.</h1>
          <p className="mt-3 max-w-3xl text-sm leading-6 text-[var(--text-secondary)]">
            This is the first user-facing alert center for Vail. It is built on the same watchlist and delivery pipeline already
            powering internal tests.
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-3">
          <div className="rounded-full border border-white/10 bg-white/5 px-3 py-1.5 text-sm text-zinc-200">{usageLabel}</div>
          <button
            type="button"
            onClick={signOut}
            disabled={saving}
            className="inline-flex items-center gap-2 rounded-xl border border-white/10 bg-white/5 px-4 py-2 text-sm text-zinc-200 transition hover:border-white/20 hover:bg-white/10 disabled:cursor-not-allowed disabled:opacity-60"
          >
            <LogOut className="h-4 w-4" />
            Sign Out
          </button>
        </div>
      </div>

      {message ? (
        <div className="flex items-start gap-3 rounded-2xl border border-emerald-500/20 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-200">
          <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0" />
          <span>{message}</span>
        </div>
      ) : null}

      {error ? (
        <div className="flex items-start gap-3 rounded-2xl border border-red-500/20 bg-red-500/10 px-4 py-3 text-sm text-red-200">
          <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
          <span>{error}</span>
        </div>
      ) : null}

      {migrationRequired ? (
        <div className="rounded-2xl border border-amber-500/20 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
          Apply the latest account migrations in Supabase, including <span className="font-mono">supabase_vail_phase5_user_accounts.sql</span> and <span className="font-mono">supabase_vail_phase6_billing.sql</span>.
        </div>
      ) : null}

      {loadingState && !accountState ? (
        <div className="glass-panel rounded-3xl p-8 text-sm text-zinc-400">Loading account state...</div>
      ) : null}

      {accountState ? (
        <>
          <section className="glass-panel rounded-3xl p-6">
            <div className="flex flex-wrap items-start justify-between gap-4">
              <div>
                <div className="text-sm uppercase tracking-[0.18em] text-zinc-500">Getting Started</div>
                <div className="mt-2 text-2xl font-semibold text-white">Finish the first alert loop.</div>
                <div className="mt-2 text-sm text-zinc-400">
                  Complete these steps to make sure Vail can actually reach you when a follow matches.
                </div>
              </div>

              <div className="rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-right">
                <div className="text-xs uppercase tracking-[0.18em] text-zinc-500">Progress</div>
                <div className="mt-2 text-lg font-semibold text-white">
                  {onboardingSteps.filter((step) => step.done).length} / {onboardingSteps.length}
                </div>
              </div>
            </div>

            <div className="mt-6 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
              {onboardingSteps.map((step) => (
                <div key={step.id} className="rounded-2xl border border-white/10 bg-white/5 p-4">
                  <div className="flex items-start gap-3">
                    <div className={`mt-0.5 ${step.done ? 'text-emerald-300' : 'text-zinc-500'}`}>
                      <CheckCircle2 className="h-5 w-5" />
                    </div>
                    <div>
                      <div className={`text-sm font-medium ${step.done ? 'text-white' : 'text-zinc-200'}`}>{step.label}</div>
                      <div className="mt-1 text-xs leading-5 text-zinc-500">{step.detail}</div>
                    </div>
                  </div>
                </div>
              ))}
            </div>

            <div className="mt-5 flex flex-wrap gap-3">
              <button
                type="button"
                onClick={handleSendTestAlert}
                disabled={saving || !hasDeliveryChannel}
                className="rounded-xl bg-emerald-500 px-4 py-2 text-sm font-medium text-[#04120a] transition hover:bg-emerald-400 disabled:cursor-not-allowed disabled:opacity-60"
              >
                Send test alert
              </button>
              <button
                type="button"
                onClick={loadState}
                disabled={saving || loadingState}
                className="inline-flex items-center gap-2 rounded-xl border border-white/10 bg-white/5 px-4 py-2 text-sm text-zinc-200 transition hover:border-white/20 hover:bg-white/10 disabled:cursor-not-allowed disabled:opacity-60"
              >
                <RefreshCw className="h-4 w-4" />
                Refresh workspace
              </button>
            </div>

            {!hasDeliveryChannel ? (
              <div className="mt-4 rounded-2xl border border-amber-500/20 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
                Connect email or Telegram first, then Vail can send a real test alert.
              </div>
            ) : null}
          </section>

          <section className="grid gap-6 xl:grid-cols-[1.05fr_0.95fr]">
            <div className="glass-panel rounded-3xl p-6">
              <div className="flex items-start justify-between gap-4">
                <div>
                  <div className="text-sm uppercase tracking-[0.18em] text-zinc-500">Account</div>
                  <div className="mt-2 text-2xl font-semibold text-white">{accountState.profile.displayName || accountState.user.email}</div>
                  <div className="mt-2 text-sm text-zinc-400">{accountState.user.email}</div>
                </div>
                <div className="rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-right">
                  <div className="text-xs uppercase tracking-[0.18em] text-zinc-500">Current Plan</div>
                  <div className="mt-2 text-lg font-semibold text-white">{accountState.billing.planName}</div>
                  <div className="mt-1 text-xs text-zinc-400">{accountState.billing.status}</div>
                </div>
              </div>

              <div className="mt-6 grid gap-4 md:grid-cols-3">
                <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                  <div className="text-sm font-medium text-white">Primary watchlist</div>
                  <div className="mt-2 text-sm text-zinc-400">{accountState.watchlist.name}</div>
                </div>
                <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                  <div className="text-sm font-medium text-white">Private delivery</div>
                  <div className="mt-2 text-sm text-zinc-400">
                    Email {accountState.profile.emailEnabled ? 'enabled' : 'off'} • Telegram {accountState.profile.telegramEnabled ? 'enabled' : 'off'}
                  </div>
                </div>
                <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                  <div className="text-sm font-medium text-white">Follow capacity</div>
                  <div className="mt-2 text-sm text-zinc-400">
                    {accountState.followCount} used of {accountState.followLimit}
                  </div>
                  <div className="mt-2 text-xs text-zinc-500">
                    Free plan: {accountState.billing.freeFollowLimit} • Pro plan: {accountState.billing.proFollowLimit}
                  </div>
                </div>
              </div>
            </div>

            <div className="glass-panel rounded-3xl p-6">
              <div className="flex items-start justify-between gap-4">
                <div>
                  <div className="text-sm uppercase tracking-[0.18em] text-zinc-500">Billing</div>
                  <div className="mt-2 text-2xl font-semibold text-white">{accountState.billing.planName}</div>
                </div>
                <ShieldAlert className="mt-1 h-5 w-5 text-amber-300" />
              </div>

              <div className="mt-4 rounded-2xl border border-white/10 bg-white/5 p-4">
                <div className="text-sm text-zinc-300">
                  Status: <span className="font-medium text-white">{accountState.billing.status}</span>
                </div>
                <div className="mt-2 text-sm text-zinc-400">
                  Your current plan supports <span className="font-medium text-white">{accountState.followLimit} follows</span>.
                </div>
                <div className="mt-2 text-xs text-zinc-500">
                  {remainingFollows} follow{remainingFollows === 1 ? '' : 's'} remaining on this plan.
                </div>
                {accountState.billing.currentPeriodEnd ? (
                  <div className="mt-2 text-xs text-zinc-500">
                    Current period ends {formatDateTime(accountState.billing.currentPeriodEnd)}
                    {accountState.billing.cancelAtPeriodEnd ? ' • Cancels at period end' : ''}
                  </div>
                ) : (
                  <div className="mt-2 text-xs text-zinc-500">
                    Upgrade to Vail Pro to unlock {accountState.billing.proFollowLimit} follows and managed billing.
                  </div>
                )}
                {accountState.followCount >= accountState.followLimit ? (
                  <div className="mt-3 rounded-xl border border-amber-500/20 bg-amber-500/10 px-3 py-2 text-xs text-amber-100">
                    You are at your current follow limit. Upgrade or remove a follow to add another signal.
                  </div>
                ) : null}
                {isFreePlan ? (
                  <div className="mt-3 rounded-xl border border-blue-500/20 bg-blue-500/10 px-3 py-2 text-xs text-blue-100">
                    Free gives you {accountState.billing.freeFollowLimit} follows. Pro unlocks {accountState.billing.proFollowLimit} follows and managed billing.
                  </div>
                ) : null}

                <div className="mt-4 flex flex-wrap gap-3">
                  <button
                    type="button"
                    onClick={handleStartCheckout}
                    disabled={saving || !accountState.billing.checkoutReady}
                    className="rounded-xl bg-blue-500 px-4 py-2 text-sm font-medium text-white transition hover:bg-blue-400 disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    {accountState.billing.planKey === 'pro' ? 'Change plan' : 'Upgrade to Vail Pro'}
                  </button>
                  <button
                    type="button"
                    onClick={handleManageBilling}
                    disabled={saving || !accountState.billing.portalReady || !accountState.billing.stripeCustomerId}
                    className="rounded-xl border border-white/10 bg-white/5 px-4 py-2 text-sm text-zinc-200 transition hover:border-white/20 hover:bg-white/10 disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    Manage billing
                  </button>
                  <button
                    type="button"
                    onClick={loadState}
                    disabled={saving || loadingState}
                    className="inline-flex items-center gap-2 rounded-xl border border-white/10 bg-white/5 px-4 py-2 text-sm text-zinc-200 transition hover:border-white/20 hover:bg-white/10 disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    <RefreshCw className="h-4 w-4" />
                    Refresh billing
                  </button>
                </div>

                {!accountState.billing.checkoutReady ? (
                  <div className="mt-3 text-xs text-zinc-500">Billing is not configured yet. Add Stripe env vars to enable checkout.</div>
                ) : null}
              </div>

              <div className="mt-6 text-sm uppercase tracking-[0.18em] text-zinc-500">Delivery Settings</div>
              <div className="mt-2 text-2xl font-semibold text-white">Channels</div>

              <div className="mt-6 space-y-4">
                <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                  <div className="flex items-center justify-between gap-3">
                    <div className="inline-flex items-center gap-2 text-sm font-medium text-white">
                      <Mail className="h-4 w-4 text-blue-300" />
                      Email alerts
                    </div>
                    <label className="inline-flex items-center gap-2 text-sm text-zinc-300">
                      <input
                        type="checkbox"
                        checked={emailEnabled}
                        onChange={(event) => setEmailEnabled(event.target.checked)}
                        className="h-4 w-4 rounded border-white/10 bg-[#0b1020]"
                      />
                      Enabled
                    </label>
                  </div>

                  <input
                    type="email"
                    value={emailInput}
                    onChange={(event) => setEmailInput(event.target.value)}
                    className="mt-4 w-full rounded-2xl border border-white/10 bg-[#0b1020] px-4 py-3 text-sm text-zinc-100 outline-none transition focus:border-blue-400/40"
                    placeholder="you@vail.finance"
                  />

                  <div className="mt-3 text-xs text-zinc-500">
                    Current destination: {destinationLabel('email', accountState.subscriptions.email.destination)}
                  </div>
                  <div className="mt-1 text-xs text-zinc-500">
                    Best for: persistent alert record and account recovery.
                  </div>

                  <button
                    type="button"
                    onClick={handleEmailSave}
                    disabled={saving}
                    className="mt-4 rounded-xl bg-blue-500 px-4 py-2 text-sm font-medium text-white transition hover:bg-blue-400 disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    Save email settings
                  </button>
                </div>

                <div className="rounded-2xl border border-white/10 bg-white/5 p-4">
                  <div className="flex items-center justify-between gap-3">
                    <div className="inline-flex items-center gap-2 text-sm font-medium text-white">
                      <Radio className="h-4 w-4 text-cyan-300" />
                      Telegram alerts
                    </div>
                    <label className="inline-flex items-center gap-2 text-sm text-zinc-300">
                      <input
                        type="checkbox"
                        checked={telegramEnabled}
                        onChange={(event) => setTelegramEnabled(event.target.checked)}
                        className="h-4 w-4 rounded border-white/10 bg-[#0b1020]"
                      />
                      Enabled
                    </label>
                  </div>

                  <div className="mt-4 rounded-2xl border border-cyan-500/20 bg-cyan-500/10 px-4 py-3 text-sm text-cyan-100">
                    <div className="font-medium text-white">Connect Telegram in 3 steps</div>
                    <ol className="mt-2 list-decimal space-y-1 pl-5 text-xs leading-6 text-cyan-100/90">
                      <li>Open the Vail bot and send it any message.</li>
                      <li>Enter your Telegram username below.</li>
                      <li>Save settings and Vail will resolve your chat automatically.</li>
                    </ol>
                    {accountState.telegramBotUsername ? (
                      <a
                        href={`https://t.me/${accountState.telegramBotUsername}`}
                        target="_blank"
                        rel="noreferrer"
                        className="mt-3 inline-flex items-center gap-2 text-sm text-cyan-100 underline-offset-4 transition hover:text-white hover:underline"
                      >
                        Open @{accountState.telegramBotUsername}
                        <ExternalLink className="h-4 w-4" />
                      </a>
                    ) : null}
                  </div>

                  <input
                    type="text"
                    value={telegramUsername}
                    onChange={(event) => setTelegramUsername(event.target.value)}
                    className="mt-4 w-full rounded-2xl border border-white/10 bg-[#0b1020] px-4 py-3 text-sm text-zinc-100 outline-none transition focus:border-blue-400/40"
                    placeholder="@yourtelegramusername"
                  />

                  <div className="mt-3 text-xs leading-6 text-zinc-500">
                    Message{' '}
                    <span className="font-medium text-zinc-300">
                      @{accountState.telegramBotUsername || 'your_vail_bot'}
                    </span>{' '}
                    first, then save this field so Vail can resolve your chat id.
                  </div>

                  <div className="mt-2 text-xs text-zinc-500">
                    Current destination: {destinationLabel('telegram', accountState.subscriptions.telegram.destination)}
                  </div>
                  <div className="mt-1 text-xs text-zinc-500">
                    Status: {accountState.profile.telegramChatId ? 'Connected to bot' : 'Not connected yet'}
                  </div>
                  <div className="mt-1 text-xs text-zinc-500">
                    Best for: fastest private signal delivery.
                  </div>

                  <button
                    type="button"
                    onClick={handleTelegramSave}
                    disabled={saving}
                    className="mt-4 rounded-xl bg-cyan-500 px-4 py-2 text-sm font-medium text-[#03111f] transition hover:bg-cyan-400 disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    Save Telegram settings
                  </button>
                </div>
              </div>
            </div>
          </section>

          <section className="grid gap-6 xl:grid-cols-[1.05fr_0.95fr]">
            <div className="space-y-6">
              <div className="glass-panel rounded-3xl p-6">
                <div className="flex items-center gap-2 text-sm uppercase tracking-[0.18em] text-zinc-500">
                  <Plus className="h-4 w-4" />
                  Add Stock Follow
                </div>
                <form className="mt-5 flex flex-col gap-3 md:flex-row" onSubmit={handleAddTicker}>
                  <div className="relative flex-1">
                    <div className="pointer-events-none absolute inset-y-0 left-4 flex items-center text-zinc-500">
                      <Search className="h-4 w-4" />
                    </div>
                    <input
                      type="text"
                      value={tickerInput}
                      onChange={(event) => {
                        setTickerInput(event.target.value);
                        setShowTickerSuggestions(true);
                      }}
                      onFocus={() => setShowTickerSuggestions(true)}
                      onBlur={() => {
                        window.setTimeout(() => setShowTickerSuggestions(false), 120);
                      }}
                      className="w-full rounded-2xl border border-white/10 bg-[#0b1020] py-3 pl-10 pr-4 text-sm text-zinc-100 outline-none transition focus:border-blue-400/40"
                      placeholder="NVIDIA or NVDA"
                      required
                    />

                    {showTickerSuggestions && (tickerSuggestions.length > 0 || tickerSearchBusy) ? (
                      <div className="absolute z-20 mt-2 w-full rounded-2xl border border-white/10 bg-[#0b1020] p-2 shadow-2xl shadow-black/30">
                        {tickerSearchBusy ? (
                          <div className="px-3 py-2 text-sm text-zinc-400">Searching…</div>
                        ) : (
                          tickerSuggestions.map((suggestion) => (
                            <button
                              key={suggestion.ticker}
                              type="button"
                              onMouseDown={(event) => {
                                event.preventDefault();
                                applyTickerSuggestion(suggestion);
                              }}
                              className="block w-full rounded-xl px-3 py-2 text-left transition hover:bg-white/5"
                            >
                              <div className="text-sm font-medium text-white">{suggestion.ticker}</div>
                              <div className="mt-1 text-xs text-zinc-500">{suggestion.companyName}</div>
                            </button>
                          ))
                        )}
                      </div>
                    ) : null}
                  </div>
                  <select
                    value={tickerMode}
                    onChange={(event) => setTickerMode(event.target.value as AlertMode)}
                    className="rounded-2xl border border-white/10 bg-[#0b1020] px-4 py-3 text-sm text-zinc-100 outline-none transition focus:border-blue-400/40"
                  >
                    {ALERT_MODE_OPTIONS.map((value) => (
                      <option key={value} value={value}>
                        {value}
                      </option>
                    ))}
                  </select>
                  <button
                    type="submit"
                    disabled={saving}
                    className="rounded-2xl bg-blue-500 px-4 py-3 text-sm font-medium text-white transition hover:bg-blue-400 disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    Add ticker
                  </button>
                </form>
                <div className="mt-3 text-xs text-zinc-500">
                  Type a ticker or a company name. Vail will resolve high-confidence matches like <span className="text-zinc-300">NVIDIA → NVDA</span>.
                </div>
                {isFreePlan && accountState.followCount >= accountState.followLimit ? (
                  <div className="mt-4 rounded-2xl border border-amber-500/20 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
                    Free is full. Upgrade to Pro or remove a follow before adding another stock.
                  </div>
                ) : null}
              </div>

              <div className="glass-panel rounded-3xl p-6">
                <div className="flex items-center gap-2 text-sm uppercase tracking-[0.18em] text-zinc-500">
                  <UserRound className="h-4 w-4" />
                  Add Person Follow
                </div>
                <form className="mt-5 space-y-3" onSubmit={handleAddActor}>
                  <div className="grid gap-3 md:grid-cols-[0.9fr_1.3fr_0.9fr]">
                    <select
                      value={actorType}
                      onChange={(event) => {
                        setActorType(event.target.value as ActorType);
                        setSelectedActorSuggestion(null);
                      }}
                      className="rounded-2xl border border-white/10 bg-[#0b1020] px-4 py-3 text-sm text-zinc-100 outline-none transition focus:border-blue-400/40"
                    >
                      <option value="politician">Politician</option>
                      <option value="insider">Insider</option>
                    </select>
                    <div className="relative">
                      <div className="pointer-events-none absolute inset-y-0 left-4 flex items-center text-zinc-500">
                        <Search className="h-4 w-4" />
                      </div>
                      <input
                        type="text"
                        value={actorName}
                        onChange={(event) => {
                          setActorName(event.target.value);
                          setShowActorSuggestions(true);
                          setSelectedActorSuggestion(null);
                        }}
                        onFocus={() => setShowActorSuggestions(true)}
                        onBlur={() => {
                          window.setTimeout(() => setShowActorSuggestions(false), 120);
                        }}
                        className="w-full rounded-2xl border border-white/10 bg-[#0b1020] py-3 pl-10 pr-4 text-sm text-zinc-100 outline-none transition focus:border-blue-400/40"
                        placeholder={actorType === 'politician' ? 'Nancy Pelosi' : 'Jensen Huang'}
                        required
                      />

                      {showActorSuggestions && (actorSuggestions.length > 0 || actorSearchBusy) ? (
                        <div className="absolute z-20 mt-2 w-full rounded-2xl border border-white/10 bg-[#0b1020] p-2 shadow-2xl shadow-black/30">
                          {actorSearchBusy ? (
                            <div className="px-3 py-2 text-sm text-zinc-400">Searching…</div>
                          ) : (
                            actorSuggestions.map((suggestion) => (
                              <button
                                key={`${suggestion.actorType}:${suggestion.actorKey}`}
                                type="button"
                                onMouseDown={(event) => {
                                  event.preventDefault();
                                  applyActorSuggestion(suggestion);
                                }}
                                className="block w-full rounded-xl px-3 py-2 text-left transition hover:bg-white/5"
                              >
                                <div className="text-sm font-medium text-white">{suggestion.actorName}</div>
                                {suggestion.subtitle ? (
                                  <div className="mt-1 text-xs text-zinc-500">{suggestion.subtitle}</div>
                                ) : null}
                              </button>
                            ))
                          )}
                        </div>
                      ) : null}
                    </div>
                    <select
                      value={actorMode}
                      onChange={(event) => setActorMode(event.target.value as AlertMode)}
                      className="rounded-2xl border border-white/10 bg-[#0b1020] px-4 py-3 text-sm text-zinc-100 outline-none transition focus:border-blue-400/40"
                    >
                      {ALERT_MODE_OPTIONS.map((value) => (
                        <option key={value} value={value}>
                          {value}
                        </option>
                      ))}
                    </select>
                  </div>

                  <div className="text-xs text-zinc-500">
                    Politician suggestions come from the live Congress member table. Insider suggestions come from recent Form 4 activity.
                  </div>
                  {isFreePlan && accountState.followCount >= accountState.followLimit ? (
                    <div className="rounded-2xl border border-amber-500/20 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
                      You have reached your free follow limit. Upgrade to Pro or remove a follow to add another person.
                    </div>
                  ) : null}

                  <button
                    type="submit"
                    disabled={saving}
                    className="rounded-2xl bg-white/10 px-4 py-3 text-sm font-medium text-white transition hover:bg-white/15 disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    Add person
                  </button>
                </form>
              </div>

              <div className="glass-panel rounded-3xl p-6">
                <div className="flex items-center gap-2 text-sm uppercase tracking-[0.18em] text-zinc-500">
                  <ShieldAlert className="h-4 w-4" />
                  Current Follows
                </div>

                <div className="mt-5 space-y-6">
                  <div>
                    <div className="mb-3 text-sm font-medium text-white">Stocks</div>
                    <div className="space-y-3">
                      {accountState.follows.tickers.length ? (
                        accountState.follows.tickers.map((follow) => (
                          <div key={follow.id} className="flex flex-col gap-3 rounded-2xl border border-white/10 bg-white/5 p-4 md:flex-row md:items-center md:justify-between">
                            <div>
                              <div className="text-sm font-semibold text-white">{follow.ticker}</div>
                              <div className="mt-1 text-xs text-zinc-500">Added {formatDateTime(follow.createdAt)}</div>
                            </div>

                            <div className="flex flex-wrap items-center gap-3">
                              <select
                                value={follow.alertMode}
                                onChange={(event) => handleUpdateFollow('ticker', follow.id, event.target.value as AlertMode)}
                                className="rounded-xl border border-white/10 bg-[#0b1020] px-3 py-2 text-sm text-zinc-100 outline-none transition focus:border-blue-400/40"
                              >
                                {ALERT_MODE_OPTIONS.map((value) => (
                                  <option key={value} value={value}>
                                    {value}
                                  </option>
                                ))}
                              </select>
                              <button
                                type="button"
                                onClick={() => handleDeleteFollow('ticker', follow.id)}
                                className="inline-flex items-center gap-2 rounded-xl border border-red-500/20 bg-red-500/10 px-3 py-2 text-sm text-red-200 transition hover:border-red-400/30 hover:bg-red-500/15"
                              >
                                <Trash2 className="h-4 w-4" />
                                Remove
                              </button>
                            </div>
                          </div>
                        ))
                      ) : (
                        <div className="rounded-2xl border border-dashed border-white/10 px-4 py-5 text-sm text-zinc-500">
                          No stock follows yet. Add a ticker you care about and Vail will watch Congress and insider activity around it.
                        </div>
                      )}
                    </div>
                  </div>

                  <div>
                    <div className="mb-3 text-sm font-medium text-white">People</div>
                    <div className="space-y-3">
                      {accountState.follows.actors.length ? (
                        accountState.follows.actors.map((follow) => (
                          <div key={follow.id} className="flex flex-col gap-3 rounded-2xl border border-white/10 bg-white/5 p-4 md:flex-row md:items-center md:justify-between">
                            <div>
                              <div className="text-sm font-semibold text-white">{follow.actorName}</div>
                              <div className="mt-1 text-xs text-zinc-500">
                                {follow.actorType} • Added {formatDateTime(follow.createdAt)}
                              </div>
                            </div>

                            <div className="flex flex-wrap items-center gap-3">
                              <select
                                value={follow.alertMode}
                                onChange={(event) => handleUpdateFollow('actor', follow.id, event.target.value as AlertMode)}
                                className="rounded-xl border border-white/10 bg-[#0b1020] px-3 py-2 text-sm text-zinc-100 outline-none transition focus:border-blue-400/40"
                              >
                                {ALERT_MODE_OPTIONS.map((value) => (
                                  <option key={value} value={value}>
                                    {value}
                                  </option>
                                ))}
                              </select>
                              <button
                                type="button"
                                onClick={() => handleDeleteFollow('actor', follow.id)}
                                className="inline-flex items-center gap-2 rounded-xl border border-red-500/20 bg-red-500/10 px-3 py-2 text-sm text-red-200 transition hover:border-red-400/30 hover:bg-red-500/15"
                              >
                                <Trash2 className="h-4 w-4" />
                                Remove
                              </button>
                            </div>
                          </div>
                        ))
                      ) : (
                        <div className="rounded-2xl border border-dashed border-white/10 px-4 py-5 text-sm text-zinc-500">
                          No person follows yet. Follow a politician or insider to collapse their filings into one usable alert.
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              </div>
            </div>

            <div className="glass-panel rounded-3xl p-6">
              <div className="flex items-center gap-2 text-sm uppercase tracking-[0.18em] text-zinc-500">
                <History className="h-4 w-4" />
                Recent Alert History
              </div>

              <div className="mt-5 space-y-4">
                {accountState.history.length ? (
                  accountState.history.map((item) => (
                    <div key={item.id} className="rounded-2xl border border-white/10 bg-white/5 p-4">
                      <div className="flex flex-wrap items-start justify-between gap-3">
                        <div>
                          <div className="text-sm font-semibold text-white">{item.title || 'Alert delivery'}</div>
                          <div className="mt-1 text-xs text-zinc-500">
                            {channelLabel(item.channel)} • {item.status} • {formatDateTime(item.sentAt || item.queuedAt)}
                          </div>
                        </div>

                        <div className="rounded-full border border-white/10 bg-white/5 px-2.5 py-1 text-xs text-zinc-300">
                          {item.ticker || item.actorName || 'Signal'}
                        </div>
                      </div>

                      {item.summary ? <div className="mt-3 text-sm leading-6 text-zinc-300">{item.summary}</div> : null}

                      <div className="mt-3 flex flex-wrap items-center gap-3 text-xs text-zinc-500">
                        <span>Destination: {destinationLabel(item.channel, item.destination)}</span>
                        {item.lastError ? <span className="text-red-300">Error: {item.lastError}</span> : null}
                        {item.publishedAt ? <span>Published {formatDateTime(item.publishedAt)}</span> : null}
                      </div>

                      {item.sourceUrl ? (
                        <a
                          href={item.sourceUrl}
                          target="_blank"
                          rel="noreferrer"
                          className="mt-3 inline-flex items-center gap-2 text-sm text-blue-300 transition hover:text-blue-200"
                        >
                          Open source filing
                          <ChevronRight className="h-4 w-4" />
                        </a>
                      ) : null}
                    </div>
                  ))
                ) : (
                  <div className="rounded-2xl border border-dashed border-white/10 px-4 py-5 text-sm text-zinc-500">
                    No alert deliveries yet. Send yourself a test alert or wait for one of your follows to match a live signal.
                  </div>
                )}
              </div>
            </div>
          </section>
        </>
      ) : null}
    </div>
  );
}
