'use client';

import Image, { type ImageLoaderProps } from 'next/image';
import { useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import type { Session } from '@supabase/supabase-js';
import { ArrowRight, Bell, Check, MessageSquare } from 'lucide-react';

import { getTickerLogoUrl } from '@/lib/company-logos';
import { getPoliticianPhotoUrl } from '@/lib/politician-photos';
import { supabase } from '@/lib/supabase';

const FREE_FEATURES = [
  '3 follows',
  'Email notifications only',
  'Politicians, insiders, and hedge funds',
];

const PRO_FEATURES = [
  '25 follows',
  'Email + text notifications',
  'Cluster alerts',
  'Priority signal processing',
];

const RECENT_POLITICIAN_BUYS = [
  {
    name: 'Ro Khanna',
    memberId: 'K000389',
    ticker: 'KMB',
    amount: '$1,001 - $15,000',
    date: 'Jun 9',
  },
  {
    name: 'Josh Gottheimer',
    memberId: 'G000583',
    ticker: 'AMD',
    amount: '$15,001 - $50,000',
    date: 'Jun 8',
  },
  {
    name: 'Michael McCaul',
    memberId: 'M001157',
    ticker: 'GOOGL',
    amount: '$1,001 - $15,000',
    date: 'Jun 12',
  },
  {
    name: 'Gilbert Cisneros',
    memberId: 'C001123',
    ticker: 'MSFT',
    amount: '$1,001 - $15,000',
    date: 'Jun 8',
  },
  {
    name: 'Tim Moore',
    memberId: 'M001235',
    ticker: 'T',
    amount: '$15,001 - $50,000',
    date: 'Jun 12',
  },
];

const passthroughImageLoader = ({ src }: ImageLoaderProps) => src;

function FeatureList({ features, strong = false }: { features: string[]; strong?: boolean }) {
  return (
    <ul className="mt-7 space-y-3">
      {features.map((feature) => (
        <li key={feature} className="flex items-center gap-3 text-sm text-zinc-300">
          <span
            className={`flex h-5 w-5 shrink-0 items-center justify-center rounded-full border ${
              strong
                ? 'border-emerald-400/25 bg-emerald-400/10 text-emerald-300'
                : 'border-white/10 bg-white/[0.03] text-zinc-400'
            }`}
          >
            <Check className="h-3 w-3" />
          </span>
          {feature}
        </li>
      ))}
    </ul>
  );
}

function TickerLogo({ ticker }: { ticker: string }) {
  const [failed, setFailed] = useState(false);
  const logoUrl = getTickerLogoUrl(ticker, 32);

  if (logoUrl && !failed) {
    return (
      <span className="flex h-8 w-8 shrink-0 items-center justify-center overflow-hidden rounded-full border border-white/10 bg-white">
        <Image
          loader={passthroughImageLoader}
          unoptimized
          src={logoUrl}
          alt={ticker}
          width={32}
          height={32}
          className="h-full w-full object-contain p-0.5"
          onError={() => setFailed(true)}
        />
      </span>
    );
  }

  return (
    <span className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full border border-white/10 bg-white/[0.04] text-[10px] font-bold text-white">
      {ticker.slice(0, 2)}
    </span>
  );
}

function PoliticianPhoto({ memberId, name }: { memberId: string; name: string }) {
  const [failed, setFailed] = useState(false);
  const photoUrl = getPoliticianPhotoUrl(memberId, '225x275', name);

  if (photoUrl && !failed) {
    return (
      <span className="flex h-9 w-9 shrink-0 overflow-hidden rounded-full border border-white/10 bg-white/[0.04]">
        <Image
          loader={passthroughImageLoader}
          unoptimized
          src={photoUrl}
          alt={name}
          width={36}
          height={36}
          className="h-full w-full object-cover"
          onError={() => setFailed(true)}
        />
      </span>
    );
  }

  return (
    <span className="flex h-9 w-9 shrink-0 items-center justify-center rounded-full border border-white/10 bg-white/[0.04] text-xs font-semibold text-white">
      {name
        .split(' ')
        .map((part) => part[0])
        .join('')
        .slice(0, 2)}
    </span>
  );
}

export default function PricingPage() {
  const router = useRouter();
  const [session, setSession] = useState<Session | null>(null);
  const [upgrading, setUpgrading] = useState(false);

  useEffect(() => {
    supabase.auth.getSession().then(({ data }) => {
      setSession(data.session);
    });
    const { data } = supabase.auth.onAuthStateChange((_event, nextSession) => {
      setSession(nextSession);
    });
    return () => data.subscription.unsubscribe();
  }, []);

  async function handleUpgrade() {
    if (!session) {
      router.push('/auth?mode=signup');
      return;
    }

    setUpgrading(true);
    try {
      const response = await fetch('/api/account/billing/checkout', {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${session.access_token}`,
          'Content-Type': 'application/json',
        },
      });

      if (response.ok) {
        const data = await response.json();
        if (data.url) {
          window.location.href = data.url;
          return;
        }
      }

      router.push('/account');
    } catch {
      router.push('/account');
    } finally {
      setUpgrading(false);
    }
  }

  return (
    <div className="mx-auto max-w-5xl px-4 py-14 sm:px-6">
      <div className="text-center">
        <h1 className="text-4xl font-semibold tracking-tight text-white sm:text-5xl">
          For the self directed investor.
        </h1>
        <p className="mx-auto mt-4 max-w-2xl text-sm leading-6 text-zinc-500">
          Start with a few follows. Upgrade when you want more coverage and alerts sent to your phone.
        </p>
      </div>

      <div className="mx-auto mt-12 grid max-w-4xl gap-6 lg:grid-cols-2">
        <div className="glass-panel rounded-3xl p-8">
          <div className="flex items-start justify-between gap-4">
            <div>
              <div className="text-[10px] uppercase tracking-[0.18em] text-zinc-500">Free</div>
              <div className="mt-3 flex items-baseline gap-1">
                <span className="text-5xl font-semibold tracking-tight text-white">$0</span>
                <span className="text-sm text-zinc-500">/month</span>
              </div>
            </div>
            <div className="rounded-2xl border border-white/[0.08] bg-white/[0.03] p-3 text-zinc-400">
              <Bell className="h-4 w-4" />
            </div>
          </div>

          <p className="mt-4 text-sm leading-6 text-zinc-500">
            Good for tracking a small watchlist by email.
          </p>

          <button
            type="button"
            onClick={() => router.push(session ? '/account' : '/auth?mode=signup')}
            className="mt-6 w-full rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3 text-sm font-medium text-white transition hover:bg-white/[0.08]"
          >
            {session ? 'Go to Account' : 'Get Started'}
          </button>

          <FeatureList features={FREE_FEATURES} />
        </div>

        <div className="relative glass-panel rounded-3xl border-emerald-500/20 p-8">
          <div className="absolute -top-3 left-1/2 -translate-x-1/2">
            <span className="rounded-full bg-emerald-500 px-4 py-1 text-xs font-semibold uppercase tracking-wider text-black">
              Best value
            </span>
          </div>

          <div className="flex items-start justify-between gap-4">
            <div>
              <div className="text-[10px] uppercase tracking-[0.18em] text-emerald-400">Vail Pro</div>
              <div className="mt-3 flex items-baseline gap-1">
                <span className="text-5xl font-semibold tracking-tight text-white">$9</span>
                <span className="text-lg text-zinc-400">.99</span>
                <span className="text-sm text-zinc-500">/month</span>
              </div>
            </div>
            <div className="rounded-2xl border border-emerald-300/20 bg-emerald-300/10 p-3 text-emerald-300">
              <MessageSquare className="h-4 w-4" />
            </div>
          </div>

          <p className="mt-4 text-sm leading-6 text-zinc-400">
            More follows, text notifications, and higher-capacity signal tracking.
          </p>

          <button
            type="button"
            onClick={handleUpgrade}
            disabled={upgrading}
            className="mt-6 flex w-full items-center justify-center gap-2 rounded-2xl bg-emerald-500 px-4 py-3 text-sm font-semibold text-black transition hover:bg-emerald-400 disabled:cursor-not-allowed disabled:opacity-60"
          >
            {upgrading ? 'Redirecting...' : 'Upgrade Now'}
            {!upgrading ? <ArrowRight className="h-4 w-4" /> : null}
          </button>

          <FeatureList features={PRO_FEATURES} strong />
        </div>
      </div>

      <section className="mx-auto mt-7 max-w-4xl overflow-hidden rounded-2xl border border-white/[0.06] bg-white/[0.018]">
        <div className="flex items-center justify-between gap-3 border-b border-white/[0.06] px-4 py-3">
          <div>
            <div className="text-sm font-medium text-white">Recent politician buys</div>
            <div className="text-xs text-zinc-600">Public filings, simplified.</div>
          </div>
          <div className="hidden rounded-full border border-white/[0.08] px-3 py-1 text-xs text-zinc-500 sm:block">
            Live feed preview
          </div>
        </div>

        <div className="pricing-buy-strip">
          <div className="pricing-buy-track">
            {[...RECENT_POLITICIAN_BUYS, ...RECENT_POLITICIAN_BUYS].map((trade, index) => (
              <div key={`${trade.name}-${trade.ticker}-${index}`} className="pricing-buy-pill">
                <PoliticianPhoto memberId={trade.memberId} name={trade.name} />
                <div className="min-w-0">
                  <div className="truncate text-sm font-medium text-white">{trade.name}</div>
                  <div className="text-xs text-zinc-500">Buy • {trade.date}</div>
                </div>
                <span className="pricing-buy-divider" aria-hidden="true" />
                <div className="ml-auto flex min-w-[128px] items-center justify-end gap-2">
                  <div className="min-w-0 text-right">
                    <div className="text-sm font-semibold text-white">{trade.ticker}</div>
                    <div className="text-xs text-zinc-500">{trade.amount}</div>
                  </div>
                  <TickerLogo ticker={trade.ticker} />
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>

      <div className="mt-8 text-center">
        <p className="text-sm text-zinc-600">
          Cancel anytime. No hidden fees. Official filings stay at the center of the signal.
        </p>
      </div>
    </div>
  );
}
