'use client';

import { type CSSProperties, type FormEvent, type ReactNode, useEffect, useState } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import Image, { type ImageLoaderProps } from 'next/image';
import { AlertTriangle, KeyRound, UserPlus } from 'lucide-react';

import { supabase } from '@/lib/supabase';
import { getTickerLogoUrl } from '@/lib/company-logos';
import { getPoliticianPhotoUrl } from '@/lib/politician-photos';

type AuthMode = 'signin' | 'signup';
type SpotlightPerson = { name: string; memberId: string; detail: string };
type SpotlightFund = { name: string; imageUrl: string; detail: string };
type AuthStats = {
  congressTradesLastWeek: number;
  insiderTradesLastWeek: number;
  fundFilingsLastWeek: number;
  politiciansTracked: number;
  clusterCount: number;
  latestCluster: {
    ticker: string | null;
    title: string | null;
  } | null;
};

const DEFAULT_AUTH_STATS: AuthStats = {
  congressTradesLastWeek: 0,
  insiderTradesLastWeek: 0,
  fundFilingsLastWeek: 0,
  politiciansTracked: 0,
  clusterCount: 0,
  latestCluster: null,
};

const CONGRESS_SPOTLIGHT: SpotlightPerson[] = [
  { name: 'Nancy Pelosi', memberId: 'P000197', detail: 'House' },
  { name: 'Ro Khanna', memberId: 'K000389', detail: 'CA' },
  { name: 'Tommy Tuberville', memberId: 'T000278', detail: 'Senate' },
];

const STOCK_SPOTLIGHT = [
  { ticker: 'NVDA', name: 'Nvidia' },
  { ticker: 'AMZN', name: 'Amazon' },
  { ticker: 'MSFT', name: 'Microsoft' },
];

const FUND_SPOTLIGHT: SpotlightFund[] = [
  {
    name: 'Leopold Aschenbrenner',
    detail: 'Situational Awareness',
    imageUrl: '/leopold-aschenbrenner.png',
  },
  {
    name: 'Warren Buffett',
    detail: 'Berkshire Hathaway',
    imageUrl: 'https://upload.wikimedia.org/wikipedia/commons/d/df/Warren_Buffett_in_2010_%28cropped%29.jpg',
  },
  {
    name: 'Bill Ackman',
    detail: 'Pershing Square',
    imageUrl: 'https://upload.wikimedia.org/wikipedia/commons/d/d8/Bill_Ackman_%2826410186110%29_%28cropped%29.jpg',
  },
];

const CLUSTER_SPOTLIGHT = [
  { label: 'CON', detail: 'Congress' },
  { label: 'SEC', detail: 'Insiders' },
  { label: '13F', detail: 'Funds' },
];

const passthroughImageLoader = ({ src }: ImageLoaderProps) => src;

function formatCompactNumber(value: number) {
  return new Intl.NumberFormat('en-US', {
    notation: value >= 1000 ? 'compact' : 'standard',
    maximumFractionDigits: 1,
  }).format(value);
}

function AuthStatsSlider({ stats, loaded }: { stats: AuthStats; loaded: boolean }) {
  const latestClusterLabel = stats.latestCluster?.ticker
    ? `${stats.latestCluster.ticker} cluster`
    : stats.latestCluster?.title || 'None';

  const items = [
    {
      value: loaded ? formatCompactNumber(stats.congressTradesLastWeek) : '—',
      label: 'Congress 7d',
    },
    {
      value: loaded ? formatCompactNumber(stats.insiderTradesLastWeek) : '—',
      label: 'Insiders 7d',
    },
    {
      value: loaded ? formatCompactNumber(stats.fundFilingsLastWeek) : '—',
      label: '13Fs 7d',
    },
    {
      value: loaded ? formatCompactNumber(stats.politiciansTracked) : '—',
      label: 'Politicians',
    },
    {
      value: loaded ? formatCompactNumber(stats.clusterCount) : '—',
      label: 'Clusters 7d',
    },
    {
      value: loaded ? latestClusterLabel : 'Checking',
      label: 'Latest',
    },
  ];

  return (
    <div
      className="auth-stats-strip max-w-full overflow-hidden rounded-[18px] border border-white/[0.065] bg-black/20"
      aria-label="Vail activity"
    >
      <div className="auth-stats-track flex w-full gap-2 overflow-x-auto p-2">
        {items.map((item) => (
          <div
            className="auth-stat-pill grid min-w-[132px] flex-1 gap-0.5 rounded-[14px] border border-white/[0.06] bg-white/[0.025] px-3 py-2.5"
            key={item.label}
          >
            <span className="truncate text-sm font-semibold tracking-[-0.01em] text-zinc-100">
              {item.value}
            </span>
            <small className="truncate text-[10px] font-bold uppercase tracking-[0.08em] text-zinc-500">
              {item.label}
            </small>
          </div>
        ))}
      </div>
    </div>
  );
}

function GoogleIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 18 18" fill="none" xmlns="http://www.w3.org/2000/svg">
      <path
        d="M17.64 9.205c0-.639-.057-1.252-.164-1.841H9v3.481h4.844a4.14 4.14 0 0 1-1.796 2.716v2.259h2.908c1.702-1.567 2.684-3.875 2.684-6.615Z"
        fill="#4285F4"
      />
      <path
        d="M9 18c2.43 0 4.467-.806 5.956-2.18l-2.908-2.259c-.806.54-1.837.86-3.048.86-2.344 0-4.328-1.584-5.036-3.711H.957v2.332A8.997 8.997 0 0 0 9 18Z"
        fill="#34A853"
      />
      <path
        d="M3.964 10.71A5.41 5.41 0 0 1 3.682 9c0-.593.102-1.17.282-1.71V4.958H.957A8.997 8.997 0 0 0 0 9c0 1.452.348 2.827.957 4.042l3.007-2.332Z"
        fill="#FBBC05"
      />
      <path
        d="M9 3.58c1.321 0 2.508.454 3.44 1.345l2.582-2.58C13.463.891 11.426 0 9 0A8.997 8.997 0 0 0 .957 4.958L3.964 7.29C4.672 5.163 6.656 3.58 9 3.58Z"
        fill="#EA4335"
      />
    </svg>
  );
}

function AuthContextRow({
  title,
  description,
  label,
  accent,
  children,
}: {
  title: string;
  description: string;
  label: string;
  accent: string;
  children: ReactNode;
}) {
  return (
    <div
      className="auth-context-row grid min-h-[86px] grid-cols-[minmax(0,1fr)_132px] items-center gap-5 rounded-[20px] border border-white/[0.06] bg-black/20 px-[18px] py-4 max-sm:grid-cols-1 max-sm:gap-3.5"
      style={{ '--auth-row-accent': accent } as CSSProperties}
    >
      <div className="auth-context-copy min-w-0">
        <div className="auth-context-label text-[10px] font-semibold uppercase tracking-[0.18em]">{label}</div>
        <div className="mt-1 text-sm font-semibold text-zinc-100">{title}</div>
        <div className="mt-0.5 text-xs leading-5 text-zinc-500">{description}</div>
      </div>
      <div className="auth-context-visual flex shrink-0 justify-end max-sm:justify-start">{children}</div>
    </div>
  );
}

function CongressFaceMini({ person, index }: { person: SpotlightPerson; index: number }) {
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  const resolvedPhotoUrl = getPoliticianPhotoUrl(person.memberId, '225x275', person.name);
  const photoUrl = resolvedPhotoUrl && failedUrl !== resolvedPhotoUrl ? resolvedPhotoUrl : null;

  return (
    <div
      className="auth-face -ml-[9px] h-[38px] w-[38px] overflow-hidden rounded-full border border-white/[0.12] bg-[#111113] ring-2 ring-[#080808] first:ml-0"
      style={{ zIndex: CONGRESS_SPOTLIGHT.length - index }}
      title={`${person.name} · ${person.detail}`}
    >
      {photoUrl ? (
        <Image
          loader={passthroughImageLoader}
          unoptimized
          src={photoUrl}
          alt={person.name}
          width={38}
          height={38}
          className="h-full w-full object-cover"
          onError={() => setFailedUrl(photoUrl)}
        />
      ) : (
        <div className="flex h-full w-full items-center justify-center text-[10px] font-semibold text-zinc-300">
          {person.name.slice(0, 2)}
        </div>
      )}
    </div>
  );
}

function CongressFacesMini() {
  return (
    <div className="auth-face-stack mr-2 flex justify-end max-sm:justify-start">
      {CONGRESS_SPOTLIGHT.map((person, index) => (
        <CongressFaceMini key={person.memberId} person={person} index={index} />
      ))}
    </div>
  );
}

function StockLogoMiniTile({ stock }: { stock: (typeof STOCK_SPOTLIGHT)[number] }) {
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  const resolvedLogoUrl = getTickerLogoUrl(stock.ticker, 96);
  const logoUrl = resolvedLogoUrl && failedUrl !== resolvedLogoUrl ? resolvedLogoUrl : null;

  return (
    <div
      title={`${stock.name} · ${stock.ticker}`}
      className="auth-mini-tile flex h-10 w-10 items-center justify-center rounded-[13px] border border-white/[0.075] bg-white/[0.035]"
    >
      {logoUrl ? (
        <Image
          loader={passthroughImageLoader}
          unoptimized
          src={logoUrl}
          alt={`${stock.name} logo`}
          width={24}
          height={24}
          className="h-6 w-6 rounded-md object-contain"
          onError={() => setFailedUrl(logoUrl)}
        />
      ) : (
        <span className="text-[9px] font-bold tracking-[0.14em] text-zinc-200">{stock.ticker}</span>
      )}
    </div>
  );
}

function StockLogosMini() {
  return (
    <div className="auth-logo-stack flex justify-end gap-2 max-sm:justify-start">
      {STOCK_SPOTLIGHT.map((stock) => (
        <StockLogoMiniTile key={stock.ticker} stock={stock} />
      ))}
    </div>
  );
}

function FundFaceMini({ fund, index }: { fund: SpotlightFund; index: number }) {
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  const initials = fund.name
    .split(/\s+/)
    .map((part) => part.charAt(0))
    .join('')
    .slice(0, 2);

  return (
    <div
      className="auth-face -ml-[9px] h-[38px] w-[38px] overflow-hidden rounded-full border border-white/[0.12] bg-[#111113] ring-2 ring-[#080808] first:ml-0"
      style={{ zIndex: FUND_SPOTLIGHT.length - index }}
      title={`${fund.name} · ${fund.detail}`}
    >
      {failedUrl !== fund.imageUrl ? (
        <Image
          loader={passthroughImageLoader}
          unoptimized
          src={fund.imageUrl}
          alt={fund.name}
          width={38}
          height={38}
          className="h-full w-full object-cover"
          onError={() => setFailedUrl(fund.imageUrl)}
        />
      ) : (
        <div className="flex h-full w-full items-center justify-center text-[10px] font-semibold text-zinc-300">
          {initials || 'HF'}
        </div>
      )}
    </div>
  );
}

function FundFacesMini() {
  return (
    <div className="auth-face-stack mr-2 flex justify-end max-sm:justify-start">
      {FUND_SPOTLIGHT.map((fund, index) => (
        <FundFaceMini key={fund.name} fund={fund} index={index} />
      ))}
    </div>
  );
}

function TextChips({ items }: { items: Array<{ label: string; symbol?: string }> }) {
  return (
    <div className="auth-logo-stack flex justify-end gap-2 max-sm:justify-start">
      {items.map((item) => (
        <div
          key={item.label}
          className="auth-mini-chip inline-flex h-8 min-w-[42px] items-center justify-center gap-1 rounded-full border border-white/[0.075] bg-white/[0.035] px-2.5 text-[10px] font-bold uppercase tracking-[0.08em] text-zinc-300"
        >
          {item.symbol ? <span aria-hidden="true">{item.symbol}</span> : null}
          <span>{item.label}</span>
        </div>
      ))}
    </div>
  );
}

export function AuthPanel() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [mode, setMode] = useState<AuthMode>('signup');
  const [displayName, setDisplayName] = useState('');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [busy, setBusy] = useState(false);
  const [googleBusy, setGoogleBusy] = useState(false);
  const [stats, setStats] = useState<AuthStats>(DEFAULT_AUTH_STATS);
  const [statsLoaded, setStatsLoaded] = useState(false);
  const [message, setMessage] = useState('');
  const [error, setError] = useState('');

  useEffect(() => {
    const requestedMode = searchParams.get('mode');
    if (requestedMode === 'signup') {
      setMode('signup');
      return;
    }
    if (requestedMode === 'signin') {
      setMode('signin');
    }
  }, [searchParams]);

  useEffect(() => {
    let mounted = true;

    supabase.auth.getSession().then(({ data }) => {
      if (mounted) {
        if (data.session) {
          router.replace('/dashboard');
        }
      }
    });

    const { data } = supabase.auth.onAuthStateChange((_event, nextSession) => {
      if (nextSession) {
        router.replace('/dashboard');
      }
    });

    return () => {
      mounted = false;
      data.subscription.unsubscribe();
    };
  }, [router]);

  useEffect(() => {
    const controller = new AbortController();

    fetch('/api/auth-stats', { signal: controller.signal })
      .then((response) => (response.ok ? response.json() : null))
      .then((payload) => {
        if (!controller.signal.aborted && payload?.stats) {
          setStats({ ...DEFAULT_AUTH_STATS, ...payload.stats });
          setStatsLoaded(true);
        }
      })
      .catch(() => {
        if (!controller.signal.aborted) {
          setStatsLoaded(true);
        }
        // Metrics are decorative; auth should never wait on them.
      });

    return () => controller.abort();
  }, []);

  async function handleGoogleSignIn() {
    setGoogleBusy(true);
    setError('');
    try {
      const { error: oauthError } = await supabase.auth.signInWithOAuth({
        provider: 'google',
        options: {
          redirectTo: `${window.location.origin}/auth/callback`,
        },
      });
      if (oauthError) throw oauthError;
    } catch (value) {
      setError(value instanceof Error ? value.message : 'Google sign-in failed.');
      setGoogleBusy(false);
    }
  }

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setBusy(true);
    setError('');
    setMessage('');

    try {
      if (mode === 'signup') {
        if (password !== confirmPassword) {
          throw new Error('Passwords do not match.');
        }

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
          router.replace('/dashboard');
          return;
        }

        setMessage('Account created. If email confirmation is enabled, confirm your email and then sign in.');
      } else {
        const response = await supabase.auth.signInWithPassword({
          email: email.trim(),
          password,
        });

        if (response.error) {
          throw response.error;
        }

        router.replace('/dashboard');
      }
    } catch (value) {
      setError(value instanceof Error ? value.message : 'Authentication failed.');
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="auth-shell flex min-h-[calc(100vh-72px)] items-center justify-center overflow-x-hidden px-4 py-12">
      <div className="min-w-0 w-full max-w-6xl">
        <div className="grid min-w-0 gap-6 lg:grid-cols-[1.08fr_0.92fr]">
          {/* Left — value proposition */}
          <div className="glass-panel auth-card auth-overview-card min-w-0 overflow-hidden rounded-3xl p-8">
            <div className="auth-live-kicker inline-flex items-center gap-2 text-[10px] font-semibold uppercase tracking-[0.2em] text-zinc-400">
              <span className="auth-live-dot h-1.5 w-1.5 rounded-full bg-emerald-300" />
              Live market data
            </div>
            <h1 className="mt-4 text-4xl font-semibold tracking-tight text-white">
              {mode === 'signin' ? 'Welcome back.' : 'Follow the signal. Skip the noise.'}
            </h1>
            <p className="mt-4 max-w-2xl text-sm leading-6 text-zinc-400">
              {mode === 'signin'
                ? 'Pick up where you left off with politician trades, insider filings, hedge fund positions, and quality clusters.'
                : 'Build a focused feed around the people, companies, and funds you care about. Catch meaningful moves before they become obvious.'}
            </p>

            <div className="mt-6">
              <AuthStatsSlider stats={stats} loaded={statsLoaded} />
            </div>

            <div className="mt-6 grid gap-2">
              <AuthContextRow
                label="Congress"
                title="Congress Trades"
                description="Track Pelosi, Khanna, and key House or Senate disclosures."
                accent="#60a5fa"
              >
                <CongressFacesMini />
              </AuthContextRow>

              <AuthContextRow
                label="Insiders"
                title="Insider Filings"
                description="Follow C-suite Form 4 activity around the stocks you care about."
                accent="#fbbf24"
              >
                <StockLogosMini />
              </AuthContextRow>

              <AuthContextRow
                label="Funds"
                title="Hedge Fund 13Fs"
                description="Watch Leopold, Buffett, Ackman, and quarterly 13F changes."
                accent="#34d399"
              >
                <FundFacesMini />
              </AuthContextRow>

              <AuthContextRow
                label="Clusters"
                title="Cross-Source Clusters"
                description="Spot buy pressure when Congress, insiders, and funds align."
                accent="#a78bfa"
              >
                <TextChips items={CLUSTER_SPOTLIGHT} />
              </AuthContextRow>
            </div>

            <div className="mt-5 rounded-2xl border border-white/[0.06] bg-black/20 px-4 py-3 text-xs text-zinc-500">
              <span className="font-medium text-zinc-300">Fast alerts</span> across Congress, SEC Form 4, 13F filings, and quality clusters.
            </div>
          </div>

          {/* Right — auth form */}
          <div className="glass-panel auth-card flex h-full min-w-0 flex-col rounded-3xl p-6 md:p-8 lg:min-h-[760px]">
            <div className="flex items-center justify-between gap-3">
              <div>
                <div className="text-[10px] uppercase tracking-[0.18em] text-zinc-500">Account</div>
                <div className="mt-2 text-2xl font-semibold text-white">{mode === 'signin' ? 'Sign in' : 'Create account'}</div>
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

            {/* Google OAuth */}
            <button
              type="button"
              onClick={handleGoogleSignIn}
              disabled={googleBusy}
              className="mt-6 inline-flex w-full items-center justify-center gap-3 rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3 text-sm font-medium text-white transition hover:border-white/20 hover:bg-white/[0.08] disabled:cursor-not-allowed disabled:opacity-60"
            >
              <GoogleIcon />
              {googleBusy ? 'Redirecting...' : 'Continue with Google'}
            </button>

            <div className="my-6 flex items-center gap-4">
              <div className="h-px flex-1 bg-white/[0.08]" />
              <span className="shrink-0 px-2 text-xs text-zinc-600">or continue with email</span>
              <div className="h-px flex-1 bg-white/[0.08]" />
            </div>

            <form className="space-y-5" onSubmit={handleSubmit}>
              {mode === 'signup' ? (
                <div>
                  <label className="mb-2 block text-sm text-zinc-300">Display name</label>
                  <input
                    type="text"
                    value={displayName}
                    onChange={(event) => setDisplayName(event.target.value)}
                    className="w-full rounded-2xl border border-white/10 bg-[#0b0b0c] px-4 py-3 text-sm text-zinc-100 outline-none transition focus:border-white/20"
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
                  className="w-full rounded-2xl border border-white/10 bg-[#0b0b0c] px-4 py-3 text-sm text-zinc-100 outline-none transition focus:border-white/20"
                  placeholder="you@example.com"
                  required
                />
              </div>

              <div>
                <label className="mb-2 block text-sm text-zinc-300">Password</label>
                <input
                  type="password"
                  value={password}
                  onChange={(event) => setPassword(event.target.value)}
                  className="w-full rounded-2xl border border-white/10 bg-[#0b0b0c] px-4 py-3 text-sm text-zinc-100 outline-none transition focus:border-white/20"
                  placeholder="••••••••"
                  minLength={8}
                  required
                />
                {mode === 'signup' ? <div className="mt-2 text-[11px] text-zinc-600">Use at least 8 characters.</div> : null}
              </div>

              {mode === 'signup' ? (
                <div>
                  <label className="mb-2 block text-sm text-zinc-300">Confirm password</label>
                  <input
                    type="password"
                    value={confirmPassword}
                    onChange={(event) => setConfirmPassword(event.target.value)}
                    className="w-full rounded-2xl border border-white/10 bg-[#0b0b0c] px-4 py-3 text-sm text-zinc-100 outline-none transition focus:border-white/20"
                    placeholder="••••••••"
                    minLength={8}
                    required
                  />
                </div>
              ) : null}

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
                className={`inline-flex w-full items-center justify-center gap-2 rounded-2xl px-4 py-3 text-sm font-semibold transition disabled:cursor-not-allowed disabled:opacity-60 ${
                  mode === 'signup'
                    ? 'border border-emerald-300/25 bg-emerald-300 text-[#06110c] hover:bg-emerald-200'
                    : 'bg-zinc-100 text-black hover:bg-white'
                }`}
              >
                {mode === 'signin' ? <KeyRound className="h-4 w-4" /> : <UserPlus className="h-4 w-4" />}
                {busy ? 'Working...' : mode === 'signin' ? 'Sign in' : 'Create account'}
              </button>
            </form>

            <div className="mt-auto pt-8 text-center text-xs text-zinc-600">
              <div>By continuing you agree to Vail&apos;s Terms of Service.</div>
              <button
                type="button"
                onClick={() => {
                  setMode(mode === 'signin' ? 'signup' : 'signin');
                  setError('');
                  setMessage('');
                }}
                className="mt-5 text-zinc-400 transition hover:text-zinc-200"
              >
                {mode === 'signin' ? 'New to Vail? Create an account' : 'Already have an account? Sign in'}
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
