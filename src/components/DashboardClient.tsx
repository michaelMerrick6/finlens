'use client';

import Image, { type ImageLoaderProps } from 'next/image';
import { lazy, Suspense, useCallback, useEffect, useMemo, useRef, useState, useTransition } from 'react';
import type { Session } from '@supabase/supabase-js';
import { useRouter } from 'next/navigation';

import PoliticianHeadshot from '@/components/PoliticianHeadshot';
import StockSearchBar from '@/components/StockSearchBar';
import type { SearchResult } from '@/components/StockSearchBar';
import { getTickerLogoUrl } from '@/lib/company-logos';
import type { DashboardPoliticianWorkspaceData } from '@/lib/politician-workspace-types';
import type { DashboardTickerWorkspaceData } from '@/lib/ticker-workspace-types';
import { addRecentTicker } from '@/lib/recent-tickers';
import { supabase } from '@/lib/supabase';

const DashboardPoliticianWorkspace = lazy(() => import('@/components/DashboardPoliticianWorkspace'));
const DashboardTickerWorkspace = lazy(() => import('@/components/DashboardTickerWorkspace'));
const CreateSignalModal = lazy(() => import('@/components/CreateSignalModal').then(m => ({ default: m.CreateSignalModal })));
const WelcomeOnboarding = lazy(() => import('@/components/WelcomeOnboarding'));

/* ------------------------------------------------------------------ */
/*  Types                                                              */
/* ------------------------------------------------------------------ */

type WorkspaceSelection =
  | { type: 'ticker'; ticker: string }
  | { type: 'politician'; id: string; name: string };

function workspaceIdentity(selection: WorkspaceSelection | null) {
  if (!selection) return 'none';
  return selection.type === 'ticker'
    ? `ticker:${selection.ticker}`
    : `politician:${selection.id}`;
}

const WORKSPACE_DISMISS_MS = 240;

/* ------------------------------------------------------------------ */
/*  Curated popular searches                                           */
/* ------------------------------------------------------------------ */

type PopularEntry =
  | { kind: 'politician'; memberId: string; label: string; party: string; chamber: string; subtitle: string; returnPct: number }
  | { kind: 'company'; ticker: string; label: string; subtitle: string; returnPct: number }
  | { kind: 'fund'; label: string; subtitle: string; imageUrl: string; href: string; returnPct: number };

const PARTY_DOT: Record<string, string> = {
  Democrat: '#3b82f6',
  Republican: '#ef4444',
  Independent: '#a855f7',
};

const passthroughImageLoader = ({ src }: ImageLoaderProps) => src;

const POPULAR_SEARCHES: PopularEntry[] = [
  // Row 1: politicians
  { kind: 'politician', memberId: 'P000197', label: 'Nancy Pelosi', party: 'Democrat', chamber: 'House', subtitle: 'Dem · House', returnPct: 41.5 },
  { kind: 'politician', memberId: 'T000278', label: 'Tommy Tuberville', party: 'Republican', chamber: 'Senate', subtitle: 'Rep · Senate', returnPct: 15.6 },
  { kind: 'politician', memberId: 'K000389', label: 'Ro Khanna', party: 'Democrat', chamber: 'House', subtitle: 'Dem · House', returnPct: 112.1 },
  // Row 2: more politicians
  { kind: 'politician', memberId: 'G000583', label: 'Josh Gottheimer', party: 'Democrat', chamber: 'House', subtitle: 'Dem · House', returnPct: 3.8 },
  { kind: 'politician', memberId: 'M001157', label: 'Michael McCaul', party: 'Republican', chamber: 'House', subtitle: 'Rep · House', returnPct: 22.4 },
  // Row 3: companies + fund
  { kind: 'company', ticker: 'NVDA', label: 'Nvidia', subtitle: 'NVDA', returnPct: 38.9 },
  { kind: 'company', ticker: 'IONQ', label: 'IonQ', subtitle: 'IONQ', returnPct: 42.7 },
  { kind: 'fund', label: 'Situational Awareness LP', subtitle: 'Leopold Aschenbrenner', imageUrl: '/leopold-aschenbrenner.png', href: '/hedge-funds/Situational%20Awareness%20LP', returnPct: 47.0 },
];

/* ------------------------------------------------------------------ */
/*  Ticker logo for popular search pills                               */
/* ------------------------------------------------------------------ */

function TickerPill({ ticker }: { ticker: string }) {
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  const logoUrl = getTickerLogoUrl(ticker, 44);
  const activeLogoUrl = logoUrl && failedUrl !== logoUrl ? logoUrl : null;
  const hue = ticker.split('').reduce((acc, ch) => acc + ch.charCodeAt(0), 0) % 360;

  if (activeLogoUrl) {
    return (
      <div className="h-[26px] w-[26px] shrink-0 overflow-hidden rounded-lg border border-white/10 bg-black/40">
        <Image loader={passthroughImageLoader} unoptimized src={activeLogoUrl} alt={ticker} width={26} height={26} sizes="26px" className="h-full w-full object-contain p-0.5" onError={() => setFailedUrl(activeLogoUrl)} />
      </div>
    );
  }

  return (
    <div
      className="flex h-[26px] w-[26px] shrink-0 items-center justify-center rounded-lg border border-white/10 text-[10px] font-bold text-white"
      style={{ background: `linear-gradient(135deg, hsl(${hue},65%,45%), hsl(${(hue + 42) % 360},65%,34%))` }}
    >
      {ticker.slice(0, 2)}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Home view                                                          */
/* ------------------------------------------------------------------ */

function DashboardHome({
  workspaceSelection,
  workspaceClosing,
  workspaceReveal,
  signalMessage,
  tickerData,
  tickerLoading,
  tickerError,
  politicianData,
  politicianLoading,
  politicianError,
  onSelectResult,
  onRetryTicker,
  onRetryPolitician,
  onDismissWorkspace,
  onOpenSignalAction,
  onOpenFund,
}: {
  workspaceSelection: WorkspaceSelection | null;
  workspaceClosing: boolean;
  workspaceReveal: boolean;
  signalMessage: string;
  tickerData: DashboardTickerWorkspaceData | null;
  tickerLoading: boolean;
  tickerError: string;
  politicianData: DashboardPoliticianWorkspaceData | null;
  politicianLoading: boolean;
  politicianError: string;
  onSelectResult: (result: SearchResult) => void;
  onRetryTicker: () => void;
  onRetryPolitician: () => void;
  onDismissWorkspace: () => void;
  onOpenSignalAction: () => void;
  onOpenFund: (href: string) => void;
}) {
  const workspaceActive = Boolean(workspaceSelection);

  return (
    <div className={`mx-auto w-full px-4 sm:px-6 ${workspaceActive ? 'max-w-[1320px]' : 'max-w-[640px]'}`}>
      {!workspaceActive ? (
        <div className="py-20 sm:py-28">
          {/* ── Hero ───────────────────────────────────── */}
          <div className="dash-fade-in relative z-20 text-center">
            <div className="relative">
              <h1 className="text-[22px] font-semibold tracking-tight text-white sm:text-[26px] leading-snug">
                Follow any Congress member, company,{' '}
                <span className="text-zinc-500">or hedge&nbsp;fund</span>
              </h1>
              <p className="mt-2 text-[13px] text-zinc-500">
                Get alerted the moment any politician, insider, or institution makes a move.
              </p>
            </div>

            <div className="relative z-[200] mt-5">
              <StockSearchBar
                onSelect={onSelectResult}
                compact
                placeholder="Search politicians, tickers, or companies…"
              />
            </div>

            {signalMessage ? (
              <div className="mt-3 rounded-lg border border-emerald-500/20 bg-emerald-500/5 px-3 py-2 text-xs text-emerald-400">
                {signalMessage}
              </div>
            ) : null}
          </div>

          {/* ── Popular searches ────────────────────────── */}
          <div
            className="dash-fade-in relative z-0 mt-6"
            style={{ animationDelay: '80ms' }}
          >
            <div className="text-[10px] font-semibold uppercase tracking-[0.15em] text-zinc-600 pl-1">
              Popular
            </div>
            <div className="mt-2.5 grid grid-cols-2 gap-2">
              {POPULAR_SEARCHES.map((entry, i) => {
                const returnBadge = (
                  <span
                    className={`ml-auto shrink-0 rounded-md px-1.5 py-0.5 text-[10px] font-semibold tabular-nums ${
                      entry.returnPct >= 0
                        ? 'bg-emerald-500/10 text-emerald-400'
                        : 'bg-red-500/10 text-red-400'
                    }`}
                  >
                    {entry.returnPct >= 0 ? '+' : ''}{entry.returnPct.toFixed(1)}%
                  </span>
                );

                if (entry.kind === 'politician') {
                  return (
                    <button
                      key={entry.memberId}
                      type="button"
                      onClick={() => onSelectResult({
                        type: 'politician',
                        id: entry.memberId,
                        fullName: entry.label,
                        party: entry.party,
                        chamber: entry.chamber.toLowerCase(),
                        state: '',
                        score: 0,
                        exactMatch: true,
                        strongMatch: true,
                      })}
                      className="pop-pill group"
                      style={{ animationDelay: `${120 + i * 35}ms` }}
                    >
                      <PoliticianHeadshot memberId={entry.memberId} name={entry.label} party={entry.party} size={26} />
                      <span className="text-[12px] font-medium text-zinc-200 group-hover:text-white">{entry.label}</span>
                      <span className="text-[10px] text-zinc-600">
                        <span className="mr-0.5 inline-block h-1.5 w-1.5 rounded-full align-middle" style={{ backgroundColor: PARTY_DOT[entry.party] || '#6b7280' }} />
                        {entry.subtitle}
                      </span>
                      {returnBadge}
                    </button>
                  );
                }

                if (entry.kind === 'company') {
                  return (
                    <button
                      key={entry.ticker}
                      type="button"
                      onClick={() => onSelectResult({
                        type: 'company',
                        id: entry.ticker,
                        ticker: entry.ticker,
                        name: entry.label,
                        logoUrl: getTickerLogoUrl(entry.ticker, 44),
                        score: 0,
                        exactMatch: true,
                        strongMatch: true,
                      })}
                      className="pop-pill group"
                      style={{ animationDelay: `${120 + i * 35}ms` }}
                    >
                      <TickerPill ticker={entry.ticker} />
                      <span className="text-[12px] font-medium text-zinc-200 group-hover:text-white">{entry.label}</span>
                      <span className="text-[10px] font-semibold tracking-wider text-zinc-600">{entry.subtitle}</span>
                      {returnBadge}
                    </button>
                  );
                }

                /* fund */
                return (
                  <button
                    key={entry.label}
                    type="button"
                    onClick={() => onOpenFund(entry.href)}
                    className="pop-pill group"
                    style={{ animationDelay: `${120 + i * 35}ms` }}
                  >
                    <div className="h-[26px] w-[26px] shrink-0 overflow-hidden rounded-full border border-white/10 bg-black/40">
                      <Image loader={passthroughImageLoader} unoptimized src={entry.imageUrl} alt={entry.label} width={26} height={26} sizes="26px" className="h-full w-full object-cover" />
                    </div>
                    <span className="text-[12px] font-medium text-zinc-200 group-hover:text-white">{entry.label}</span>
                    <span className="text-[10px] text-zinc-600">{entry.subtitle}</span>
                    {returnBadge}
                  </button>
                );
              })}
            </div>
            <div className="mt-2 text-center text-[9px] text-zinc-700">
              1Y returns · Estimates based on public data
            </div>
          </div>
        </div>
      ) : (
        /* ── Workspace mode ────────────────────────── */
        <div className={`${workspaceClosing ? 'workspace-exit' : 'dash-fade-in'} py-6`}>
          <div className="relative z-[200]">
            <StockSearchBar
              onSelect={onSelectResult}
              compact
              placeholder="Search another politician or stock…"
            />
          </div>
          {signalMessage ? (
            <div className="mt-3 rounded-lg border border-emerald-500/20 bg-emerald-500/5 px-3 py-2 text-xs text-emerald-400">
              {signalMessage}
            </div>
          ) : null}

          <div
            className={`mt-6 transform-gpu transition-all duration-300 ease-[cubic-bezier(0.22,1,0.36,1)] ${
              workspaceReveal
                ? 'translate-y-0 scale-100 opacity-100 blur-0'
                : 'pointer-events-none -translate-y-1 opacity-0 blur-[1.5px]'
            }`}
          >
            {workspaceSelection?.type === 'ticker' ? (
              <Suspense fallback={null}>
                <DashboardTickerWorkspace
                  data={tickerData}
                  requestedTicker={workspaceSelection.ticker}
                  loading={tickerLoading}
                  error={tickerError}
                  onRetry={onRetryTicker}
                  onDismiss={onDismissWorkspace}
                  onOpenPriceAlert={onOpenSignalAction}
                />
              </Suspense>
            ) : workspaceSelection ? (
              <Suspense fallback={null}>
                <DashboardPoliticianWorkspace
                  data={politicianData}
                  requestedMemberId={workspaceSelection.id}
                  requestedMemberName={workspaceSelection.name}
                  loading={politicianLoading}
                  error={politicianError}
                  onRetry={onRetryPolitician}
                  onDismiss={onDismissWorkspace}
                  onOpenSignals={onOpenSignalAction}
                />
              </Suspense>
            ) : null}
          </div>
        </div>
      )}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Main export                                                        */
/* ------------------------------------------------------------------ */

export default function DashboardClient() {
  const router = useRouter();
  const [session, setSession] = useState<Session | null>(null);
  const [showCreateSignal, setShowCreateSignal] = useState(false);
  const [signalMessage, setSignalMessage] = useState('');
  const [workspaceSelection, setWorkspaceSelection] = useState<WorkspaceSelection | null>(null);
  const [workspaceReveal, setWorkspaceReveal] = useState(false);
  const [workspaceClosing, setWorkspaceClosing] = useState(false);
  const [tickerData, setTickerData] = useState<DashboardTickerWorkspaceData | null>(null);
  const [tickerError, setTickerError] = useState('');
  const [tickerLoading, setTickerLoading] = useState(false);
  const [tickerWorkspaceCache, setTickerWorkspaceCache] = useState<Record<string, DashboardTickerWorkspaceData>>({});
  const [tickerReloadToken, setTickerReloadToken] = useState(0);
  const [politicianData, setPoliticianData] = useState<DashboardPoliticianWorkspaceData | null>(null);
  const [politicianError, setPoliticianError] = useState('');
  const [politicianLoading, setPoliticianLoading] = useState(false);
  const [politicianProfileCache, setPoliticianProfileCache] = useState<Record<string, DashboardPoliticianWorkspaceData>>({});
  const [politicianReloadToken, setPoliticianReloadToken] = useState(0);
  const [hasHydratedWorkspace, setHasHydratedWorkspace] = useState(false);
  const [isSelectingWorkspace, startWorkspaceTransition] = useTransition();
  const dismissTimerRef = useRef<number | null>(null);

  const workspaceKey = workspaceIdentity(workspaceSelection);
  const activeTicker = workspaceSelection?.type === 'ticker' ? workspaceSelection.ticker : null;
  const activePolitician = workspaceSelection?.type === 'politician' ? workspaceSelection : null;

  /* ── Session ────────────────────────────────── */

  useEffect(() => {
    let mounted = true;
    supabase.auth.getSession().then(({ data }) => {
      if (!mounted) return;
      setSession(data.session);
    });
    const { data } = supabase.auth.onAuthStateChange((_event, nextSession) => {
      setSession(nextSession);
    });
    return () => {
      mounted = false;
      data.subscription.unsubscribe();
    };
  }, []);

  /* ── Hydrate workspace from URL ─────────────── */

  useEffect(() => {
    if (typeof window === 'undefined') return;
    const url = new URL(window.location.href);
    const initialTicker = url.searchParams.get('ticker');
    const initialMemberId = url.searchParams.get('memberId');
    const initialMemberName = url.searchParams.get('memberName');
    if (initialTicker) {
      setWorkspaceSelection({ type: 'ticker', ticker: initialTicker.trim().toUpperCase() });
    } else if (initialMemberId) {
      setWorkspaceSelection({
        type: 'politician',
        id: initialMemberId.trim(),
        name: (initialMemberName || initialMemberId).trim(),
      });
    }
    setHasHydratedWorkspace(true);
  }, []);

  /* ── Sync workspace → URL ───────────────────── */

  useEffect(() => {
    if (!hasHydratedWorkspace || typeof window === 'undefined') return;
    const url = new URL(window.location.href);
    if (workspaceSelection?.type === 'ticker') {
      url.searchParams.set('ticker', workspaceSelection.ticker);
      url.searchParams.delete('memberId');
      url.searchParams.delete('memberName');
    } else if (workspaceSelection?.type === 'politician') {
      url.searchParams.set('memberId', workspaceSelection.id);
      url.searchParams.set('memberName', workspaceSelection.name);
      url.searchParams.delete('ticker');
    } else {
      url.searchParams.delete('ticker');
      url.searchParams.delete('memberId');
      url.searchParams.delete('memberName');
    }
    window.history.replaceState({}, '', `${url.pathname}${url.search}${url.hash}`);
    window.dispatchEvent(new Event('vail-dashboard-location-change'));
  }, [hasHydratedWorkspace, workspaceSelection]);

  /* ── Reveal animation ───────────────────────── */

  useEffect(() => {
    if (!workspaceSelection) return;
    if (typeof window !== 'undefined') {
      window.scrollTo({ top: 0, behavior: 'smooth' });
    }
    const timer = window.setTimeout(() => setWorkspaceReveal(true), 40);
    return () => window.clearTimeout(timer);
  }, [workspaceKey, workspaceSelection]);

  /* ── Ticker data loader ─────────────────────── */

  useEffect(() => {
    if (!activeTicker) return;
    const controller = new AbortController();
    let cancelled = false;

    const loadTicker = async () => {
      const cached = tickerWorkspaceCache[activeTicker];
      if (cached) {
        setTickerData(cached);
        setTickerError('');
        setTickerLoading(false);
        return;
      }

      setTickerLoading(true);
      setTickerError('');
      setTickerData(null);
      try {
        const response = await fetch(`/api/ticker-workspace/${encodeURIComponent(activeTicker)}/lite?limit=10`, {
          signal: controller.signal,
          cache: 'no-store',
        });
        if (!response.ok) {
          const payload = (await response.json().catch(() => null)) as { error?: string } | null;
          throw new Error(payload?.error || 'Could not load this stock right now.');
        }
        const payload = (await response.json()) as DashboardTickerWorkspaceData;
        if (cancelled) return;
        setTickerData(payload);
        setTickerWorkspaceCache((cache) => ({ ...cache, [activeTicker]: payload }));
      } catch (error) {
        if (cancelled || (error instanceof Error && error.name === 'AbortError')) return;
        setTickerError(error instanceof Error ? error.message : 'Could not load this stock right now.');
      } finally {
        if (!cancelled) setTickerLoading(false);
      }
    };

    void loadTicker();
    return () => { cancelled = true; controller.abort(); };
  }, [activeTicker, tickerReloadToken, tickerWorkspaceCache]);

  /* ── Politician data loader ─────────────────── */

  useEffect(() => {
    if (!activePolitician) return;
    const controller = new AbortController();
    let cancelled = false;

    const loadPolitician = async () => {
      const cached = politicianProfileCache[activePolitician.id];
      if (cached) {
        setPoliticianData(cached);
        setPoliticianError('');
        setPoliticianLoading(false);
        return;
      }
      setPoliticianLoading(true);
      setPoliticianError('');
      setPoliticianData(null);
      try {
        const response = await fetch(`/api/politician-workspace/${encodeURIComponent(activePolitician.id)}?limit=8`, {
          signal: controller.signal,
        });
        if (!response.ok) {
          const payload = (await response.json().catch(() => null)) as { error?: string } | null;
          throw new Error(payload?.error || 'Could not load this politician right now.');
        }
        const payload = (await response.json()) as DashboardPoliticianWorkspaceData;
        if (cancelled) return;
        setPoliticianData(payload);
        setPoliticianProfileCache((c) => ({ ...c, [activePolitician.id]: payload }));
      } catch (error) {
        if (cancelled || (error instanceof Error && error.name === 'AbortError')) return;
        setPoliticianError(error instanceof Error ? error.message : 'Could not load this politician right now.');
      } finally {
        if (!cancelled) setPoliticianLoading(false);
      }
    };

    void loadPolitician();
    return () => { cancelled = true; controller.abort(); };
  }, [activePolitician, politicianProfileCache, politicianReloadToken]);

  /* ── Auto-clear signal message ──────────────── */

  useEffect(() => {
    if (!signalMessage) return;
    const timer = setTimeout(() => setSignalMessage(''), 4000);
    return () => clearTimeout(timer);
  }, [signalMessage]);

  /* ── Workspace actions ──────────────────────── */

  const clearPendingDismiss = useCallback(() => {
    if (dismissTimerRef.current) {
      window.clearTimeout(dismissTimerRef.current);
      dismissTimerRef.current = null;
    }
  }, []);

  useEffect(() => {
    return () => clearPendingDismiss();
  }, [clearPendingDismiss]);

  const openTickerWorkspace = useCallback(
    (ticker: string) => {
      const t = ticker.trim().toUpperCase();
      if (!t) return;
      clearPendingDismiss();
      setWorkspaceClosing(false);
      addRecentTicker(t);
      if (workspaceSelection?.type === 'ticker' && workspaceSelection.ticker === t) {
        setTickerWorkspaceCache((cache) => { const next = { ...cache }; delete next[t]; return next; });
        setTickerReloadToken((v) => v + 1);
        return;
      }
      setWorkspaceReveal(false);
      startWorkspaceTransition(() => {
        setWorkspaceSelection({ type: 'ticker', ticker: t });
        setTickerData(null);
        setTickerError('');
        setPoliticianData(null);
        setPoliticianError('');
      });
    },
    [clearPendingDismiss, workspaceSelection],
  );

  const openPoliticianWorkspace = useCallback(
    (id: string, name: string) => {
      const nid = id.trim();
      if (!nid) return;
      clearPendingDismiss();
      setWorkspaceClosing(false);
      if (workspaceSelection?.type === 'politician' && workspaceSelection.id === nid) {
        setPoliticianProfileCache((c) => { const n = { ...c }; delete n[nid]; return n; });
        setPoliticianReloadToken((v) => v + 1);
        return;
      }
      setWorkspaceReveal(false);
      startWorkspaceTransition(() => {
        setWorkspaceSelection({ type: 'politician', id: nid, name: name.trim() || nid });
        setPoliticianData(null);
        setPoliticianError('');
        setTickerData(null);
        setTickerError('');
      });
    },
    [clearPendingDismiss, workspaceSelection],
  );

  const handleSelectResult = useCallback(
    (result: SearchResult) => {
      if (result.type === 'company') {
        openTickerWorkspace(result.ticker);
        return;
      }
      openPoliticianWorkspace(result.id, result.fullName);
    },
    [openPoliticianWorkspace, openTickerWorkspace],
  );

  const handleOpenSignalAction = useCallback(() => {
    if (!session) {
      router.push('/auth');
      return;
    }
    setShowCreateSignal(true);
  }, [router, session]);

  const handleOpenFund = useCallback((href: string) => {
    router.push(href);
  }, [router]);

  const handleRetryTicker = useCallback(() => {
    if (!activeTicker) return;
    setTickerWorkspaceCache((cache) => { const next = { ...cache }; delete next[activeTicker]; return next; });
    setTickerReloadToken((v) => v + 1);
  }, [activeTicker]);

  const handleRetryPolitician = useCallback(() => {
    if (!activePolitician) return;
    setPoliticianProfileCache((c) => { const n = { ...c }; delete n[activePolitician.id]; return n; });
    setPoliticianReloadToken((v) => v + 1);
  }, [activePolitician]);

  const handleDismissWorkspace = useCallback(() => {
    if (!workspaceSelection) return;
    clearPendingDismiss();
    setWorkspaceClosing(true);
    setWorkspaceReveal(false);
    dismissTimerRef.current = window.setTimeout(() => {
      dismissTimerRef.current = null;
      if (typeof window !== 'undefined') {
        window.scrollTo({ top: 0, behavior: 'auto' });
      }
      startWorkspaceTransition(() => {
        setWorkspaceSelection(null);
        setTickerData(null);
        setTickerError('');
        setTickerLoading(false);
        setPoliticianData(null);
        setPoliticianError('');
        setPoliticianLoading(false);
      });
      setWorkspaceClosing(false);
    }, WORKSPACE_DISMISS_MS);
  }, [clearPendingDismiss, workspaceSelection]);

  /* ── Signal modal helpers ───────────────────── */

  const initialSignalKind = workspaceSelection?.type === 'politician' ? 'politician' : 'ticker';
  const initialSignalQuery =
    workspaceSelection?.type === 'ticker'
      ? workspaceSelection.ticker
      : workspaceSelection?.type === 'politician'
        ? politicianData?.summary.displayName || workspaceSelection.name
        : '';
  const createSignalMessage = useMemo(() => {
    if (workspaceSelection?.type === 'ticker') return `Price alert created for ${workspaceSelection.ticker}.`;
    if (workspaceSelection?.type === 'politician')
      return `Signal created for ${politicianData?.summary.displayName || workspaceSelection.name}.`;
    return 'Signal created.';
  }, [politicianData?.summary.displayName, workspaceSelection]);

  /* ── Render ─────────────────────────────────── */

  return (
    <>
      <DashboardHome
        workspaceSelection={workspaceSelection}
        workspaceClosing={workspaceClosing}
        workspaceReveal={workspaceReveal && !isSelectingWorkspace}
        signalMessage={signalMessage}
        tickerData={tickerData}
        tickerLoading={tickerLoading || (isSelectingWorkspace && workspaceSelection?.type === 'ticker')}
        tickerError={tickerError}
        politicianData={politicianData}
        politicianLoading={politicianLoading || (isSelectingWorkspace && workspaceSelection?.type === 'politician')}
        politicianError={politicianError}
        onSelectResult={handleSelectResult}
        onRetryTicker={handleRetryTicker}
        onRetryPolitician={handleRetryPolitician}
        onDismissWorkspace={handleDismissWorkspace}
        onOpenSignalAction={handleOpenSignalAction}
        onOpenFund={handleOpenFund}
      />

      {showCreateSignal && session ? (
        <Suspense fallback={null}>
          <CreateSignalModal
            session={session}
            initialKind={initialSignalKind}
            initialQuery={initialSignalQuery}
            lockTickerContext={workspaceSelection?.type === 'ticker'}
            onCreated={() => setSignalMessage(createSignalMessage)}
            onClose={() => setShowCreateSignal(false)}
          />
        </Suspense>
      ) : null}

      <Suspense fallback={null}>
        <WelcomeOnboarding />
      </Suspense>
    </>
  );
}
