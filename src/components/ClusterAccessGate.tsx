'use client';

import Link from 'next/link';
import { useEffect, useRef, useState } from 'react';
import type { Session } from '@supabase/supabase-js';
import { Lock, Sparkles } from 'lucide-react';

import ClustersPage, { type ClusterSignal } from '@/components/ClustersPage';
import { supabase } from '@/lib/supabase';

type LoadState = 'loading-session' | 'signed-out' | 'loading-account' | 'free' | 'loading-clusters' | 'ready' | 'error';

const CLUSTER_FEED_CACHE_VERSION = 'v5';
const CLUSTER_FEED_CACHE_MAX_AGE_MS = 10 * 60 * 1000;

let clusterAuthSessionCache: Session | null = null;

let clusterFeedMemoryCache: {
  userId: string;
  clusters: ClusterSignal[];
  cachedAt: number;
} | null = null;

function clusterFeedCacheKey(userId: string) {
  return `vail:cluster-feed:${CLUSTER_FEED_CACHE_VERSION}:${userId}`;
}

function isFreshClusterFeedCache(cachedAt: number) {
  return Date.now() - cachedAt < CLUSTER_FEED_CACHE_MAX_AGE_MS;
}

function readCachedClusterFeed(userId: string) {
  if (
    clusterFeedMemoryCache?.userId === userId &&
    isFreshClusterFeedCache(clusterFeedMemoryCache.cachedAt)
  ) {
    return clusterFeedMemoryCache.clusters;
  }

  try {
    const raw = window.sessionStorage.getItem(clusterFeedCacheKey(userId));
    if (!raw) {
      return null;
    }

    const parsed = JSON.parse(raw) as { clusters?: ClusterSignal[]; cachedAt?: number };
    if (!Array.isArray(parsed.clusters) || !parsed.cachedAt || !isFreshClusterFeedCache(parsed.cachedAt)) {
      return null;
    }

    clusterFeedMemoryCache = {
      userId,
      clusters: parsed.clusters,
      cachedAt: parsed.cachedAt,
    };
    return parsed.clusters;
  } catch {
    return null;
  }
}

function writeCachedClusterFeed(userId: string, clusters: ClusterSignal[]) {
  const cachedAt = Date.now();
  clusterFeedMemoryCache = { userId, clusters, cachedAt };

  try {
    window.sessionStorage.setItem(
      clusterFeedCacheKey(userId),
      JSON.stringify({ clusters, cachedAt }),
    );
  } catch {
    // If storage is unavailable, the in-memory cache still speeds up same-session navigation.
  }
}

function clearCachedClusterFeed(userId: string) {
  if (clusterFeedMemoryCache?.userId === userId) {
    clusterFeedMemoryCache = null;
  }

  try {
    window.sessionStorage.removeItem(clusterFeedCacheKey(userId));
  } catch {
    // Ignore storage cleanup failures.
  }
}

function initialClusterGateState() {
  const session = clusterAuthSessionCache;
  const cachedClusters =
    session &&
    clusterFeedMemoryCache?.userId === session.user.id &&
    isFreshClusterFeedCache(clusterFeedMemoryCache.cachedAt)
      ? clusterFeedMemoryCache.clusters
      : [];
  return {
    session,
    accessToken: session?.access_token || '',
    signals: cachedClusters,
    loadState: (cachedClusters.length ? 'ready' : session ? 'loading-account' : 'loading-session') as LoadState,
  };
}

function ProClusterGateCard({
  mode,
  error,
}: {
  mode: 'signed-out' | 'free' | 'error';
  error?: string;
}) {
  const signedOut = mode === 'signed-out';
  return (
    <div className="mx-auto max-w-2xl rounded-3xl border border-white/[0.08] bg-white/[0.02] p-6 text-center shadow-2xl shadow-black/30 sm:p-8">
      <div className="mx-auto flex h-11 w-11 items-center justify-center rounded-2xl border border-emerald-500/20 bg-emerald-500/[0.08] text-emerald-300">
        {mode === 'error' ? <Sparkles className="h-5 w-5" /> : <Lock className="h-5 w-5" />}
      </div>
      <h2 className="mt-5 text-2xl font-semibold tracking-tight text-white">
        {mode === 'error' ? 'Clusters are unavailable right now.' : 'Clusters are a Pro feature.'}
      </h2>
      <p className="mx-auto mt-2 max-w-lg text-sm leading-6 text-zinc-500">
        {mode === 'error'
          ? error || 'Try refreshing in a moment.'
          : 'Cluster detection groups coordinated activity across Congress, insiders, and hedge funds. Upgrade to Pro to unlock the live cluster feed and cluster alerts.'}
      </p>
      <div className="mt-6 flex flex-col justify-center gap-3 sm:flex-row">
        {signedOut ? (
          <Link
            href="/auth?mode=signup"
            className="inline-flex items-center justify-center rounded-xl bg-emerald-300 px-4 py-2.5 text-sm font-semibold text-black transition hover:bg-emerald-200"
          >
            Create account
          </Link>
        ) : null}
        <Link
          href="/pricing"
          className="inline-flex items-center justify-center rounded-xl border border-white/[0.08] bg-white/[0.04] px-4 py-2.5 text-sm font-semibold text-white transition hover:bg-white/[0.08]"
        >
          View Pro
        </Link>
      </div>
    </div>
  );
}

function ClusterLoadingState({ label }: { label: string }) {
  return (
    <div className="rounded-2xl border border-white/[0.06] bg-white/[0.015] p-4">
      <div className="h-10 max-w-lg animate-pulse rounded-lg bg-white/[0.04]" />
      <div className="mt-4 overflow-hidden rounded-2xl border border-white/[0.05]">
        {Array.from({ length: 7 }).map((_, index) => (
          <div key={index} className="flex items-center gap-3 border-b border-white/[0.05] px-4 py-4 last:border-0">
            <div className="h-9 w-9 animate-pulse rounded-full bg-white/[0.05]" />
            <div className="min-w-0 flex-1">
              <div className="h-3 w-24 animate-pulse rounded bg-white/[0.05]" />
              <div className="mt-2 h-4 w-2/3 animate-pulse rounded bg-white/[0.04]" />
            </div>
            <div className="hidden h-4 w-24 animate-pulse rounded bg-white/[0.04] sm:block" />
          </div>
        ))}
      </div>
      <div className="mt-3 text-xs text-zinc-600">{label}</div>
    </div>
  );
}

export default function ClusterAccessGate() {
  const [initialState] = useState(initialClusterGateState);
  const [session, setSession] = useState<Session | null>(initialState.session);
  const [accessToken, setAccessToken] = useState(initialState.accessToken);
  const [loadState, setLoadState] = useState<LoadState>(initialState.loadState);
  const [signals, setSignals] = useState<ClusterSignal[]>(initialState.signals);
  const [error, setError] = useState('');
  const userIdRef = useRef(initialState.session?.user.id || '');

  useEffect(() => {
    let mounted = true;

    function applySession(nextSession: Session | null) {
      if (!mounted) return;
      const previousUserId = userIdRef.current;
      const nextUserId = nextSession?.user.id || '';
      clusterAuthSessionCache = nextSession;
      setSession(nextSession);
      setAccessToken(nextSession?.access_token || '');
      if (previousUserId === nextUserId) {
        return;
      }

      userIdRef.current = nextUserId;
      const cachedClusters = nextSession ? readCachedClusterFeed(nextUserId) : null;
      setSignals(cachedClusters || []);
      setError('');
      setLoadState(cachedClusters ? 'ready' : nextSession ? 'loading-account' : 'signed-out');
    }

    supabase.auth.getSession().then(({ data }) => applySession(data.session));

    const { data } = supabase.auth.onAuthStateChange((_event, nextSession) => {
      applySession(nextSession);
    });

    return () => {
      mounted = false;
      data.subscription.unsubscribe();
    };
  }, []);

  useEffect(() => {
    if (!session) {
      return;
    }

    const activeSession = session;
    const controller = new AbortController();
    let cancelled = false;

    async function load() {
      const cachedClusters = readCachedClusterFeed(activeSession.user.id);
      if (cachedClusters) {
        setSignals(cachedClusters);
        setLoadState('ready');
      } else {
        setLoadState('loading-clusters');
      }
      setError('');

      try {
        const clusterResponse = await fetch('/api/dashboard-clusters', {
          cache: 'no-store',
          signal: controller.signal,
          headers: { Authorization: `Bearer ${activeSession.access_token}` },
        });
        const clusterPayload = (await clusterResponse.json()) as {
          clusters?: ClusterSignal[];
          code?: string;
          error?: string;
        };

        if (!clusterResponse.ok) {
          if (clusterPayload.code === 'PRO_REQUIRED') {
            if (!cancelled) {
              clearCachedClusterFeed(activeSession.user.id);
              setSignals([]);
              setLoadState('free');
            }
            return;
          }
          throw new Error(clusterPayload.error || 'Could not load clusters.');
        }

        if (!cancelled) {
          const nextClusters = clusterPayload.clusters || [];
          writeCachedClusterFeed(activeSession.user.id, nextClusters);
          setSignals(nextClusters);
          setLoadState('ready');
        }
      } catch (value) {
        if (cancelled || (value instanceof Error && value.name === 'AbortError')) {
          return;
        }
        if (cachedClusters) {
          setLoadState('ready');
        } else {
          setError(value instanceof Error ? value.message : 'Could not load clusters.');
          setLoadState('error');
        }
      }
    }

    load();

    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [session]);

  if (loadState === 'signed-out') {
    return <ProClusterGateCard mode="signed-out" />;
  }

  if (loadState === 'free') {
    return <ProClusterGateCard mode="free" />;
  }

  if (loadState === 'error') {
    return <ProClusterGateCard mode="error" error={error} />;
  }

  if (loadState !== 'ready') {
    return (
      <ClusterLoadingState
        label={loadState === 'loading-clusters' ? 'Loading Pro cluster feed…' : 'Checking cluster access…'}
      />
    );
  }

  return <ClustersPage signals={signals} accessToken={accessToken} />;
}
