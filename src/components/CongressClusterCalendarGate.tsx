'use client';

import { useEffect, useRef, useState } from 'react';
import type { Session } from '@supabase/supabase-js';

import CongressClusterCalendar from '@/components/CongressClusterCalendar';
import { ProClusterGateCard } from '@/components/ClusterAccessGate';
import type {
  CongressClusterCalendarData,
  CongressClusterRange,
} from '@/lib/congress-cluster-calendar-types';
import { supabase } from '@/lib/supabase';

type GateState = 'loading-session' | 'signed-out' | 'loading-data' | 'ready' | 'free' | 'error';

const CACHE_MAX_AGE_MS = 5 * 60 * 1000;
const CACHE_REVALIDATE_AFTER_MS = 2 * 60 * 1000;
const CACHE_VERSION = 'v2';
const calendarCache = new Map<string, { data: CongressClusterCalendarData; cachedAt: number }>();

function cacheKey(userId: string, range: CongressClusterRange) {
  return `vail:congress-accumulation:${CACHE_VERSION}:${userId}:${range}`;
}

function readCache(userId: string, range: CongressClusterRange) {
  const key = cacheKey(userId, range);
  const memoryValue = calendarCache.get(key);
  if (memoryValue && Date.now() - memoryValue.cachedAt < CACHE_MAX_AGE_MS) {
    return memoryValue;
  }

  try {
    const raw = window.sessionStorage.getItem(key);
    if (!raw) return null;
    const stored = JSON.parse(raw) as { data?: CongressClusterCalendarData; cachedAt?: number };
    if (!stored.data || !stored.cachedAt || Date.now() - stored.cachedAt >= CACHE_MAX_AGE_MS) {
      window.sessionStorage.removeItem(key);
      return null;
    }
    const cached = { data: stored.data, cachedAt: stored.cachedAt };
    calendarCache.set(key, cached);
    return cached;
  } catch {
    return null;
  }
}

function writeCache(userId: string, range: CongressClusterRange, data: CongressClusterCalendarData) {
  const key = cacheKey(userId, range);
  const cached = { data, cachedAt: Date.now() };
  calendarCache.set(key, cached);
  try {
    window.sessionStorage.setItem(key, JSON.stringify(cached));
  } catch {
    // The in-memory cache still provides instant same-session navigation.
  }
}

function CalendarLoadingState() {
  return (
    <div className="mx-auto max-w-[980px] space-y-5">
      <div className="h-24 animate-pulse border-b border-white/[0.06] bg-white/[0.01]" />
      <div className="h-44 animate-pulse rounded-2xl border border-white/[0.06] bg-white/[0.018]" />
      <div className="overflow-hidden rounded-2xl border border-white/[0.06]">
        {Array.from({ length: 5 }).map((_, index) => (
          <div key={index} className="h-[70px] animate-pulse border-b border-white/[0.045] bg-white/[0.012] last:border-0" />
        ))}
      </div>
    </div>
  );
}

export default function CongressClusterCalendarGate() {
  const [session, setSession] = useState<Session | null>(null);
  const [range, setRange] = useState<CongressClusterRange>('month');
  const [data, setData] = useState<CongressClusterCalendarData | null>(null);
  const [state, setState] = useState<GateState>('loading-session');
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState('');
  const dataRef = useRef<CongressClusterCalendarData | null>(null);

  useEffect(() => {
    dataRef.current = data;
  }, [data]);

  useEffect(() => {
    let mounted = true;

    function applySession(nextSession: Session | null) {
      if (!mounted) return;
      setSession(nextSession);
      setState(nextSession ? 'loading-data' : 'signed-out');
    }

    supabase.auth.getSession().then(({ data: sessionData }) => applySession(sessionData.session));
    const { data: listener } = supabase.auth.onAuthStateChange((_event, nextSession) => applySession(nextSession));

    return () => {
      mounted = false;
      listener.subscription.unsubscribe();
    };
  }, []);

  useEffect(() => {
    if (!session) return;

    const cached = readCache(session.user.id, range);
    if (cached) {
      setData(cached.data);
      setState('ready');
      setError('');
      if (Date.now() - cached.cachedAt < CACHE_REVALIDATE_AFTER_MS) return;
    }

    const controller = new AbortController();
    let cancelled = false;
    const currentData = cached?.data || dataRef.current;
    if (currentData) {
      setRefreshing(true);
    } else {
      setState('loading-data');
    }
    setError('');

    (async () => {
      try {
        const response = await fetch(`/api/congress-cluster-calendar?range=${range}`, {
          cache: 'no-store',
          signal: controller.signal,
          headers: { Authorization: `Bearer ${session.access_token}` },
        });
        const payload = (await response.json()) as CongressClusterCalendarData & {
          code?: string;
          error?: string;
        };

        if (!response.ok) {
          if (payload.code === 'PRO_REQUIRED') {
            if (!cancelled) setState('free');
            return;
          }
          throw new Error(payload.error || 'Could not load Congress accumulation.');
        }

        if (!cancelled) {
          writeCache(session.user.id, range, payload);
          setData(payload);
          setState('ready');
        }
      } catch (value) {
        if (cancelled || (value instanceof Error && value.name === 'AbortError')) return;
        const message = value instanceof Error ? value.message : 'Could not load Congress accumulation.';
        if (currentData) {
          setError(message);
          setState('ready');
        } else {
          setError(message);
          setState('error');
        }
      } finally {
        if (!cancelled) setRefreshing(false);
      }
    })();

    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [range, session]);

  if (state === 'signed-out') return <ProClusterGateCard mode="signed-out" />;
  if (state === 'free') return <ProClusterGateCard mode="free" />;
  if (state === 'error') return <ProClusterGateCard mode="error" error={error} />;
  if (!data || state !== 'ready') return <CalendarLoadingState />;

  return (
    <CongressClusterCalendar
      data={data}
      range={range}
      loading={refreshing}
      error={error}
      onRangeChange={setRange}
    />
  );
}
