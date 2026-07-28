'use client';

import { useEffect, useState } from 'react';
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
const calendarCache = new Map<string, { data: CongressClusterCalendarData; cachedAt: number }>();

function cacheKey(userId: string, range: CongressClusterRange) {
  return `${userId}:${range}`;
}

function readCache(userId: string, range: CongressClusterRange) {
  const cached = calendarCache.get(cacheKey(userId, range));
  return cached && Date.now() - cached.cachedAt < CACHE_MAX_AGE_MS ? cached.data : null;
}

function CalendarLoadingState() {
  return (
    <div className="space-y-4">
      <div className="h-40 animate-pulse rounded-3xl border border-white/[0.06] bg-white/[0.02]" />
      <div className="grid gap-2.5 sm:grid-cols-2 lg:grid-cols-4">
        {Array.from({ length: 4 }).map((_, index) => (
          <div key={index} className="h-24 animate-pulse rounded-2xl border border-white/[0.05] bg-white/[0.015]" />
        ))}
      </div>
      <div className="grid gap-4 xl:grid-cols-2">
        <div className="h-[440px] animate-pulse rounded-2xl border border-white/[0.05] bg-white/[0.015]" />
        <div className="h-[440px] animate-pulse rounded-2xl border border-white/[0.05] bg-white/[0.015]" />
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
      setData(cached);
      setState('ready');
      setError('');
      return;
    }

    const controller = new AbortController();
    let cancelled = false;
    if (data) {
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
          throw new Error(payload.error || 'Could not load the Congress cluster calendar.');
        }

        if (!cancelled) {
          calendarCache.set(cacheKey(session.user.id, range), { data: payload, cachedAt: Date.now() });
          setData(payload);
          setState('ready');
        }
      } catch (value) {
        if (cancelled || (value instanceof Error && value.name === 'AbortError')) return;
        const message = value instanceof Error ? value.message : 'Could not load the Congress cluster calendar.';
        if (data) {
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
  }, [data, range, session]);

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
