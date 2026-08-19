'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import type { Session } from '@supabase/supabase-js';

import CongressBuyingCalendar from '@/components/CongressBuyingCalendar';
import CongressCalendarTransactionsModal from '@/components/CongressCalendarTransactionsModal';
import { ProClusterGateCard } from '@/components/ClusterAccessGate';
import type {
  CongressBuyingCalendarData,
  CongressCalendarCompany,
  CongressCalendarTransactionsData,
} from '@/lib/congress-cluster-calendar-types';
import { supabase } from '@/lib/supabase';

type GateState = 'loading-session' | 'signed-out' | 'loading-data' | 'ready' | 'free' | 'error';

const CACHE_MAX_AGE_MS = 5 * 60 * 1000;
const CACHE_REVALIDATE_AFTER_MS = 2 * 60 * 1000;
const calendarCache = new Map<string, { data: CongressBuyingCalendarData; cachedAt: number }>();
const transactionCache = new Map<string, { data: CongressCalendarTransactionsData; cachedAt: number }>();

function currentPacificMonth() {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/Los_Angeles',
    year: 'numeric',
    month: '2-digit',
  }).formatToParts(new Date());
  const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${values.year}-${values.month}`;
}

function cacheKey(userId: string, month: string) {
  return `vail:congress-buying-calendar:v2:${userId}:${month}`;
}

function addDays(value: string, days: number) {
  const date = new Date(`${value}T12:00:00Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

function transactionCacheKey(userId: string, date: string, ticker: string) {
  return `vail:congress-calendar-transactions:v1:${userId}:${date}:${ticker}`;
}

function readCache(userId: string, month: string) {
  const key = cacheKey(userId, month);
  const memoryValue = calendarCache.get(key);
  if (memoryValue && Date.now() - memoryValue.cachedAt < CACHE_MAX_AGE_MS) return memoryValue;
  try {
    const raw = window.sessionStorage.getItem(key);
    if (!raw) return null;
    const stored = JSON.parse(raw) as { data?: CongressBuyingCalendarData; cachedAt?: number };
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

function writeCache(userId: string, month: string, data: CongressBuyingCalendarData) {
  const key = cacheKey(userId, month);
  const cached = { data, cachedAt: Date.now() };
  calendarCache.set(key, cached);
  try {
    window.sessionStorage.setItem(key, JSON.stringify(cached));
  } catch {
    // The in-memory cache still makes repeat navigation immediate.
  }
}

function CalendarLoadingState() {
  return (
    <div className="mx-auto max-w-[1180px] space-y-4">
      <div className="ml-auto h-10 w-56 animate-pulse rounded-xl bg-white/[0.025]" />
      <div className="h-24 animate-pulse border-b border-white/[0.06] bg-white/[0.01]" />
      <div className="h-20 animate-pulse rounded-2xl border border-white/[0.06] bg-white/[0.018]" />
      <div className="grid grid-cols-7 gap-2">
        {Array.from({ length: 7 }, (_, index) => (
          <div key={index} className="h-16 animate-pulse rounded-xl border border-white/[0.06] bg-white/[0.012] md:h-40" />
        ))}
      </div>
      <div className="h-44 animate-pulse rounded-2xl border border-white/[0.06] bg-white/[0.012]" />
    </div>
  );
}

export default function CongressBuyingCalendarGate() {
  const [session, setSession] = useState<Session | null>(null);
  const [month, setMonth] = useState(currentPacificMonth);
  const [requestedWeekStart, setRequestedWeekStart] = useState<string | null>(null);
  const [data, setData] = useState<CongressBuyingCalendarData | null>(null);
  const [state, setState] = useState<GateState>('loading-session');
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState('');
  const [selection, setSelection] = useState<{ date: string; company: CongressCalendarCompany } | null>(null);
  const [transactionData, setTransactionData] = useState<CongressCalendarTransactionsData | null>(null);
  const [transactionsLoading, setTransactionsLoading] = useState(false);
  const [transactionsError, setTransactionsError] = useState('');
  const dataRef = useRef<CongressBuyingCalendarData | null>(null);
  const autoFocusedLatestMonthRef = useRef(false);
  const transactionRequestRef = useRef<AbortController | null>(null);

  useEffect(() => {
    dataRef.current = data;
  }, [data]);

  useEffect(() => () => transactionRequestRef.current?.abort(), []);

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
    const cached = readCache(session.user.id, month);
    if (cached) {
      if (
        !autoFocusedLatestMonthRef.current &&
        month === currentPacificMonth() &&
        cached.data.totals.tradeCount === 0 &&
        cached.data.latestTransactionDate &&
        !cached.data.latestTransactionDate.startsWith(month)
      ) {
        autoFocusedLatestMonthRef.current = true;
        setMonth(cached.data.latestTransactionDate.slice(0, 7));
        return;
      }
      setData(cached.data);
      setState('ready');
      setError('');
      if (Date.now() - cached.cachedAt < CACHE_REVALIDATE_AFTER_MS) return;
    }

    const controller = new AbortController();
    let cancelled = false;
    const currentData = cached?.data || dataRef.current;
    if (currentData) setRefreshing(true);
    else setState('loading-data');
    setError('');

    (async () => {
      try {
        const response = await fetch(`/api/congress-buying-calendar?month=${month}`, {
          cache: 'no-store',
          signal: controller.signal,
          headers: { Authorization: `Bearer ${session.access_token}` },
        });
        const payload = (await response.json()) as CongressBuyingCalendarData & { code?: string; error?: string };
        if (!response.ok) {
          if (payload.code === 'PRO_REQUIRED') {
            if (!cancelled) setState('free');
            return;
          }
          throw new Error(payload.error || 'Could not load the Congress buying calendar.');
        }
        if (!cancelled) {
          writeCache(session.user.id, month, payload);
          if (
            !autoFocusedLatestMonthRef.current &&
            month === currentPacificMonth() &&
            payload.totals.tradeCount === 0 &&
            payload.latestTransactionDate &&
            !payload.latestTransactionDate.startsWith(month)
          ) {
            autoFocusedLatestMonthRef.current = true;
            setMonth(payload.latestTransactionDate.slice(0, 7));
            return;
          }
          setData(payload);
          setState('ready');
        }
      } catch (value) {
        if (cancelled || (value instanceof Error && value.name === 'AbortError')) return;
        const message = value instanceof Error ? value.message : 'Could not load the Congress buying calendar.';
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
  }, [month, session]);

  const closeTransactions = useCallback(() => {
    transactionRequestRef.current?.abort();
    transactionRequestRef.current = null;
    setSelection(null);
    setTransactionData(null);
    setTransactionsLoading(false);
    setTransactionsError('');
  }, []);

  const openTransactions = useCallback(async (date: string, company: CongressCalendarCompany) => {
    if (!session) return;
    transactionRequestRef.current?.abort();
    const controller = new AbortController();
    transactionRequestRef.current = controller;
    const key = transactionCacheKey(session.user.id, date, company.ticker);
    const cached = transactionCache.get(key);

    setSelection({ date, company });
    setTransactionsError('');
    if (cached && Date.now() - cached.cachedAt < CACHE_MAX_AGE_MS) {
      setTransactionData(cached.data);
      setTransactionsLoading(false);
      transactionRequestRef.current = null;
      return;
    }

    setTransactionData(null);
    setTransactionsLoading(true);
    try {
      const response = await fetch(
        `/api/congress-buying-calendar/transactions?date=${date}&ticker=${encodeURIComponent(company.ticker)}`,
        {
          cache: 'no-store',
          signal: controller.signal,
          headers: { Authorization: `Bearer ${session.access_token}` },
        },
      );
      const payload = (await response.json()) as CongressCalendarTransactionsData & { error?: string };
      if (!response.ok) throw new Error(payload.error || 'Could not load calendar transactions.');
      transactionCache.set(key, { data: payload, cachedAt: Date.now() });
      setTransactionData(payload);
    } catch (value) {
      if (value instanceof Error && value.name === 'AbortError') return;
      setTransactionsError(value instanceof Error ? value.message : 'Could not load calendar transactions.');
    } finally {
      if (transactionRequestRef.current === controller) {
        transactionRequestRef.current = null;
        setTransactionsLoading(false);
      }
    }
  }, [session]);

  const changeWeek = useCallback((weekStart: string) => {
    setRequestedWeekStart(weekStart);
    setMonth(addDays(weekStart, 3).slice(0, 7));
  }, []);

  if (state === 'signed-out') return <ProClusterGateCard mode="signed-out" />;
  if (state === 'free') return <ProClusterGateCard mode="free" />;
  if (state === 'error') return <ProClusterGateCard mode="error" error={error} />;
  if (!data || state !== 'ready') return <CalendarLoadingState />;

  return (
    <>
      <CongressBuyingCalendar
        key={`${data.month}:${requestedWeekStart || 'latest'}`}
        data={data}
        loading={refreshing}
        error={error}
        initialWeekStart={requestedWeekStart}
        onWeekChange={changeWeek}
        onCompanySelect={openTransactions}
      />
      <CongressCalendarTransactionsModal
        selection={selection}
        data={transactionData}
        loading={transactionsLoading}
        error={transactionsError}
        onClose={closeTransactions}
      />
    </>
  );
}
