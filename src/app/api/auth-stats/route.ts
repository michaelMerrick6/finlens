import { NextResponse } from 'next/server';

import { routeErrorMessage } from '@/lib/api-errors';
import { getPublicSupabase } from '@/lib/supabase-server';

export const revalidate = 300;

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

type LatestClusterRow = {
  ticker: string | null;
  title: string | null;
  created_at: string | null;
};

const EMPTY_STATS: AuthStats = {
  congressTradesLastWeek: 0,
  insiderTradesLastWeek: 0,
  fundFilingsLastWeek: 0,
  politiciansTracked: 0,
  clusterCount: 0,
  latestCluster: null,
};

const CLUSTER_SIGNAL_TYPES = [
  'politician_cluster',
  'insider_cluster',
  'cross_source_accumulation',
  'cross_source_sell',
  'cluster_gain_milestone',
];
const CLUSTER_SOURCES = ['congress', 'insider', 'cross_source'];

function sevenDaysAgoTimestamp() {
  const date = new Date();
  date.setUTCDate(date.getUTCDate() - 7);
  return date.toISOString();
}

export async function GET() {
  try {
    const supabase = getPublicSupabase();
    const since = sevenDaysAgoTimestamp();

    const [
      congressResponse,
      insiderResponse,
      fundResponse,
      politicianResponse,
      ...clusterResponses
    ] = await Promise.all([
      supabase.from('politician_trades').select('id').gte('created_at', since).limit(1000),
      supabase.from('insider_trades').select('id').gte('created_at', since).limit(1000),
      supabase.from('institutional_holdings').select('id').gte('created_at', since).limit(1000),
      supabase.from('congress_members').select('id', { count: 'exact', head: true }).eq('active', true),
      ...CLUSTER_SOURCES.map((source) =>
        supabase
          .from('signal_events')
          .select('ticker,title,created_at')
          .eq('source', source)
          .in('signal_type', CLUSTER_SIGNAL_TYPES)
          .gte('created_at', since)
          .order('created_at', { ascending: false, nullsFirst: false })
          .limit(100),
      ),
    ]);

    const clusterRows = clusterResponses.flatMap((response) => (response.data || []) as LatestClusterRow[]);
    const latestCluster =
      [...clusterRows].sort((left, right) => String(right.created_at || '').localeCompare(String(left.created_at || '')))[0] ||
      null;

    return NextResponse.json({
      stats: {
        congressTradesLastWeek: congressResponse.data?.length || 0,
        insiderTradesLastWeek: insiderResponse.data?.length || 0,
        fundFilingsLastWeek: fundResponse.data?.length || 0,
        politiciansTracked: politicianResponse.count || 0,
        clusterCount: clusterRows.length,
        latestCluster: latestCluster
          ? {
              ticker: latestCluster.ticker || null,
              title: latestCluster.title || null,
            }
          : null,
      } satisfies AuthStats,
      errors: process.env.NODE_ENV === 'production'
        ? []
        : [
            congressResponse.error?.message,
            insiderResponse.error?.message,
            fundResponse.error?.message,
            politicianResponse.error?.message,
            ...clusterResponses.map((response) => response.error?.message),
          ].filter(Boolean),
    });
  } catch (error) {
    return NextResponse.json(
      {
        stats: EMPTY_STATS,
        error: routeErrorMessage(error, 'Failed to load auth stats.', 'auth-stats'),
      },
      { status: 200 },
    );
  }
}
