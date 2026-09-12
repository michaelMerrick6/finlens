import { NextResponse } from 'next/server';

import { routeErrorMessage } from '@/lib/api-errors';
import { getPublicSupabase } from '@/lib/supabase-server';

export const revalidate = 300;

type AuthStats = {
  congressTradesLastWeek: number;
  insiderTradesLastWeek: number;
  fundHoldingRowsLastWeek: number;
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
      supabase.from('politician_trades').select('id', { count: 'exact', head: true }).gte('created_at', since),
      supabase.from('insider_trades').select('id', { count: 'exact', head: true }).gte('created_at', since),
      supabase.from('institutional_holdings').select('id', { count: 'exact', head: true }).gte('created_at', since),
      supabase.from('congress_members').select('id', { count: 'exact', head: true }).eq('active', true),
      ...CLUSTER_SOURCES.map((source) =>
        supabase
          .from('signal_events')
          .select('ticker,title,created_at', { count: 'exact' })
          .eq('source', source)
          .in('signal_type', CLUSTER_SIGNAL_TYPES)
          .gte('created_at', since)
          .order('created_at', { ascending: false, nullsFirst: false })
          .order('id', { ascending: false })
          .limit(1),
      ),
    ]);

    const responses = [congressResponse, insiderResponse, fundResponse, politicianResponse, ...clusterResponses];
    if (responses.some((response) => response.error || response.count === null)) {
      return NextResponse.json({ stats: null, error: 'Statistics are temporarily unavailable.' }, { status: 503 });
    }

    const clusterRows = clusterResponses.flatMap((response) => (response.data || []) as LatestClusterRow[]);
    const latestCluster =
      [...clusterRows].sort((left, right) => String(right.created_at || '').localeCompare(String(left.created_at || '')))[0] ||
      null;

    return NextResponse.json({
      stats: {
        congressTradesLastWeek: congressResponse.count ?? 0,
        insiderTradesLastWeek: insiderResponse.count ?? 0,
        fundHoldingRowsLastWeek: fundResponse.count ?? 0,
        politiciansTracked: politicianResponse.count || 0,
        clusterCount: clusterResponses.reduce((total, response) => total + (response.count ?? 0), 0),
        latestCluster: latestCluster
          ? {
              ticker: latestCluster.ticker || null,
              title: latestCluster.title || null,
            }
          : null,
      } satisfies AuthStats,

    });
  } catch (error) {
    return NextResponse.json(
      {
        stats: null,
        error: routeErrorMessage(error, 'Failed to load auth stats.', 'auth-stats'),
      },
      { status: 503 },
    );
  }
}
