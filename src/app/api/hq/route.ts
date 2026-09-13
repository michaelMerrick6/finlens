import { NextResponse } from 'next/server';
import { requireHqOwner } from '@/lib/hq-auth';
import { getAdminSupabase } from '@/lib/supabase-admin';
import { ApiRouteError } from '@/lib/auth-server';
export const dynamic = 'force-dynamic';
export async function GET(request: Request) {
  const headers = { 'Cache-Control': 'private, no-store' };
  try {
    await requireHqOwner(request);
    const db = getAdminSupabase();
    const now = new Date();
    const periods = [7,30].map(days => ({label: `Last ${days} days`, start:new Date(now.getTime()-days*86400000).toISOString()}));
    periods.push({label:'Year to date',start:`${now.getUTCFullYear()}-01-01T00:00:00.000Z`});
    const counts = await Promise.all(periods.map(async period => {
      const result = await Promise.all(['politician_trades','insider_trades'].map(async table => {
        const r = await db.from(table).select('id',{count:'exact'}).limit(1).gte('created_at',period.start).lte('created_at',now.toISOString());
        return r.error ? null : r.count;
      }));
      return {...period, politicians:result[0],insiders:result[1]};
    }));
    const [runs, analytics, audience] = await Promise.all([
      db.from('scraper_runs').select('id,scraper_name,source_name,status,started_at,finished_at,duration_ms,records_seen,records_inserted,records_updated,records_skipped,error_count').order('started_at',{ascending:false}).limit(40),
      db.rpc('get_site_analytics',{p_days:30}),
      db.rpc('get_hq_audience'),
    ]);
    return NextResponse.json({generatedAt:now.toISOString(),counts,audience:audience.error?null:audience.data,runs:runs.error?null:runs.data,analytics:analytics.error?null:analytics.data},{headers});
  } catch(error) {
    return NextResponse.json({error:error instanceof ApiRouteError?error.message:'HQ is temporarily unavailable.'},{status:error instanceof ApiRouteError?error.status:503,headers});
  }
}
