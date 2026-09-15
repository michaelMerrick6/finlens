import { twitterEditorial } from '@/lib/twitter-editorial';
import { NextResponse } from 'next/server';
import { requireHqOwner } from '@/lib/hq-auth';
import { ApiRouteError } from '@/lib/auth-server';
import { getAdminSupabase } from '@/lib/supabase-admin';
export const dynamic = 'force-dynamic';
const headers = { 'Cache-Control': 'private, no-store' };
function failure(error: unknown) {
  return NextResponse.json({ error: error instanceof ApiRouteError ? error.message : 'The Twitter queue is unavailable. Please try again.' }, { status: error instanceof ApiRouteError ? error.status : 503, headers });
}
export async function GET(request: Request) {
  try {
    await requireHqOwner(request);
    const result = await getAdminSupabase().from('tweet_candidates').select('id,title,draft_text,rationale,status,created_at,payload').eq('channel','twitter').order('created_at',{ascending:false}).limit(100);
    if(result.error) throw result.error;
    const configured = Boolean(process.env.X_USER_ACCESS_TOKEN || process.env.TWITTER_USER_ACCESS_TOKEN) || ['API_KEY','API_SECRET','ACCESS_TOKEN','ACCESS_TOKEN_SECRET'].every(key=>Boolean(process.env[`X_${key}`] || process.env[`TWITTER_${key}`]));
    return NextResponse.json({ candidates:result.data.flatMap(row=>{const draft=twitterEditorial(row);return draft===null?[]:[{...row,draft_text:draft}];}), configured },{headers});
  } catch(error) { return failure(error); }
}
export async function PATCH(request: Request) {
  try {
    await requireHqOwner(request);
    const body = await request.json();
    if(typeof body.id !== 'string' || typeof body.draft !== 'string' || body.draft.length > 10000 || !['save','dismiss'].includes(body.action)) return NextResponse.json({error:'Invalid draft update.'},{status:400,headers});
    const result = await getAdminSupabase().from('tweet_candidates').update({draft_text:body.draft,...(body.action==='dismiss'?{status:'rejected'}:{})}).eq('id',body.id).eq('channel','twitter').eq('status','pending_review').select('id').maybeSingle();
    if(result.error) throw result.error;
    if(!result.data) return NextResponse.json({error:'This candidate has changed. Refresh the queue.'},{status:409,headers});
    return NextResponse.json({ok:true},{headers});
  } catch(error) { return failure(error); }
}
