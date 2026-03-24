import { NextResponse } from 'next/server';

import { getAdminSupabase } from '@/lib/supabase-admin';

export const dynamic = 'force-dynamic';

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const status = (searchParams.get('status') || 'pending_review').trim();
    const limit = Number(searchParams.get('limit') || 30);
    const supabase = getAdminSupabase();

    const response = await supabase
      .from('tweet_candidates')
      .select(
        'id, status, rule_key, score, title, draft_text, rationale, created_at, reviewed_at, posted_at, review_notes, signal_event_id, signal_events(ticker, actor_name, signal_type, source_url)'
      )
      .eq('status', status)
      .order('score', { ascending: false })
      .order('created_at', { ascending: false })
      .limit(Number.isFinite(limit) ? limit : 30);

    if (response.error) {
      throw response.error;
    }

    return NextResponse.json({
      ok: true,
      enabled: true,
      candidates: response.data ?? [],
    });
  } catch (error) {
    const payload = error && typeof error === 'object' ? (error as { code?: string; message?: string }) : null;
    const code = payload?.code || '';
    const message = payload?.message || (error instanceof Error ? error.message : 'Unknown tweet candidate error.');

    if (code === 'PGRST205' || message.includes("Could not find the table 'public.tweet_candidates'")) {
      return NextResponse.json({
        ok: true,
        enabled: false,
        candidates: [],
      });
    }

    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
