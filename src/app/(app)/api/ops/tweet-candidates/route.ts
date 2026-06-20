import { NextResponse } from 'next/server';

import { fetchTweetCandidateStories } from '@/lib/tweet-candidates';

export const dynamic = 'force-dynamic';

const WINDOW_DAYS = {
  day: 1,
  week: 7,
  month: 30,
  '3m': 90,
  '6m': 180,
  '1y': 365,
} as const;

function sinceDateForWindow(window: string | null) {
  const normalized = (window || 'week').trim().toLowerCase() as keyof typeof WINDOW_DAYS;
  const days = WINDOW_DAYS[normalized];
  if (!days) {
    return null;
  }
  const since = new Date();
  since.setUTCDate(since.getUTCDate() - (days - 1));
  return since.toISOString().slice(0, 10);
}

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const status = (searchParams.get('status') || 'pending_review').trim();
    const limit = Number(searchParams.get('limit') || 30);
    const sinceDate = sinceDateForWindow(searchParams.get('window'));
    const category = (searchParams.get('category') || 'all').trim();
    const sort = (searchParams.get('sort') || 'score').trim();
    const queryText = (searchParams.get('q') || '').trim();
    const stories = await fetchTweetCandidateStories({
      status,
      sinceDate,
      storyLimit: Number.isFinite(limit) ? limit : 60,
      category,
      sort,
      queryText,
    });

    return NextResponse.json({
      ok: true,
      enabled: true,
      stories,
    });
  } catch (error) {
    const payload = error && typeof error === 'object' ? (error as { code?: string; message?: string }) : null;
    const code = payload?.code || '';
    const message = payload?.message || (error instanceof Error ? error.message : 'Unknown tweet candidate error.');

    if (code === 'PGRST205' || message.includes("Could not find the table 'public.tweet_candidates'")) {
      return NextResponse.json({
        ok: true,
        enabled: false,
        stories: [],
      });
    }

    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
