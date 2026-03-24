import 'server-only';

import { getAdminSupabase } from '@/lib/supabase-admin';

export type TweetCandidateRow = {
  id: string;
  status: string;
  rule_key: string;
  score: number | null;
  title: string;
  draft_text: string;
  rationale: string | null;
  created_at: string;
  reviewed_at: string | null;
  posted_at: string | null;
  review_notes: string | null;
  signal_event_id: string;
  signal_events?: {
    ticker?: string | null;
    actor_name?: string | null;
    signal_type?: string | null;
    source_url?: string | null;
  } | null;
};

export async function tweetCandidatesEnabled() {
  const supabase = getAdminSupabase();
  try {
    const response = await supabase.from('tweet_candidates').select('id', { count: 'exact', head: true }).limit(1);
    return !response.error;
  } catch {
    return false;
  }
}

export async function fetchTweetCandidates(status: string, limit: number) {
  const supabase = getAdminSupabase();
  const response = await supabase
    .from('tweet_candidates')
    .select(
      'id, status, rule_key, score, title, draft_text, rationale, created_at, reviewed_at, posted_at, review_notes, signal_event_id, signal_events(ticker, actor_name, signal_type, source_url)'
    )
    .eq('status', status)
    .order('score', { ascending: false })
    .order('created_at', { ascending: false })
    .limit(limit);

  if (response.error) {
    throw response.error;
  }

  return (response.data ?? []) as TweetCandidateRow[];
}

export async function updateTweetCandidate(
  id: string,
  payload: {
    status: 'pending_review' | 'approved' | 'rejected' | 'posted';
    review_notes?: string;
    reviewed_by?: string;
    external_post_id?: string;
  }
) {
  const supabase = getAdminSupabase();
  const update: Record<string, string | null> = {
    status: payload.status,
    review_notes: payload.review_notes || null,
    reviewed_by: payload.reviewed_by || 'ops_ui',
    reviewed_at: new Date().toISOString(),
  };

  if (payload.status === 'posted') {
    update.posted_at = new Date().toISOString();
    update.external_post_id = payload.external_post_id || null;
  }

  const response = await supabase.from('tweet_candidates').update(update).eq('id', id).select('id').limit(1);
  if (response.error) {
    throw response.error;
  }
  return response.data?.[0] ?? null;
}
