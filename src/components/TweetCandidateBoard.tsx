'use client';

import { useCallback, useEffect, useState, useTransition } from 'react';
import { AlertTriangle, CheckCircle2, Megaphone, RefreshCw, XCircle } from 'lucide-react';

type TweetCandidate = {
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

type TweetBoardResponse = {
  enabled: boolean;
  candidates: TweetCandidate[];
};

const STATUS_OPTIONS = ['pending_review', 'approved', 'rejected', 'posted'] as const;

function formatDateTime(value: string | null | undefined) {
  if (!value) {
    return 'N/A';
  }

  return new Intl.DateTimeFormat('en-US', {
    dateStyle: 'medium',
    timeStyle: 'short',
    timeZone: 'America/Los_Angeles',
  }).format(new Date(value));
}

export function TweetCandidateBoard() {
  const [status, setStatus] = useState<(typeof STATUS_OPTIONS)[number]>('pending_review');
  const [board, setBoard] = useState<TweetBoardResponse | null>(null);
  const [message, setMessage] = useState('');
  const [error, setError] = useState('');
  const [isPending, startTransition] = useTransition();

  const loadBoard = useCallback(async (nextStatus = status) => {
    setError('');
    const response = await fetch(`/api/ops/tweet-candidates?status=${nextStatus}&limit=40`, { cache: 'no-store' });
    const payload = (await response.json()) as {
      ok?: boolean;
      enabled?: boolean;
      candidates?: TweetCandidate[];
      error?: string;
    };
    if (!response.ok || !payload.ok) {
      throw new Error(payload.error || 'Failed to load tweet candidates.');
    }
    setBoard({
      enabled: Boolean(payload.enabled),
      candidates: payload.candidates || [],
    });
  }, [status]);

  useEffect(() => {
    startTransition(async () => {
      try {
        await loadBoard(status);
      } catch (value) {
        setError(value instanceof Error ? value.message : 'Failed to load tweet candidates.');
      }
    });
  }, [loadBoard, status]);

  async function queueCandidates() {
    setMessage('');
    setError('');
    startTransition(async () => {
      try {
        const response = await fetch('/api/ops/tweet-candidates/queue', {
          method: 'POST',
        });
        const payload = (await response.json()) as {
          ok?: boolean;
          error?: string;
          summary?: { tweet_candidates_enabled?: boolean; tweet_candidates_upserted?: number; reason?: string } | null;
        };
        if (!response.ok || !payload.ok) {
          throw new Error(payload.error || 'Failed to queue tweet candidates.');
        }

        if (payload.summary?.tweet_candidates_enabled === false) {
          setMessage('Tweet queue is still disabled because the tweet_candidates table is not in Supabase yet.');
        } else {
          setMessage(`Tweet queue ran. Upserted ${payload.summary?.tweet_candidates_upserted ?? 0} candidates.`);
        }
        await loadBoard(status);
      } catch (value) {
        setError(value instanceof Error ? value.message : 'Failed to queue tweet candidates.');
      }
    });
  }

  async function reviewCandidate(id: string, nextStatus: 'approved' | 'rejected' | 'posted') {
    setMessage('');
    setError('');
    startTransition(async () => {
      try {
        const response = await fetch(`/api/ops/tweet-candidates/${id}`, {
          method: 'PATCH',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            status: nextStatus,
            reviewed_by: 'ops_ui',
          }),
        });
        const payload = (await response.json()) as { ok?: boolean; error?: string };
        if (!response.ok || !payload.ok) {
          throw new Error(payload.error || 'Failed to review tweet candidate.');
        }
        setMessage(`Candidate marked ${nextStatus}.`);
        await loadBoard(status);
      } catch (value) {
        setError(value instanceof Error ? value.message : 'Failed to review tweet candidate.');
      }
    });
  }

  return (
    <div className="space-y-6">
      <div className="glass-panel rounded-2xl p-5">
        <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
          <div>
            <div className="text-sm uppercase tracking-[0.18em] text-zinc-500">Social Review</div>
            <div className="mt-2 text-xl font-semibold text-white">Manual tweet review queue</div>
            <p className="mt-2 max-w-3xl text-sm text-[var(--text-secondary)]">
              Policy-driven candidate queue for public posts. Nothing auto-posts here. Review first, then approve or reject.
            </p>
          </div>

          <div className="flex flex-wrap items-center gap-3">
            <select
              value={status}
              onChange={(event) => setStatus(event.target.value as (typeof STATUS_OPTIONS)[number])}
              className="rounded-xl border border-white/10 bg-[#0b1020] px-4 py-2 text-sm text-zinc-100 outline-none transition focus:border-blue-400/40"
            >
              {STATUS_OPTIONS.map((value) => (
                <option key={value} value={value}>
                  {value}
                </option>
              ))}
            </select>

            <button
              type="button"
              onClick={queueCandidates}
              disabled={isPending}
              className="inline-flex items-center gap-2 rounded-xl bg-blue-500 px-4 py-2 text-sm font-medium text-white transition hover:bg-blue-400 disabled:cursor-not-allowed disabled:opacity-60"
            >
              <RefreshCw className="h-4 w-4" />
              {isPending ? 'Running...' : 'Run Queue'}
            </button>
          </div>
        </div>

        {message ? (
          <div className="mt-4 flex items-start gap-3 rounded-2xl border border-emerald-500/20 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-200">
            <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0" />
            <span>{message}</span>
          </div>
        ) : null}

        {error ? (
          <div className="mt-4 flex items-start gap-3 rounded-2xl border border-red-500/20 bg-red-500/10 px-4 py-3 text-sm text-red-200">
            <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
            <span>{error}</span>
          </div>
        ) : null}
      </div>

      {board && !board.enabled ? (
        <div className="glass-panel rounded-2xl border border-amber-500/20 bg-amber-500/10 p-5 text-sm text-amber-100/90">
          Tweet candidate review is disabled because `tweet_candidates` has not been added to Supabase yet. Apply
          `supabase_vail_phase3_social.sql` when you are ready to turn on the review queue.
        </div>
      ) : null}

      {board && board.enabled ? (
        <div className="space-y-4">
          {board.candidates.length === 0 ? (
            <div className="glass-panel rounded-2xl p-5 text-sm text-zinc-400">No candidates in this status right now.</div>
          ) : (
            board.candidates.map((candidate) => (
              <div key={candidate.id} className="glass-panel rounded-2xl p-5">
                <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                  <div className="space-y-3">
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="inline-flex items-center gap-2 rounded-full border border-cyan-500/20 bg-cyan-500/10 px-2.5 py-1 text-xs font-medium text-cyan-300">
                        <Megaphone className="h-3.5 w-3.5" />
                        {candidate.rule_key}
                      </span>
                      <span className="rounded-full border border-white/10 bg-white/5 px-2.5 py-1 text-xs text-zinc-300">
                        Score {candidate.score?.toFixed(2) ?? '0.00'}
                      </span>
                    </div>

                    <div>
                      <div className="text-lg font-semibold text-white">{candidate.title}</div>
                      <div className="mt-1 text-sm text-zinc-400">
                        {candidate.signal_events?.actor_name || 'Unknown actor'}
                        {candidate.signal_events?.ticker ? ` • ${candidate.signal_events.ticker}` : ''}
                        {candidate.signal_events?.signal_type ? ` • ${candidate.signal_events.signal_type}` : ''}
                      </div>
                    </div>

                    <div className="rounded-2xl border border-white/10 bg-[#0b1020]/80 p-4 font-mono text-sm leading-6 text-zinc-100">
                      {candidate.draft_text}
                    </div>

                    {candidate.rationale ? (
                      <div className="text-sm text-[var(--text-secondary)]">
                        <span className="font-medium text-white">Why it was queued:</span> {candidate.rationale}
                      </div>
                    ) : null}

                    <div className="text-xs text-zinc-500">
                      Created {formatDateTime(candidate.created_at)}
                      {candidate.reviewed_at ? ` • Reviewed ${formatDateTime(candidate.reviewed_at)}` : ''}
                      {candidate.posted_at ? ` • Posted ${formatDateTime(candidate.posted_at)}` : ''}
                    </div>
                  </div>

                  {status === 'pending_review' ? (
                    <div className="flex shrink-0 flex-wrap gap-2">
                      <button
                        type="button"
                        onClick={() => reviewCandidate(candidate.id, 'approved')}
                        disabled={isPending}
                        className="inline-flex items-center gap-2 rounded-xl bg-emerald-500 px-4 py-2 text-sm font-medium text-white transition hover:bg-emerald-400 disabled:cursor-not-allowed disabled:opacity-60"
                      >
                        <CheckCircle2 className="h-4 w-4" />
                        Approve
                      </button>
                      <button
                        type="button"
                        onClick={() => reviewCandidate(candidate.id, 'rejected')}
                        disabled={isPending}
                        className="inline-flex items-center gap-2 rounded-xl bg-red-500 px-4 py-2 text-sm font-medium text-white transition hover:bg-red-400 disabled:cursor-not-allowed disabled:opacity-60"
                      >
                        <XCircle className="h-4 w-4" />
                        Reject
                      </button>
                    </div>
                  ) : status === 'approved' ? (
                    <div className="flex shrink-0 flex-wrap gap-2">
                      <button
                        type="button"
                        onClick={() => reviewCandidate(candidate.id, 'posted')}
                        disabled={isPending}
                        className="inline-flex items-center gap-2 rounded-xl bg-blue-500 px-4 py-2 text-sm font-medium text-white transition hover:bg-blue-400 disabled:cursor-not-allowed disabled:opacity-60"
                      >
                        <Megaphone className="h-4 w-4" />
                        Mark Posted
                      </button>
                    </div>
                  ) : null}

                  {candidate.signal_events?.source_url ? (
                    <a
                      href={candidate.signal_events.source_url}
                      target="_blank"
                      rel="noreferrer"
                      className="text-sm text-blue-300 transition hover:text-blue-200"
                    >
                      Open source filing
                    </a>
                  ) : null}
                </div>
              </div>
            ))
          )}
        </div>
      ) : null}
    </div>
  );
}
