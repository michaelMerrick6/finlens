'use client';

import { useCallback, useDeferredValue, useEffect, useState, useTransition } from 'react';
import { AlertTriangle, CheckCircle2, Megaphone, RefreshCw, Save, Search, Send, XCircle } from 'lucide-react';

import { formatCalendarDate, formatDateTimeValue } from '@/lib/date-format';

type BroadcastStoryChannel = {
  id: string;
  channel: string;
  status: string;
  title: string;
  draftText: string;
  reviewNotes: string | null;
  reviewedAt: string | null;
  postedAt: string | null;
  externalPostId: string | null;
  score: number;
};

type BroadcastChannelKey = 'twitter' | 'discord_premium';

type BroadcastStory = {
  candidateKey: string;
  ruleKey: string;
  category: string;
  title: string;
  rationale: string | null;
  score: number;
  ticker: string | null;
  actorName: string | null;
  signalType: string | null;
  direction: string | null;
  actorCount: number;
  amountFloor: number;
  amountLabel: string | null;
  amountRanges: string[];
  tradeDateStart: string | null;
  tradeDateEnd: string | null;
  sourceMix: {
    congress: number;
    insiders: number;
    funds: number;
  };
  actorLabels: string[];
  committees: string[];
  themes: string[];
  latestPublishedAt: string | null;
  sourceUrl: string | null;
  createdAt: string;
  reviewedAt: string | null;
  postedAt: string | null;
  reviewNotes: string | null;
  channels: Partial<Record<BroadcastChannelKey, BroadcastStoryChannel>>;
};

type TweetBoardResponse = {
  enabled: boolean;
  stories: BroadcastStory[];
};

type QueueState = {
  status: 'idle' | 'running' | 'completed' | 'failed';
  started_at?: string;
  finished_at?: string;
  pid?: number;
  log_path?: string;
  exit_code?: number;
  current_step?: string;
  progress_percent?: number;
  stderr?: string | null;
  summary?: {
    tweet_candidates_enabled?: boolean;
    tweet_candidates_upserted?: number;
    tweet_candidates_deleted?: number;
    signal_events_seen?: number;
  } | null;
};

const STATUS_OPTIONS = ['pending_review', 'approved', 'rejected', 'posted'] as const;
const CATEGORY_OPTIONS = ['all', 'politicians', 'insiders', 'updates'] as const;
const TIME_RANGE_OPTIONS = ['day', 'week', 'month', '3m', '6m', '1y'] as const;
const SORT_OPTIONS = ['score', 'newest', 'size'] as const;

function channelLabel(channel: string) {
  if (channel === 'twitter') {
    return 'X';
  }
  if (channel === 'discord_premium') {
    return 'Discord Premium';
  }
  return channel;
}

function categoryLabel(category: string) {
  if (category === 'politicians') {
    return 'Politicians';
  }
  if (category === 'insiders') {
    return 'Insiders';
  }
  if (category === 'updates') {
    return 'Updates';
  }
  return 'All Sources';
}

function timeRangeLabel(range: string) {
  if (range === 'day') {
    return 'Last Day';
  }
  if (range === 'week') {
    return 'Last Week';
  }
  if (range === 'month') {
    return 'Last Month';
  }
  if (range === '3m') {
    return 'Last 3 Months';
  }
  if (range === '6m') {
    return 'Last 6 Months';
  }
  if (range === '1y') {
    return 'Last Year';
  }
  return range;
}

function sortLabel(value: string) {
  if (value === 'score') {
    return 'Highest Score';
  }
  if (value === 'newest') {
    return 'Newest';
  }
  if (value === 'size') {
    return 'Largest Size';
  }
  return value;
}

function statusLabel(value: string) {
  return value
    .split('_')
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function trim(value: string | null | undefined) {
  return (value || '').trim();
}

function formatDateTime(value: string | null | undefined) {
  return formatDateTimeValue(value);
}

function formatDate(value: string | null | undefined) {
  return formatCalendarDate(value);
}

function sourceMixLabel(story: BroadcastStory) {
  const parts: string[] = [];
  if (story.sourceMix.congress) {
    parts.push(`Congress ${story.sourceMix.congress}`);
  }
  if (story.sourceMix.insiders) {
    parts.push(`Insiders ${story.sourceMix.insiders}`);
  }
  if (story.sourceMix.funds) {
    parts.push(`Funds ${story.sourceMix.funds}`);
  }
  return parts.length ? parts.join(' • ') : 'Single-source';
}

function directionLabel(direction: string | null | undefined) {
  const normalized = trim(direction).toLowerCase();
  if (!normalized) {
    return null;
  }
  return normalized.charAt(0).toUpperCase() + normalized.slice(1);
}

function ruleLabel(ruleKey: string) {
  if (ruleKey === 'congress_cluster') {
    return 'Congress Cluster';
  }
  if (ruleKey === 'cross_source_accumulation') {
    return 'Cross-Source Cluster';
  }
  if (ruleKey === 'insider_cluster') {
    return 'Insider Cluster';
  }
  if (ruleKey === 'politician_gain_milestone') {
    return 'Politician Update';
  }
  if (ruleKey === 'cluster_gain_milestone') {
    return 'Cluster Update';
  }
  if (ruleKey === 'meaningful_insider_change') {
    return 'Meaningful Insider Change';
  }
  if (ruleKey === 'first_quantum_politician_buy') {
    return 'New Quantum Position';
  }
  return ruleKey
    .split('_')
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function storyIds(story: BroadcastStory) {
  return [story.channels.twitter?.id, story.channels.discord_premium?.id].filter((value): value is string => Boolean(value));
}

function storyChannels(story: BroadcastStory) {
  return [story.channels.twitter, story.channels.discord_premium].filter(
    (value): value is BroadcastStoryChannel => Boolean(value)
  );
}

function formatActorPreview(story: BroadcastStory) {
  if (!story.actorLabels.length) {
    return null;
  }
  return story.actorLabels.slice(0, 6).join(', ');
}

function amountRangeLabel(story: BroadcastStory) {
  if (!story.amountRanges.length) {
    return null;
  }
  if (story.amountRanges.length <= 3) {
    return story.amountRanges.join(', ');
  }
  return `${story.amountRanges.slice(0, 3).join(', ')}, +${story.amountRanges.length - 3} more`;
}

function tradeDateLabel(story: BroadcastStory) {
  const start = formatDate(story.tradeDateStart);
  const end = formatDate(story.tradeDateEnd);
  if (start && end) {
    return start === end ? start : `${start} to ${end}`;
  }
  return start || end || null;
}

function publishLabel(channel: string) {
  if (channel === 'twitter') {
    return 'Post To X';
  }
  if (channel === 'discord_premium') {
    return 'Send To Discord';
  }
  return `Send To ${channelLabel(channel)}`;
}

function channelBadgeClass(channel: string) {
  if (channel === 'twitter') {
    return 'border border-violet-500/20 bg-violet-500/10 text-violet-200';
  }
  if (channel === 'discord_premium') {
    return 'border border-emerald-500/20 bg-emerald-500/10 text-emerald-200';
  }
  return 'border border-white/10 bg-white/5 text-zinc-300';
}

export function TweetCandidateBoard() {
  const [status, setStatus] = useState<(typeof STATUS_OPTIONS)[number]>('pending_review');
  const [category, setCategory] = useState<(typeof CATEGORY_OPTIONS)[number]>('all');
  const [timeRange, setTimeRange] = useState<(typeof TIME_RANGE_OPTIONS)[number]>('week');
  const [sortBy, setSortBy] = useState<(typeof SORT_OPTIONS)[number]>('score');
  const [query, setQuery] = useState('');
  const deferredQuery = useDeferredValue(query);
  const [board, setBoard] = useState<TweetBoardResponse | null>(null);
  const [message, setMessage] = useState('');
  const [error, setError] = useState('');
  const [draftEdits, setDraftEdits] = useState<Record<string, string>>({});
  const [noteEdits, setNoteEdits] = useState<Record<string, string>>({});
  const [queueState, setQueueState] = useState<QueueState | null>(null);
  const [isPending, startTransition] = useTransition();

  const loadBoard = useCallback(
    async (
      nextStatus = status,
      nextTimeRange = timeRange,
      nextCategory = category,
      nextSort = sortBy,
      nextQuery = deferredQuery
    ) => {
      setError('');
      const searchParams = new URLSearchParams({
        status: nextStatus,
        window: nextTimeRange,
        category: nextCategory,
        sort: nextSort,
        limit: '80',
      });
      const cleanQuery = trim(nextQuery);
      if (cleanQuery) {
        searchParams.set('q', cleanQuery);
      }
      const response = await fetch(`/api/ops/tweet-candidates?${searchParams.toString()}`, {
        cache: 'no-store',
      });
      const payload = (await response.json()) as {
        ok?: boolean;
        enabled?: boolean;
        stories?: BroadcastStory[];
        error?: string;
      };
      if (!response.ok || !payload.ok) {
        throw new Error(payload.error || 'Failed to load broadcast stories.');
      }
      setBoard({
        enabled: Boolean(payload.enabled),
        stories: payload.stories || [],
      });
      setDraftEdits({});
      setNoteEdits({});
    },
    [status, timeRange, category, sortBy, deferredQuery]
  );

  useEffect(() => {
    startTransition(async () => {
      try {
        await loadBoard(status, timeRange, category, sortBy, deferredQuery);
      } catch (value) {
        setError(value instanceof Error ? value.message : 'Failed to load broadcast stories.');
      }
    });
  }, [loadBoard, status, timeRange, category, sortBy, deferredQuery]);

  const loadQueueState = useCallback(async () => {
    const response = await fetch('/api/ops/tweet-candidates/queue', {
      cache: 'no-store',
    });
    const payload = (await response.json()) as {
      ok?: boolean;
      state?: QueueState;
      error?: string;
    };
    if (!response.ok || !payload.ok) {
      throw new Error(payload.error || 'Failed to load queue state.');
    }
    const nextState = payload.state || { status: 'idle' as const };
    setQueueState(nextState);
    return nextState;
  }, []);

  useEffect(() => {
    startTransition(async () => {
      try {
        await loadQueueState();
      } catch {
        setQueueState({ status: 'idle' });
      }
    });
  }, [loadQueueState]);

  useEffect(() => {
    if (queueState?.status !== 'running') {
      return;
    }

    const timer = window.setInterval(async () => {
      try {
        const nextState = await loadQueueState();
        if (nextState.status === 'completed') {
          const upserted = nextState.summary?.tweet_candidates_upserted ?? 0;
          const deleted = nextState.summary?.tweet_candidates_deleted ?? 0;
          setMessage(`Broadcast queue finished. Upserted ${upserted} candidates and deleted ${deleted} stale rows.`);
          await loadBoard(status, timeRange, category, sortBy, deferredQuery);
        } else if (nextState.status === 'failed') {
          setError(nextState.stderr || 'Broadcast queue failed.');
        }
      } catch (value) {
        setError(value instanceof Error ? value.message : 'Failed to refresh queue state.');
      }
    }, 4000);

    return () => window.clearInterval(timer);
  }, [queueState?.status, loadQueueState, loadBoard, status, timeRange, category, sortBy, deferredQuery]);

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
          started?: boolean;
          running?: boolean;
          state?: QueueState;
        };
        if (!response.ok || !payload.ok) {
          throw new Error(payload.error || 'Failed to queue broadcast stories.');
        }
        const nextState = payload.state || { status: payload.running ? 'running' : 'idle' };
        setQueueState(nextState);

        if (payload.started === false && payload.running) {
          setMessage('Broadcast queue is already running in the background. The board will refresh when it finishes.');
        } else {
          setMessage('Broadcast queue started in the background. The board will refresh automatically when it finishes.');
        }
      } catch (value) {
        setError(value instanceof Error ? value.message : 'Failed to queue broadcast stories.');
      }
    });
  }

  function noteValue(story: BroadcastStory) {
    return noteEdits[story.candidateKey] ?? story.reviewNotes ?? '';
  }

  function draftValue(channel: BroadcastStoryChannel) {
    return draftEdits[channel.id] ?? channel.draftText;
  }

  function storyHasUnsavedChanges(story: BroadcastStory) {
    if (noteEdits[story.candidateKey] !== undefined && noteEdits[story.candidateKey] !== (story.reviewNotes ?? '')) {
      return true;
    }
    return storyChannels(story).some((channel) => draftEdits[channel.id] !== undefined && draftEdits[channel.id] !== channel.draftText);
  }

  async function persistStoryEdits(story: BroadcastStory) {
    const note = noteValue(story);
    for (const channel of storyChannels(story)) {
      const response = await fetch(`/api/ops/tweet-candidates/${channel.id}`, {
        method: 'PATCH',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          draft_text: draftValue(channel),
          review_notes: note,
        }),
      });
      const payload = (await response.json()) as { ok?: boolean; error?: string };
      if (!response.ok || !payload.ok) {
        throw new Error(payload.error || `Failed to save edits for ${channelLabel(channel.channel)}.`);
      }
    }
  }

  async function saveStoryEdits(story: BroadcastStory) {
    setMessage('');
    setError('');
    startTransition(async () => {
      try {
        await persistStoryEdits(story);
        setMessage(`Saved drafts for ${story.title}.`);
        await loadBoard(status, timeRange, category, sortBy, deferredQuery);
      } catch (value) {
        setError(value instanceof Error ? value.message : 'Failed to save story edits.');
      }
    });
  }

  async function reviewStory(story: BroadcastStory, nextStatus: 'approved' | 'rejected' | 'posted') {
    setMessage('');
    setError('');
    startTransition(async () => {
      try {
        await persistStoryEdits(story);
        for (const id of storyIds(story)) {
          const response = await fetch(`/api/ops/tweet-candidates/${id}`, {
            method: 'PATCH',
            headers: {
              'Content-Type': 'application/json',
            },
            body: JSON.stringify({
              status: nextStatus,
              reviewed_by: 'ops_ui',
              review_notes: noteValue(story),
            }),
          });
          const payload = (await response.json()) as { ok?: boolean; error?: string };
          if (!response.ok || !payload.ok) {
            throw new Error(payload.error || 'Failed to review broadcast story.');
          }
        }
        setMessage(`Story marked ${nextStatus}.`);
        await loadBoard(status, timeRange, category, sortBy, deferredQuery);
      } catch (value) {
        setError(value instanceof Error ? value.message : 'Failed to review broadcast story.');
      }
    });
  }

  async function publishStory(story: BroadcastStory, channels: BroadcastStoryChannel[]) {
    setMessage('');
    setError('');
    startTransition(async () => {
      const messages: string[] = [];
      const failures: string[] = [];
      try {
        await persistStoryEdits(story);
        for (const channel of channels) {
          try {
            const response = await fetch(`/api/ops/tweet-candidates/${channel.id}/publish`, {
              method: 'POST',
            });
            const payload = (await response.json()) as {
              ok?: boolean;
              error?: string;
              summary?: { candidates_posted?: number; x_auth_mode?: string | null } | null;
            };
            if (!response.ok || !payload.ok) {
              failures.push(payload.error || `Failed to publish to ${channelLabel(channel.channel)}.`);
              continue;
            }
            const postedCount = payload.summary?.candidates_posted ?? 0;
            const authMode = payload.summary?.x_auth_mode ? ` via ${payload.summary.x_auth_mode}` : '';
            messages.push(
              `Sent ${postedCount} candidate${postedCount === 1 ? '' : 's'} to ${channelLabel(channel.channel)}${
                channel.channel === 'twitter' ? authMode : ''
              }.`
            );
          } catch (value) {
            failures.push(value instanceof Error ? value.message : `Failed to publish to ${channelLabel(channel.channel)}.`);
          }
        }
        setMessage(messages.join(' '));
        setError(failures.join(' '));
        await loadBoard(status, timeRange, category, sortBy, deferredQuery);
      } catch (value) {
        setError(value instanceof Error ? value.message : 'Failed to publish story.');
      }
    });
  }

  return (
    <div className="space-y-6">
      <div className="glass-panel rounded-2xl p-5">
        <div className="flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between">
          <div>
            <div className="text-sm uppercase tracking-[0.18em] text-zinc-500">Broadcast Review</div>
            <div className="mt-2 text-xl font-semibold text-white">Quality insights ready for broadcast</div>
            <p className="mt-2 max-w-3xl text-sm text-[var(--text-secondary)]">
              Review by time period, narrow by source, verify the amount, then edit and send to X or Discord.
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
                  {statusLabel(value)}
                </option>
              ))}
            </select>
            <select
              value={sortBy}
              onChange={(event) => setSortBy(event.target.value as (typeof SORT_OPTIONS)[number])}
              className="rounded-xl border border-white/10 bg-[#0b1020] px-4 py-2 text-sm text-zinc-100 outline-none transition focus:border-blue-400/40"
            >
              {SORT_OPTIONS.map((value) => (
                <option key={value} value={value}>
                  {sortLabel(value)}
                </option>
              ))}
            </select>
            <button
              type="button"
              onClick={queueCandidates}
              disabled={isPending || queueState?.status === 'running'}
              className="inline-flex items-center gap-2 rounded-xl bg-blue-500 px-4 py-2 text-sm font-medium text-white transition hover:bg-blue-400 disabled:cursor-not-allowed disabled:opacity-60"
            >
              <RefreshCw className="h-4 w-4" />
              {queueState?.status === 'running' ? 'Queue Running...' : isPending ? 'Starting...' : 'Run Queue'}
            </button>
          </div>
        </div>

        <div className="mt-4 flex flex-wrap items-center gap-2">
          {TIME_RANGE_OPTIONS.map((value) => (
            <button
              key={value}
              type="button"
              onClick={() => setTimeRange(value)}
              disabled={isPending}
              className={`rounded-full px-3 py-1.5 text-sm transition ${
                timeRange === value
                  ? 'border border-cyan-400/30 bg-cyan-500/15 text-cyan-100'
                  : 'border border-white/10 bg-white/5 text-zinc-300 hover:border-white/20 hover:bg-white/10'
              } disabled:cursor-not-allowed disabled:opacity-60`}
            >
              {timeRangeLabel(value)}
            </button>
          ))}
        </div>

        <div className="mt-4 flex flex-col gap-3 lg:flex-row lg:items-center">
          <div className="flex flex-wrap items-center gap-2">
            {CATEGORY_OPTIONS.map((value) => (
              <button
                key={value}
                type="button"
                onClick={() => setCategory(value)}
                disabled={isPending}
                className={`rounded-full px-3 py-1.5 text-sm transition ${
                  category === value
                    ? 'border border-violet-400/30 bg-violet-500/15 text-violet-100'
                    : 'border border-white/10 bg-white/5 text-zinc-300 hover:border-white/20 hover:bg-white/10'
                } disabled:cursor-not-allowed disabled:opacity-60`}
              >
                {categoryLabel(value)}
              </button>
            ))}
          </div>

          <label className="relative block flex-1">
            <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-zinc-500" />
            <input
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Search ticker, actor, theme, committee, or copy"
              className="w-full rounded-xl border border-white/10 bg-[#0b1020] py-2 pl-10 pr-4 text-sm text-zinc-100 outline-none transition focus:border-blue-400/40"
            />
          </label>
        </div>

        {board?.enabled ? (
          <div className="mt-4 text-sm text-zinc-400">
            Showing <span className="font-medium text-white">{board.stories.length}</span> quality insights
            {queueState?.status === 'running' && queueState.started_at ? (
              <span>
                {' '}
                • Queue started {formatDateTime(queueState.started_at)}
                {queueState.current_step ? ` • ${queueState.current_step}` : ''}
                {typeof queueState.progress_percent === 'number' ? ` • ${Math.round(queueState.progress_percent)}%` : ''}
              </span>
            ) : null}
          </div>
        ) : null}

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
          Broadcast review is disabled because `tweet_candidates` has not been added to Supabase yet. Apply
          `supabase_vail_phase3_social.sql` when you are ready to turn on the review queue.
        </div>
      ) : null}

      {board && board.enabled ? (
        <div className="space-y-4">
          {board.stories.length === 0 ? (
            <div className="glass-panel rounded-2xl p-5 text-sm text-zinc-400">No stories in this view right now.</div>
          ) : (
            board.stories.map((story) => (
              <div key={story.candidateKey} className="glass-panel rounded-2xl p-5">
                <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
                  <div className="min-w-0 space-y-4">
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="inline-flex items-center gap-2 rounded-full border border-cyan-500/20 bg-cyan-500/10 px-2.5 py-1 text-xs font-medium text-cyan-300">
                        <Megaphone className="h-3.5 w-3.5" />
                        {ruleLabel(story.ruleKey)}
                      </span>
                      <span className="rounded-full border border-white/10 bg-white/5 px-2.5 py-1 text-xs text-zinc-300">
                        {categoryLabel(story.category)}
                      </span>
                      {storyChannels(story).map((channel) => (
                        <span key={channel.id} className={`rounded-full px-2.5 py-1 text-xs ${channelBadgeClass(channel.channel)}`}>
                          {channelLabel(channel.channel)}
                        </span>
                      ))}
                      <span className="rounded-full border border-white/10 bg-white/5 px-2.5 py-1 text-xs text-zinc-300">
                        Score {story.score.toFixed(2)}
                      </span>
                    </div>

                    <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_240px]">
                      <div className="min-w-0">
                        <div className="text-xl font-semibold text-white">{story.title}</div>
                        <div className="mt-2 flex flex-wrap items-center gap-2 text-xs text-zinc-400">
                          {story.ticker ? (
                            <span className="rounded-full border border-blue-400/20 bg-blue-500/10 px-2.5 py-1 text-blue-100">
                              {story.ticker}
                            </span>
                          ) : null}
                          {directionLabel(story.direction) ? (
                            <span className="rounded-full border border-white/10 bg-white/5 px-2.5 py-1">
                              {directionLabel(story.direction)}
                            </span>
                          ) : null}
                          <span className="rounded-full border border-white/10 bg-white/5 px-2.5 py-1">
                            {story.actorCount} actor{story.actorCount === 1 ? '' : 's'}
                          </span>
                          <span className="rounded-full border border-white/10 bg-white/5 px-2.5 py-1">
                            {sourceMixLabel(story)}
                          </span>
                          {tradeDateLabel(story) ? (
                            <span className="rounded-full border border-white/10 bg-white/5 px-2.5 py-1">
                              Trade {tradeDateLabel(story)}
                            </span>
                          ) : null}
                          {story.latestPublishedAt ? (
                            <span className="rounded-full border border-white/10 bg-white/5 px-2.5 py-1">
                              Filed {formatDate(story.latestPublishedAt)}
                            </span>
                          ) : null}
                        </div>
                        <div className="mt-4 rounded-2xl border border-white/10 bg-[#0b1020]/70 p-4">
                          <div className="text-xs uppercase tracking-[0.16em] text-zinc-500">Evidence</div>
                          <div className="mt-2 text-sm leading-6 text-zinc-100">
                            {formatActorPreview(story) || story.actorName || 'No actor summary yet'}
                          </div>
                          <div className="mt-3 text-sm leading-6 text-zinc-300">
                            {story.rationale || 'Policy-selected broadcast candidate.'}
                          </div>
                          {Boolean(story.committees.length || story.themes.length) && (
                            <div className="mt-3 flex flex-wrap items-center gap-2 text-xs text-zinc-400">
                              {story.committees.slice(0, 3).map((committee) => (
                                <span key={committee} className="rounded-full border border-white/10 bg-white/5 px-2.5 py-1">
                                  {committee}
                                </span>
                              ))}
                              {story.themes.slice(0, 4).map((theme) => (
                                <span key={theme} className="rounded-full border border-white/10 bg-white/5 px-2.5 py-1">
                                  {theme}
                                </span>
                              ))}
                            </div>
                          )}
                        </div>
                      </div>

                      <div className="grid gap-3 rounded-2xl border border-white/10 bg-[#0b1020]/70 p-4 sm:grid-cols-2 lg:grid-cols-1">
                        <div>
                          <div className="text-xs uppercase tracking-[0.16em] text-zinc-500">Amount</div>
                          <div className="mt-2 text-2xl font-semibold text-white">{story.amountLabel || 'Unknown'}</div>
                          <div className="mt-1 text-xs text-zinc-500">Aggregated lower bound across the supporting events.</div>
                          {amountRangeLabel(story) ? (
                            <div className="mt-2 text-xs leading-5 text-zinc-300">
                              Disclosed ranges: <span className="text-zinc-100">{amountRangeLabel(story)}</span>
                            </div>
                          ) : null}
                          {tradeDateLabel(story) ? (
                            <div className="mt-1 text-xs leading-5 text-zinc-400">
                              Trade dates: <span className="text-zinc-200">{tradeDateLabel(story)}</span>
                            </div>
                          ) : null}
                        </div>
                        <div className="grid grid-cols-2 gap-3">
                          <div>
                            <div className="text-xs uppercase tracking-[0.16em] text-zinc-500">Actors</div>
                            <div className="mt-2 text-lg font-semibold text-white">{story.actorCount}</div>
                          </div>
                          <div>
                            <div className="text-xs uppercase tracking-[0.16em] text-zinc-500">Source Mix</div>
                            <div className="mt-2 text-sm text-zinc-100">{sourceMixLabel(story)}</div>
                          </div>
                          <div>
                            <div className="text-xs uppercase tracking-[0.16em] text-zinc-500">Ticker</div>
                            <div className="mt-2 text-sm font-medium text-zinc-100">{story.ticker || 'Unknown'}</div>
                          </div>
                          <div>
                            <div className="text-xs uppercase tracking-[0.16em] text-zinc-500">Filed</div>
                            <div className="mt-2 text-sm text-zinc-100">{formatDate(story.latestPublishedAt) || 'Unknown'}</div>
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>

                  <div className="flex shrink-0 flex-wrap gap-2 xl:max-w-md xl:justify-end">
                    <button
                      type="button"
                      onClick={() => saveStoryEdits(story)}
                      disabled={isPending || !storyHasUnsavedChanges(story)}
                      className="inline-flex items-center gap-2 rounded-xl border border-white/10 bg-white/5 px-4 py-2 text-sm font-medium text-zinc-100 transition hover:border-white/20 hover:bg-white/10 disabled:cursor-not-allowed disabled:opacity-60"
                    >
                      <Save className="h-4 w-4" />
                      Save Drafts
                    </button>
                    {status === 'pending_review' ? (
                      <>
                        <button
                          type="button"
                          onClick={() => reviewStory(story, 'approved')}
                          disabled={isPending}
                          className="inline-flex items-center gap-2 rounded-xl bg-emerald-500 px-4 py-2 text-sm font-medium text-white transition hover:bg-emerald-400 disabled:cursor-not-allowed disabled:opacity-60"
                        >
                          <CheckCircle2 className="h-4 w-4" />
                          Approve
                        </button>
                        <button
                          type="button"
                          onClick={() => reviewStory(story, 'rejected')}
                          disabled={isPending}
                          className="inline-flex items-center gap-2 rounded-xl bg-red-500 px-4 py-2 text-sm font-medium text-white transition hover:bg-red-400 disabled:cursor-not-allowed disabled:opacity-60"
                        >
                          <XCircle className="h-4 w-4" />
                          Reject
                        </button>
                      </>
                    ) : null}
                    {status === 'approved' ? (
                      <>
                        {storyChannels(story).length > 1 ? (
                          <button
                            type="button"
                            onClick={() => publishStory(story, storyChannels(story))}
                            disabled={isPending}
                            className="inline-flex items-center gap-2 rounded-xl border border-cyan-400/30 bg-cyan-500/10 px-4 py-2 text-sm font-medium text-cyan-100 transition hover:border-cyan-300/40 hover:bg-cyan-500/20 disabled:cursor-not-allowed disabled:opacity-60"
                          >
                            <Send className="h-4 w-4" />
                            Send All
                          </button>
                        ) : null}
                        {storyChannels(story).map((channel) => (
                          <button
                            key={channel.id}
                            type="button"
                            onClick={() => publishStory(story, [channel])}
                            disabled={isPending}
                            className={`inline-flex items-center gap-2 rounded-xl px-4 py-2 text-sm font-medium text-white transition disabled:cursor-not-allowed disabled:opacity-60 ${
                              channel.channel === 'twitter'
                                ? 'bg-blue-500 hover:bg-blue-400'
                                : 'bg-emerald-500 hover:bg-emerald-400'
                            }`}
                          >
                            <Send className="h-4 w-4" />
                            {publishLabel(channel.channel)}
                          </button>
                        ))}
                        <button
                          type="button"
                          onClick={() => reviewStory(story, 'posted')}
                          disabled={isPending}
                          className="inline-flex items-center gap-2 rounded-xl border border-white/10 bg-white/5 px-4 py-2 text-sm font-medium text-zinc-100 transition hover:border-white/20 hover:bg-white/10 disabled:cursor-not-allowed disabled:opacity-60"
                        >
                          <CheckCircle2 className="h-4 w-4" />
                          Mark Posted Manually
                        </button>
                      </>
                    ) : null}
                    {story.sourceUrl ? (
                      <a
                        href={story.sourceUrl}
                        target="_blank"
                        rel="noreferrer"
                        className="inline-flex items-center gap-2 rounded-xl border border-white/10 bg-white/5 px-4 py-2 text-sm text-blue-300 transition hover:border-white/20 hover:bg-white/10 hover:text-blue-200"
                      >
                        Open source filing
                      </a>
                    ) : null}
                  </div>
                </div>

                <div className="mt-5 grid gap-4 xl:grid-cols-2">
                  {storyChannels(story).map((channel) => (
                    <div key={channel.id} className="rounded-2xl border border-white/10 bg-[#0b1020]/80 p-4">
                      <div className="flex flex-wrap items-center justify-between gap-2">
                        <div className="text-sm font-medium text-white">{channelLabel(channel.channel)} Draft</div>
                        <div className="text-xs text-zinc-500">
                          {channel.postedAt ? `Posted ${formatDateTime(channel.postedAt)}` : `Score ${channel.score.toFixed(2)}`}
                        </div>
                      </div>
                      <textarea
                        value={draftValue(channel)}
                        onChange={(event) =>
                          setDraftEdits((current) => ({
                            ...current,
                            [channel.id]: event.target.value,
                          }))
                        }
                        rows={6}
                        className="mt-3 w-full rounded-2xl border border-white/10 bg-[#060914] px-4 py-3 font-mono text-sm leading-6 text-zinc-100 outline-none transition focus:border-blue-400/40"
                      />
                      {channel.externalPostId ? (
                        <div className="mt-2 text-xs text-zinc-500">External post id: {channel.externalPostId}</div>
                      ) : null}
                    </div>
                  ))}
                </div>

                <div className="mt-4 rounded-2xl border border-white/10 bg-[#0b1020]/70 p-4">
                  <div className="flex items-center justify-between gap-2">
                    <div className="text-sm font-medium text-white">Editorial Notes</div>
                    {storyHasUnsavedChanges(story) ? (
                      <span className="text-xs text-amber-200">Unsaved edits</span>
                    ) : (
                      <span className="text-xs text-zinc-500">Saved</span>
                    )}
                  </div>
                  <textarea
                    value={noteValue(story)}
                    onChange={(event) =>
                      setNoteEdits((current) => ({
                        ...current,
                        [story.candidateKey]: event.target.value,
                      }))
                    }
                    rows={3}
                    placeholder="Add curation notes, caveats, or post guidance"
                    className="mt-3 w-full rounded-2xl border border-white/10 bg-[#060914] px-4 py-3 text-sm leading-6 text-zinc-100 outline-none transition focus:border-blue-400/40"
                  />
                </div>

                <div className="mt-4 text-xs text-zinc-500">
                  Created {formatDateTime(story.createdAt)}
                  {story.reviewedAt ? ` • Reviewed ${formatDateTime(story.reviewedAt)}` : ''}
                  {story.postedAt ? ` • Posted ${formatDateTime(story.postedAt)}` : ''}
                </div>
              </div>
            ))
          )}
        </div>
      ) : null}
    </div>
  );
}
