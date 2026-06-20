import { readFile } from 'node:fs/promises';
import path from 'node:path';

import type { Metadata } from 'next';
import Link from 'next/link';
import type { ReactNode } from 'react';
import {
  Activity,
  AlertTriangle,
  ArrowRight,
  Bell,
  GitBranch,
  Megaphone,
  Radio,
  ShieldCheck,
  XCircle,
} from 'lucide-react';

import { formatDateTimeValue } from '@/lib/date-format';
import { getAdminSupabase } from '@/lib/supabase-admin';

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

export const metadata: Metadata = {
  title: 'Vail Mission Control',
  description: 'Private Vail pipeline and broadcast mission control.',
  robots: {
    index: false,
    follow: false,
  },
};

type JsonRecord = Record<string, unknown>;

type ScraperRun = {
  id: string;
  parent_run_id: string | null;
  scraper_name: string;
  source_name: string;
  mode: string;
  status: string;
  started_at: string;
  finished_at: string | null;
  duration_ms: number | null;
  records_seen: number | null;
  records_inserted: number | null;
  records_updated: number | null;
  records_skipped: number | null;
  signal_events_created: number | null;
  error_count: number | null;
  stdout_excerpt: string | null;
  stderr_excerpt: string | null;
  run_metadata: JsonRecord | null;
};

type ScraperError = {
  id: string;
  run_id: string | null;
  stage: string;
  severity: string;
  message: string;
  created_at: string;
  details: JsonRecord | null;
};

type BroadcastCandidate = {
  id: string;
  channel: string;
  status: string;
  score: number | string | null;
  title: string;
  created_at: string;
  posted_at: string | null;
};

type QueueState = {
  status: 'idle' | 'running' | 'completed' | 'failed';
  started_at?: string;
  finished_at?: string;
  current_step?: string;
  progress_percent?: number;
  stderr?: string | null;
  summary?: JsonRecord | null;
};

const RUN_SELECT =
  'id,parent_run_id,scraper_name,source_name,mode,status,started_at,finished_at,duration_ms,records_seen,records_inserted,records_updated,records_skipped,signal_events_created,error_count,stdout_excerpt,stderr_excerpt,run_metadata';

const QUEUE_STATE_PATH = path.join(process.cwd(), 'artifacts', 'ops', 'tweet_candidate_queue_status.json');

const BROADCAST_CHANNELS = [
  { key: 'twitter', label: 'X' },
  { key: 'discord_premium', label: 'Discord' },
];

function asRecord(value: unknown): JsonRecord {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return {};
  }
  return value as JsonRecord;
}

function formatDateTime(value: string | null | undefined) {
  return formatDateTimeValue(value);
}

function formatDuration(durationMs: number | null | undefined) {
  if (!durationMs || durationMs < 1000) {
    return 'Under 1s';
  }

  const totalSeconds = Math.round(durationMs / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;

  if (!minutes) {
    return `${seconds}s`;
  }
  if (!seconds) {
    return `${minutes}m`;
  }
  return `${minutes}m ${seconds}s`;
}

function runTime(run: ScraperRun | null | undefined) {
  return run?.finished_at || run?.started_at || null;
}

function isFailureStatus(status: string | null | undefined) {
  const normalized = String(status || '').toLowerCase();
  return normalized === 'failed' || normalized === 'timed_out';
}

function isHealthyStatus(status: string | null | undefined) {
  const normalized = String(status || '').toLowerCase();
  return normalized === 'success' || normalized === 'completed' || normalized === 'warning' || normalized === 'skipped';
}

function statusTone(status: string | null | undefined) {
  switch ((status || '').toLowerCase()) {
    case 'success':
    case 'completed':
      return 'border-emerald-500/20 bg-emerald-500/10 text-emerald-300';
    case 'warning':
      return 'border-amber-500/20 bg-amber-500/10 text-amber-300';
    case 'skipped':
      return 'border-white/[0.08] bg-white/[0.03] text-zinc-400';
    case 'failed':
    case 'timed_out':
      return 'border-red-500/20 bg-red-500/10 text-red-300';
    case 'running':
      return 'border-cyan-500/20 bg-cyan-500/10 text-cyan-300';
    case 'idle':
      return 'border-white/[0.08] bg-white/[0.03] text-zinc-400';
    default:
      return 'border-amber-500/20 bg-amber-500/10 text-amber-300';
  }
}

function summarizeRun(run: ScraperRun) {
  const metadata = asRecord(run.run_metadata);
  const results = asRecord(metadata.results);
  const resultEntries = Object.entries(results);

  if (resultEntries.length > 0) {
    const passed = resultEntries.filter(([, value]) => Boolean(value)).length;
    const failed = resultEntries
      .filter(([, value]) => !value)
      .map(([key]) => key);

    if (failed.length) {
      return `${passed}/${resultEntries.length} stages passed; failed: ${failed.slice(0, 3).join(', ')}${failed.length > 3 ? '...' : ''}`;
    }
    return `${passed}/${resultEntries.length} stages passed`;
  }

  const facts = [
    run.records_seen != null ? `${run.records_seen.toLocaleString()} seen` : null,
    run.records_inserted != null ? `${run.records_inserted.toLocaleString()} inserted` : null,
    run.records_updated != null ? `${run.records_updated.toLocaleString()} updated` : null,
    run.signal_events_created != null ? `${run.signal_events_created.toLocaleString()} signals` : null,
    run.error_count ? `${run.error_count.toLocaleString()} errors` : null,
  ].filter(Boolean);

  return facts.length ? facts.join(' / ') : run.source_name;
}

function firstUsefulLine(value: string | null | undefined) {
  return String(value || '')
    .split('\n')
    .map((line) => line.trim())
    .find(Boolean) || null;
}

function failureHint(run: ScraperRun, errorsByRunId: Map<string, ScraperError[]>) {
  const directError = errorsByRunId.get(run.id)?.[0];
  if (directError) {
    return directError.message;
  }
  return firstUsefulLine(run.stderr_excerpt) || firstUsefulLine(run.stdout_excerpt);
}

function numericMetadata(run: ScraperRun, key: string) {
  const value = asRecord(run.run_metadata)[key];
  if (typeof value === 'number') return value;
  if (typeof value === 'string') {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }
  return 0;
}

function isConfigBlockedRun(run: ScraperRun) {
  const combinedOutput = `${run.stdout_excerpt || ''}\n${run.stderr_excerpt || ''}`.toLowerCase();
  return numericMetadata(run, 'deliveries_blocked_config') > 0 || combinedOutput.includes('blocked: missing');
}

function isParseWarningRun(run: ScraperRun) {
  return numericMetadata(run, 'parse_failures') > 0 || numericMetadata(run, 'carryover_parse_failures') > 0;
}

function classifyRunIssue(run: ScraperRun, errorsByRunId: Map<string, ScraperError[]>) {
  if (isConfigBlockedRun(run)) {
    const channel = run.scraper_name.includes('sms') ? 'Text' : run.scraper_name.includes('email') ? 'Email' : 'Delivery';
    return {
      tone: 'amber',
      label: 'Setup',
      title: `${channel} delivery is not configured`,
      detail:
        channel === 'Text'
          ? 'Add TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, and TWILIO_FROM_PHONE to send text alerts, or keep text off without failing the pipeline.'
          : 'Add RESEND_API_KEY and RESEND_FROM_EMAIL to send email alerts, or keep email off without failing the pipeline.',
    };
  }

  if (isParseWarningRun(run)) {
    const failedDocs = asRecord(run.run_metadata).failed_doc_ids;
    const docCount = Array.isArray(failedDocs) ? failedDocs.length : numericMetadata(run, 'parse_failures');
    return {
      tone: 'amber',
      label: 'Parser',
      title: `${run.scraper_name} has ${docCount || 'some'} document${docCount === 1 ? '' : 's'} to review`,
      detail: summarizeRun(run),
    };
  }

  if (String(run.status).toLowerCase() === 'timed_out') {
    return {
      tone: 'red',
      label: 'Timeout',
      title: `${run.scraper_name} exceeded its run window`,
      detail: failureHint(run, errorsByRunId) || summarizeRun(run),
    };
  }

  return {
    tone: isFailureStatus(run.status) ? 'red' : 'amber',
    label: isFailureStatus(run.status) ? 'Failure' : 'Warning',
    title: run.scraper_name,
    detail: failureHint(run, errorsByRunId) || summarizeRun(run),
  };
}

function issueToneClasses(tone: string) {
  switch (tone) {
    case 'red':
      return {
        card: 'border-red-500/15 bg-red-500/[0.06]',
        icon: 'bg-red-500/10 text-red-300',
        pill: 'border-red-500/20 bg-red-500/10 text-red-200',
      };
    case 'amber':
      return {
        card: 'border-amber-500/15 bg-amber-500/[0.06]',
        icon: 'bg-amber-500/10 text-amber-300',
        pill: 'border-amber-500/20 bg-amber-500/10 text-amber-200',
      };
    default:
      return {
        card: 'border-white/[0.06] bg-black/20',
        icon: 'bg-white/[0.04] text-zinc-300',
        pill: 'border-white/[0.08] bg-white/[0.04] text-zinc-300',
      };
  }
}

function countCandidates(candidates: BroadcastCandidate[], channel: string, status: string) {
  return candidates.filter((candidate) => candidate.channel === channel && candidate.status === status).length;
}

function postingConfig() {
  const xApiConfigured = Boolean(
    process.env.X_API_KEY &&
      process.env.X_API_SECRET &&
      ((process.env.X_ACCESS_TOKEN && process.env.X_ACCESS_TOKEN_SECRET) || process.env.X_USER_ACCESS_TOKEN),
  );

  return {
    twitterEnabled: ['1', 'true', 'yes', 'on'].includes(String(process.env.TWITTER_POSTING_ENABLED || '').toLowerCase()),
    twitterConfigured: xApiConfigured,
    discordConfigured: Boolean(process.env.DISCORD_GLOBAL_WEBHOOK_URL || process.env.DISCORD_WEBHOOK_URL),
  };
}

async function readQueueState(): Promise<QueueState> {
  try {
    const raw = await readFile(QUEUE_STATE_PATH, 'utf8');
    return JSON.parse(raw) as QueueState;
  } catch {
    return { status: 'idle' };
  }
}

async function getMissionControlData() {
  const supabase = getAdminSupabase();
  const candidatesPromise = (async () => {
    try {
      const response = await supabase
        .from('tweet_candidates')
        .select('id,channel,status,score,title,created_at,posted_at')
        .order('created_at', { ascending: false })
        .limit(120);

      return { response, error: response.error || null };
    } catch (error) {
      return { response: null, error };
    }
  })();

  const [recentRunsRes, recentErrorsRes, candidatesResult, queueState] = await Promise.all([
    supabase
      .from('scraper_runs')
      .select(RUN_SELECT)
      .order('started_at', { ascending: false })
      .limit(60),
    supabase
      .from('scraper_errors')
      .select('id,run_id,stage,severity,message,created_at,details')
      .order('created_at', { ascending: false })
      .limit(20),
    candidatesPromise,
    readQueueState(),
  ]);

  if (recentRunsRes.error) {
    throw recentRunsRes.error;
  }
  if (recentErrorsRes.error) {
    throw recentErrorsRes.error;
  }

  const recentRuns = (recentRunsRes.data || []) as ScraperRun[];
  const recentErrors = (recentErrorsRes.data || []) as ScraperError[];
  const broadcastCandidatesEnabled = !candidatesResult.error && !candidatesResult.response?.error;
  const broadcastCandidates = broadcastCandidatesEnabled
    ? ((candidatesResult.response?.data || []) as BroadcastCandidate[])
    : [];

  const latestPipeline =
    recentRuns.find((run) => ['core_product_pipeline', 'daily_pipeline'].includes(run.scraper_name)) ||
    recentRuns.find((run) => run.source_name === 'orchestrator') ||
    null;

  let latestPipelineChildren: ScraperRun[] = [];
  if (latestPipeline?.id) {
    const childrenRes = await supabase
      .from('scraper_runs')
      .select(RUN_SELECT)
      .eq('parent_run_id', latestPipeline.id)
      .order('started_at', { ascending: true });

    if (!childrenRes.error) {
      latestPipelineChildren = (childrenRes.data || []) as ScraperRun[];
    }
  }

  let deliveryPending = 0;
  let deliveryFailed = 0;
  try {
    const [pendingRes, failedRes] = await Promise.all([
      supabase.from('alert_deliveries').select('id', { count: 'exact', head: true }).eq('status', 'pending'),
      supabase.from('alert_deliveries').select('id', { count: 'exact', head: true }).eq('status', 'failed'),
    ]);
    deliveryPending = pendingRes.count || 0;
    deliveryFailed = failedRes.count || 0;
  } catch {
    // Optional tables may not exist on early environments.
  }

  return {
    recentRuns,
    recentErrors,
    latestPipeline,
    latestPipelineChildren,
    broadcastCandidates,
    broadcastCandidatesEnabled,
    queueState,
    deliveryPending,
    deliveryFailed,
  };
}

function MetricCard({
  icon,
  label,
  value,
  sublabel,
  tone = 'text-cyan-300',
}: {
  icon: ReactNode;
  label: string;
  value: string;
  sublabel: string;
  tone?: string;
}) {
  return (
    <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-4">
      <div className={`mb-4 inline-flex h-9 w-9 items-center justify-center rounded-xl bg-white/[0.04] ${tone}`}>
        {icon}
      </div>
      <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-zinc-600">{label}</div>
      <div className="mt-2 text-2xl font-semibold tracking-tight text-white">{value}</div>
      <div className="mt-2 text-sm leading-5 text-zinc-500">{sublabel}</div>
    </div>
  );
}

function StatusPill({ status }: { status: string | null | undefined }) {
  return (
    <span className={`inline-flex shrink-0 rounded-full border px-2.5 py-1 text-[11px] font-semibold capitalize ${statusTone(status)}`}>
      {status || 'unknown'}
    </span>
  );
}

function ActionLink({ href, children }: { href: string; children: ReactNode }) {
  return (
    <Link
      href={href}
      className="inline-flex items-center gap-2 rounded-xl border border-white/[0.08] bg-white/[0.03] px-3 py-2 text-sm font-medium text-zinc-200 transition hover:border-white/[0.16] hover:bg-white/[0.06] hover:text-white"
    >
      {children}
      <ArrowRight className="h-3.5 w-3.5" />
    </Link>
  );
}

export default async function OpsPage() {
  try {
    const {
      recentRuns,
      recentErrors,
      latestPipeline,
      latestPipelineChildren,
      broadcastCandidates,
      broadcastCandidatesEnabled,
      queueState,
      deliveryPending,
      deliveryFailed,
    } = await getMissionControlData();

    const activeRuns = recentRuns.filter((run) => run.status === 'running').length;
    const failedRuns = recentRuns.filter((run) => isFailureStatus(run.status));
    const runsById = new Map(recentRuns.map((run) => [run.id, run]));
    const errorsByRunId = new Map<string, ScraperError[]>();
    for (const error of recentErrors) {
      if (!error.run_id) continue;
      const existing = errorsByRunId.get(error.run_id) || [];
      existing.push(error);
      errorsByRunId.set(error.run_id, existing);
    }

    const latestFailedRequiredSteps = Object.entries(asRecord(latestPipeline?.run_metadata).results || {})
      .filter(([, value]) => !value)
      .map(([key]) => key);

    const latestChildIssues = latestPipelineChildren.filter(
      (run) => !isHealthyStatus(run.status) || isConfigBlockedRun(run) || isParseWarningRun(run),
    );
    const latestSuccessfulChildren = latestPipelineChildren.filter((run) => isHealthyStatus(run.status) && !isConfigBlockedRun(run));
    const seenIssueNames = new Set<string>();
    const actionItems = (latestChildIssues.length ? latestChildIssues : failedRuns)
      .filter((run) => {
        const key = run.scraper_name;
        if (seenIssueNames.has(key)) return false;
        seenIssueNames.add(key);
        return true;
      })
      .slice(0, 6)
      .map((run) => ({ run, issue: classifyRunIssue(run, errorsByRunId) }));
    const hardActionCount = actionItems.filter(({ issue }) => issue.tone === 'red').length;
    const warningActionCount = actionItems.filter(({ issue }) => issue.tone !== 'red').length;
    const recentHardFailures = recentRuns.filter(
      (run) => isFailureStatus(run.status) && !isConfigBlockedRun(run) && !isParseWarningRun(run),
    );
    const visibleRecentErrors = recentErrors.filter((error) => {
      const run = error.run_id ? runsById.get(error.run_id) : null;
      return !run || !isConfigBlockedRun(run);
    });
    const pendingReview = broadcastCandidates.filter((candidate) => candidate.status === 'pending_review').length;
    const approvedBroadcasts = broadcastCandidates.filter((candidate) => candidate.status === 'approved').length;
    const config = postingConfig();
    const overallStatus = activeRuns
      ? 'Running'
      : hardActionCount > 0
        ? 'Needs Fix'
        : warningActionCount > 0
          ? 'Warnings'
          : 'Healthy';

    return (
      <div className="mx-auto max-w-[1220px] space-y-5 px-4 py-6 sm:px-6 lg:px-8">
        <section className="flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
          <div>
            <div className="inline-flex items-center gap-2 rounded-full border border-cyan-500/20 bg-cyan-500/10 px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.2em] text-cyan-300">
              <ShieldCheck className="h-3.5 w-3.5" />
              Private Ops
            </div>
            <h1 className="mt-4 text-3xl font-semibold tracking-tight text-white md:text-4xl">Mission Control</h1>
            <p className="mt-2 max-w-2xl text-sm leading-6 text-zinc-500">
              Private owner view for pipeline health, failures, alerts, and broadcast review.
            </p>
          </div>

          <div className="flex flex-wrap gap-2">
            <ActionLink href="/ops/social">Broadcast Control</ActionLink>
            <ActionLink href="/ops/clusters">Cluster Ops</ActionLink>
            <ActionLink href="/ops/policy">Policy</ActionLink>
          </div>
        </section>

        <section className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
          <MetricCard
            icon={<Activity className="h-5 w-5" />}
            label="System"
            value={overallStatus}
            sublabel={`${activeRuns} running / ${hardActionCount} fix / ${warningActionCount} review`}
            tone={
              overallStatus === 'Healthy'
                ? 'text-emerald-300'
                : overallStatus === 'Running'
                  ? 'text-cyan-300'
                  : overallStatus === 'Warnings'
                    ? 'text-amber-300'
                    : 'text-red-300'
            }
          />
          <MetricCard
            icon={<GitBranch className="h-5 w-5" />}
            label="Latest Pipeline"
            value={latestPipeline?.status || 'Unknown'}
            sublabel={`${formatDateTime(runTime(latestPipeline))} / ${formatDuration(latestPipeline?.duration_ms)}`}
            tone={
              isFailureStatus(latestPipeline?.status)
                ? 'text-red-300'
                : String(latestPipeline?.status || '').toLowerCase() === 'warning'
                  ? 'text-amber-300'
                  : 'text-emerald-300'
            }
          />
          <MetricCard
            icon={<Bell className="h-5 w-5" />}
            label="Alert Queue"
            value={`${deliveryPending}/${deliveryFailed}`}
            sublabel="pending / failed user deliveries"
            tone={deliveryFailed ? 'text-red-300' : 'text-amber-300'}
          />
          <MetricCard
            icon={<Megaphone className="h-5 w-5" />}
            label="Broadcast"
            value={broadcastCandidatesEnabled ? `${pendingReview}/${approvedBroadcasts}` : 'Off'}
            sublabel={broadcastCandidatesEnabled ? 'pending review / approved posts' : 'tweet_candidates table unavailable'}
            tone={approvedBroadcasts || pendingReview ? 'text-cyan-300' : 'text-zinc-400'}
          />
        </section>

        <section className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-5">
          <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
            <div>
              <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-zinc-600">Priority</div>
              <h2 className="mt-1 text-lg font-semibold text-white">What needs action</h2>
            </div>
            <div className="text-xs text-zinc-600">
              {hardActionCount} fix / {warningActionCount} setup or parser review
            </div>
          </div>

          {actionItems.length ? (
            <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
              {actionItems.map(({ run, issue }) => {
                const classes = issueToneClasses(issue.tone);
                return (
                  <div key={run.id} className={`rounded-xl border p-4 ${classes.card}`}>
                    <div className="flex items-start justify-between gap-3">
                      <div className={`rounded-lg px-2 py-1 text-[10px] font-semibold uppercase tracking-[0.16em] ${classes.icon}`}>
                        {issue.label}
                      </div>
                      <div className="truncate text-[11px] text-zinc-600">{run.scraper_name}</div>
                    </div>
                    <div className="mt-3 text-sm font-semibold leading-5 text-white">{issue.title}</div>
                    <div className="mt-2 line-clamp-3 text-xs leading-5 text-zinc-400">{issue.detail}</div>
                    <div className="mt-3 text-[11px] text-zinc-600">{formatDateTime(run.started_at)}</div>
                  </div>
                );
              })}
            </div>
          ) : (
            <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/10 p-4 text-sm text-emerald-200">
              No action items in the latest pipeline run.
            </div>
          )}
        </section>

        <section className="grid gap-4 xl:grid-cols-[1.15fr_0.85fr]">
          <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-5">
            <div className="mb-4 flex flex-wrap items-start justify-between gap-3">
              <div>
                <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-zinc-600">Latest Run</div>
                <h2 className="mt-1 text-lg font-semibold text-white">{latestPipeline?.scraper_name || 'No pipeline recorded'}</h2>
              </div>
              <StatusPill status={latestPipeline?.status} />
            </div>

            <div className="grid gap-3 sm:grid-cols-3">
              <div className="rounded-xl border border-white/[0.06] bg-black/20 p-3">
                <div className="text-[10px] uppercase tracking-[0.18em] text-zinc-600">Started</div>
                <div className="mt-2 text-sm text-white">{formatDateTime(latestPipeline?.started_at)}</div>
              </div>
              <div className="rounded-xl border border-white/[0.06] bg-black/20 p-3">
                <div className="text-[10px] uppercase tracking-[0.18em] text-zinc-600">Duration</div>
                <div className="mt-2 text-sm text-white">{formatDuration(latestPipeline?.duration_ms)}</div>
              </div>
              <div className="rounded-xl border border-white/[0.06] bg-black/20 p-3">
                <div className="text-[10px] uppercase tracking-[0.18em] text-zinc-600">Signals</div>
                <div className="mt-2 text-sm text-white">{latestPipeline?.signal_events_created?.toLocaleString() || '0'}</div>
              </div>
            </div>

            <div className="mt-4 rounded-xl border border-white/[0.06] bg-black/20 p-3">
              <div className="mb-3 flex items-center justify-between gap-3">
                <div className="text-[10px] uppercase tracking-[0.18em] text-zinc-600">Stage Check</div>
                <div className="text-xs text-zinc-500">
                  {latestChildIssues.length} issue / {latestSuccessfulChildren.length} clear
                </div>
              </div>

              {latestChildIssues.length > 0 ? (
                <div className="space-y-2">
                  {latestChildIssues.map((run) => {
                    const issue = classifyRunIssue(run, errorsByRunId);
                    const classes = issueToneClasses(issue.tone);
                    return (
                      <div key={run.id} className={`rounded-lg border px-3 py-2 ${classes.card}`}>
                        <div className="flex items-start justify-between gap-3">
                          <div className="min-w-0">
                            <div className="truncate text-xs font-semibold text-white">{issue.title}</div>
                            <div className="mt-1 line-clamp-2 text-[11px] leading-4 text-zinc-500">{issue.detail}</div>
                          </div>
                          <span className={`inline-flex shrink-0 rounded-full border px-2 py-0.5 text-[10px] font-semibold ${classes.pill}`}>
                            {issue.label}
                          </span>
                        </div>
                      </div>
                    );
                  })}
                </div>
              ) : latestFailedRequiredSteps.length ? (
                <div className="flex flex-wrap gap-2">
                  {latestFailedRequiredSteps.map((stage) => (
                    <span key={stage} className="rounded-full border border-red-500/20 bg-red-500/10 px-2.5 py-1 text-xs font-semibold text-red-200">
                      {stage}
                    </span>
                  ))}
                </div>
              ) : (
                <div className="text-sm text-zinc-500">No child-stage detail recorded for the latest pipeline.</div>
              )}

              {latestSuccessfulChildren.length > 0 ? (
                <details className="mt-3 rounded-lg border border-white/[0.05] bg-white/[0.02] px-3 py-2">
                  <summary className="cursor-pointer text-xs font-medium text-zinc-400">
                    Show {latestSuccessfulChildren.length} clear stages
                  </summary>
                  <div className="mt-3 grid gap-2 md:grid-cols-2">
                    {latestSuccessfulChildren.map((run) => (
                      <div key={run.id} className="flex items-center justify-between gap-3 rounded-lg bg-black/20 px-3 py-2">
                        <div className="min-w-0">
                          <div className="truncate text-xs font-medium text-white">{run.scraper_name}</div>
                          <div className="mt-0.5 text-[11px] text-zinc-600">{formatDuration(run.duration_ms)}</div>
                        </div>
                        <StatusPill status={run.status} />
                      </div>
                    ))}
                  </div>
                </details>
              ) : null}
            </div>
          </div>

          <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-5">
            <div className="mb-4 flex items-center justify-between gap-3">
              <div>
                <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-zinc-600">Broadcast Control</div>
                <h2 className="mt-1 text-lg font-semibold text-white">X / Discord</h2>
              </div>
              <span className="rounded-full border border-cyan-500/20 bg-cyan-500/10 px-2.5 py-1 text-xs font-semibold text-cyan-200">
                Review queue
              </span>
            </div>

            <div className="space-y-2">
              {BROADCAST_CHANNELS.map((channel) => (
                <div key={channel.key} className="rounded-xl border border-white/[0.06] bg-black/20 p-3">
                  <div className="flex items-center justify-between gap-3">
                    <div className="text-sm font-semibold text-white">{channel.label}</div>
                    <div className="text-xs text-zinc-500">{countCandidates(broadcastCandidates, channel.key, 'posted')} posted</div>
                  </div>
                  <div className="mt-2 flex flex-wrap gap-2 text-[11px]">
                    <span className="rounded-full border border-amber-500/20 bg-amber-500/10 px-2 py-0.5 text-amber-200">
                      {countCandidates(broadcastCandidates, channel.key, 'pending_review')} pending
                    </span>
                    <span className="rounded-full border border-cyan-500/20 bg-cyan-500/10 px-2 py-0.5 text-cyan-200">
                      {countCandidates(broadcastCandidates, channel.key, 'approved')} approved
                    </span>
                  </div>
                </div>
              ))}
            </div>

            <div className="mt-4 rounded-xl border border-white/[0.06] bg-black/20 p-3">
              <div className="text-[10px] uppercase tracking-[0.18em] text-zinc-600">Posting Config</div>
              <div className="mt-3 grid gap-2 text-xs sm:grid-cols-2">
                <div className={config.twitterEnabled && config.twitterConfigured ? 'text-emerald-300' : 'text-amber-300'}>
                  X {config.twitterEnabled && config.twitterConfigured ? 'ready' : 'not live'}
                </div>
                <div className={config.discordConfigured ? 'text-emerald-300' : 'text-amber-300'}>
                  Discord {config.discordConfigured ? 'ready' : 'missing webhook'}
                </div>
                <div className="text-zinc-500">
                  Last queue: {queueState.current_step || queueState.status}
                </div>
              </div>
            </div>

            <div className="mt-4 flex flex-wrap gap-2">
              <ActionLink href="/ops/social">Review posts</ActionLink>
              <ActionLink href="/ops/policy">Tune policy</ActionLink>
            </div>
          </div>
        </section>

        <section className="grid gap-4 xl:grid-cols-[1.15fr_0.85fr]">
          <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-5">
            <div className="mb-4 flex items-center justify-between gap-3">
              <div>
                <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-zinc-600">Run Ledger</div>
                <h2 className="mt-1 text-lg font-semibold text-white">Full run history</h2>
              </div>
              <div className="text-xs text-zinc-600">{recentRuns.length} shown</div>
            </div>

            <details className="rounded-2xl border border-white/[0.06] bg-black/20">
              <summary className="cursor-pointer px-4 py-3 text-sm font-medium text-zinc-300">
                Open all recent runs
              </summary>
              <div className="overflow-hidden border-t border-white/[0.06]">
                {recentRuns.map((run) => {
                  const hint = isFailureStatus(run.status) ? failureHint(run, errorsByRunId) : null;
                  return (
                    <div key={run.id} className="grid gap-3 border-b border-white/[0.06] px-4 py-3 last:border-0 md:grid-cols-[1fr_1.35fr_auto] md:items-center">
                      <div className="min-w-0">
                        <div className="truncate text-sm font-semibold text-white">{run.scraper_name}</div>
                        <div className="mt-1 flex flex-wrap gap-2 text-[11px] text-zinc-600">
                          <span>{run.source_name}</span>
                          <span>/</span>
                          <span>{run.mode}</span>
                        </div>
                      </div>
                      <div className="min-w-0">
                        <div className="truncate text-xs text-zinc-400">{summarizeRun(run)}</div>
                        {hint ? <div className="mt-1 truncate text-xs text-red-300">{hint}</div> : null}
                      </div>
                      <div className="flex flex-wrap items-center gap-2 md:justify-end">
                        <StatusPill status={run.status} />
                        <span className="text-xs text-zinc-600">{formatDuration(run.duration_ms)}</span>
                        <span className="text-xs text-zinc-600">{formatDateTime(run.started_at)}</span>
                      </div>
                    </div>
                  );
                })}
              </div>
            </details>
          </div>

          <div className="space-y-4">
            <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-5">
              <div className="mb-4 flex items-center justify-between gap-3">
                <div>
                  <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-zinc-600">Failures</div>
                  <h2 className="mt-1 text-lg font-semibold text-white">Recent hard failures</h2>
                </div>
                <span className="rounded-full border border-red-500/20 bg-red-500/10 px-2.5 py-1 text-xs font-semibold text-red-300">
                  {recentHardFailures.length}
                </span>
              </div>

              <div className="space-y-2">
                {recentHardFailures.slice(0, 5).length ? (
                  recentHardFailures.slice(0, 5).map((run) => (
                    <div key={run.id} className="rounded-xl border border-white/[0.06] bg-black/20 p-3">
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <div className="truncate text-sm font-semibold text-white">{run.scraper_name}</div>
                          <div className="mt-1 text-xs text-zinc-600">{formatDateTime(run.started_at)}</div>
                        </div>
                        <StatusPill status={run.status} />
                      </div>
                      <div className="mt-2 line-clamp-2 text-xs leading-5 text-red-200">
                        {failureHint(run, errorsByRunId) || summarizeRun(run)}
                      </div>
                    </div>
                  ))
                ) : (
                  <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/10 p-3 text-sm text-emerald-200">
                    No hard failures in the recent ledger.
                  </div>
                )}
              </div>
            </div>

            <div className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-5">
              <div className="mb-4">
                <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-zinc-600">Error Log</div>
                <h2 className="mt-1 text-lg font-semibold text-white">Latest scraper errors</h2>
              </div>

              <div className="space-y-2">
                {visibleRecentErrors.length ? (
                  visibleRecentErrors.slice(0, 6).map((error) => (
                    <div key={error.id} className="rounded-xl border border-white/[0.06] bg-black/20 p-3">
                      <div className="flex items-start gap-2">
                        <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-amber-300" />
                        <div className="min-w-0">
                          <div className="line-clamp-2 text-sm font-medium text-white">{error.message}</div>
                          <div className="mt-1 text-[11px] uppercase tracking-[0.15em] text-zinc-600">
                            {error.stage} / {error.severity} / {formatDateTime(error.created_at)}
                          </div>
                        </div>
                      </div>
                    </div>
                  ))
                ) : (
                  <div className="rounded-xl border border-emerald-500/20 bg-emerald-500/10 p-3 text-sm text-emerald-200">
                    No recent hard scraper errors logged.
                  </div>
                )}
              </div>
            </div>
          </div>
        </section>

        <section className="rounded-2xl border border-white/[0.06] bg-white/[0.02] p-5">
          <div className="mb-4">
            <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-zinc-600">Control Links</div>
            <h2 className="mt-1 text-lg font-semibold text-white">Operational panels</h2>
          </div>

          <div className="grid gap-3 md:grid-cols-4">
            <Link href="/ops/social" className="rounded-xl border border-white/[0.06] bg-black/20 p-4 transition hover:bg-white/[0.04]">
              <Megaphone className="h-5 w-5 text-cyan-300" />
              <div className="mt-3 text-sm font-semibold text-white">Broadcast Review</div>
              <div className="mt-1 text-xs leading-5 text-zinc-500">Approve, reject, and publish X/Discord posts.</div>
            </Link>
            <Link href="/ops/clusters" className="rounded-xl border border-white/[0.06] bg-black/20 p-4 transition hover:bg-white/[0.04]">
              <Radio className="h-5 w-5 text-emerald-300" />
              <div className="mt-3 text-sm font-semibold text-white">Cluster Ops</div>
              <div className="mt-1 text-xs leading-5 text-zinc-500">Review cluster quality and source mix.</div>
            </Link>
            <Link href="/ops/policy" className="rounded-xl border border-white/[0.06] bg-black/20 p-4 transition hover:bg-white/[0.04]">
              <ShieldCheck className="h-5 w-5 text-blue-300" />
              <div className="mt-3 text-sm font-semibold text-white">Signal Policy</div>
              <div className="mt-1 text-xs leading-5 text-zinc-500">Tune thresholds without code changes.</div>
            </Link>
            <Link href="/alerts" className="rounded-xl border border-white/[0.06] bg-black/20 p-4 transition hover:bg-white/[0.04]">
              <Bell className="h-5 w-5 text-amber-300" />
              <div className="mt-3 text-sm font-semibold text-white">User Alerts</div>
              <div className="mt-1 text-xs leading-5 text-zinc-500">Check the customer-facing alert experience.</div>
            </Link>
          </div>
        </section>
      </div>
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown mission control error.';

    return (
      <div className="mx-auto max-w-[920px] px-4 py-8 sm:px-6">
        <section className="rounded-2xl border border-red-500/20 bg-red-500/10 p-6">
          <div className="flex items-start gap-4">
            <XCircle className="mt-1 h-5 w-5 shrink-0 text-red-300" />
            <div>
              <h1 className="text-2xl font-semibold text-white">Mission Control unavailable</h1>
              <p className="mt-3 text-sm leading-6 text-red-100/80">{message}</p>
            </div>
          </div>
        </section>
      </div>
    );
  }
}
