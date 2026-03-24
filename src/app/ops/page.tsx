import type { Metadata } from 'next';
import Link from 'next/link';
import {
  Activity,
  AlertTriangle,
  CheckCircle2,
  Clock3,
  Database,
  Megaphone,
  RefreshCw,
  ShieldCheck,
  Siren,
  XCircle,
} from 'lucide-react';

import { getAdminSupabase } from '@/lib/supabase-admin';

export const dynamic = 'force-dynamic';

export const metadata: Metadata = {
  title: 'Vail Ops',
  description: 'Internal scraper health and Congress coverage monitoring.',
  robots: {
    index: false,
    follow: false,
  },
};

type JsonRecord = Record<string, unknown>;

type ScraperRun = {
  id: string;
  scraper_name: string;
  source_name: string;
  status: string;
  started_at: string;
  finished_at: string | null;
  duration_ms: number | null;
  error_count: number | null;
  records_seen: number | null;
  records_inserted: number | null;
  records_updated: number | null;
  records_skipped: number | null;
  signal_events_created: number | null;
  run_metadata: JsonRecord | null;
};

type ScraperError = {
  id: string;
  run_id: string;
  stage: string;
  severity: string;
  message: string;
  created_at: string;
  details: JsonRecord | null;
};

type TweetCandidate = {
  id: string;
  status: string;
  rule_key: string;
  score: number | null;
  title: string;
  draft_text: string;
  created_at: string;
};

function asRecord(value: unknown): JsonRecord {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return {};
  }
  return value as JsonRecord;
}

function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function asNumber(value: unknown): number {
  return typeof value === 'number' && Number.isFinite(value) ? value : 0;
}

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

  return `${minutes}m ${seconds}s`;
}

function relativeFreshness(value: string | null | undefined) {
  if (!value) {
    return { label: 'Unknown', tone: 'text-amber-300 bg-amber-500/10 border-amber-500/20' };
  }

  const diffMs = Date.now() - new Date(value).getTime();
  const diffHours = diffMs / 3_600_000;

  if (diffHours <= 2) {
    return { label: 'Fresh', tone: 'text-emerald-300 bg-emerald-500/10 border-emerald-500/20' };
  }

  if (diffHours <= 6) {
    return { label: 'Stale Warning', tone: 'text-amber-300 bg-amber-500/10 border-amber-500/20' };
  }

  return { label: 'Stale', tone: 'text-red-300 bg-red-500/10 border-red-500/20' };
}

function statusTone(status: string | null | undefined) {
  switch ((status || '').toLowerCase()) {
    case 'success':
      return 'text-emerald-300 bg-emerald-500/10 border-emerald-500/20';
    case 'failed':
      return 'text-red-300 bg-red-500/10 border-red-500/20';
    case 'running':
      return 'text-blue-300 bg-blue-500/10 border-blue-500/20';
    case 'timed_out':
      return 'text-amber-300 bg-amber-500/10 border-amber-500/20';
    default:
      return 'text-zinc-300 bg-white/5 border-white/10';
  }
}

function congressTodayIso() {
  const formatter = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  });

  const parts = formatter.formatToParts(new Date());
  const year = parts.find((part) => part.type === 'year')?.value ?? '0000';
  const month = parts.find((part) => part.type === 'month')?.value ?? '01';
  const day = parts.find((part) => part.type === 'day')?.value ?? '01';

  return `${year}-${month}-${day}`;
}

function getLatestAudit(run: ScraperRun | null) {
  const metadata = asRecord(run?.run_metadata);
  const house = asRecord(metadata.house);
  const senate = asRecord(metadata.senate);

  return {
    parseFailures: asNumber(metadata.parse_failures),
    recentUnknownRows: asArray(metadata.recent_unknown_rows).length,
    houseFilingsSeen: asNumber(house.filings_seen),
    houseTradeFilings: asNumber(house.filings_with_trades),
    houseMismatches:
      asArray(house.row_count_mismatches).length +
      asArray(house.published_date_mismatches).length +
      asArray(house.source_parse_failures).length +
      asArray(house.unexpected_rows_for_no_trade_filings).length +
      asArray(house.unknown_member_docs).length,
    senateFilingsSeen: asNumber(senate.filings_seen),
    senateTradeFilings: asNumber(senate.filings_with_trades),
    senateMismatches:
      asArray(senate.row_count_mismatches).length +
      asArray(senate.published_date_mismatches).length +
      asArray(senate.source_parse_failures).length +
      asArray(senate.unknown_member_docs).length,
    senatePaperUnmapped: asArray(senate.paper_unmapped_filings).length,
  };
}

function getPipelineResults(run: ScraperRun | null) {
  const metadata = asRecord(run?.run_metadata);
  const results = asRecord(metadata.results);
  return Object.entries(results);
}

function getErrorHint(error: ScraperError) {
  const details = asRecord(error.details);
  const summary = asRecord(details.summary);
  const failedDocIds = asArray(summary.failed_doc_ids);

  if (failedDocIds.length > 0) {
    return `Docs: ${failedDocIds.slice(0, 3).join(', ')}`;
  }

  const stderr = details.stderr;
  if (typeof stderr === 'string' && stderr.length > 0) {
    return stderr.split('\n')[0];
  }

  return null;
}

function StatCard({
  icon,
  label,
  value,
  sublabel,
}: {
  icon: React.ReactNode;
  label: string;
  value: string;
  sublabel: string;
}) {
  return (
    <div className="glass-panel rounded-2xl p-5">
      <div className="mb-4 flex h-11 w-11 items-center justify-center rounded-xl bg-white/5 text-blue-300">
        {icon}
      </div>
      <div className="text-sm uppercase tracking-[0.18em] text-zinc-500">{label}</div>
      <div className="mt-2 text-3xl font-semibold text-white">{value}</div>
      <div className="mt-2 text-sm text-[var(--text-secondary)]">{sublabel}</div>
    </div>
  );
}

export default async function OpsPage() {
  const todayIso = congressTodayIso();

  try {
    const supabase = getAdminSupabase();

    const [
      latestPipelineRes,
      latestAuditRes,
      latestHouseRes,
      latestSenateRes,
      todayHouseCountRes,
      todaySenateCountRes,
      recentRunsRes,
      recentErrorsRes,
      tweetCandidatesRes,
    ] = await Promise.all([
      supabase
        .from('scraper_runs')
        .select(
          'id, scraper_name, source_name, status, started_at, finished_at, duration_ms, error_count, records_seen, records_inserted, records_updated, records_skipped, signal_events_created, run_metadata'
        )
        .eq('scraper_name', 'daily_pipeline')
        .order('started_at', { ascending: false })
        .limit(1),
      supabase
        .from('scraper_runs')
        .select(
          'id, scraper_name, source_name, status, started_at, finished_at, duration_ms, error_count, records_seen, records_inserted, records_updated, records_skipped, signal_events_created, run_metadata'
        )
        .eq('scraper_name', 'congress_recent_audit')
        .order('started_at', { ascending: false })
        .limit(1),
      supabase
        .from('politician_trades')
        .select('published_date')
        .eq('chamber', 'House')
        .order('published_date', { ascending: false })
        .limit(1),
      supabase
        .from('politician_trades')
        .select('published_date')
        .eq('chamber', 'Senate')
        .order('published_date', { ascending: false })
        .limit(1),
      supabase
        .from('politician_trades')
        .select('id', { count: 'exact', head: true })
        .eq('chamber', 'House')
        .eq('published_date', todayIso),
      supabase
        .from('politician_trades')
        .select('id', { count: 'exact', head: true })
        .eq('chamber', 'Senate')
        .eq('published_date', todayIso),
      supabase
        .from('scraper_runs')
        .select(
          'id, scraper_name, source_name, status, started_at, finished_at, duration_ms, error_count, records_seen, records_inserted, records_updated, records_skipped, signal_events_created, run_metadata'
        )
        .order('started_at', { ascending: false })
        .limit(16),
      supabase
        .from('scraper_errors')
        .select('id, run_id, stage, severity, message, created_at, details')
        .order('created_at', { ascending: false })
        .limit(8),
      supabase
        .from('tweet_candidates')
        .select('id, status, rule_key, score, title, draft_text, created_at', { count: 'exact' })
        .eq('status', 'pending_review')
        .order('score', { ascending: false })
        .order('created_at', { ascending: false })
        .limit(6),
    ]);

    const latestPipeline = (latestPipelineRes.data?.[0] ?? null) as ScraperRun | null;
    const latestAudit = (latestAuditRes.data?.[0] ?? null) as ScraperRun | null;
    const latestHouseDate = latestHouseRes.data?.[0]?.published_date ?? null;
    const latestSenateDate = latestSenateRes.data?.[0]?.published_date ?? null;
    const recentRuns = (recentRunsRes.data ?? []) as ScraperRun[];
    const recentErrors = (recentErrorsRes.data ?? []) as ScraperError[];
    const tweetCandidates = (tweetCandidatesRes.data ?? []) as TweetCandidate[];
    const tweetCandidatesEnabled = !tweetCandidatesRes.error;
    const pendingTweetCandidates = tweetCandidatesRes.count ?? 0;

    const audit = getLatestAudit(latestAudit);
    const freshness = relativeFreshness(latestPipeline?.finished_at);
    const pipelineResults = getPipelineResults(latestPipeline);
    const totalCongressToday = (todayHouseCountRes.count ?? 0) + (todaySenateCountRes.count ?? 0);

    return (
      <div className="space-y-8 animate-in fade-in duration-500">
        <section className="flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
          <div className="space-y-3">
            <div className="inline-flex items-center gap-2 rounded-full border border-cyan-500/20 bg-cyan-500/10 px-3 py-1 text-sm font-medium text-cyan-300">
              <ShieldCheck size={16} />
              Internal Operations
            </div>
            <div>
              <h1 className="text-4xl font-extrabold tracking-tight text-white md:text-5xl">Scraper Health</h1>
              <p className="mt-3 max-w-3xl text-lg text-[var(--text-secondary)]">
                Live view of pipeline freshness, Congress source parity, and the most recent scraper failures.
              </p>
            </div>
          </div>

          <div className={`inline-flex items-center gap-2 rounded-full border px-4 py-2 text-sm font-medium ${freshness.tone}`}>
            <RefreshCw size={15} />
            {freshness.label}
          </div>
        </section>

        <section className="glass-panel rounded-3xl p-5">
          <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
            <div>
              <div className="text-sm uppercase tracking-[0.18em] text-zinc-500">Signal Policy</div>
              <div className="mt-2 text-lg font-semibold text-white">Edit thresholds, themes, and notable names</div>
              <p className="mt-2 max-w-3xl text-sm text-[var(--text-secondary)]">
                Use the internal policy editor to tune unusual alert rules without changing Python code.
              </p>
            </div>

            <Link
              href="/ops/policy"
              className="inline-flex items-center justify-center rounded-xl border border-blue-500/20 bg-blue-500/10 px-4 py-2 text-sm font-medium text-blue-200 transition hover:border-blue-400/30 hover:bg-blue-500/15"
            >
              Open Policy Editor
            </Link>
          </div>
        </section>

        <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          <StatCard
            icon={<Activity size={22} />}
            label="Pipeline"
            value={latestPipeline?.status === 'success' ? 'Green' : latestPipeline?.status ? latestPipeline.status : 'Unknown'}
            sublabel={`Last finished ${formatDateTime(latestPipeline?.finished_at)} • ${formatDuration(latestPipeline?.duration_ms)}`}
          />
          <StatCard
            icon={<Database size={22} />}
            label="Congress Today (ET)"
            value={`${totalCongressToday}`}
            sublabel={`House ${todayHouseCountRes.count ?? 0} • Senate ${todaySenateCountRes.count ?? 0} for source day ${todayIso}`}
          />
          <StatCard
            icon={<CheckCircle2 size={22} />}
            label="Audit"
            value={audit.parseFailures === 0 ? 'Pass' : 'Fail'}
            sublabel={`House ${audit.houseFilingsSeen}/${audit.houseTradeFilings} • Senate ${audit.senateFilingsSeen}/${audit.senateTradeFilings}`}
          />
          <StatCard
            icon={<Clock3 size={22} />}
            label="Latest Filed Dates"
            value={`${latestHouseDate ?? 'N/A'} / ${latestSenateDate ?? 'N/A'}`}
            sublabel="House latest / Senate latest published dates"
          />
          <StatCard
            icon={<Megaphone size={22} />}
            label="Tweet Queue"
            value={tweetCandidatesEnabled ? `${pendingTweetCandidates}` : 'Off'}
            sublabel={
              tweetCandidatesEnabled
                ? `${tweetCandidates.length} pending review drafts loaded`
                : 'Apply the social migration to enable reviewable tweet drafts'
            }
          />
        </section>

        <section className="grid gap-6 xl:grid-cols-[1.2fr_0.8fr]">
          <div className="glass-panel rounded-3xl p-6">
            <div className="mb-5 flex items-center justify-between gap-3">
              <div>
                <h2 className="text-xl font-semibold text-white">Daily Pipeline</h2>
                <p className="mt-1 text-sm text-[var(--text-secondary)]">
                  Step-by-step status from the latest full run.
                </p>
              </div>
              <div className={`rounded-full border px-3 py-1 text-xs font-medium ${statusTone(latestPipeline?.status)}`}>
                {latestPipeline?.status ?? 'unknown'}
              </div>
            </div>

            <div className="grid gap-3 sm:grid-cols-2">
              {pipelineResults.map(([name, ok]) => (
                <div
                  key={name}
                  className={`rounded-2xl border px-4 py-3 ${
                    ok ? 'border-emerald-500/20 bg-emerald-500/10' : 'border-red-500/20 bg-red-500/10'
                  }`}
                >
                  <div className="flex items-center justify-between gap-2">
                    <div className="text-sm font-medium text-white">{name}</div>
                    {ok ? (
                      <CheckCircle2 size={16} className="text-emerald-300" />
                    ) : (
                      <XCircle size={16} className="text-red-300" />
                    )}
                  </div>
                </div>
              ))}
            </div>

            <div className="mt-5 grid gap-4 sm:grid-cols-3">
              <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                <div className="text-xs uppercase tracking-[0.18em] text-zinc-500">Started</div>
                <div className="mt-2 text-sm text-white">{formatDateTime(latestPipeline?.started_at)}</div>
              </div>
              <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                <div className="text-xs uppercase tracking-[0.18em] text-zinc-500">Finished</div>
                <div className="mt-2 text-sm text-white">{formatDateTime(latestPipeline?.finished_at)}</div>
              </div>
              <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                <div className="text-xs uppercase tracking-[0.18em] text-zinc-500">Duration</div>
                <div className="mt-2 text-sm text-white">{formatDuration(latestPipeline?.duration_ms)}</div>
              </div>
            </div>
          </div>

          <div className="glass-panel rounded-3xl p-6">
            <div className="mb-5">
              <h2 className="text-xl font-semibold text-white">Congress Source Audit</h2>
              <p className="mt-1 text-sm text-[var(--text-secondary)]">
                The run only stays green if recent official filings match the database.
              </p>
            </div>

            <div className="space-y-4">
              <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                <div className="mb-3 flex items-center justify-between">
                  <div className="text-sm font-medium text-white">House</div>
                  <div className="text-sm text-emerald-300">{audit.houseMismatches === 0 ? 'Clean' : `${audit.houseMismatches} issues`}</div>
                </div>
                <div className="grid grid-cols-3 gap-3 text-sm">
                  <div>
                    <div className="text-zinc-500">Filings Seen</div>
                    <div className="mt-1 text-white">{audit.houseFilingsSeen}</div>
                  </div>
                  <div>
                    <div className="text-zinc-500">Trade Filings</div>
                    <div className="mt-1 text-white">{audit.houseTradeFilings}</div>
                  </div>
                  <div>
                    <div className="text-zinc-500">Mismatches</div>
                    <div className="mt-1 text-white">{audit.houseMismatches}</div>
                  </div>
                </div>
              </div>

              <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                <div className="mb-3 flex items-center justify-between">
                  <div className="text-sm font-medium text-white">Senate</div>
                  <div className="text-sm text-emerald-300">{audit.senateMismatches === 0 ? 'Clean' : `${audit.senateMismatches} issues`}</div>
                </div>
                <div className="grid grid-cols-3 gap-3 text-sm">
                  <div>
                    <div className="text-zinc-500">Filings Seen</div>
                    <div className="mt-1 text-white">{audit.senateFilingsSeen}</div>
                  </div>
                  <div>
                    <div className="text-zinc-500">Trade Filings</div>
                    <div className="mt-1 text-white">{audit.senateTradeFilings}</div>
                  </div>
                  <div>
                    <div className="text-zinc-500">Paper Unmapped</div>
                    <div className="mt-1 text-white">{audit.senatePaperUnmapped}</div>
                  </div>
                </div>
              </div>

              <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                <div className="mb-2 text-sm font-medium text-white">Recent unresolved rows</div>
                <div className="text-sm text-[var(--text-secondary)]">
                  {audit.recentUnknownRows === 0
                    ? 'No recent unknown-member Congress rows remain.'
                    : `${audit.recentUnknownRows} recent unknown-member rows still need repair.`}
                </div>
              </div>
            </div>
          </div>
        </section>

        <section className="grid gap-6 xl:grid-cols-[1.2fr_0.8fr]">
          <div className="glass-panel rounded-3xl p-6">
            <div className="mb-5">
              <h2 className="text-xl font-semibold text-white">Recent Runs</h2>
              <p className="mt-1 text-sm text-[var(--text-secondary)]">
                Latest scraper executions across Congress, events, and delivery.
              </p>
            </div>

            <div className="overflow-x-auto">
              <table className="min-w-full border-collapse text-left">
                <thead>
                  <tr className="border-b border-white/10 text-xs uppercase tracking-[0.18em] text-zinc-500">
                    <th className="px-0 py-3 font-medium">Run</th>
                    <th className="px-4 py-3 font-medium">Status</th>
                    <th className="px-4 py-3 font-medium">Started</th>
                    <th className="px-4 py-3 font-medium">Duration</th>
                    <th className="px-4 py-3 font-medium">Signals</th>
                    <th className="px-4 py-3 font-medium">Errors</th>
                  </tr>
                </thead>
                <tbody>
                  {recentRuns.map((run) => (
                    <tr key={run.id} className="border-b border-white/5 align-top">
                      <td className="px-0 py-4">
                        <div className="font-medium text-white">{run.scraper_name}</div>
                        <div className="mt-1 text-sm text-[var(--text-secondary)]">{run.source_name}</div>
                      </td>
                      <td className="px-4 py-4">
                        <span className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-medium ${statusTone(run.status)}`}>
                          {run.status}
                        </span>
                      </td>
                      <td className="px-4 py-4 text-sm text-zinc-300">{formatDateTime(run.started_at)}</td>
                      <td className="px-4 py-4 text-sm text-zinc-300">{formatDuration(run.duration_ms)}</td>
                      <td className="px-4 py-4 text-sm text-zinc-300">
                        {run.signal_events_created ?? run.records_inserted ?? 0}
                      </td>
                      <td className="px-4 py-4 text-sm text-zinc-300">{run.error_count ?? 0}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          <div className="glass-panel rounded-3xl p-6">
            <div className="mb-5 flex items-center gap-2">
              <Siren size={18} className="text-amber-300" />
              <div>
                <h2 className="text-xl font-semibold text-white">Recent Errors</h2>
                <p className="mt-1 text-sm text-[var(--text-secondary)]">
                  Last recorded scraper failures and parse errors.
                </p>
              </div>
            </div>

            <div className="space-y-3">
              {recentErrors.length === 0 ? (
                <div className="rounded-2xl border border-emerald-500/20 bg-emerald-500/10 p-4 text-sm text-emerald-200">
                  No recent scraper errors logged.
                </div>
              ) : (
                recentErrors.map((error) => {
                  const hint = getErrorHint(error);
                  return (
                    <div key={error.id} className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                      <div className="flex items-start justify-between gap-3">
                        <div className="flex items-start gap-3">
                          <AlertTriangle size={16} className="mt-0.5 shrink-0 text-amber-300" />
                          <div>
                            <div className="text-sm font-medium text-white">{error.message}</div>
                            <div className="mt-1 text-xs uppercase tracking-[0.18em] text-zinc-500">
                              {error.stage} • {error.severity}
                            </div>
                            {hint ? (
                              <div className="mt-2 text-sm text-[var(--text-secondary)]">{hint}</div>
                            ) : null}
                          </div>
                        </div>
                        <div className="shrink-0 text-xs text-zinc-500">{formatDateTime(error.created_at)}</div>
                      </div>
                    </div>
                  );
                })
              )}
            </div>
          </div>
        </section>

        <section className="glass-panel rounded-3xl p-6">
          <div className="mb-5 flex items-center gap-2">
            <Megaphone size={18} className="text-cyan-300" />
            <div>
              <h2 className="text-xl font-semibold text-white">Tweet Candidates</h2>
              <p className="mt-1 text-sm text-[var(--text-secondary)]">
                Draft social posts collected for manual review. Nothing auto-posts from this queue.
              </p>
            </div>
          </div>

          <div className="mb-5">
            <Link
              href="/ops/social"
              className="inline-flex items-center rounded-xl border border-cyan-500/20 bg-cyan-500/10 px-4 py-2 text-sm font-medium text-cyan-200 transition hover:border-cyan-400/30 hover:bg-cyan-500/15"
            >
              Open Social Review
            </Link>
          </div>

          {!tweetCandidatesEnabled ? (
            <div className="rounded-2xl border border-amber-500/20 bg-amber-500/10 p-4 text-sm text-amber-100/90">
              Tweet candidate queue is not enabled in this database yet. Apply the social migration first.
            </div>
          ) : tweetCandidates.length === 0 ? (
            <div className="rounded-2xl border border-emerald-500/20 bg-emerald-500/10 p-4 text-sm text-emerald-200">
              No pending tweet candidates right now.
            </div>
          ) : (
            <div className="space-y-3">
              {tweetCandidates.map((candidate) => (
                <div key={candidate.id} className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                  <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
                    <div className="space-y-2">
                      <div className="flex items-center gap-2">
                        <div className="text-sm font-medium text-white">{candidate.title}</div>
                        <span className="inline-flex rounded-full border border-cyan-500/20 bg-cyan-500/10 px-2.5 py-1 text-xs font-medium text-cyan-300">
                          {candidate.rule_key}
                        </span>
                      </div>
                      <div className="text-sm text-[var(--text-secondary)]">{candidate.draft_text}</div>
                    </div>
                    <div className="shrink-0 space-y-1 text-right">
                      <div className="text-sm font-medium text-white">{candidate.score?.toFixed(2) ?? '0.00'}</div>
                      <div className="text-xs text-zinc-500">{formatDateTime(candidate.created_at)}</div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </section>
      </div>
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown ops dashboard error.';

    return (
      <div className="space-y-6">
        <section className="glass-panel rounded-3xl border border-red-500/20 bg-red-500/10 p-8">
          <div className="flex items-start gap-4">
            <XCircle className="mt-1 text-red-300" size={22} />
            <div>
              <h1 className="text-2xl font-semibold text-white">Ops dashboard unavailable</h1>
              <p className="mt-3 max-w-2xl text-sm text-red-100/80">{message}</p>
            </div>
          </div>
        </section>
      </div>
    );
  }
}
