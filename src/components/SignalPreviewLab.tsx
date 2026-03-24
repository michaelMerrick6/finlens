'use client';

import { useState, useTransition } from 'react';
import { Activity, AlertTriangle, Search, Sparkles } from 'lucide-react';

type PreviewMatch = {
  signal_type?: string;
  title?: string;
  ticker?: string;
  actor_name?: string;
  direction?: string | null;
  importance_score?: number;
  published_at?: string;
  match_reasons?: string[];
  behavior_labels?: string[];
  amount_range?: string | null;
  value?: number | null;
  source_url?: string | null;
};

type PreviewPayload = {
  events_scanned?: number;
  matches_found?: number;
  matches?: PreviewMatch[];
};

export function SignalPreviewLab() {
  const [ticker, setTicker] = useState('');
  const [politician, setPolitician] = useState('');
  const [insider, setInsider] = useState('');
  const [mode, setMode] = useState<'activity' | 'unusual' | 'both'>('unusual');
  const [preview, setPreview] = useState<PreviewPayload | null>(null);
  const [error, setError] = useState('');
  const [isPending, startTransition] = useTransition();

  function runPreview() {
    setError('');

    startTransition(async () => {
      try {
        const params = new URLSearchParams();
        if (ticker.trim()) {
          params.set('ticker', ticker.trim());
        }
        if (politician.trim()) {
          params.set('politician', politician.trim());
        }
        if (insider.trim()) {
          params.set('insider', insider.trim());
        }
        params.set('mode', mode);

        const response = await fetch(`/api/ops/follow-preview?${params.toString()}`, {
          cache: 'no-store',
        });
        const payload = (await response.json()) as {
          ok?: boolean;
          error?: string;
          preview?: PreviewPayload;
        };

        if (!response.ok || !payload.ok || !payload.preview) {
          throw new Error(payload.error || 'Failed to preview follow matches.');
        }

        setPreview(payload.preview);
      } catch (value) {
        setPreview(null);
        setError(value instanceof Error ? value.message : 'Failed to preview follow matches.');
      }
    });
  }

  const hasInputs = Boolean(ticker.trim() || politician.trim() || insider.trim());

  return (
    <div className="space-y-6">
      <div className="glass-panel rounded-2xl p-5">
        <div className="mb-5">
          <div className="text-sm uppercase tracking-[0.18em] text-zinc-500">Signal Lab</div>
          <div className="mt-2 text-xl font-semibold text-white">Preview a follow against live signal events</div>
          <p className="mt-2 max-w-3xl text-sm text-[var(--text-secondary)]">
            Test a stock or person follow against the current signal window using the active policy file. This helps you tune
            thresholds before users feel the changes.
          </p>
        </div>

        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          <label className="block">
            <div className="mb-2 text-xs uppercase tracking-[0.16em] text-zinc-500">Ticker</div>
            <input
              value={ticker}
              onChange={(event) => setTicker(event.target.value)}
              placeholder="NVDA"
              className="w-full rounded-xl border border-white/10 bg-[#0b1020] px-4 py-3 text-sm text-zinc-100 outline-none transition focus:border-blue-400/40"
            />
          </label>

          <label className="block">
            <div className="mb-2 text-xs uppercase tracking-[0.16em] text-zinc-500">Politician</div>
            <input
              value={politician}
              onChange={(event) => setPolitician(event.target.value)}
              placeholder="Nancy Pelosi"
              className="w-full rounded-xl border border-white/10 bg-[#0b1020] px-4 py-3 text-sm text-zinc-100 outline-none transition focus:border-blue-400/40"
            />
          </label>

          <label className="block">
            <div className="mb-2 text-xs uppercase tracking-[0.16em] text-zinc-500">Insider</div>
            <input
              value={insider}
              onChange={(event) => setInsider(event.target.value)}
              placeholder="Jensen Huang"
              className="w-full rounded-xl border border-white/10 bg-[#0b1020] px-4 py-3 text-sm text-zinc-100 outline-none transition focus:border-blue-400/40"
            />
          </label>

          <label className="block">
            <div className="mb-2 text-xs uppercase tracking-[0.16em] text-zinc-500">Mode</div>
            <select
              value={mode}
              onChange={(event) => setMode(event.target.value as 'activity' | 'unusual' | 'both')}
              className="w-full rounded-xl border border-white/10 bg-[#0b1020] px-4 py-3 text-sm text-zinc-100 outline-none transition focus:border-blue-400/40"
            >
              <option value="unusual">Unusual</option>
              <option value="activity">Activity</option>
              <option value="both">Both</option>
            </select>
          </label>
        </div>

        <div className="mt-5 flex flex-wrap items-center gap-3">
          <button
            type="button"
            onClick={runPreview}
            disabled={isPending || !hasInputs}
            className="inline-flex items-center gap-2 rounded-xl bg-blue-500 px-4 py-2 text-sm font-medium text-white transition hover:bg-blue-400 disabled:cursor-not-allowed disabled:opacity-60"
          >
            <Search className="h-4 w-4" />
            {isPending ? 'Running Preview...' : 'Run Preview'}
          </button>

          <span className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-sm text-zinc-300">
            Live signal window
          </span>
        </div>

        {error ? (
          <div className="mt-4 flex items-start gap-3 rounded-2xl border border-red-500/20 bg-red-500/10 px-4 py-3 text-sm text-red-200">
            <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
            <span>{error}</span>
          </div>
        ) : null}
      </div>

      {preview ? (
        <div className="glass-panel rounded-2xl p-5">
          <div className="mb-5 flex flex-wrap items-center gap-3">
            <div className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-sm text-zinc-300">
              Events scanned: {preview.events_scanned ?? 0}
            </div>
            <div className="rounded-full border border-emerald-500/20 bg-emerald-500/10 px-3 py-1 text-sm text-emerald-300">
              Matches: {preview.matches_found ?? 0}
            </div>
          </div>

          <div className="space-y-4">
            {(preview.matches || []).map((match, index) => (
              <div key={`${match.title || 'match'}-${index}`} className="rounded-2xl border border-white/10 bg-white/[0.03] p-4">
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <div>
                    <div className="text-lg font-semibold text-white">{match.title || 'Untitled match'}</div>
                    <div className="mt-2 flex flex-wrap gap-2 text-xs">
                      {match.signal_type ? (
                        <span className="rounded-full border border-white/10 bg-white/5 px-2.5 py-1 text-zinc-300">
                          {match.signal_type}
                        </span>
                      ) : null}
                      {match.ticker && match.ticker !== 'MULTI' ? (
                        <span className="rounded-full border border-white/10 bg-white/5 px-2.5 py-1 text-zinc-300">
                          {match.ticker}
                        </span>
                      ) : null}
                      {typeof match.importance_score === 'number' ? (
                        <span className="rounded-full border border-blue-500/20 bg-blue-500/10 px-2.5 py-1 text-blue-200">
                          Importance {match.importance_score.toFixed(2)}
                        </span>
                      ) : null}
                    </div>
                  </div>

                  <div className="text-sm text-zinc-400">{match.published_at || 'No date'}</div>
                </div>

                <div className="mt-4 grid gap-3 md:grid-cols-2">
                  <div className="rounded-xl border border-white/10 bg-[#0b1020]/80 p-3">
                    <div className="mb-2 inline-flex items-center gap-2 text-xs uppercase tracking-[0.16em] text-zinc-500">
                      <Sparkles className="h-3.5 w-3.5" />
                      Why Vail Flagged It
                    </div>
                    <div className="flex flex-wrap gap-2">
                      {(match.behavior_labels || []).length > 0 ? (
                        (match.behavior_labels || []).map((label) => (
                          <span key={label} className="rounded-full border border-emerald-500/20 bg-emerald-500/10 px-2.5 py-1 text-xs text-emerald-200">
                            {label}
                          </span>
                        ))
                      ) : (
                        <span className="text-sm text-zinc-400">No unusual labels.</span>
                      )}
                    </div>
                  </div>

                  <div className="rounded-xl border border-white/10 bg-[#0b1020]/80 p-3">
                    <div className="mb-2 inline-flex items-center gap-2 text-xs uppercase tracking-[0.16em] text-zinc-500">
                      <Activity className="h-3.5 w-3.5" />
                      Match Details
                    </div>
                    <div className="space-y-1.5 text-sm text-zinc-300">
                      {match.actor_name ? <div>Actor: {match.actor_name}</div> : null}
                      {match.direction ? <div>Direction: {match.direction}</div> : null}
                      {match.amount_range ? <div>Amount: {match.amount_range}</div> : null}
                      {typeof match.value === 'number' && match.value > 0 ? <div>Value: ${match.value.toLocaleString()}</div> : null}
                      {(match.match_reasons || []).length > 0 ? <div>Matched By: {(match.match_reasons || []).join(', ')}</div> : null}
                    </div>
                  </div>
                </div>

                {match.source_url ? (
                  <div className="mt-4">
                    <a
                      href={match.source_url}
                      target="_blank"
                      rel="noreferrer"
                      className="text-sm text-blue-300 transition hover:text-blue-200"
                    >
                      Open source filing
                    </a>
                  </div>
                ) : null}
              </div>
            ))}

            {(preview.matches || []).length === 0 ? (
              <div className="rounded-2xl border border-white/10 bg-white/[0.03] p-4 text-sm text-zinc-400">
                No matches found in the current signal window for this preview.
              </div>
            ) : null}
          </div>
        </div>
      ) : null}
    </div>
  );
}
