'use client';

import { useMemo, useState } from 'react';
import { AlertTriangle, CheckCircle2, Save } from 'lucide-react';

type SignalPolicyEditorProps = {
  initialText: string;
  policyPath: string;
};

export function SignalPolicyEditor({ initialText, policyPath }: SignalPolicyEditorProps) {
  const [rawText, setRawText] = useState(initialText);
  const [status, setStatus] = useState<'idle' | 'saving' | 'saved' | 'error'>('idle');
  const [message, setMessage] = useState('');

  const parsedSummary = useMemo(() => {
    try {
      const parsed = JSON.parse(rawText) as Record<string, unknown>;
      const thresholds = parsed.thresholds && typeof parsed.thresholds === 'object' ? Object.keys(parsed.thresholds).length : 0;
      const themeTickers =
        parsed.theme_tickers && typeof parsed.theme_tickers === 'object'
          ? Object.values(parsed.theme_tickers as Record<string, unknown[]>).reduce((sum, value) => {
              return sum + (Array.isArray(value) ? value.length : 0);
            }, 0)
          : 0;
      const notablePoliticians = Array.isArray(parsed.notable_politician_keys) ? parsed.notable_politician_keys.length : 0;
      return {
        valid: true,
        thresholds,
        themeTickers,
        notablePoliticians,
      };
    } catch {
      return {
        valid: false,
        thresholds: 0,
        themeTickers: 0,
        notablePoliticians: 0,
      };
    }
  }, [rawText]);

  async function savePolicy() {
    setStatus('saving');
    setMessage('');

    try {
      const response = await fetch('/api/ops/signal-policy', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ rawText }),
      });

      const payload = (await response.json()) as { ok?: boolean; error?: string; policy?: unknown };
      if (!response.ok || !payload.ok) {
        throw new Error(payload.error || 'Failed to save signal policy.');
      }

      setRawText(`${JSON.stringify(payload.policy, null, 2)}\n`);
      setStatus('saved');
      setMessage('Policy saved. New alert previews and queue runs will use this file.');
    } catch (error) {
      setStatus('error');
      setMessage(error instanceof Error ? error.message : 'Failed to save signal policy.');
    }
  }

  function formatPolicy() {
    try {
      setRawText(`${JSON.stringify(JSON.parse(rawText), null, 2)}\n`);
      setStatus('idle');
      setMessage('');
    } catch (error) {
      setStatus('error');
      setMessage(error instanceof Error ? error.message : 'Invalid JSON.');
    }
  }

  return (
    <div className="space-y-6">
      <div className="glass-panel rounded-2xl p-5">
        <div className="flex flex-wrap items-center justify-between gap-4">
          <div>
            <div className="text-sm uppercase tracking-[0.18em] text-zinc-500">Policy File</div>
            <div className="mt-2 font-mono text-sm text-zinc-200">{policyPath}</div>
          </div>

          <div className="flex items-center gap-3">
            <button
              type="button"
              onClick={formatPolicy}
              className="rounded-xl border border-white/10 bg-white/5 px-4 py-2 text-sm text-zinc-200 transition hover:border-white/20 hover:bg-white/10"
            >
              Format JSON
            </button>
            <button
              type="button"
              onClick={savePolicy}
              disabled={status === 'saving'}
              className="inline-flex items-center gap-2 rounded-xl bg-blue-500 px-4 py-2 text-sm font-medium text-white transition hover:bg-blue-400 disabled:cursor-not-allowed disabled:opacity-60"
            >
              <Save className="h-4 w-4" />
              {status === 'saving' ? 'Saving...' : 'Save Policy'}
            </button>
          </div>
        </div>

        <div className="mt-4 flex flex-wrap gap-3 text-sm">
          <span className={`rounded-full border px-3 py-1 ${parsedSummary.valid ? 'border-emerald-500/20 bg-emerald-500/10 text-emerald-300' : 'border-red-500/20 bg-red-500/10 text-red-300'}`}>
            {parsedSummary.valid ? 'Valid JSON' : 'Invalid JSON'}
          </span>
          <span className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-zinc-300">
            Thresholds: {parsedSummary.thresholds}
          </span>
          <span className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-zinc-300">
            Theme Tickers: {parsedSummary.themeTickers}
          </span>
          <span className="rounded-full border border-white/10 bg-white/5 px-3 py-1 text-zinc-300">
            Notable Politicians: {parsedSummary.notablePoliticians}
          </span>
        </div>

        {message ? (
          <div
            className={`mt-4 flex items-start gap-3 rounded-2xl border px-4 py-3 text-sm ${
              status === 'error'
                ? 'border-red-500/20 bg-red-500/10 text-red-200'
                : 'border-emerald-500/20 bg-emerald-500/10 text-emerald-200'
            }`}
          >
            {status === 'error' ? <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" /> : <CheckCircle2 className="mt-0.5 h-4 w-4 shrink-0" />}
            <span>{message}</span>
          </div>
        ) : null}
      </div>

      <div className="glass-panel rounded-2xl p-5">
        <label className="mb-3 block text-sm uppercase tracking-[0.18em] text-zinc-500">Signal Policy JSON</label>
        <textarea
          value={rawText}
          onChange={(event) => {
            setRawText(event.target.value);
            setStatus('idle');
            setMessage('');
          }}
          spellCheck={false}
          className="min-h-[640px] w-full rounded-2xl border border-white/10 bg-[#0b1020] p-4 font-mono text-sm leading-6 text-zinc-100 outline-none transition focus:border-blue-400/40"
        />
      </div>
    </div>
  );
}
