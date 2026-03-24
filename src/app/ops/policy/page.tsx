import type { Metadata } from 'next';
import Link from 'next/link';
import { ArrowLeft, SlidersHorizontal } from 'lucide-react';

import { SignalPolicyEditor } from '@/components/SignalPolicyEditor';
import { SignalPreviewLab } from '@/components/SignalPreviewLab';
import { getSignalPolicyPath, readSignalPolicyText } from '@/lib/signal-policy';

export const dynamic = 'force-dynamic';

export const metadata: Metadata = {
  title: 'Vail Signal Policy',
  description: 'Internal signal policy editor for themes, thresholds, and notable names.',
  robots: {
    index: false,
    follow: false,
  },
};

export default async function SignalPolicyPage() {
  const [initialText, policyPath] = await Promise.all([readSignalPolicyText(), Promise.resolve(getSignalPolicyPath())]);

  return (
    <div className="space-y-8">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <div className="mb-3 inline-flex items-center gap-2 rounded-full border border-blue-500/20 bg-blue-500/10 px-3 py-1 text-xs uppercase tracking-[0.22em] text-blue-300">
            <SlidersHorizontal className="h-3.5 w-3.5" />
            Signal Policy
          </div>
          <h1 className="text-3xl font-semibold text-white">Alert Configuration</h1>
          <p className="mt-3 max-w-3xl text-sm leading-6 text-[var(--text-secondary)]">
            Tune unusual thresholds, priority names, and theme baskets from one shared policy file. Queue previews and alert
            runs use this file directly.
          </p>
        </div>

        <Link
          href="/ops"
          className="inline-flex items-center gap-2 rounded-xl border border-white/10 bg-white/5 px-4 py-2 text-sm text-zinc-200 transition hover:border-white/20 hover:bg-white/10"
        >
          <ArrowLeft className="h-4 w-4" />
          Back To Ops
        </Link>
      </div>

      <SignalPreviewLab />
      <SignalPolicyEditor initialText={initialText} policyPath={policyPath} />
    </div>
  );
}
