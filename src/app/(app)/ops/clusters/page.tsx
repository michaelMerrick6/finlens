import type { Metadata } from 'next';
import Link from 'next/link';
import { ArrowLeft, Orbit } from 'lucide-react';

import ClusterOpsBoard from '@/components/ClusterOpsBoard';
import { getClusterOpsData } from '@/lib/cluster-ops';

export const dynamic = 'force-dynamic';

export const metadata: Metadata = {
  title: 'Vail Cluster Ops',
  description: 'Internal cluster review, retention, and threshold monitoring.',
  robots: {
    index: false,
    follow: false,
  },
};

export default async function ClusterOpsPage() {
  const data = await getClusterOpsData();

  return (
    <div className="space-y-8">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <div className="mb-3 inline-flex items-center gap-2 rounded-full border border-emerald-500/20 bg-emerald-500/10 px-3 py-1 text-xs uppercase tracking-[0.22em] text-emerald-300">
            <Orbit className="h-3.5 w-3.5" />
            Cluster Ops
          </div>
          <h1 className="text-3xl font-semibold text-white">Cluster Capture Control</h1>
          <p className="mt-3 max-w-3xl text-sm leading-6 text-[var(--text-secondary)]">
            Internal view of retained cluster stories, current rule gates, and the review pipeline feeding the public
            clusters page.
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

      <ClusterOpsBoard data={data} />
    </div>
  );
}
