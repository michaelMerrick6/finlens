import type { Metadata } from 'next';
import Link from 'next/link';
import { ArrowLeft, Megaphone } from 'lucide-react';

import { TweetCandidateBoard } from '@/components/TweetCandidateBoard';

export const dynamic = 'force-dynamic';

export const metadata: Metadata = {
  title: 'Vail Social Review',
  description: 'Internal tweet candidate review queue for Vail.',
  robots: {
    index: false,
    follow: false,
  },
};

export default function SocialReviewPage() {
  return (
    <div className="space-y-8">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <div className="mb-3 inline-flex items-center gap-2 rounded-full border border-cyan-500/20 bg-cyan-500/10 px-3 py-1 text-xs uppercase tracking-[0.22em] text-cyan-300">
            <Megaphone className="h-3.5 w-3.5" />
            Social Review
          </div>
          <h1 className="text-3xl font-semibold text-white">Tweet Candidate Queue</h1>
          <p className="mt-3 max-w-3xl text-sm leading-6 text-[var(--text-secondary)]">
            Manual review board for public posts. This queue is policy-driven and separate from private user alerts.
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

      <TweetCandidateBoard />
    </div>
  );
}
