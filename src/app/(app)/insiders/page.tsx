import { ShieldAlert } from 'lucide-react';

import InsidersFeed from '@/components/InsidersFeed';
import { getPublicInsiderFeedTrades } from '@/lib/public-data';

export const dynamic = 'force-dynamic';

export default async function InsidersPage() {
  const trades = await getPublicInsiderFeedTrades();

  return (
    <div className="mx-auto max-w-[1280px] px-4 py-6 sm:px-6 lg:px-8">
      <div className="mb-6">
        <div className="inline-flex items-center gap-2 rounded-full border border-amber-500/20 bg-amber-500/10 px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.2em] text-amber-400">
          <ShieldAlert className="h-3 w-3" />
          SEC Form 4 Filings
        </div>
        <h1 className="mt-3 text-2xl font-semibold tracking-tight text-white sm:text-3xl">
          Insider Trading Feed
        </h1>
        <p className="mt-1 max-w-4xl text-sm text-zinc-500">
          Real-time Form 4 filings from executives, directors, and 10% owners, with faster scanning across insider,
          ticker, transaction direction, and filing date.
        </p>
      </div>

      <InsidersFeed initialTrades={trades} />
    </div>
  );
}
