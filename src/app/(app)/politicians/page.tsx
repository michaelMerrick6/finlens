import { Landmark } from 'lucide-react';

import PoliticiansFeed from '@/components/PoliticiansFeed';
import { getPublicPoliticianFeedTrades } from '@/lib/public-data';

export const dynamic = 'force-dynamic';

export default async function PoliticiansPage() {
  const trades = await getPublicPoliticianFeedTrades();

  return (
    <div className="mx-auto max-w-[1280px] px-4 py-6 sm:px-6 lg:px-8">
      <div className="mb-6">
        <div className="inline-flex items-center gap-2 rounded-full border border-blue-500/20 bg-blue-500/10 px-3 py-1 text-[10px] font-semibold uppercase tracking-[0.2em] text-blue-400">
          <Landmark className="h-3 w-3" />
          Capitol Hill Trades
        </div>
        <h1 className="mt-3 text-2xl font-semibold tracking-tight text-white sm:text-3xl">
          Congressional Trading Feed
        </h1>
        <p className="mt-1 max-w-4xl text-sm text-zinc-500">
          Live Periodic Transaction Reports from active United States Congress members, with faster scanning across politician,
          asset, filing date, and trade direction.
        </p>
      </div>

      <PoliticiansFeed initialTrades={trades} />
    </div>
  );
}
