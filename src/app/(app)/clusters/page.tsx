import { Info } from 'lucide-react';

import ClusterAlertButton from '@/components/ClusterAlertButton';
import ClustersPage from '@/components/ClustersPage';
import { getPublicClusterSignals } from '@/lib/public-data';

export const revalidate = 60;

export default async function ClusterSignalsPage() {
  const signals = await getPublicClusterSignals();

  return (
    <div className="mx-auto max-w-[1280px] px-4 py-6 sm:px-6 lg:px-8">
      <div className="mb-5 flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <div className="flex items-center gap-2">
            <h1 className="text-2xl font-semibold tracking-tight text-white sm:text-3xl">
              Cluster feed
            </h1>
            <div className="group relative">
              <button
                type="button"
                aria-label="What is a cluster feed?"
                className="flex h-5 w-5 items-center justify-center rounded-full border border-white/[0.1] text-zinc-500 transition hover:border-white/[0.2] hover:text-zinc-300 focus:border-white/[0.2] focus:text-zinc-300 focus:outline-none"
              >
                <Info className="h-3 w-3" />
              </button>
              <div className="pointer-events-none absolute left-1/2 top-7 z-20 hidden w-72 -translate-x-1/2 rounded-xl border border-white/[0.1] bg-[#101010] p-3 text-xs leading-5 text-zinc-400 shadow-2xl group-hover:block group-focus-within:block sm:left-0 sm:translate-x-0">
                Clusters group related moves around one stock inside a short window. Repeated activity from Congress,
                insiders, or funds can be more meaningful than a single trade.
              </div>
            </div>
          </div>
          <p className="mt-1.5 max-w-3xl text-sm leading-6 text-zinc-500">
            See when multiple smart-money moves start pointing at the same stock.
          </p>
        </div>
        <ClusterAlertButton />
      </div>

      <ClustersPage signals={signals} />
    </div>
  );
}
