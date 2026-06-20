import type { Metadata } from 'next';

import { HedgeFundDirectory } from '@/components/HedgeFundDirectory';
import type { FundDirectoryEntry } from '@/lib/hedge-funds';
import { getCachedFundDirectory } from '@/lib/public-data';

export const dynamic = 'force-dynamic';

export const metadata: Metadata = {
  title: 'Hedge Funds — Vail',
  description: 'Browse tracked 13F managers by portfolio size.',
};

export default async function HedgeFundsPage() {
  let funds: FundDirectoryEntry[] = [];
  let loadError: string | null = null;

  try {
    funds = await getCachedFundDirectory();
  } catch (error) {
    console.error('Failed to load hedge fund directory', error);
    loadError = 'Unable to load hedge fund filings right now.';
  }

  return <HedgeFundDirectory funds={funds} loadError={loadError} />;
}
