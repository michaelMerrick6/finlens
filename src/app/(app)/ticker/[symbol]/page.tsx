import { notFound } from 'next/navigation';

import TickerIntelligencePage from '@/components/TickerIntelligencePage';
import { getTickerIntelligence } from '@/lib/ticker-intelligence';

export const revalidate = 60;

export default async function TickerPage({ params }: { params: Promise<{ symbol: string }> }) {
  const { symbol } = await params;
  const payload = await getTickerIntelligence(symbol);

  if (!payload) {
    notFound();
  }

  return <TickerIntelligencePage {...payload} />;
}
