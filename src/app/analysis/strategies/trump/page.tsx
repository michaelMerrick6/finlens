import { identifiedStockHoldings, reviewedStockActivity, trumpAnnual, trumpActivity, trumpDjt } from '@/lib/strategies/trump';
import { TrumpDashboard } from './trump-dashboard';

export const metadata = {
  title: 'Trump Strategy',
  description: 'Explore Trump’s disclosed stocks, sector breakdown and reviewed trades, with original filing sources.',
};
export const dynamic = 'force-dynamic';
type Search = { stock?: string | string[] };

export default async function TrumpStrategyPage({ searchParams }: { searchParams: Promise<Search> }) {
  const params = await searchParams;
  const query = typeof params.stock === 'string' ? params.stock.slice(0, 120).trim() : '';
  const stocks = identifiedStockHoldings().sort((a, b) =>
    (b.estimatedReportedValue ?? -Infinity) - (a.estimatedReportedValue ?? -Infinity) || a.ticker.localeCompare(b.ticker));
  return <TrumpDashboard key={query} initialQuery={query} stocks={stocks} trades={reviewedStockActivity()}
    annual={{ sourceUrl: trumpAnnual.sourceUrl, holdingsAsOf: trumpAnnual.holdingsAsOf, publishedOn: trumpAnnual.publishedOn }}
    activity={{ ...trumpActivity.source, reviewedOn: trumpActivity.reviewedOn }} djt={trumpDjt}/>;
}
