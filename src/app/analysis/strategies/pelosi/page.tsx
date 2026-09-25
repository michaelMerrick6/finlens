import { getLivePelosiHoldings } from '@/lib/pelosi-live-holdings';
import { getHoldingPrices } from '@/lib/pelosi-holding-prices';
import { holdingMarketTicker, valuePelosiPortfolio } from '@/lib/pelosi-portfolio';
import { reconcilePelosiShares } from '@/lib/pelosi-share-reconciliation';
import { pelosiCompanyNames, pelosiHoldingsCoverage } from '@/lib/pelosi-holdings';
import { analysisSector, analysisSectorSources } from '@/lib/analysis-sectors';
import { pelosiStrategyActivity } from '@/lib/strategies/pelosi-overview';
import { PelosiDashboard } from './pelosi-dashboard';

export const metadata = { title: 'Pelosi Strategy' };
export const dynamic = 'force-dynamic';

async function Allocation() {
  const live = await getLivePelosiHoldings();
  const quantityFor = (position: typeof live.positions[number]) => live.quantities.get(position.key) ?? reconcilePelosiShares(position);
  const stocks = live.positions.filter(position => (position.kind === 'stock' || position.kind === 'units')
    && quantityFor(position).shares !== 0 && quantityFor(position).range?.max !== 0);
  const quotes = await getHoldingPrices(stocks.map(position => position.ticker));
  const portfolio = valuePelosiPortfolio(stocks.map(position => ({ position,
    quantity: quantityFor(position), quote: quotes.get(position.ticker) ?? null,
  })), new Date().toISOString().slice(0, 10));
  const rows = portfolio.rows.map(row => {
    const ticker = holdingMarketTicker(row.position.ticker);
    return { ...row, ticker, name: ticker === 'XYZ' ? 'Block' : pelosiCompanyNames[row.position.ticker] || ticker,
      sector: analysisSector(ticker) || 'Unclassified' };
  });
  return <PelosiDashboard rows={rows} activity={pelosiStrategyActivity(live.events, pelosiCompanyNames)}
    note={live.note} checkedAt={live.checkedAt} latestReviewedFiling={pelosiHoldingsCoverage.latestFiling}
    sectorSources={analysisSectorSources.map(source => ({ id: source.id, url: source.url, holdingsAsOf: source.holdingsAsOf }))}/>;
}

export default async function PelosiStrategyPage() {
  return Allocation();
}
