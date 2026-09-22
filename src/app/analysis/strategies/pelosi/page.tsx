import Link from 'next/link';
import { Suspense } from 'react';
import { Avatar, CompanyLogo } from '@/components/identity-images';
import { getLivePelosiHoldings } from '@/lib/pelosi-live-holdings';
import { getHoldingPrices } from '@/lib/pelosi-holding-prices';
import { holdingMarketTicker, valuePelosiPortfolio } from '@/lib/pelosi-portfolio';
import { reconcilePelosiShares } from '@/lib/pelosi-share-reconciliation';
import { pelosiCompanyNames } from '@/lib/pelosi-holdings';
import styles from '../strategies.module.css';

export const metadata = { title: 'Pelosi Strategy' };
export const dynamic = 'force-dynamic';

async function Allocation() {
  const live = await getLivePelosiHoldings();
  const stocks = live.positions.filter(p => p.kind === 'stock' || p.kind === 'units');
  const quotes = await getHoldingPrices(stocks.map(p => p.ticker));
  const portfolio = valuePelosiPortfolio(stocks.map(position => ({ position,
    quantity: live.quantities.get(position.key) ?? reconcilePelosiShares(position),
    quote: quotes.get(position.ticker) ?? null,
  })), new Date().toISOString().slice(0, 10));
  const rows = portfolio.rows.filter(r => r.weight !== null && r.weight > 0);
  const excluded = portfolio.rows.filter(r => r.midpoint === null);
  const dates = [...new Set(rows.flatMap(r => r.quote ? [r.quote.date] : []))].sort();
  return <>
    <div className={styles.stats}>
      <div><strong>{rows.length}</strong><span>Positions in the preview</span></div>
      <div><strong>Value weighted</strong><span>Based on estimated holdings</span></div>
      <div><strong>Preview</strong><span>Performance tracking not yet started</span></div>
    </div>
    <p className={styles.caption}>{live.note}{live.checkedAt ? ` Filing check: ${new Date(live.checkedAt).toISOString().slice(0, 10)}.` : ''}</p>
    <div className={styles.layout}>
      <section aria-labelledby="allocation-title">
        <h2 id="allocation-title">Inside the allocation</h2>
        <p className={styles.caption}>Company weights use estimated market values. Prices: {dates.length ? dates.join(' · ') : 'unavailable'}. Select a company to inspect its holdings evidence.</p>
        {rows.length ? rows.map(r => <div className={styles.row} key={r.position.key}>
          <Link className={styles.identity} href={`/politicians/P000197/holdings#holding-${r.position.ticker}`}>
            <CompanyLogo ticker={holdingMarketTicker(r.position.ticker)}/>
            <div><strong>{pelosiCompanyNames[r.position.ticker] || r.position.ticker}</strong><small>{holdingMarketTicker(r.position.ticker)}</small></div>
          </Link>
          <span className={styles.track} aria-hidden="true"><span style={{width:`${(r.weight ?? 0)*100}%`}}/></span>
          <strong className={styles.weight}>{(r.weight ?? 0)*100 < 0.1 ? '<0.1' : ((r.weight ?? 0)*100).toFixed(1)}%</strong>
        </div>) : <p>No allocation can be calculated with the available prices. <Link href="/politicians/P000197/holdings">View the underlying holdings estimates.</Link></p>}
        {excluded.length > 0 && <p className={styles.caption}>Excluded because a supported quantity or recent price is unavailable: {excluded.map(r => r.position.ticker).join(', ')}. Weights are normalized across the included positions only.</p>}
      </section>
      <aside className={styles.rules}>
        <span className="eyebrow">HOW THIS PREVIEW WORKS</span>
        <h3>The starting portfolio</h3>
        <p>Public stocks and partnership units from the Pelosi household’s reviewed disclosures. Options, cash, real estate and private investments are excluded.</p>
        <h3>How weights are calculated</h3>
        <p>Estimated shares × available market price, divided by the total estimated value of included positions. For quantity ranges, the midpoint sets the weight; it is not a known balance.</p>
        <h3>What comes next</h3>
        <p>Define the trading and rebalancing rules, then track the model’s performance from a recorded start date. This preview is not an executed portfolio or Pelosi’s actual return.</p>
        <Link href="/politicians/P000197/holdings">View holdings and sources →</Link>
      </aside>
    </div>
  </>;
}

export default function PelosiStrategyPage() {
  return <main id="main" className={`container ${styles.page}`}>
    <Link href="/analysis/strategies" className="breadcrumb">← All strategies</Link>
    <div className={styles.heading}><Avatar name="Nancy Pelosi" memberId="P000197" large/><div><span className="eyebrow">STRATEGY · ALLOCATION PREVIEW</span><h1>Pelosi strategy.</h1></div></div>
    <p className={styles.intro}>Explore a model allocation based on the Pelosi household’s estimated public stock holdings.</p>
    <p className={styles.preview}>Allocation preview. Historical performance and scheduled rebalancing are not yet enabled.</p>
    <Suspense fallback={<p role="status">Loading the Pelosi allocation…</p>}><Allocation/></Suspense>
  </main>;
}
