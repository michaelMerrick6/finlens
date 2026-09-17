import { getLivePelosiHoldings } from '@/lib/pelosi-live-holdings';
import { valuePelosiPortfolio, holdingDollars, holdingMarketTicker } from '@/lib/pelosi-portfolio';
import { getHoldingPrices } from '@/lib/pelosi-holding-prices';
import { CompanyLogo } from './identity-images';
import { reconcilePelosiShares, pelosiQuantityBaselineSources } from '@/lib/pelosi-share-reconciliation';
import { getPelosiOtherAssets, getPelosiExcludedAssets } from '@/lib/pelosi-full-holdings';
import { pelosiCompanyNames, type ReviewedPosition } from '@/lib/pelosi-holdings';
import { checkPelosiHoldingsCoverage } from '@/lib/pelosi-holdings-coverage';
import styles from './pelosi-holdings.module.css';

function Evidence({ position }: { position: ReviewedPosition }) {
  return <details className={styles.evidence}><summary>View evidence · {position.evidence.length}</summary>
    <ul>{position.evidence.map((e, i) => <li key={i}>{e.date} — {e.note}. <a href={`${e.url}#page=${e.page}`} target="_blank" rel="noreferrer">Source · page {e.page} ↗</a></li>)}</ul>
  </details>;
}
export async function PelosiHoldings() {
  const live = await getLivePelosiHoldings();
  const positions = live.positions;
  const quantityFor = (p: ReviewedPosition) => live.quantities.get(p.key) ?? reconcilePelosiShares(p);
  const coverage = await checkPelosiHoldingsCoverage();
  const options = positions.filter(p => p.kind === 'call' && p.status === 'reconstructed');
  const stocks = positions.filter(p => (p.kind === 'stock' || p.kind === 'units') && quantityFor(p).shares !== 0 && quantityFor(p).range?.max !== 0);
  const prices = await getHoldingPrices(stocks.map(p => p.ticker));
  const portfolio = valuePelosiPortfolio(stocks.map(position => ({ position, quantity: quantityFor(position), quote: prices.get(position.ticker) ?? null })), new Date().toISOString().slice(0, 10));
  const ranked = portfolio.rows;
  const quoteDates = [...new Set(ranked.flatMap(r => r.quote ? [r.quote.date] : []))].sort();
  const other = positions.filter(p => p.kind === 'call' && (p.status === 'closed' || p.status === 'unresolved'));
  const otherAssets = getPelosiOtherAssets().filter(p => p.reported_value !== 'None');
  const excluded = getPelosiExcludedAssets();
  const quantityEstimates = stocks.filter(p => quantityFor(p).status === 'estimated').length;
  const modeledRanges = stocks.filter(p => quantityFor(p).status === 'modeled-range').length;
  return <section className={styles.section} aria-labelledby="holdings-heading">
    <span className="eyebrow">NANCY PELOSI · HOUSEHOLD PORTFOLIO</span>
    <h1 id="holdings-heading">Estimated holdings</h1>
    <p className={styles.intro}>A clearer picture of the stocks in her household’s disclosed portfolio.</p>
    <div className={styles.stats}>
      <div><strong>{stocks.length}</strong><span>Stocks & partnership units</span></div>
      <div><strong>{quantityEstimates}</strong><span>Quantity estimates · plus {modeledRanges} modeled ranges</span></div>
      <div><strong>Aug 21, 2026</strong><span>Latest filing reviewed</span></div>
    </div>
    <p className={styles.caption} role="status">{live.note}{live.checkedAt ? ` Last checked ${new Date(live.checkedAt).toLocaleString('en-US', {timeZone: 'UTC'})} UTC.` : ''}</p>
    {coverage !== 'matched' && !live.checkedAt && <p className={styles.note} role="status">{coverage === 'review-needed' ? 'New filing activity needs review. Showing the last reviewed estimate.' : 'Freshness check unavailable. Showing the last reviewed estimate.'}</p>}
    <div className={styles.visual}>
      <div><span className="eyebrow">ESTIMATED STOCK MIX</span><h2>Where the portfolio is concentrated</h2>
        <p>Illustrative allocation across {portfolio.priced} priced stock and partnership positions.</p>
        <p className={styles.caption}>Range midpoints set the bar sizes and ranking; they are not known balances. Options, cash, property and private assets are excluded.</p>
        <p className={styles.caption}>Yahoo Finance daily prices: {quoteDates.length ? quoteDates.join(' · ') : 'unavailable'}. {portfolio.priced} of {stocks.length} positions priced. Missing or stale prices are excluded.</p>
      </div>
      <div className={styles.allocation}>{ranked.filter(r => r.weight !== null).slice(0, 10).map(r => <a href={`#holding-${r.position.ticker}`} className={styles.allocationRow} key={r.position.key}>
        <span className={styles.rank}>{String(r.rank).padStart(2, '0')}</span><CompanyLogo ticker={holdingMarketTicker(r.position.ticker)} />
        <strong>{r.position.ticker}</strong><span className={styles.allocationTrack}><span style={{ width: `${(r.weight ?? 0) / (ranked[0]?.weight || 1) * 100}%` }} /></span>
        <span>{((r.weight ?? 0) * 100).toFixed(1)}%</span>
      </a>)}{portfolio.priced > 10 && <div className={styles.remaining}><span>Remaining {portfolio.priced - 10} positions · listed below</span><strong>{(ranked.slice(10).reduce((sum, r) => sum + (r.weight ?? 0), 0) * 100).toFixed(1)}%</strong></div>}{!portfolio.priced && <p>Prices are unavailable. Quantity estimates and source records remain below.</p>}</div>
    </div>
    <div className={styles.listHeading}><h2>Stock holdings</h2><span>Largest modeled value first</span></div>
    <p className={styles.caption}>Numbered by midpoint estimated market value. Overlapping value ranges make the order uncertain; select a holding to inspect its assumptions.</p>
    <div className={styles.tableHead}><span>Rank / company</span><span>Estimated quantity</span><span>Estimated market value</span></div>
    <div className={styles.stockList}>{ranked.map(({ position: p, min, max, rank, quote }) => {
      const q = quantityFor(p);
      return <details className={styles.stockRow} key={p.key} id={`holding-${p.ticker}`}>
        <summary>
          <div className={styles.identity}><span className={styles.rank}>{rank === null ? '—' : String(rank).padStart(2, '0')}</span>
            <CompanyLogo ticker={holdingMarketTicker(p.ticker)} /><div><strong>{pelosiCompanyNames[p.ticker] || p.ticker}</strong><span>{p.ticker}{p.kind === 'units' ? ' · Partnership units' : ''}</span></div></div>
          <span className={styles.quantity}>{q.range ? `${q.range.min.toLocaleString('en-US')}–${q.range.max.toLocaleString('en-US')}*` : q.shares !== null ? `~${q.shares.toLocaleString('en-US')}` : 'Not determined'}</span>
          <strong className={styles.value}>{min !== null && max !== null ? min === max ? `~${holdingDollars(min)}` : `${holdingDollars(min)}–${holdingDollars(max)}` : 'Not available'} <span>⌄</span></strong>
        </summary>
        <div className={styles.expanded}>
          {quote && <p>Valuation price: ${quote.price.toFixed(2)} · {quote.date} · Yahoo Finance · {holdingMarketTicker(p.ticker)}. Estimated market value equals estimated quantity × this price.</p>}
          <p><strong>Estimated quantity:</strong> {q.range ? `${q.range.min.toLocaleString('en-US')}–${q.range.max.toLocaleString('en-US')} modeled shares. ` : q.shares !== null ? `~${q.shares.toLocaleString('en-US')} ${p.kind === 'units' ? 'units' : 'shares'}. ` : 'Not determined. '}{q.reason}</p>
          {p.shareChange !== 0 && <p>Net share change since the baseline: {p.shareChange > 0 ? '+' : ''}{p.shareChange.toLocaleString('en-US')} {p.kind === 'units' ? 'units' : 'shares'}.</p>}
          {p.reportedValue && <p>Exact disclosed bracket: {p.reportedValue} at 2025 year end.</p>}
          <ul>{q.steps.map(step => <li key={`${step.date}-${step.note}`}>{step.date}: {step.factor ? `${step.factor}-for-1 split` : `${step.delta > 0 ? '+' : ''}${step.delta.toLocaleString('en-US')} ${p.kind === 'units' ? 'units' : 'shares'}`} — {step.note}. <a href={step.source} target="_blank" rel="noreferrer">Source ↗</a></li>)}</ul>
          {q.assumptions.map(a => <p key={a}>{a}</p>)}
          {q.range && <p>Baseline sources: {[['2022', '10053231'], ['2023', '10059734'], ['2024', '10066169'], ['2025', '10075701']].filter(([year]) => ['AAPL', 'CRM'].includes(p.ticker) || year === '2025').map(([year, doc]) => <a key={year} href={`https://disclosures-clerk.house.gov/public_disc/financial-pdfs/${year}/${doc}.pdf`} target="_blank" rel="noreferrer">{year} annual ↗ </a>)} · {p.ticker === 'CMCSA' ? <a href="https://www.sec.gov/Archives/edgar/data/1166691/000119312526177138/cmcsa-20260424.htm" target="_blank" rel="noreferrer">Price: Comcast proxy statement · $29.89 at December 31, 2025 ↗</a> : 'Prices: Yahoo Finance historical closing prices, retrieved September 17, 2026.'}</p>}
          {p.ticker === 'WBD' && <p><a href="https://paramount.gcs-web.com/news-releases/news-release-details/paramount-skydance-moves-protect-against-costs-delay-wbd-merger" target="_blank" rel="noreferrer">Merger status · issuer update September 8, 2026 ↗</a></p>}
          <Evidence position={p}/>
        </div>
      </details>;
    })}</div>
    <p className={styles.caption}>* Value-based range, using annual brackets and historical prices. Other quantities use disclosed share changes. Estimates assume no undisclosed changes. Securities are reported as spouse-owned. Select a stock for its calculation and sources.</p>
    <details className={styles.group}><summary>Options · {options.length} series</summary>
    <h2>Option quantities reconstructed</h2>
    <p>Contracts remaining after the reviewed changes. Separate expirations are separate positions. These are not stock share counts.</p>
    <div className={styles.grid}>{options.map(p => <article className={styles.card} key={p.key}>
      <div className={styles.row}><strong>{p.ticker} calls</strong><strong>{p.contracts} contracts</strong></div>
      <p>${p.strike} strike · expires {p.expiration}</p><span className={styles.label}>Reconstructed from reviewed records</span><Evidence position={p}/>
    </article>)}</div>
    <details className={styles.group}><summary>Consumed or unresolved option series · {other.length}</summary>
      {other.map(p => <article className={styles.card} key={p.key}><strong>{pelosiCompanyNames[p.ticker] || p.ticker} · {p.ticker}{p.kind === 'call' ? ` $${p.strike} calls · ${p.expiration}` : ''}</strong>
        <p>{p.status === 'closed' ? 'Zero contracts remaining in this series after reviewed exercises. Resulting shares appear separately above.' : 'Quantity or outcome unresolved; excluded from reconstructed option positions.'}</p><Evidence position={p}/></article>)}
    </details>
    </details>
    <details className={styles.group}><summary>Other assets · {otherAssets.length} entries</summary>
    <h2>Other disclosed assets</h2>
    <p>Private investments, property, cash accounts and other interests carried forward from the annual filing. Values are reported 2025 ranges, not updated appraisals. These are asset entries, not additional stock tickers.</p>
    <div className={styles.grid}>{otherAssets.map(p => <article className={styles.card} key={p.name}>
      <strong>{p.name}</strong><p>{p.kind.replaceAll('-', ' ')} · {p.owner === 'SP' ? 'Spouse' : p.owner === 'JT' ? 'Joint' : 'Owner field blank in filing'}</p>
      <p>Reported value: {p.reported_value}</p>
      {p.changes.map(e => <p key={e.doc_id}>Additional investment reported {e.date}; ownership quantity remains unknown. <a href={`${e.source}#page=${e.page}`} target="_blank" rel="noreferrer">View transaction ↗</a></p>)}
      <a href={p.source} target="_blank" rel="noreferrer">Annual source · page {p.page} ↗</a>
    </article>)}</div>
    </details>
    <details className={styles.group}><summary>Excluded annual entries · {excluded.length} reported with value “None”</summary>
      <p>These entries are not counted in the estimated holdings list. “None” is preserved as filed, rather than converted to a current account balance.</p>
      <ul>{excluded.map(p => <li key={p.name}><a href={p.source} target="_blank" rel="noreferrer">{p.name} ↗</a></li>)}</ul>
    </details>
    <details className={styles.group}><summary>How we estimate holdings</summary>
      <p>Starting holdings + purchases − sales − gifts, adjusted for stock splits, spinoffs and option exercises. We use disclosed share or contract quantities where available. An exercise removes the option contracts and adds the resulting shares.</p>
      <p>The annual disclosure was filed May 15, 2026 and covers 2025. Changes use transaction dates after December 31, 2025, so late-filed 2025 activity is not counted twice. Owner, asset type, strike and expiration identify separate positions.</p>
      <p>Annual entries with value “None” (AVGO options, PYPL and DIS stock) are not counted as positive positions. All 68 asset entries in the reviewed annual Schedule A are accounted for: 32 securities entries and 36 other entries, including seven with value “None.” REOF XXV’s later investment is attached to its existing annual entry. Assets not disclosed in Schedule A are not inferred. This is a gross asset inventory, not net worth; liabilities are not subtracted, and related property/company interests are not summed.</p>
      <p>{quantityEstimates} stock and partnership quantities are conditional estimates using disclosed changes and an assumed opening balance; no stock total is labeled verified. We do not infer stock quantities from value-range midpoints or assume an option was exercised when it expires. The allocation graphic uses midpoint modeled market values only to illustrate the mix of priced stocks and partnership units; it is not a full household portfolio or a confirmed allocation. Valuation ranges reflect quantity uncertainty only, not every source of error. This ledger does not establish cost basis or returns.</p>
      <ul>{pelosiQuantityBaselineSources.map(s => <li key={s.url}><a href={s.url} target="_blank" rel="noreferrer">{s.label} ↗</a></li>)}</ul>
      <a href="https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2025/10075701.pdf" target="_blank" rel="noreferrer">Read the annual disclosure ↗</a>
    </details>
  </section>;
}
