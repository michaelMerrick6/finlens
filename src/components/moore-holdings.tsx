import snapshot from '@/data/moore-holdings-snapshot.json';
import { CompanyLogo } from './identity-images';
import { holdingDollars } from '@/lib/pelosi-portfolio';
import styles from './pelosi-holdings.module.css';

const shares = (n: number) => n.toLocaleString('en-US', { maximumFractionDigits: 1 });
const range = (a: number, b: number) => `${holdingDollars(a)}–${holdingDollars(b)}`;

export function MooreHoldings() {
  const midpoint = (snapshot.subtotalMin + snapshot.subtotalMax) / 2;
  return <section className={styles.section}>
    <span className="eyebrow">TIM MOORE · PARTIAL ESTIMATE</span>
    <h1>Estimated holdings.</h1>
    <p className={styles.intro}>A reconstruction from his annual disclosure and subsequent reported trades. Snapshot: {snapshot.asOf}. Exact share counts and some account assignments are inferred.</p>
    <div className={styles.stats}>
      <div><strong>{range(snapshot.subtotalMin, snapshot.subtotalMax)}</strong><span>Modeled portion only · not total portfolio value</span></div>
      <div><strong>{snapshot.rows.length}</strong><span>Stocks and funds with estimates</span></div>
      <div><strong>{snapshot.unresolved.length}</strong><span>Unresolved positions · excluded</span></div>
    </div>
    <div className={styles.visual}>
      <div><span className="eyebrow">ILLUSTRATIVE ALLOCATION</span><h2>The modeled portion.</h2><p>Largest ten positions by estimated midpoint value. Percentages use only the modeled subtotal; actual weights and order may differ.</p></div>
      <div className={styles.allocation}>{snapshot.rows.slice(0,10).map((r,i) => <a className={styles.allocationRow} key={r.ticker} href={`#holding-${r.ticker}`}>
        <span>{String(i+1).padStart(2,'0')}</span><CompanyLogo ticker={r.ticker}/><strong>{r.ticker}</strong>
        <span className={styles.allocationTrack}><span style={{width:`${(r.minValue+r.maxValue)/2/midpoint*100}%`}}/></span>
        <span>{((r.minValue+r.maxValue)/2/midpoint*100).toFixed(1)}%</span>
      </a>)}</div>
    </div>
    <div className={styles.listHeading}><h2>Estimated holdings</h2><span>Largest modeled midpoint first</span></div>
    <div className={styles.tableHead}><span>Rank / company</span><span>Estimated shares</span><span>Estimated value</span></div>
    {snapshot.rows.map((r,i) => <details className={styles.stockRow} key={r.ticker} id={`holding-${r.ticker}`}>
      <summary><div className={styles.identity}><span className={styles.rank}>{String(i+1).padStart(2,'0')}</span><CompanyLogo ticker={r.ticker}/><div><strong>{r.name}</strong><span>{r.ticker}</span>{r.minShares === 0 && r.maxShares > 0 && <span>Possibly sold out. Reported dollar ranges leave the remaining shares uncertain.</span>}</div></div>
        <span className={styles.quantity}>{shares(r.minShares)}–{shares(r.maxShares)}</span><strong className={styles.value}>{range(r.minValue,r.maxValue)} <span>⌄</span></strong></summary>
      <div className={styles.expanded}>{r.accounts.map((a,i) => <div key={`${a.account}-${i}`}>
        <p><strong>{a.account || 'Account not stated'}</strong> · Annual value: {a.reportedRange}. Valued using the completed close on {a.priceDate}.</p>
        <ul>{a.assumptions.map(note => <li key={note}>{note}</li>)}</ul>
        <a href={`${a.source}#page=${a.page}`} target="_blank" rel="noreferrer">Annual source · page {a.page} ↗</a>
      </div>)}</div>
    </details>)}
    <h2>Unresolved positions</h2><p>These are not treated as zero. They are excluded from all values and percentages above.</p>
    <div className={styles.grid}>{snapshot.unresolved.map(r => <article className={styles.card} key={r.ticker}>
      <div className={styles.row}><CompanyLogo ticker={r.ticker}/><strong>{r.ticker}</strong><span>Balance unresolved</span></div>
      <p>{r.name}</p><p>{r.reason}</p>
      {r.ticker==='HON' && <p>The Honeywell/Honeywell Aerospace conversion has a conditional share scenario, but neither position is included in the ranked subtotal.</p>}
      <a href={`${r.source}#page=${r.page}`} target="_blank" rel="noreferrer">Source filing · page {r.page} ↗</a>
    </article>)}</div>
    <details className={styles.group}><summary>Zero-balance scenarios · {snapshot.closed.length} accounts</summary><p>These modeled zeros are conditional on the reviewed records; they are not confirmed account closures.</p>{snapshot.closed.map((r,i) => <p key={i}>{r.ticker} · {r.account} · <a href={`${r.source}#page=${r.page}`}>Annual source ↗</a></p>)}</details>
    <details className={styles.group}><summary>Sources and methodology</summary>
      <p>Baseline: {snapshot.baselineDate}. Reviewed 72 annual asset rows and 35 transaction rows. Share ranges use disclosed dollar brackets and historical prices, with reviewed split adjustments. Unknown execution prices, omitted account labels and unreported changes limit accuracy.</p>
      <p>Duplicate tickers in separate accounts are combined in the list. Cash, private assets, liabilities, unresolved balances and potential Honeywell Aerospace shares are excluded. This is not net worth or a verified complete portfolio.</p>
      <p><a href="https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2025/10075481.pdf">Annual disclosure ↗</a> · <a href="https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2026FD.txt">2026 filing index ↗</a></p>
    </details>
  </section>;
}
