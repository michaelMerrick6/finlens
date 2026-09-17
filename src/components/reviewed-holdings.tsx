import { getReviewedBaseline } from '@/lib/reviewed-holdings';
import { getReviewedHoldings } from '@/lib/reviewed-holdings-server';
import { holdingDollars } from '@/lib/pelosi-portfolio';
import { CompanyLogo } from './identity-images';
import styles from './pelosi-holdings.module.css';

export async function ReviewedHoldings({ memberId }: { memberId: string }) {
  const baseline = getReviewedBaseline(memberId);
  if (!baseline) return null;
  const snapshot = await getReviewedHoldings(memberId);
  const dateLabel = (value: string) => new Date(`${value}T00:00:00Z`).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' });
  const dollars = (value: number) => value.toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 });
  const rows = [...snapshot.rows].sort((a,b) => (b.estimate?.midpoint ?? -1) - (a.estimate?.midpoint ?? -1) || a.ticker.localeCompare(b.ticker));
  const complete = rows.every(row => row.estimate !== null);
  const totalMin = rows.reduce((n, r) => n + (r.estimate?.min ?? 0), 0);
  const totalMax = rows.reduce((n, r) => n + (r.estimate?.max ?? 0), 0);
  const totalMid = (totalMin + totalMax) / 2;
  const shares = (n: number) => n.toLocaleString('en-US', { maximumFractionDigits: 1 });
  return <section className={styles.section}>
    <span className="eyebrow">{baseline.name.toUpperCase()} · ESTIMATED STOCK HOLDINGS</span>
    <h1>Estimated stock holdings.</h1>
    <p className={styles.intro}>{baseline.positions.length} {baseline.positions.length === 1 ? 'stock' : 'stocks'} carried forward from the reviewed annual disclosure. Values assume no unreported changes; share counts are modeled from disclosed dollar ranges.</p>
    <div className={styles.stats}>
      <div><strong>{complete ? `${holdingDollars(totalMin)}–${holdingDollars(totalMax)}` : 'Unavailable'}</strong><span>Modeled stock value · excludes liabilities</span></div>
      <div><strong>{baseline.positions.length}</strong><span>Stocks in the annual baseline</span></div>
      <div><strong>{dateLabel(baseline.date)}</strong><span>Baseline date · filed {dateLabel(baseline.filed)}</span></div>
    </div>
    <p className={styles.note}>{snapshot.status === 'checked'
      ? `No additional filings found in the checked ${baseline.date.slice(0,4)}–${new Date().getUTCFullYear()} House indexes. Source check: ${new Date(snapshot.checkedAt!).toLocaleString('en-US', { timeZone: 'UTC' })} UTC. Undisclosed or delayed changes remain possible.`
      : snapshot.status === 'review-needed' ? 'A new filing needs review. Current estimates are paused; the reviewed annual entries remain below.'
      : 'The source check is unavailable. Current estimates are paused; the reviewed annual entries remain below.'}</p>
    {complete && <div className={styles.visual}>
      <div><span className="eyebrow">ILLUSTRATIVE ALLOCATION</span><h2>The public-stock portion.</h2><p>{rows.length === 1 ? 'One public stock appears in this baseline. This is not the allocation of all assets.' : 'Ranked by modeled midpoint value. The broad ranges overlap, so the actual order and weights may differ.'}</p></div>
      <div className={styles.allocation}>{rows.map((row, i) => <a className={styles.allocationRow} href={`#holding-${row.ticker}`} key={row.ticker}>
        <span>{String(i+1).padStart(2,'0')}</span><CompanyLogo ticker={row.ticker}/><strong>{row.ticker}</strong>
        <span className={styles.allocationTrack}><span style={{width: `${row.estimate!.midpoint / totalMid * 100}%`}}/></span>
        <span>{(row.estimate!.midpoint / totalMid * 100).toFixed(0)}%</span>
      </a>)}</div>
    </div>}
    <div className={styles.listHeading}><h2>{complete ? 'Estimated holdings' : 'Reviewed annual holdings'}</h2><span>{complete ? 'Largest modeled midpoint first' : 'Current values unavailable'}</span></div>
    <div className={styles.tableHead}><span>Company</span><span>Modeled shares</span><span>Estimated value</span></div>
    {rows.map((row, i) => <details className={styles.stockRow} id={`holding-${row.ticker}`} key={row.ticker}>
      <summary><div className={styles.identity}><span className={styles.rank}>{complete ? String(i+1).padStart(2,'0') : '—'}</span><CompanyLogo ticker={row.ticker}/><div><strong>{row.name}</strong><span>{row.ticker}</span></div></div>
        <span className={styles.quantity}>{row.estimate ? `${shares(row.estimate.minShares)}–${shares(row.estimate.maxShares)}` : 'Unavailable'}</span>
        <strong className={styles.value}>{row.estimate ? `${holdingDollars(row.estimate.min)}–${holdingDollars(row.estimate.max)}` : 'Unavailable'} <span>⌄</span></strong>
      </summary>
      <div className={styles.expanded}>
        <p>Reported {dateLabel(baseline.date)} value: <strong>{dollars(row.min)}–{dollars(row.max)}</strong>. The owner column is blank. Exact shares are not disclosed.</p>
        {row.estimate && <p>Calculation: {dollars(row.min)}–{dollars(row.max)} ÷ ${row.estimate.baseline.close.toFixed(2)} closing price on {row.estimate.baseline.date} = {shares(row.estimate.minShares)}–{shares(row.estimate.maxShares)} modeled shares on the current split basis. Multiplied by ${row.estimate.latest.close.toFixed(2)} on {row.estimate.latest.date} for the value shown.</p>}
        <p><a href={`${baseline.source}#page=1`} target="_blank" rel="noreferrer">Annual filing · Schedule A, page 1 ↗</a> · <a href={`https://finance.yahoo.com/quote/${row.ticker}/history/`} target="_blank" rel="noreferrer">Historical prices ↗</a></p>
      </div>
    </details>)}
    {baseline.otherAssets.length > 0 && <details className={styles.group}><summary>Other disclosed assets · {baseline.otherAssets.length}</summary>
      <p>Historical values as of {dateLabel(baseline.date)}. These are excluded from the stock graphic and current stock estimate; they have not been revalued.</p>
      <div className={styles.grid}>{baseline.otherAssets.map(asset => <article className={styles.card} key={asset.name}>
        <strong>{asset.name}</strong><p>{asset.kind} · {dollars(asset.min)}–{dollars(asset.max)}</p>
        <a href={`${baseline.source}#page=1`}>Annual filing · page 1 ↗</a>
      </article>)}</div>
    </details>}
    <details className={styles.group}><summary>Sources and assumptions</summary>
      <p>Both pages of annual filing {baseline.docId} were visually reviewed on {baseline.reviewed}. {baseline.reviewNote}</p>
      <p>This model divides each year-end value bracket by its historical closing price, then carries the resulting range forward with no reported changes. Prices use the same split-adjusted, non-dividend-adjusted daily-close series at both endpoints. Dividends are not reinvested. This is a conditional estimate, not a confirmed account balance or net worth.</p>
      <p>Sources and prices refresh on the first visit each UTC day. A new filing or changed annual PDF pauses the estimates for review. This pilot does not yet automatically interpret new purchases or sales.</p>
      <p><a href="https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2025FD.txt">2025 House index</a> · <a href="https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2026FD.txt">2026 House index</a> · <a href={baseline.source}>Original annual disclosure</a></p>
    </details>
  </section>;
}
