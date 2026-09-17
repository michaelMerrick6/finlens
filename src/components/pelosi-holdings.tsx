import { buildPelosiHoldings, pelosiHoldingsCoverage, type ReviewedPosition } from '@/lib/pelosi-holdings';
import { checkPelosiHoldingsCoverage } from '@/lib/pelosi-holdings-coverage';
import styles from './pelosi-holdings.module.css';

function Evidence({ position }: { position: ReviewedPosition }) {
  return <details className={styles.evidence}><summary>View evidence · {position.evidence.length}</summary>
    <ul>{position.evidence.map((e, i) => <li key={i}>{e.date} — {e.note}. <a href={`${e.url}#page=${e.page}`} target="_blank" rel="noreferrer">Source · page {e.page} ↗</a></li>)}</ul>
  </details>;
}
export async function PelosiHoldings() {
  const positions = buildPelosiHoldings();
  const coverage = await checkPelosiHoldingsCoverage();
  const options = positions.filter(p => p.kind === 'call' && p.status === 'reconstructed');
  const stocks = positions.filter(p => p.kind === 'stock' || p.kind === 'units');
  const other = positions.filter(p => p.status === 'closed' || p.status === 'unresolved');
  return <section className={styles.section} aria-labelledby="holdings-heading">
    <span className="eyebrow">PELOSI HOUSEHOLD · REVIEWED DISCLOSURES</span>
    <h2 id="holdings-heading">Estimated disclosed holdings</h2>
    <p className={styles.intro}>A reconstruction of reported positions, with the evidence behind each one. These securities were disclosed as spouse-owned.</p>
    <div className={styles.note}>
      <strong>2025 year-end baseline · filings reviewed through {pelosiHoldingsCoverage.latestFiling}</strong>
      <p>{coverage === 'matched' ? 'The 2026 House index matches the three reviewed filings (checked at most hourly).' : coverage === 'review-needed' ? 'The House index has changed. Additional review is needed; these positions remain based on the dated filings below.' : 'The latest House index could not be verified. These positions remain based on the dated filings below.'} Public records can omit or delay changes; current ownership is not confirmed.</p>
    </div>
    <h3>Option quantities reconstructed</h3>
    <p>Contracts remaining after the reviewed changes. Separate expirations are separate positions. These are not stock share counts.</p>
    <div className={styles.grid}>{options.map(p => <article className={styles.card} key={p.key}>
      <div className={styles.row}><strong>{p.ticker} calls</strong><strong>{p.contracts} contracts</strong></div>
      <p>${p.strike} strike · expires {p.expiration}</p><span className={styles.label}>Reconstructed from reviewed records</span><Evidence position={p}/>
    </article>)}</div>
    <details className={styles.group}><summary>Stocks & partnership units · {stocks.length} entries with uncertain total quantities</summary>
      <p>Ranges below are reported 2025 year-end values, not current market values. Known additions do not establish total shares; absence from the baseline does not prove a zero starting balance. Symbols are shown as filed.</p>
      <div className={styles.grid}>{stocks.map(p => <article className={styles.card} key={p.key}>
        <div className={styles.row}><strong>{p.ticker}{p.kind === 'units' ? ' units' : ''}</strong><span>Total quantity unknown</span></div>
        <p>2025 reported value: {p.reportedValue || 'No entry in reviewed baseline'}</p>
        <p>{p.shareChange ? `Known additions: +${p.shareChange.toLocaleString('en-US')} ${p.kind === 'units' ? 'units' : 'shares'}` : 'No quantity changes in the reviewed subsequent filings.'}</p>
        <Evidence position={p}/>
      </article>)}</div>
    </details>
    <details className={styles.group}><summary>Consumed options & unresolved entries · {other.length}</summary>
      {other.map(p => <article className={styles.card} key={p.key}><strong>{p.ticker}{p.kind === 'call' ? ` $${p.strike} calls · ${p.expiration}` : ''}</strong>
        <p>{p.status === 'closed' ? 'Zero contracts remaining in this series after reviewed exercises. Resulting shares appear separately above.' : 'Quantity or outcome unresolved; excluded from reconstructed option positions.'}</p><Evidence position={p}/></article>)}
    </details>
    <details className={styles.group}><summary>Coverage & method</summary>
      <p>The annual disclosure was filed May 15, 2026 and covers 2025. Changes use transaction dates after December 31, 2025, so late-filed 2025 activity is not counted twice. Owner, asset type, strike and expiration identify separate positions.</p>
      <p>Annual entries with value “None” (AVGO options, PYPL and DIS stock) are not counted as positive positions. Private assets, cash and untickered funds are outside the baseline. The subsequent REOF investment is retained as unresolved.</p>
      <p>We do not infer stock quantities from value-range midpoints, assume an option was exercised when it expires, or calculate portfolio totals from incomplete quantities. This ledger does not establish current market value, cost basis or returns.</p>
      <a href="https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2025/10075701.pdf" target="_blank" rel="noreferrer">Read the annual disclosure ↗</a>
    </details>
  </section>;
}
