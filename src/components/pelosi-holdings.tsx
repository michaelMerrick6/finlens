import { reconcilePelosiShares, pelosiQuantityBaselineSources } from '@/lib/pelosi-share-reconciliation';
import { getPelosiOtherAssets, getPelosiExcludedAssets } from '@/lib/pelosi-full-holdings';
import { buildPelosiHoldings, pelosiCompanyNames, pelosiHoldingsCoverage, type ReviewedPosition } from '@/lib/pelosi-holdings';
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
  const other = positions.filter(p => p.kind === 'call' && (p.status === 'closed' || p.status === 'unresolved'));
  const otherAssets = getPelosiOtherAssets().filter(p => p.reported_value !== 'None');
  const excluded = getPelosiExcludedAssets();
  const quantityEstimates = stocks.filter(p => reconcilePelosiShares(p).status === 'estimated').length;
  return <section className={styles.section} aria-labelledby="holdings-heading">
    <span className="eyebrow">PELOSI HOUSEHOLD · REVIEWED DISCLOSURES</span>
    <h1 id="holdings-heading">Estimated current holdings</h1>
    <p className={styles.intro}>Our best reconstruction from the latest reviewed annual holdings and subsequent position changes. Public securities were disclosed as spouse-owned; other ownership labels are shown individually.</p>
    <div className={styles.note}>
      <strong>2025 year-end baseline · filings reviewed through {pelosiHoldingsCoverage.latestFiling}</strong>
      <p>{coverage === 'matched' ? 'The 2025–2026 House indexes match the reviewed filing inventory (checked at most hourly).' : coverage === 'review-needed' ? 'The House index has changed. Additional review is needed; these positions remain based on the dated filings below.' : 'The latest House index could not be verified. These positions remain based on the dated filings below.'} Public records can omit or delay changes; current ownership is not confirmed.</p>
    </div>
    <h2>Stocks & partnership units · {stocks.length}</h2>
    <p>Our estimated holdings list carries forward securities with a positive annual value and adds subsequent acquisitions. It assumes no undisclosed disposals. There are {quantityEstimates} conditional share estimates; the remaining {stocks.length - quantityEstimates} positions have unresolved total quantities.</p>
    <div>
      <p>Ranges below are reported 2025 year-end values, not current market values. Known additions do not establish total shares; absence from the baseline does not prove a zero starting balance. Symbols are shown as filed.</p>
      <div className={styles.grid}>{stocks.map(p => <article className={styles.card} key={p.key}>
        <div className={styles.row}><strong>{pelosiCompanyNames[p.ticker] || p.ticker} · {p.ticker}{p.kind === 'units' ? ' units' : ''}</strong><span>{reconcilePelosiShares(p).shares !== null ? `Estimated: ${reconcilePelosiShares(p).shares!.toLocaleString('en-US')} shares` : 'Quantity unresolved'}</span></div>
        <p>2025 reported value: {p.reportedValue || 'No entry in reviewed baseline'}</p>
        <p>{p.shareChange ? `Known additions: +${p.shareChange.toLocaleString('en-US')} ${p.kind === 'units' ? 'units' : 'shares'}` : 'No quantity changes in the reviewed subsequent filings.'}</p>
        <details className={styles.evidence}><summary>Quantity reconciliation · {reconcilePelosiShares(p).status}</summary>
          <p>{reconcilePelosiShares(p).reason}</p>
          <ul>{reconcilePelosiShares(p).steps.map(step => <li key={`${step.date}-${step.note}`}>{step.date}: {step.delta > 0 ? '+' : ''}{step.delta.toLocaleString('en-US')} shares — {step.note}. <a href={step.source} target="_blank" rel="noreferrer">Source ↗</a></li>)}</ul>
          {p.shareChange !== 0 && <p>Subsequent reviewed net change: {p.shareChange > 0 ? '+' : ''}{p.shareChange.toLocaleString('en-US')} {p.kind === 'units' ? 'units' : 'shares'} (filings below).</p>}
          {reconcilePelosiShares(p).assumptions.map(a => <p key={a}>{a}</p>)}
        </details>
        <Evidence position={p}/>
      </article>)}</div>
    </div>
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
    <h2>Other disclosed assets · {otherAssets.length}</h2>
    <p>Private investments, property, cash accounts and other interests carried forward from the annual filing. Values are reported 2025 ranges, not updated appraisals. These are asset entries, not additional stock tickers.</p>
    <div className={styles.grid}>{otherAssets.map(p => <article className={styles.card} key={p.name}>
      <strong>{p.name}</strong><p>{p.kind.replaceAll('-', ' ')} · {p.owner === 'SP' ? 'Spouse' : p.owner === 'JT' ? 'Joint' : 'Owner field blank in filing'}</p>
      <p>Reported value: {p.reported_value}</p>
      {p.changes.map(e => <p key={e.doc_id}>Additional investment reported {e.date}; ownership quantity remains unknown. <a href={`${e.source}#page=${e.page}`} target="_blank" rel="noreferrer">View transaction ↗</a></p>)}
      <a href={p.source} target="_blank" rel="noreferrer">Annual source · page {p.page} ↗</a>
    </article>)}</div>
    <details className={styles.group}><summary>Excluded annual entries · {excluded.length} reported with value “None”</summary>
      <p>These entries are not counted in the estimated holdings list. “None” is preserved as filed, rather than converted to a current account balance.</p>
      <ul>{excluded.map(p => <li key={p.name}><a href={p.source} target="_blank" rel="noreferrer">{p.name} ↗</a></li>)}</ul>
    </details>
    <details className={styles.group}><summary>How we estimate holdings</summary>
      <p>Starting holdings + purchases − sales − gifts, adjusted for stock splits, spinoffs and option exercises. We use disclosed share or contract quantities where available. An exercise removes the option contracts and adds the resulting shares.</p>
      <p>The annual disclosure was filed May 15, 2026 and covers 2025. Changes use transaction dates after December 31, 2025, so late-filed 2025 activity is not counted twice. Owner, asset type, strike and expiration identify separate positions.</p>
      <p>Annual entries with value “None” (AVGO options, PYPL and DIS stock) are not counted as positive positions. All 68 asset entries in the reviewed annual Schedule A are accounted for: 32 securities entries and 36 other entries, including seven with value “None.” REOF XXV’s later investment is attached to its existing annual entry. Assets not disclosed in Schedule A are not inferred. This is a gross asset inventory, not net worth; liabilities are not subtracted, and related property/company interests are not summed.</p>
      <p>Eight stock quantities are conditional estimates using disclosed changes and an assumed opening balance; no stock total is labeled verified. We do not infer stock quantities from value-range midpoints, assume an option was exercised when it expires, or calculate portfolio totals from incomplete quantities. This ledger does not establish current market value, cost basis or returns.</p>
      <ul>{pelosiQuantityBaselineSources.map(s => <li key={s.url}><a href={s.url} target="_blank" rel="noreferrer">{s.label} ↗</a></li>)}</ul>
      <a href="https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2025/10075701.pdf" target="_blank" rel="noreferrer">Read the annual disclosure ↗</a>
    </details>
  </section>;
}
