'use client';

import Link from 'next/link';
import { useRef, useState } from 'react';
import { Avatar, CompanyLogo } from '@/components/identity-images';
import { TrackButton } from '@/components/account-provider';
import { Icon } from '@/components/icon';
import { StrategyOverview } from '@/components/strategy-overview';
import { pelosiStrategyOverview, type PelosiStrategyHolding, type PelosiStrategyActivity } from '@/lib/strategies/pelosi-overview';
import { compactMoney, strategyDate as date, strategyMoney as money } from '@/lib/strategies/strategy-format';
import styles from '@/components/strategy-dashboard.module.css';

type Props = {
  rows: PelosiStrategyHolding[]; activity: PelosiStrategyActivity[];
  note: string; checkedAt: string | null; latestReviewedFiling: string;
  sectorSources: { id: string; url: string; holdingsAsOf: string }[];
};
const number = (value: number) => value.toLocaleString('en-US');
const percent = (weight: number) => weight * 100 < .1 ? '<0.1%' : `${(weight * 100).toFixed(1)}%`;
const matches = (row: { ticker: string; name: string }, query: string) => query.trim().toLowerCase().split(/\s+/)
  .every(term => `${row.ticker} ${row.name}`.toLowerCase().includes(term));

export function PelosiDashboard({ rows, activity, note, checkedAt, latestReviewedFiling, sectorSources }: Props) {
  const [tab, setTab] = useState<'holdings' | 'activity'>('holdings');
  const [query, setQuery] = useState('');
  const [sector, setSector] = useState('');
  const [other, setOther] = useState(false);
  const [tradeQuery, setTradeQuery] = useState('');
  const [direction, setDirection] = useState('');
  const [tradeLimit, setTradeLimit] = useState(10);
  const tableRef = useRef<HTMLElement>(null);
  const { mix, sectors } = pelosiStrategyOverview(rows);
  const top = new Set(mix.segments.filter(row => row.ticker !== 'Other').map(row => row.ticker));
  const shown = rows.filter(row => matches(row, query) && (!sector || row.sector === sector)
    && (!other || (row.midpoint !== null && row.midpoint > 0 && !top.has(row.ticker))));
  const trades = activity.filter(row => matches(row, tradeQuery) && (!direction || row.direction === direction));
  const quoteDates = [...new Set(rows.flatMap(row => row.quote ? [row.quote.date] : []))].sort();
  const priceDate = quoteDates.length > 1 ? `${date(quoteDates[0])} – ${date(quoteDates[quoteDates.length - 1])}` : quoteDates.length ? date(quoteDates[0]) : 'Unavailable';
  const excluded = rows.filter(row => row.midpoint === null);
  const clear = () => { setQuery(''); setSector(''); setOther(false); };
  const viewHoldings = (ticker = '', nextSector = '') => {
    setQuery(ticker === 'Other' ? '' : ticker); setOther(ticker === 'Other'); setSector(nextSector); setTab('holdings');
    tableRef.current?.scrollIntoView({ behavior: 'auto', block: 'start' });
  };
  const viewActivity = () => { setTab('activity'); tableRef.current?.scrollIntoView({ behavior: 'auto', block: 'start' }); };
  const badge = (row: PelosiStrategyActivity) => row.direction === 'buy' ? styles.buy : row.direction === 'sell' ? styles.sell : styles.badge;

  return <main id="main" className={`container ${styles.page}`}>
    <Link href="/analysis/strategies" className={styles.back}>← All strategies</Link>
    <header className={styles.header}>
      <div className={styles.title}><Avatar name="Nancy Pelosi" memberId="P000197"/><div><div className={styles.eyebrow}>DISCLOSURE STRATEGY</div><h1>Pelosi strategy</h1></div></div>
      <TrackButton id="P000197" name="Nancy Pelosi"/>
    </header>
    <p className={styles.intro}>The stocks, sectors and disclosed activity behind the household’s estimated portfolio.</p>
    <div className={styles.summaryStrip}>
      <div><span>Estimated positions</span><strong>{rows.length}<small>stocks &amp; units</small></strong></div>
      <div><span>Sectors represented</span><strong>{sectors.filter(row => row.name !== 'Unclassified').length}<small>across the reviewed list</small></strong></div>
      <div><span>Market prices as of</span><strong>{priceDate}</strong></div>
      <div><span>Latest filing reviewed</span><strong><Link href="/politicians/P000197/holdings">{date(latestReviewedFiling)} <span aria-hidden="true">↗</span></Link></strong></div>
    </div>
    <StrategyOverview mix={mix} sectors={sectors} onSelect={viewHoldings}
      mixDescription="Share of estimated stock and unit values"
      mixLabel="Estimated market-value allocation of priced Pelosi household stocks and partnership units"
      mixNote={`Based on ${mix.positions} of ${rows.length} positions with supported quantities and recent prices. Range midpoints set the weights; these are estimates.`}
      sectorDescription="By number of stock and unit positions"/>
    <section className={styles.recent} aria-labelledby="recent-title">
      <div className={styles.sectionHeading}><div><h2 id="recent-title">Latest reviewed activity</h2><p>Disclosed stock and partnership changes</p></div><button onClick={viewActivity}>View activity <span aria-hidden="true">→</span></button></div>
      <div className={styles.tradeCards}>{activity.slice(0, 3).map(row => <a key={row.id} href={row.source} target="_blank" rel="noreferrer" className={styles.tradeCard}>
        <div className={styles.tradeCardTop}><CompanyLogo ticker={row.ticker}/><strong>{row.ticker}</strong><span className={badge(row)}>{row.action}</span></div>
        <strong className={styles.tradeCardAmount}>{number(row.quantity)} {row.unit}</strong>
        <span className={styles.tradeCardDate}>{date(row.date)} <Icon name="external" size={12}/></span>
      </a>)}</div>
      {!activity.length && <p className={styles.chartNote}>No reviewed stock changes are available.</p>}
    </section>
    <section ref={tableRef} id="holdings" className={styles.holdings} aria-label="Strategy holdings and activity">
      <div className={styles.toolbar}>
        <div className={styles.tabs} role="tablist" aria-label="Strategy data">
          {(['holdings', 'activity'] as const).map(value => <button key={value} id={`tab-${value}`} role="tab" aria-selected={tab === value}
            aria-controls={`panel-${value}`} tabIndex={tab === value ? 0 : -1} onClick={() => setTab(value)}
            onKeyDown={event => { if (['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) {
              event.preventDefault(); const next = event.key === 'Home' ? 'holdings' : event.key === 'End' ? 'activity' : tab === 'holdings' ? 'activity' : 'holdings';
              setTab(next); document.getElementById(`tab-${next}`)?.focus();
            } }}>{value === 'holdings' ? 'Holdings' : 'Recent activity'} <span>{value === 'holdings' ? rows.length : activity.length}</span></button>)}
        </div>
        <label className={styles.search}><Icon name="search" size={15}/><span className="sr-only">Search {tab === 'holdings' ? 'holdings' : 'activity'}</span>
          <input type="search" placeholder="Search stocks" maxLength={120} value={tab === 'holdings' ? query : tradeQuery}
            onChange={event => { if (tab === 'holdings') setQuery(event.target.value); else { setTradeQuery(event.target.value); setTradeLimit(10); } }}/></label>
      </div>
      <div role="tabpanel" id="panel-holdings" aria-labelledby="tab-holdings" hidden={tab !== 'holdings'}>
        <div className={styles.tableIntro}><p>Estimated allocation · prices {priceDate}</p><label className={styles.sectorSelect}><span className="sr-only">Filter holdings by sector</span><select value={sector} onChange={event => setSector(event.target.value)}><option value="">All sectors</option>{sectors.map(row => <option key={row.name}>{row.name}</option>)}</select></label></div>
        {(query || sector || other) && <div className={styles.searchResult} role="status">{shown.length} result{shown.length === 1 ? '' : 's'}{query ? ` for “${query}”` : ''}{sector ? ` in ${sector}` : ''}{other ? ' · Other priced positions' : ''}<button onClick={clear}>Clear filters ×</button></div>}
        <div className={styles.columns} aria-hidden="true"><span>Company</span><span>Sector</span><span>Estimated allocation</span><span/></div>
        {shown.map(row => <details key={row.position.key} id={`holding-${row.ticker}`} className={styles.holding}>
          <summary><span className={styles.identity}><CompanyLogo ticker={row.ticker}/><span><strong>{row.ticker}</strong><small>{row.name}</small></span></span><span className={styles.sectorCell}>{row.sector}</span>
            <span className={styles.amount}><strong>{row.weight === null ? 'Unavailable' : percent(row.weight)}</strong><small>{row.midpoint === null ? 'Recent price or quantity missing' : `${compactMoney(row.midpoint)} estimated value`}</small></span><span className={styles.chevron} aria-hidden="true">⌄</span></summary>
          <div className={styles.evidence}>
            <p><strong>Estimated quantity:</strong> {row.quantity.range ? `${number(row.quantity.range.min)}–${number(row.quantity.range.max)}` : row.quantity.shares !== null ? `~${number(row.quantity.shares)}` : 'Not determined'} {row.position.kind === 'units' ? 'units' : 'shares'}. {row.quantity.reason}</p>
            {row.min !== null && row.max !== null && <p><strong>Estimated market value:</strong> {row.min === row.max ? money(row.min) : `${money(row.min)}–${money(row.max)}`}. {row.quote && `Price: $${row.quote.price.toFixed(2)} · ${date(row.quote.date)} · Yahoo Finance.`} Range midpoints set the allocation weights.</p>}
            {row.position.reportedValue && <p>Annual reported value: {row.position.reportedValue} at December 31, 2025.</p>}
            <ul>{row.position.evidence.map((source, i) => <li key={i}>{date(source.date)} · {source.note}. <a href={`${source.url}#page=${source.page}`} target="_blank" rel="noreferrer">Original filing ↗</a></li>)}</ul>
            <Link href={`/politicians/P000197/holdings#holding-${row.position.ticker}`}>Full calculation &amp; assumptions →</Link>
          </div>
        </details>)}
        {!shown.length && <div className={styles.empty}><Icon name="search" size={24}/><h3>No holdings found</h3><p>Try another company or sector.</p><button onClick={clear}>Clear filters</button></div>}
        {excluded.length > 0 && <p className={styles.activityNote}>Excluded from allocation weights because a supported quantity or recent price is unavailable: {excluded.map(row => row.ticker).join(', ')}.</p>}
      </div>
      <div role="tabpanel" id="panel-activity" aria-labelledby="tab-activity" hidden={tab !== 'activity'}>
        <div className={styles.tableIntro}><p>{activity.length} reviewed stock and unit changes since the annual snapshot</p><div className={styles.tradeFilters} role="group" aria-label="Activity type">{[['', 'All'], ['buy', 'Buys'], ['sell', 'Sells']].map(([value, label]) => <button key={value} aria-pressed={direction === value} onClick={() => { setDirection(value); setTradeLimit(10); }}>{label}</button>)}</div></div>
        <div className={styles.tradeTableWrap}><table className={styles.tradeTable} data-activity><thead><tr><th scope="col">Company</th><th scope="col">Action</th><th scope="col">Activity date</th><th scope="col">Disclosed quantity</th><th scope="col"><span className="sr-only">Original filing</span></th></tr></thead><tbody>{trades.slice(0, tradeLimit).map(row => <tr key={row.id}>
          <td><span className={styles.identity}><CompanyLogo ticker={row.ticker}/><span><strong>{row.ticker}</strong><small>{row.name}</small></span></span></td><td><span className={badge(row)}>{row.action}</span></td><td><time dateTime={row.date}>{date(row.date)}</time></td><td className={styles.tradeAmount}>{number(row.quantity)} {row.unit}</td><td><a href={row.source} target="_blank" rel="noreferrer" aria-label={`Source for ${row.ticker} ${row.action}, ${date(row.date)}`}><Icon name="external" size={14}/></a></td>
        </tr>)}</tbody></table></div>
        {!trades.length && <div className={styles.empty}><h3>No reviewed activity matches</h3><button onClick={() => { setTradeQuery(''); setDirection(''); }}>Clear filters</button></div>}
        {trades.length > tradeLimit && <button className={styles.loadMore} onClick={() => setTradeLimit(limit => limit + 20)}>Show more activity <span>({trades.length - tradeLimit} remaining)</span></button>}
        <p className={styles.activityNote}>Stock and partnership changes used in the holdings estimate. Option exercises are labeled separately from purchases. <Link href="/politicians/P000197">View the household’s full trading history →</Link></p>
      </div>
    </section>
    <details className={styles.notes} id="sources"><summary><span className={styles.status}>Estimated holdings · {checkedAt ? 'allocation preview' : 'reviewed baseline'}</span><span>Sources &amp; methodology <span aria-hidden="true">⌄</span></span></summary><div className={styles.notesBody}>
      <p>{note}{checkedAt ? ` Filing check: ${date(checkedAt)}.` : ''}</p>
      <p>The baseline covers December 31, 2025, with reviewed stock purchases, sales, gifts, exercises and corporate actions applied afterward. Quantities remain estimates and assume no undisclosed changes. These are household holdings, including spouse-owned securities.</p>
      <p>Allocation weights use estimated quantities multiplied by available market prices. Quantity-range midpoints set the weights, normalized across priced positions only. Missing prices and prices older than five days are excluded. Prices: {priceDate}.</p>
      <p>Sector percentages count stock and partnership positions, including unclassified holdings. Options, cash, real estate and private assets are outside this allocation. Historical performance and scheduled rebalancing are not enabled.</p>
      <div className={styles.sourceLinks}><Link href="/politicians/P000197/holdings">Holdings, options &amp; source records →</Link><a href="https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2025/10075701.pdf" target="_blank" rel="noreferrer">House annual disclosure ↗</a><Link href="/tracking">Notification settings →</Link></div>
      <p className={styles.sectorSource}>Sector classifications: {sectorSources.map((source, i) => <span key={source.id}>{i ? ' · ' : ''}<a href={source.url} target="_blank" rel="noreferrer">iShares {source.id}</a> ({date(source.holdingsAsOf)})</span>)}. Unmatched securities remain unclassified.</p>
    </div></details>
  </main>;
}
