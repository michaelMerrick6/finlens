'use client';

import Link from 'next/link';
import { useState, useRef } from 'react';
import { Avatar, CompanyLogo } from '@/components/identity-images';
import { Icon } from '@/components/icon';
import { TrackButton } from '@/components/account-provider';
import { compactMoney, filterStrategyHoldings, filterStrategyTrades, holdingSector, holdingsMix, sectorBreakdown,
  sectorSources, strategyDate as date, strategyMoney as money, strategyRange as range,
  type StrategyHolding, type StrategyTrade } from '@/lib/strategies/trump-overview';
import { StrategyOverview } from '@/components/strategy-overview';
import styles from '@/components/strategy-dashboard.module.css';

type Props = {
  initialQuery: string;
  stocks: StrategyHolding[];
  trades: StrategyTrade[];
  annual: { sourceUrl: string; holdingsAsOf: string; publishedOn: string };
  activity: { url: string; signedOn: string; receivedOn: string; reviewedOn: string; totalTransactionRows: number };
  djt: { shares: number; ownershipAsOf: string; sourceUrl: string; beneficiarySource: string };
};

export function TrumpDashboard({ initialQuery, stocks, trades, annual, activity, djt }: Props) {
  const [query, setQuery] = useState(initialQuery);
  const [sector, setSector] = useState('');
  const [otherStocks, setOtherStocks] = useState(false);
  const [tab, setTab] = useState<'holdings' | 'activity'>('holdings');
  const [direction, setDirection] = useState('');
  const [tradeQuery, setTradeQuery] = useState('');
  const [tradeLimit, setTradeLimit] = useState(10);
  const tableRef = useRef<HTMLElement>(null);
  const allPositions = [{ ticker: 'DJT', name: 'Trump Media & Technology Group' }, ...stocks];
  const sectors = sectorBreakdown(allPositions.map(row => row.ticker));
  const mix = holdingsMix(stocks);
  const topTickers = new Set(mix.segments.filter(row => row.ticker !== 'Other').map(row => row.ticker));
  const eligible = otherStocks ? allPositions.filter(row => row.ticker !== 'DJT' && !topTickers.has(row.ticker)) : allPositions;
  const shown = filterStrategyHoldings(eligible, query, sector);
  const shownTickers = new Set(shown.map(row => row.ticker));
  const filteredTrades = filterStrategyTrades(trades, tradeQuery, direction);
  const latest = filterStrategyTrades(trades, '', '').slice(0, 3);
  const annualSource = (page: number) => `${annual.sourceUrl}#page=${page}`;
  const viewHoldings = (ticker = '', nextSector = '') => {
    setQuery(ticker === 'Other' ? '' : ticker); setOtherStocks(ticker === 'Other'); setSector(nextSector); setTab('holdings');
    tableRef.current?.scrollIntoView({ behavior: 'auto', block: 'start' });
  };
  const viewActivity = () => { setTab('activity'); tableRef.current?.scrollIntoView({ behavior: 'auto', block: 'start' }); };

  return <main id="main" className={`container ${styles.page}`}>
    <Link href="/analysis/strategies" className={styles.back}>← All strategies</Link>
    <header className={styles.header}>
      <div className={styles.title}><Avatar name="Donald Trump" src="/images/politicians/donald-trump.png"/>
        <div><div className={styles.eyebrow}>DISCLOSURE STRATEGY</div><h1>Trump strategy</h1></div>
      </div>
      <TrackButton id="trump" name="Trump strategy" strategy/>
    </header>
    <p className={styles.intro}>The stocks, sectors and disclosed trades behind the portfolio.</p>
    <div className={styles.summaryStrip}>
      <div><span>Disclosed positions</span><strong>{allPositions.length}<small>including DJT</small></strong></div>
      <div><span>Sectors represented</span><strong>{sectors.length}<small>across the reviewed list</small></strong></div>
      <div><span>Annual holdings snapshot</span><strong><time dateTime={annual.holdingsAsOf}>{date(annual.holdingsAsOf)}</time></strong></div>
      <div><span>Latest report reviewed</span><strong><a href={activity.url} target="_blank" rel="noreferrer">{date(activity.signedOn)} <span aria-hidden="true">↗</span></a></strong></div>
    </div>
    <StrategyOverview mix={mix} sectors={sectors} onSelect={viewHoldings}
      mixDescription="Share of displayed annual dollar estimates"
      mixLabel="Breakdown of the displayed annual stock estimates; excludes DJT"
      mixNote={`DJT is shown separately in shares. The chart covers ${mix.positions} reviewed stocks, not the full portfolio.`}
      sectorDescription="By number of positions · includes DJT"
      highlight={
        <button className={styles.djtHighlight} onClick={() => viewHoldings('DJT')}>
          <CompanyLogo ticker="DJT"/><span><strong>DJT · Trump Media</strong><small>Trust stake · {date(djt.ownershipAsOf)}</small></span>
          <span className={styles.djtValue}><strong>{djt.shares.toLocaleString('en-US')}</strong><small>reported shares <span aria-hidden="true">↗</span></small></span>
        </button>
      }/>
    <section className={styles.recent} aria-labelledby="recent-title">
      <div className={styles.sectionHeading}><div><h2 id="recent-title">Latest reviewed trades</h2><p>Selected entries from the report signed {date(activity.signedOn)}</p></div><button onClick={viewActivity}>View activity <span aria-hidden="true">→</span></button></div>
      <div className={styles.tradeCards}>{latest.map(trade => <a key={trade.id} href={`${activity.url}#page=${trade.page}`} target="_blank" rel="noreferrer" className={styles.tradeCard}>
        <div className={styles.tradeCardTop}><CompanyLogo ticker={trade.ticker}/><strong>{trade.ticker}</strong><span className={trade.type === 'sale' ? styles.sell : styles.buy}>{trade.type === 'sale' ? 'Sell' : 'Buy'}</span></div>
        <strong className={styles.tradeCardAmount}>{compactMoney(trade.low)}–{compactMoney(trade.high)}</strong>
        <span className={styles.tradeCardDate}>Traded {date(trade.date)} <Icon name="external" size={12}/></span>
      </a>)}</div>
    </section>
    <section id="holdings" ref={tableRef} className={styles.holdings} aria-label="Strategy holdings and activity">
      <div className={styles.toolbar}>
        <div className={styles.tabs} role="tablist" aria-label="Strategy data">
          {(['holdings', 'activity'] as const).map(value => <button key={value} id={`tab-${value}`} role="tab"
            aria-selected={tab === value} aria-controls={`panel-${value}`} tabIndex={tab === value ? 0 : -1}
            onKeyDown={event => { if (['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) {
              event.preventDefault(); const next = event.key === 'Home' ? 'holdings' : event.key === 'End' ? 'activity' : tab === 'holdings' ? 'activity' : 'holdings';
              setTab(next); document.getElementById(`tab-${next}`)?.focus();
            } }} onClick={() => setTab(value)}>{value === 'holdings' ? 'Holdings' : 'Recent activity'} <span>{value === 'holdings' ? allPositions.length : trades.length}</span></button>)}
        </div>
        <label className={styles.search}><Icon name="search" size={15}/><span className="sr-only">Search {tab === 'holdings' ? 'holdings' : 'trades'}</span>
          <input type="search" value={tab === 'holdings' ? query : tradeQuery} placeholder="Search stocks" maxLength={120}
            onChange={event => tab === 'holdings' ? setQuery(event.target.value) : (setTradeQuery(event.target.value), setTradeLimit(10))}/></label>
      </div>
      <div role="tabpanel" id="panel-holdings" aria-labelledby="tab-holdings" hidden={tab !== 'holdings'}>
        <div className={styles.tableIntro}><p>Annual value estimates · {date(annual.holdingsAsOf)}</p><label className={styles.sectorSelect}><span className="sr-only">Filter holdings by sector</span><select value={sector} onChange={event => setSector(event.target.value)}><option value="">All sectors</option>{sectors.map(row => <option key={row.name}>{row.name}</option>)}</select></label></div>
        {(query || sector || otherStocks) && <div className={styles.searchResult} role="status">{shown.length} result{shown.length === 1 ? '' : 's'}{query ? ` for “${query}”` : ''}{sector ? ` in ${sector}` : ''}{otherStocks ? ' · Other annual stocks' : ''}<button onClick={() => { setQuery(''); setSector(''); setOtherStocks(false); }}>Clear filters ×</button></div>}
        <div className={styles.columns} aria-hidden="true"><span>Company</span><span>Sector</span><span>Disclosed holding</span><span/></div>
        {shownTickers.has('DJT') && <details id="holding-DJT" className={styles.holding}>
          <summary><span className={styles.identity}><CompanyLogo ticker="DJT"/><span><strong>DJT</strong><small>Trump Media &amp; Technology Group</small></span></span><span className={styles.sectorCell}>{holdingSector('DJT')}</span><span className={styles.amount}><strong>{djt.shares.toLocaleString('en-US')}</strong><small>reported shares</small></span><span className={styles.chevron} aria-hidden="true">⌄</span></summary>
          <div className={styles.evidence}><p><strong>{djt.shares.toLocaleString('en-US')} shares</strong> reported as of {date(djt.ownershipAsOf)}, held through the Donald J. Trump Revocable Trust. The trustee’s personal shares are excluded.</p><div className={styles.sourceLinks}><a href={djt.sourceUrl} target="_blank" rel="noreferrer">SEC share filing ↗</a><a href={djt.beneficiarySource} target="_blank" rel="noreferrer">Trust ownership ↗</a><a href={annualSource(865)} target="_blank" rel="noreferrer">Annual filing · p. 865 ↗</a></div></div>
        </details>}
        {stocks.filter(stock => shownTickers.has(stock.ticker)).map(stock => <details key={stock.ticker} id={`holding-${stock.ticker}`} className={styles.holding}>
          <summary><span className={styles.identity}><CompanyLogo ticker={stock.ticker}/><span><strong>{stock.ticker}</strong><small>{stock.name}</small></span></span><span className={styles.sectorCell}>{holdingSector(stock.ticker)}</span><span className={styles.amount} title={`Reported range: ${range(stock.low, stock.high)}`}><strong>{stock.estimatedReportedValue === null ? 'Unavailable' : money(stock.estimatedReportedValue)}</strong><small>estimated value</small></span><span className={styles.chevron} aria-hidden="true">⌄</span></summary>
          <div className={styles.evidence}><p><strong>{range(stock.low, stock.high)}</strong> reported as of {date(annual.holdingsAsOf)}. The displayed estimate is the midpoint, rounded to the nearest dollar.</p><p>{stock.rows.length} reviewed entries. Additional entries may remain unidentified.</p><ul>{stock.rows.map(row => <li key={row.id}><a href={annualSource(row.page)} target="_blank" rel="noreferrer">Page {row.page}, line {row.line} ↗</a><span>{row.account.replace('INVESTMENT ACCOUNT', 'Account')} · {range(row.low, row.high)}</span><span>{row.name}</span></li>)}</ul></div>
        </details>)}
        {!shown.length && <div className={styles.empty}><Icon name="search" size={24}/><h3>No holdings found</h3><p>Try another company or sector.</p><button onClick={() => { setQuery(''); setSector(''); setOtherStocks(false); }}>Clear filters</button></div>}
      </div>
      <div role="tabpanel" id="panel-activity" aria-labelledby="tab-activity" hidden={tab !== 'activity'}>
        <div className={styles.tableIntro}><p>{trades.length} reviewed entries of {activity.totalTransactionRows.toLocaleString('en-US')} in the August report</p><div className={styles.tradeFilters} role="group" aria-label="Trade direction">{[['', 'All'], ['purchase', 'Buys'], ['sale', 'Sells']].map(([value, label]) => <button key={value} aria-pressed={direction === value} onClick={() => { setDirection(value); setTradeLimit(10); }}>{label}</button>)}</div></div>
        <div className={styles.tradeTableWrap}><table className={styles.tradeTable}><thead><tr><th scope="col">Company</th><th scope="col">Action</th><th scope="col">Trade date</th><th scope="col">Reported range</th><th scope="col"><span className="sr-only">Original filing</span></th></tr></thead><tbody>{filteredTrades.slice(0, tradeLimit).map(trade => <tr key={trade.id}>
          <td><span className={styles.identity}><CompanyLogo ticker={trade.ticker}/><span><strong>{trade.ticker}</strong><small>{trade.name}</small></span></span></td><td><span className={trade.type === 'sale' ? styles.sell : styles.buy}>{trade.type === 'sale' ? 'Sell' : 'Buy'}</span></td><td><time dateTime={trade.date}>{date(trade.date)}</time></td><td className={styles.tradeAmount}>{range(trade.low, trade.high)}</td><td><a href={`${activity.url}#page=${trade.page}`} target="_blank" rel="noreferrer" aria-label={`Source for ${trade.ticker} ${trade.type}, ${date(trade.date)}, page ${trade.page}, line ${trade.line}`}><Icon name="external" size={14}/></a></td>
        </tr>)}</tbody></table></div>
        {!filteredTrades.length && <div className={styles.empty}><h3>No reviewed trades match</h3><button onClick={() => { setTradeQuery(''); setDirection(''); }}>Clear filters</button></div>}
        {filteredTrades.length > tradeLimit && <button className={styles.loadMore} onClick={() => setTradeLimit(limit => limit + 20)}>Show more trades <span>({filteredTrades.length - tradeLimit} remaining)</span></button>}
        <p className={styles.activityNote}>Report signed {date(activity.signedOn)} · received {date(activity.receivedOn)}. This is a reviewed sample; newer trades may appear in unreviewed entries. Sales do not necessarily close a position.</p>
      </div>
    </section>
    <details className={styles.notes} id="sources"><summary><span className={styles.status}>Partial disclosure coverage</span><span>Sources &amp; methodology <span aria-hidden="true">⌄</span></span></summary><div className={styles.notesBody}>
      <p>The annual snapshot is dated {date(annual.holdingsAsOf)} and was published {date(annual.publishedOn)}. Values are midpoints of disclosed ranges, not exact balances or current market prices. The stock list is partially reviewed and later trades have not been applied to holdings.</p>
      <p>The holdings donut uses only the {mix.positions} displayed annual dollar estimates. DJT remains in the holdings list and sector breakdown, but its reported shares cannot be combined with those dollar amounts. Sector percentages count ticker positions, including separate share classes, and are not portfolio weights.</p>
      <p>Trade dates come from the filing; report signing and receipt dates are shown separately. Original publication timing is unverified. Funds, bonds and private assets are excluded.</p>
      <div className={styles.sourceLinks}><a href={annual.sourceUrl} target="_blank" rel="noreferrer">OGE annual filing ↗</a><a href={activity.url} target="_blank" rel="noreferrer">White House trade filing ↗</a><a href="https://www.whitehouse.gov/disclosures/" target="_blank" rel="noreferrer">All public filings ↗</a><a href="/api/strategies/trump/holdings">Download source data ↓</a></div>
      <p className={styles.sectorSource}>Sector classifications: {sectorSources.map((source, i) => <span key={source.id}>{i ? ' · ' : ''}<a href={source.url} target="_blank" rel="noreferrer">iShares {source.id}</a> ({date(source.holdingsAsOf)})</span>)}.</p>
    </div></details>
  </main>;
}
