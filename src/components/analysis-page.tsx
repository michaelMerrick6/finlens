'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import { readPublicPage, storePublicPage } from '@/lib/public-page-cache';
import { DashboardSkeleton } from './loading-state';
import { AnalysisStockTools } from './analysis-stock-tools';
import { AnalysisStockTrades } from './analysis-stock-trades';
import { AnalysisCharts } from './analysis-charts';
import { CompanyLogo } from './identity-images';
import { TradeDetails } from './disclosure-feed';
import { Icon } from './icon';
import { dateLabel } from '@/lib/ui-format';
import { analysisMoney as money, analysisNumber as number, matchesAnalysisStock,
  type AnalysisData, type AnalysisStock, type AnalysisPeriod, type AnalysisBasis } from '@/lib/analysis-overview';
import type { AnalysisTrade } from '@/lib/congress-analysis';
import styles from './analysis.module.css';

const periods = [['7', '1 week'], ['30', '30 days'], ['ytd', 'Year to date'], ['year', '1 year']] as const;
const range = (min: number, max: number) => max ? `${money(min)}–${money(max)}` : '—';

function NetAmount({ stock }: { stock: AnalysisStock }) {
  const net = (stock.purchaseMin + stock.purchaseMax - stock.saleMin - stock.saleMax) / 2;
  const incomplete = stock.unknownAmounts > 0;
  return <span className="analysis-net" title="Purchase range midpoints minus sale range midpoints. An estimate, not profit or an exact dollar amount.">
    <small className={styles.rowLabel}>Net</small><strong className={incomplete || net === 0 ? 'net-neutral' : net > 0 ? 'net-positive' : 'net-negative'}>
      {incomplete ? 'Incomplete' : `${net > 0 ? '+' : net < 0 ? '−' : ''}${money(Math.abs(net))}`}
    </strong>{!incomplete && <small className={stock.inferredAmounts ? undefined : styles.rowLabel}>{stock.inferredAmounts ? 'Estimated · standard ranges' : 'Estimated'}</small>}
  </span>;
}

export function AnalysisPage({ initialData }: { initialData: AnalysisData }) {
  const [query, setQuery] = useState('');
  const [sector, setSector] = useState('');
  const [period, setPeriod] = useState<AnalysisPeriod>('30');
  const [basis, setBasis] = useState<AnalysisBasis>('filed');
  const [instrument, setInstrument] = useState('stocks');
  const [sort, setSort] = useState('buyers');
  const [revision, setRevision] = useState(0);
  const cacheKey = `analysis-summary:${period}:${basis}:${instrument}`;
  const [data, setData] = useState<AnalysisData | null>(() => initialData);
  const [displayedFilters, setDisplayedFilters] = useState({ period, basis, instrument });
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(!data);
  const [selected, setSelected] = useState<AnalysisTrade | null>(null);
  const [limit, setLimit] = useState(25);
  const [expandedStocks, setExpandedStocks] = useState<Set<string>>(() => new Set());
  const seeded = useRef(false);
  const listRef = useRef<HTMLElement>(null);

  useEffect(() => {
    const controller = new AbortController();
    async function loadAnalysis() {
      const saved = readPublicPage<AnalysisData>(cacheKey);
      if (saved) return saved;
      if (!seeded.current && !revision && period === '30' && basis === 'filed' && instrument === 'stocks' && initialData.end === new Date().toISOString().slice(0, 10)) {
        seeded.current = true;
        storePublicPage(cacheKey, initialData);
        return initialData;
      }
      const response = await fetch(`/api/analysis?${new URLSearchParams({ period, basis, instrument })}`, { signal: controller.signal });
      const result = await response.json();
      if (!response.ok) throw Error(result.error || 'Analysis is temporarily unavailable.');
      if (!controller.signal.aborted) storePublicPage(cacheKey, result);
      return result as AnalysisData;
    }
    loadAnalysis().then(result => {
        if (controller.signal.aborted) return;
        setData(result);
        setDisplayedFilters({ period, basis, instrument });
      })
      .catch(error => { if (!controller.signal.aborted) setError(error.message); })
      .finally(() => { if (!controller.signal.aborted) setBusy(false); });
    return () => controller.abort();
  }, [period, basis, instrument, revision, cacheKey, initialData]);

  function reset() {
    setBusy(true); setError(''); setLimit(25); setSelected(null); setSector(''); setExpandedStocks(new Set());
  }
  const stocks = useMemo(() => [...(data?.stocks || [])].sort((a, b) => (sort === 'net'
    ? (Number(a.unknownAmounts > 0) - Number(b.unknownAmounts > 0) || (a.unknownAmounts > 0 ? 0
      : (b.purchaseMin + b.purchaseMax - b.saleMin - b.saleMax - a.purchaseMin - a.purchaseMax + a.saleMin + a.saleMax) / 2))
    : sort === 'money' ? b.purchaseMin - a.purchaseMin : sort === 'sellers' ? b.sellers - a.sellers : b.buyers - a.buyers)
    || a.ticker.localeCompare(b.ticker)), [data, sort]);
  const visibleStocks = useMemo(() => stocks.filter(stock => matchesAnalysisStock(stock, query, sector)), [stocks, query, sector]);
  const showOptions = displayedFilters.instrument === 'options';

  return <div className={styles.page}>
    <div className={`page-heading ${styles.heading}`}><span className="eyebrow">CONGRESSIONAL ACTIVITY</span><h1>Activity overview.</h1><p>See what Congress bought and sold, and explore the disclosures behind each stock.</p></div>
    <div className={styles.controls}>
      <div className={styles.periodField}><span className="sr-only">Time period</span>
        <div className={styles.periods} role="group" aria-label="Time period">{periods.map(([value, label]) => <button key={value}
          aria-pressed={period === value} onClick={() => { if (period !== value) { reset(); setPeriod(value); } }}>{label}</button>)}</div>
      </div>
      <details className={styles.filterMenu} onKeyDown={event => {
        if (event.key === 'Escape') { event.currentTarget.open = false; event.currentTarget.querySelector('summary')?.focus(); }
      }}><summary>Filters<Icon name="chevron" size={14}/></summary><div className={styles.filterPanel}>
      <label className={styles.selectField}>Timing<span className={styles.selectWrap}><select value={basis} onChange={event => { reset(); setBasis(event.target.value as AnalysisBasis); }}>
        <option value="filed">Newly disclosed</option><option value="trade">Transaction date</option>
      </select><Icon name="chevron" size={14}/></span></label>
      <label className={styles.selectField}>Assets<span className={styles.selectWrap}><select value={instrument} onChange={event => { reset(); setInstrument(event.target.value); }}>
        <option value="stocks">Stocks & ETFs</option><option value="options">Options</option>
      </select><Icon name="chevron" size={14}/></span></label>
      </div></details>
    </div>
    <div className={styles.scope}>
      <span role="status">{busy && data ? 'Updating activity… Previous results are shown below.' : data ? `${dateLabel(data.start)} – ${dateLabel(data.end)}` : periods.find(([value]) => value === period)?.[1]}</span>
      <span>{instrument === 'options' ? 'Options' : 'Stocks & ETFs'} · {basis === 'filed' ? 'Newly disclosed' : 'By transaction date'}</span>
    </div>
    {busy && !data && <DashboardSkeleton label="Loading activity for this period…"/>}
    {error && <div className="empty-state" role="alert"><p>{error}</p>{data && <p>The previous results are still shown below.</p>}<button className={`button secondary ${styles.action}`} onClick={() => { reset(); setRevision(value => value + 1); }}>Try again</button></div>}
    {data && <div className={`content-enter results-frame ${busy ? 'results-pending' : ''}`} aria-busy={busy} inert={busy}>
      <AnalysisCharts data={data} options={showOptions} sector={sector} onSector={value => {
        setSector(value); setLimit(25); listRef.current?.scrollIntoView({ behavior: 'auto', block: 'start' });
      }}/>
      <section ref={listRef} className={styles.details} aria-labelledby="stock-activity-title">
        <div className={styles.listHeading}>
          <div><h2 id="stock-activity-title">{showOptions ? 'Option activity by stock' : 'Explore the stocks'}<span>{number(visibleStocks.length)}</span></h2><p className={styles.listHint}>Choose a stock to see its trades and original filings.</p></div>
          <div className={styles.listTools}>
            <label className={styles.search}><span className="sr-only">Find a stock</span><Icon name="search" size={16}/><input type="search" placeholder="Ticker or company" value={query} onChange={event => { setQuery(event.target.value); setLimit(25); }}/></label>
            <label className={styles.selectField}><span className="sr-only">Rank stocks by</span><span className={styles.selectWrap}><select value={sort} onChange={event => { setSort(event.target.value); setLimit(25); }}>
              <option value="buyers">Most buyers</option><option value="money">Largest purchases</option><option value="sellers">Most sellers</option><option value="net">Estimated net</option>
            </select><Icon name="chevron" size={14}/></span></label>
          </div>
        </div>
        {sector && <div className={styles.filterNotice}><span>Showing stocks with {sector.toLowerCase()} activity</span><button onClick={() => { setSector(''); setLimit(25); }}>Clear sector<Icon name="close" size={13}/></button></div>}
        {(query || sector) && <p className={styles.filterHint}>Charts show the full period. The list below matches your {query && sector ? 'search and sector' : query ? 'search' : 'sector'} filter.</p>}
        {!visibleStocks.length && <div className="empty-state">{query || sector ? 'No stocks match these filters in this period.' : 'No eligible activity in this period.'}
          {(query || sector) && <button className={`button secondary ${styles.action}`} onClick={() => { setQuery(''); setSector(''); setLimit(25); }}>Clear filters</button>}
        </div>}
        {!!visibleStocks.length && <div className={styles.tableHeading} aria-hidden="true"><span>Stock</span><span>Buying</span><span>Selling</span><span>Net activity <small>Estimated</small></span><span/></div>}
        <div className="analysis-list list-enter">{visibleStocks.slice(0, limit).map(stock => {
          const sectors = stock.sectors.join(' · ');
          const assetName = stock.companyName || sectors;
          return <details className={`analysis-stock ${styles.stock}`} key={`${displayedFilters.period}-${displayedFilters.basis}-${displayedFilters.instrument}-${stock.ticker}`}
            open={expandedStocks.has(stock.ticker)} onToggle={event => {
              const open = event.currentTarget.open;
              setExpandedStocks(current => {
                if (current.has(stock.ticker) === open) return current;
                const next = new Set(current);
                if (open) next.add(stock.ticker); else next.delete(stock.ticker);
                return next;
              });
            }}>
          <summary><span className={`analysis-identity ${styles.stockIdentity}`}><CompanyLogo ticker={stock.ticker}/><span><strong>{stock.ticker}</strong><small className={styles.stockName} title={assetName}>{assetName}</small></span></span>
            <span className={styles.tradeAmounts}><strong>{stock.buyers} {stock.buyers === 1 ? 'buyer' : 'buyers'}</strong><span className="sr-only">Purchases</span><small>{range(stock.purchaseMin, stock.purchaseMax)}</small></span>
            <span className={styles.tradeAmounts}><strong>{stock.sellers} {stock.sellers === 1 ? 'seller' : 'sellers'}</strong><span className="sr-only">Sales</span><small>{range(stock.saleMin, stock.saleMax)}</small></span>
            <NetAmount stock={stock}/><span className={styles.viewActivity}><span className="sr-only">View {stock.ticker} activity</span><Icon name="chevron" size={14}/></span>
          </summary>
          {expandedStocks.has(stock.ticker) && <div className="analysis-evidence content-enter"><AnalysisStockTools ticker={stock.ticker}/>
            <p>{sectors} · Latest disclosure: {dateLabel(stock.latest)} · {number(stock.tradeCount)} recorded transactions{stock.unknownAmounts ? ` · ${number(stock.unknownAmounts)} amounts excluded from dollar totals` : ''}{stock.inferredAmounts ? ` · ${number(stock.inferredAmounts)} standard ranges estimated from stored lower bounds` : ''}</p>
            <AnalysisStockTrades ticker={stock.ticker} {...displayedFilters} end={data.end} onSelect={setSelected}/>
          </div>}
        </details>})}</div>
        {visibleStocks.length > limit && <div className="load-more"><button className={`button secondary ${styles.action}`} onClick={() => setLimit(value => value + 25)}>Show more stocks<Icon name="plus" size={15}/></button></div>}
      </section>
      <details className={styles.method}><summary>Sources & methodology<Icon name="chevron" size={14}/></summary>
        <p>{displayedFilters.basis === 'filed' ? 'This view covers filings disclosed during the selected period. The trades themselves may be older.' : 'This view covers trades made during the selected period. Later disclosures may change these totals.'}</p>
        <p>Charts count all eligible transactions for the selected period, timing and asset type, including transactions with unknown amounts. They show activity, not portfolio holdings, returns or invested-dollar allocation. Buyer and seller counts cover distinct politicians, including household disclosures; one person can appear in both groups.</p>
        <p>Company names and sector labels use the latest verified iShares holdings, not historical sector membership on each trade date. ETFs identified in the filing are grouped separately, without looking through to their holdings. Options use the underlying company sector. Missing classifications stay in Unclassified and remain in the percentage denominator.</p>
        <div className={styles.sources}>{data.sectorSources.map(source => <a key={source.id} href={source.url} target="_blank" rel="noreferrer">iShares {source.id} · {dateLabel(source.holdingsAsOf)}<Icon name="external" size={12}/></a>)}</div>
        <p>Dollar ranges sum reported ranges and estimated standard ranges where only a recognized lower bound was retained. Reconstructed upper bounds are assumptions. Unknown or open-ended amounts are excluded from dollar totals. Largest purchases ranks by the lower bound; net uses range midpoints and shows Incomplete when amounts are missing. Displayed net estimates are rounded to whole dollars.</p>
        <p>Known contributions, unidentified tickers and unsupported asset types are excluded. Duplicate source row IDs count once; separate amended filings may still need review. {number(data.scanned)} records scanned; {number(data.excluded)} excluded. Available records only.</p>
      </details>
    </div>}
    {selected && <TradeDetails trade={selected} onClose={() => setSelected(null)}/>}
  </div>;
}
