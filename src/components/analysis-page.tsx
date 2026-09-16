"use client";
import { readPublicPage, storePublicPage } from "@/lib/public-page-cache";
import { useEffect,useState } from 'react';
import { AnalysisStockTools } from './analysis-stock-tools';
import { Avatar, CompanyLogo } from './identity-images';
import { TradeDetails } from './disclosure-feed';
import { dateLabel } from '@/lib/ui-format';
import type { AnalysisTrade } from '@/lib/congress-analysis';
type Stock={ticker:string;buyers:number;sellers:number;purchaseMin:number;purchaseMax:number;saleMin:number;saleMax:number;unknownAmounts:number;inferredAmounts:number;together:number;latest:string;trades:AnalysisTrade[]};
type Data={stocks:Stock[];excluded:number;scanned:number;start:string;end:string};
const money=(n:number)=>'$'+n.toLocaleString();
const range=(min:number,max:number)=>max?`${money(min)}–${money(max)}`:'—';
function NetAmount({stock}:{stock:Stock}) {
 const net=(stock.purchaseMin+stock.purchaseMax-stock.saleMin-stock.saleMax)/2;
 const incomplete=stock.unknownAmounts>0;
 return <span className="analysis-net" title="Purchase range midpoints minus sale range midpoints. An estimate, not profit or an exact dollar amount.">
 <small>Net</small>
 <strong className={incomplete || net===0 ? "net-neutral" : net>0 ? "net-positive" : "net-negative"}>
 {incomplete ? "Incomplete" : `${net>0?"+":net<0?"−":""}${money(Math.abs(net))}`}
 </strong>
 {!incomplete && <small>{stock.inferredAmounts ? "Estimated · standard ranges" : "Estimated"}</small>}
 </span>;
}
export function AnalysisPage(){
 const [query,setQuery]=useState('');
 const [period,setPeriod]=useState('30'),[basis,setBasis]=useState('filed'),[instrument,setInstrument]=useState('stocks'),[sort,setSort]=useState('buyers'),[revision,setRevision]=useState(0);
 const [data,setData]=useState<Data|null>(()=>readPublicPage<Data>('analysis:30:filed:stocks') || null),[error,setError]=useState(''),[busy,setBusy]=useState(()=>!readPublicPage('analysis:30:filed:stocks')),[selected,setSelected]=useState<AnalysisTrade|null>(null),[limit,setLimit]=useState(25);
 useEffect(()=>{const c=new AbortController();fetch(`/api/analysis?${new URLSearchParams({period,basis,instrument})}`,{signal:c.signal}).then(async r=>{const d=await r.json();if(!r.ok)throw Error(d.error);return d;}).then(d=>{if(!c.signal.aborted){storePublicPage(`analysis:${period}:${basis}:${instrument}`,d);setData(d);}}).catch(e=>{if(!c.signal.aborted)setError(e.message);}).finally(()=>{if(!c.signal.aborted)setBusy(false);});return()=>c.abort();},[period,basis,instrument,revision]);
 function reset(){setBusy(true);setError('');setData(null);setLimit(25);setSelected(null);}
 const stocks=[...(data?.stocks||[])].sort((a,b)=>(sort==='net'?(Number(a.unknownAmounts>0)-Number(b.unknownAmounts>0) || (a.unknownAmounts>0 ? 0 : (b.purchaseMin+b.purchaseMax-b.saleMin-b.saleMax-a.purchaseMin-a.purchaseMax+a.saleMin+a.saleMax)/2)):sort==='money'?b.purchaseMin-a.purchaseMin:sort==='sellers'?b.sellers-a.sellers:b.buyers-a.buyers)||a.ticker.localeCompare(b.ticker));
 const visibleStocks=stocks.filter(s=>`${s.ticker} ${[...s.trades].sort((a,b)=>(b.published_date||'').localeCompare(a.published_date||'')||(b.transaction_date||'').localeCompare(a.transaction_date||'')).map(t=>t.asset_name||'').join(' ')}`.toLowerCase().includes(query.trim().toLowerCase()));
 return <><div className="page-heading"><span className="eyebrow">CONGRESSIONAL ACTIVITY</span><h1>Activity overview.</h1><p>Explore the stocks politicians bought and sold during your selected period.</p></div>
 <div className="analysis-controls">{[
 ['Period',period,setPeriod,[['7','Last 7 days'],['30','Last 30 days'],['ytd','Year to date'],['year','Last 12 months']]],
 ['Timing',basis,setBasis,[['filed','Newly disclosed'],['trade','Transaction date']]],
 ['Assets',instrument,setInstrument,[['stocks','Stocks & ETFs'],['options','Options']]],
 ].map(([label,value,setter,options])=><label key={label as string}>{label as string}<select value={value as string} onChange={e=>{reset();(setter as (v:string)=>void)(e.target.value);}}>{(options as string[][]).map(([v,l])=><option key={v} value={v}>{l}</option>)}</select></label>)}
 <label>Rank by<select value={sort} onChange={e=>{setSort(e.target.value);setLimit(25);}}><option value="buyers">Most buyers</option><option value="money">Largest purchases</option><option value="sellers">Most sellers</option><option value="net">Net</option></select></label></div>
 <p className="fine-print">{basis==='filed'?'Ranked by filings disclosed during this period; trades may be older.':'Ranked by transaction dates; later disclosures may change these totals.'} Buyers and sellers count distinct politicians, including their household disclosures. Available records only.</p>
 {busy&&<div className="feed-loading" role="status">Loading analysis…</div>}
 {error&&<div className="empty-state" role="alert"><p>{error}</p><button className="button secondary" onClick={()=>{reset();setRevision(n=>n+1);}}>Try again</button></div>}
 {data&&<><div className="analysis-search"><label htmlFor="analysis-search">Find a stock<input id="analysis-search" type="search" placeholder="Ticker or company name" value={query} onChange={e=>{setQuery(e.target.value);setLimit(25);}} /></label>{query&&<button className="button secondary" onClick={()=>{setQuery('');setLimit(25);}}>Clear</button>}</div><p className="analysis-period">{dateLabel(data.start)} – {dateLabel(data.end)} · {visibleStocks.length} {query ? 'matching stocks' : 'stocks'}</p><details className="analysis-method"><summary>How these numbers work</summary><p>Ranges sum reported ranges and estimated standard ranges where only a recognized lower bound was retained. These reconstructed upper bounds are assumptions, not source-verified amounts. Unrecognized or open-ended amounts are excluded from dollar totals and counted below each stock. Largest purchases ranks by the lower bound. Net estimates purchases minus sales using range midpoints; it shows Incomplete if any amounts are missing. Stocks and options are separate. Known contributions, unidentified tickers and unsupported asset types are excluded. Duplicate source row IDs are counted once; separate amended filings may still require review. Buyer and seller counts cover the selected period; the same politician can appear in both groups.</p><p>{data.scanned} records scanned; {data.excluded} excluded by these rules.</p></details>
 {!visibleStocks.length&&<div className="empty-state">{query ? 'No stocks match your search in this period.' : 'No eligible activity in this period.'}</div>}
 <div className="analysis-list">{visibleStocks.slice(0,limit).map((s)=><details className="analysis-stock" key={`${period}-${basis}-${instrument}-${s.ticker}`}><summary><span className="analysis-identity"><span className="muted">{stocks.indexOf(s)+1}</span><CompanyLogo ticker={s.ticker}/><strong>{s.ticker}</strong></span><span><b>{s.buyers}</b> buyers · <b>{s.sellers}</b> sellers</span><span><small>Purchases</small>{range(s.purchaseMin,s.purchaseMax)}</span><span><small>Sales</small>{range(s.saleMin,s.saleMax)}</span><NetAmount stock={s}/><span>View activity ↓</span></summary><div className="analysis-evidence"><AnalysisStockTools ticker={s.ticker}/><p>Latest disclosure: {dateLabel(s.latest)} · {s.trades.length} recorded transactions{s.unknownAmounts?` · ${s.unknownAmounts} amounts excluded from dollar totals`:''}{s.inferredAmounts?` · ${s.inferredAmounts} standard ranges estimated from stored lower bounds`:''}</p><div className="analysis-trade-heading" aria-hidden="true"><span>Politician</span><span>Activity</span><span>Disclosed amount</span><span>Transaction date</span><span>Source</span></div>{[...s.trades].sort((a,b)=>(b.published_date||'').localeCompare(a.published_date||'')||(b.transaction_date||'').localeCompare(a.transaction_date||'')).map(t=><button key={t.id} className="analysis-trade" onClick={()=>setSelected(t)}><span className="analysis-person"><Avatar name={t.politician_name||'Unknown'} memberId={t.member_id}/><span>{t.politician_name}</span></span><span className={`analysis-direction ${t.transaction_type === "buy" ? "purchase" : "sale"}`}>{t.transaction_type==='buy'?'Purchase':'Sale'}</span><span>{t.amount_range||'Amount unavailable'}</span><span className="analysis-dates">{dateLabel(t.transaction_date)}<small>Disclosed {dateLabel(t.published_date)}</small></span><span>View filing details →</span></button>)}</div></details>)}</div>
 {visibleStocks.length>limit&&<div className="load-more"><button className="button secondary" onClick={()=>setLimit(n=>n+25)}>Show more stocks</button></div>}</>}
 {selected&&<TradeDetails trade={selected} onClose={()=>setSelected(null)}/>}</>;
}
