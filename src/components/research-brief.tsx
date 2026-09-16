'use client';
import Link from 'next/link';
import { useState } from 'react';
import { CompanyLogo } from './identity-images';
import { TradeDetails } from './disclosure-feed';
import { buildResearchBrief } from '@/lib/research-brief';
import { formatPurchaseRange, type ScreenResult } from '@/lib/research-screen';
import type { AnalysisTrade } from '@/lib/congress-analysis';
import { dateLabel } from '@/lib/ui-format';

export function ResearchBrief({result}:{result:ScreenResult}) {
 const brief=buildResearchBrief(result);
 const [selected,setSelected]=useState<AnalysisTrade|null>(null);
 const [limit,setLimit]=useState(25);
 function evidence(trades:AnalysisTrade[]){return <div className="research-evidence">{trades.map(t=><button className="research-trade" key={t.id} onClick={()=>setSelected(t)}><span>{t.politician_name}</span><span>{t.transaction_type==='buy'?'Purchase':'Sale'} · {t.amount_range||'Amount unavailable'}</span><span>Traded {dateLabel(t.transaction_date)}<br/>Disclosed {dateLabel(t.published_date)}</span><span>View filing →</span></button>)}</div>;}
 return <article className="research-editorial" aria-label="Research brief">
 <div className="brief-edition"><span>{dateLabel(result.start)} – {dateLabel(result.end)}</span><span>{result.filters.basis==='disclosure'?'By disclosure date':'By transaction date'}</span></div>
 <h2 className="research-brief-title">{brief.title}</h2><p className="research-brief-lead">{brief.lead}</p>
 <p className="brief-window">{result.filters.basis==='disclosure'?'These are newly disclosed records. The trades themselves may be older.':'These are reported transaction dates. Later filings can change this picture.'} Reported activity does not establish investment merit or why a politician traded.</p>
 {brief.sections.map((section,index)=>{const stock=result.stocks.find(s=>s.ticker===section.ticker)!;return <section className="brief-stock-story" key={section.ticker}>
 <div className="brief-stock-heading"><CompanyLogo ticker={stock.ticker}/><div><strong>{section.name} <span className="muted">{stock.ticker}</span></strong><small>{stock.politicians} distinct politicians · {stock.trades.length} matching transactions</small></div><span className="muted">{String(index+1).padStart(2,'0')}</span></div>
 <h3>{section.headline}</h3>{section.paragraphs.map((p,i)=><p key={i}>{p}</p>)}
 <p className="research-brief-people">Reported by {section.members.slice(0,6).map((m,i)=><span key={m.id}>{i?', ':''}<Link href={`/politicians/${m.id}`}>{m.name}</Link></span>)}{section.members.length>6?` and ${section.members.length-6} others`:''}.</p>
 <p className="brief-story-note">{section.dates}</p>
 <details className="research-inline-evidence"><summary>Explore {stock.ticker} disclosures · {stock.trades.length} records</summary>{evidence(stock.trades)}</details>
 </section>;})}
 {result.stocks.length>3&&<details className="research-all-matches"><summary>View all {result.stocks.length} matching companies</summary><p className="fine-print">{result.filters.rank==='purchase_amount'?'Ranked by summed disclosed purchase lower bounds; missing amounts excluded. Ranges may overlap.':'Ranked by distinct politicians. Transaction counts break display ties.'}</p>{result.stocks.slice(0,limit).map(stock=><details className="research-stock" key={stock.ticker}><summary><strong>{stock.ticker}</strong><span>{stock.company?.company_name||stock.trades[0]?.asset_name}</span><span>{result.filters.rank==='purchase_amount'?formatPurchaseRange(stock.disclosedPurchases):`${stock.politicians} politicians · ${stock.trades.length} transactions`}</span></summary>{evidence(stock.trades)}</details>)}{result.stocks.length>limit&&<button className="button secondary" onClick={()=>setLimit(n=>n+25)}>Show more companies</button>}</details>}
 <details className="research-sources"><summary>Sources & methodology</summary><p>Computed {new Date(result.computedAt).toLocaleString()}. Available records only; recent imports may take five minutes to appear. This brief uses stocks and ETFs matching {result.filters.activity==='all'?'purchases and sales':result.filters.activity==='buy'?'purchases':'sales'}, with at least {result.filters.minPoliticians} distinct politician(s), {result.filters.chamber==='all'?'both chambers':result.filters.chamber}.</p>
 <p>{result.coverage.classifiedTickers} of {result.coverage.eligibleTickers} eligible tickers have an SEC classification. {result.coverage.unclassifiedTickers} are unclassified{result.filters.industry?' and excluded from this industry search':''}. Industry and committee classifications reflect current information, not necessarily their historical status.</p>
 {result.filters.committeeId&&<p>Current committee roster verified {dateLabel(result.committeeVerifiedAt)}. This does not establish membership at the time of a trade.</p>}
 <p>Distinct counts attribute household transactions to the reporting politician. Duplicate source row IDs and known superseded disclosures are excluded; separate amendments may still require review. Dollar ranges are reported amounts, not exact values or profits. No company news, trade motives, or returns are inferred.</p>
 {result.stocks.slice(0,3).filter(s=>s.company).map(s=><p key={s.ticker}><a href={s.company!.source_url} target="_blank" rel="noreferrer">{s.ticker} · SEC company classification</a> · verified {dateLabel(s.company!.verified_at)}</p>)}
 </details>{selected&&<TradeDetails trade={selected} onClose={()=>setSelected(null)}/>}
 </article>;
}
