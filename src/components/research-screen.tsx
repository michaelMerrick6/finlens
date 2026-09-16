'use client';
import { useEffect, useRef, useState } from 'react';
import { useAccount } from './account-provider';
import { DEFAULT_SCREEN, type ScreenFilters, type ScreenCatalog, type ScreenResult } from '@/lib/research-screen';
import { TradeDetails } from './disclosure-feed';
import type { AnalysisTrade } from '@/lib/congress-analysis';

type Saved={id:string;name:string;filters:ScreenFilters};
export function ResearchScreen(){
 const {session}=useAccount();
 return <ResearchScreenContent key={session?.user.id||"signed-out"}/>;
}
function ResearchScreenContent(){
 const {session,openSignIn}=useAccount();
 const [catalog,setCatalog]=useState<ScreenCatalog|null>(null),[available,setAvailable]=useState(false),[filters,setFilters]=useState<ScreenFilters>(DEFAULT_SCREEN),[question,setQuestion]=useState(''),[message,setMessage]=useState(''),[error,setError]=useState(''),[busy,setBusy]=useState(false),[result,setResult]=useState<ScreenResult|null>(null),[saved,setSaved]=useState<Saved[]>([]),[name,setName]=useState(''),[selected,setSelected]=useState<AnalysisTrade|null>(null),[limit,setLimit]=useState(20),[saving,setSaving]=useState(false);
 const sequence=useRef(0);
 useEffect(()=>{const c=new AbortController();fetch('/api/research',{signal:c.signal}).then(async r=>{const d=await r.json();if(!r.ok)throw Error(d.error);setCatalog(d.catalog);setAvailable(d.assistantAvailable);}).catch(e=>{if(!c.signal.aborted)setError(e.message);});return()=>c.abort();},[]);
 useEffect(()=>{if(!session)return;const c=new AbortController();fetch('/api/account/research-screens',{signal:c.signal,headers:{Authorization:`Bearer ${session.access_token}`}}).then(async r=>{const d=await r.json();if(!r.ok)throw Error(d.error);setSaved(d.screens);}).catch(e=>{if(!c.signal.aborted)setError(e.message);});return()=>c.abort();},[session]);
 function edit(key:keyof ScreenFilters,value:ScreenFilters[keyof ScreenFilters]){sequence.current++;setFilters(f=>({...f,[key]:value}));setResult(null);setMessage('');setError('');setBusy(false);}
 async function screen(f:ScreenFilters,version:number){const r=await fetch('/api/research',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'screen',filters:f})});const d=await r.json();if(!r.ok)throw Error(d.error);if(sequence.current===version){setResult(d);setLimit(20);}}
 async function run(f=filters){const version=++sequence.current;setFilters(f);setBusy(true);setError('');setResult(null);try{await screen(f,version);}catch(e){if(version===sequence.current)setError(e instanceof Error?e.message:'Research failed.');}finally{if(version===sequence.current)setBusy(false);}}
 async function ask(){if(!session){openSignIn();return;}const version=++sequence.current;setBusy(true);setError('');setResult(null);setMessage('');try{
 const r=await fetch('/api/research',{method:'POST',headers:{'Content-Type':'application/json',Authorization:`Bearer ${session.access_token}`},body:JSON.stringify({action:'interpret',question,previous:filters})});const d=await r.json();if(!r.ok)throw Error(d.error);if(sequence.current!==version)return;
 setMessage(d.message);if(d.supported){setFilters(d.filters);await screen(d.filters,version);}
 }catch(e){if(version===sequence.current)setError(e instanceof Error?e.message:'Research failed.');}finally{if(version===sequence.current)setBusy(false);}}
 return <section className="research-panel" aria-labelledby="research-heading">
 <div className="research-heading"><span className="eyebrow">RESEARCH · BETA</span><h1 id="research-heading">Research screener.</h1><p>Screen companies using disclosed congressional activity and current committee assignments.</p></div>
 <form className="research-question" onSubmit={e=>{e.preventDefault();void ask();}}><label htmlFor="research-question">What would you like to research?</label><textarea id="research-question" maxLength={1000} value={question} onChange={e=>setQuestion(e.target.value)} placeholder="Which stocks had at least three politicians buying in the last 30 days?" rows={2}/><button className="button primary" disabled={busy||!catalog||!available||question.trim().length<3}>{busy?'Researching…':'Ask Vail'}</button></form>
 <div className="research-examples">{['Stocks bought by at least three politicians in the last 30 days','What has Ro Khanna disclosed this year?','Semiconductor stocks bought by two politicians in the last 90 days'].map(q=><button key={q} type="button" onClick={()=>setQuestion(q)}>{q}</button>)}</div>
 <p className="fine-print">{available?'Sign in to ask up to 20 questions per day. Refine a result with a follow-up question or edit its filters.':'Conversational research is not configured. You can still run a screen using the filters.'}</p>
 {message&&<p className="research-interpretation" role="status">{message}</p>}
 <details className="research-filter-details"><summary>Screening filters</summary><div className="research-filters">
 <label>Last days<input type="number" min={1} max={366} value={filters.days} onChange={e=>edit('days',Number(e.target.value))}/></label>
 <label>Timing<select value={filters.basis} onChange={e=>edit('basis',e.target.value as ScreenFilters['basis'])}><option value="disclosure">Disclosure date</option><option value="trade">Transaction date</option></select></label>
 <label>Activity<select value={filters.activity} onChange={e=>edit('activity',e.target.value as ScreenFilters['activity'])}><option value="buy">Purchases</option><option value="sell">Sales</option><option value="all">Purchases & sales</option></select></label>
 <label>Minimum distinct politicians<input type="number" min={1} max={550} value={filters.minPoliticians} onChange={e=>edit('minPoliticians',Number(e.target.value))}/></label>
 <label>Ticker<input placeholder="Any ticker" value={filters.ticker||''} maxLength={12} onChange={e=>edit('ticker',e.target.value.trim().toUpperCase()||null)}/></label>
 <label>Industry · SEC classification<select value={filters.industry||''} onChange={e=>edit('industry',e.target.value||null)}><option value="">All industries</option>{catalog?.industries.map(i=><option key={i}>{i}</option>)}</select></label>
 <label>Politician<select value={filters.memberId||''} onChange={e=>edit('memberId',e.target.value||null)}><option value="">All politicians</option>{catalog?.members.map(m=><option key={m.id} value={m.id}>{m.name}</option>)}</select></label>
 <label>Current committee<select value={filters.committeeId||''} onChange={e=>edit('committeeId',e.target.value||null)}><option value="">All committees</option>{catalog?.committees.map(c=><option key={c.id} value={c.id}>{c.id.startsWith('house:')?'House':'Senate'} · {c.name}</option>)}</select></label>
 <label>Chamber<select value={filters.chamber} onChange={e=>edit('chamber',e.target.value as ScreenFilters['chamber'])}><option value="all">Both chambers</option><option>House</option><option>Senate</option></select></label>
 </div><div className="research-actions"><button type="button" className="button primary" disabled={busy||!catalog} onClick={()=>void run()}>Run screen</button><button type="button" className="button secondary" disabled={busy} onClick={()=>{sequence.current++;setFilters(DEFAULT_SCREEN);setResult(null);setMessage('');setError('');}}>Reset filters</button></div></details>
 {error&&<p role="alert" className="error">{error}</p>}
 {busy&&<p role="status">Checking the available records…</p>}
 {result&&<div className="research-results"><h3>{result.stocks.length} matching {result.stocks.length===1?'company':'companies'}</h3>
 <p>{result.start} – {result.end} · {result.filters.basis==='disclosure'?'Disclosure dates':'Transaction dates'} · Stocks & ETFs</p>
 <p className="fine-print">Computed {new Date(result.computedAt).toLocaleString()}. Available records only; recent imports may take up to five minutes to appear. Disclosed trades may have occurred weeks earlier.</p>
 <p className="fine-print">{result.coverage.classifiedTickers} of {result.coverage.eligibleTickers} eligible tickers in this period have an SEC industry classification. {result.coverage.unclassifiedTickers} are unclassified{result.filters.industry?' and excluded from this industry screen':''}. Classifications describe the current issuer, not necessarily its industry at the time of a historical trade.</p>
 {result.filters.committeeId&&<p className="research-interpretation">Uses current committee membership, verified {result.committeeVerifiedAt?.slice(0,10)}. It does not establish committee membership when each trade occurred.</p>}
 <details className="analysis-method"><summary>Screen methodology</summary><p>Counts distinct politicians, including household disclosures, across the selected activity. Four purchases by one politician count as one buyer. Duplicate source row IDs and known superseded disclosures are excluded. Separate amendments may still require review. Dollar amounts remain the reported ranges; this screen makes no return or profit estimate. Missing industries are never inferred by the assistant.</p></details>
 {!result.stocks.length&&<p className="empty-state">No matches in the available records for these filters.</p>}
 {result.stocks.slice(0,limit).map(s=><details className="research-stock" key={s.ticker}><summary><strong>{s.ticker}</strong><span>{s.company?.company_name||s.trades[0]?.asset_name}</span><span>{s.politicians} politicians · {s.trades.length} transactions</span></summary><div className="research-evidence">
 {s.company?.industry&&<p className="fine-print">{s.company.industry} · <a href={s.company.source_url} target="_blank" rel="noreferrer">SEC classification</a> · Verified {s.company.verified_at.slice(0,10)}</p>}
 {s.trades.map(t=><button className="research-trade" key={t.id} onClick={()=>setSelected(t)}><span>{t.politician_name}</span><span>{t.transaction_type==='buy'?'Purchase':'Sale'} · {t.amount_range||'Amount unavailable'}</span><span>Traded {t.transaction_date||'Unknown'}<br/>Disclosed {t.published_date||'Unknown'}</span><span>View source details →</span></button>)}</div></details>)}
 {result.stocks.length>limit&&<button className="button secondary" onClick={()=>setLimit(n=>n+20)}>Show more companies</button>}
 <form className="research-save" onSubmit={async e=>{e.preventDefault();if(!session){openSignIn();return;}setSaving(true);setError('');try{const r=await fetch('/api/account/research-screens',{method:'POST',headers:{'Content-Type':'application/json',Authorization:`Bearer ${session.access_token}`},body:JSON.stringify({name,filters:result.filters})});const d=await r.json();if(!r.ok)throw Error(d.error);setSaved(s=>[d.screen,...s]);setName('');}catch(e){setError(e instanceof Error?e.message:'Could not save.');}finally{setSaving(false);}}}><label>Name this screen<input value={name} onChange={e=>setName(e.target.value)} placeholder="My congressional research" required maxLength={100}/></label><button className="button secondary" disabled={saving}>{saving?'Saving…':'Save screen'}</button><p className="fine-print">Save these rules to rerun with a fresh rolling date range. Saved-screen email updates are not enabled in this beta.</p></form>
 </div>}
 {session&&saved.length>0&&<div className="research-saved"><h3>Your saved screens</h3>{saved.map(s=><div key={s.id}><button type="button" disabled={busy} onClick={()=>{setMessage('');void run(s.filters);}}>{s.name} <span>Run →</span></button><button type="button" aria-label={`Delete ${s.name}`} onClick={async()=>{try{const r=await fetch('/api/account/research-screens?id='+s.id,{method:'DELETE',headers:{Authorization:`Bearer ${session.access_token}`}});if(!r.ok)throw Error('Could not delete screen.');setSaved(rows=>rows.filter(x=>x.id!==s.id));}catch(e){setError(e instanceof Error?e.message:'Could not delete.');}}}>Delete</button></div>)}</div>}
 {selected&&<TradeDetails trade={selected} onClose={()=>setSelected(null)}/>}
 </section>;
}
