"use client";
import { useEffect, useState } from 'react';
import { useAccount } from './account-provider';
import { HqMarketing } from './hq-marketing';
type Run = {id:string;scraper_name:string;source_name:string;status:string;started_at:string;duration_ms:number|null;records_seen:number|null;records_inserted:number|null;records_updated:number|null;error_count:number|null};
type Data = {audience:{accounts:{total:number;last30:number;lastMonth:number};engagement:{days:number;browsers:number;accounts:number;seconds:number;average_seconds:number}[];trackingSince:string|null}|null;generatedAt:string;counts:{label:string;start:string;politicians:number|null;insiders:number|null}[];runs:Run[]|null;analytics:{summary:{visitors:number;views:number;sessions:number}}|null};
const number=(value:number|null|undefined)=>value == null?'Unavailable':value.toLocaleString();
export function HqDashboard(){
 const {session}=useAccount();
 return <HqContent key={session?.user.id || "signed-out"} />;
}
function HqContent(){
 const [section,setSection]=useState('analytics');
 const [password,setPassword]=useState("");
 const {session,loading,openSignIn,reauthenticate,hqAccess}=useAccount();
 const [submittedPassword,setSubmittedPassword]=useState(()=>hqAccess.current?.userId===session?.user.id ? hqAccess.current?.password || "" : "");
 const [data,setData]=useState<Data|null>(null),[error,setError]=useState(''),[busy,setBusy]=useState(false),[revision,setRevision]=useState(0);
 useEffect(()=>{
  if(!session || !submittedPassword)return;
  const controller=new AbortController();
  queueMicrotask(()=>{if(!controller.signal.aborted){setData(null);setError('');setBusy(true);}});
  fetch('/api/hq',{headers:{Authorization:`Bearer ${session.access_token}`, "x-hq-password":submittedPassword},cache:'no-store',signal:controller.signal}).then(async r=>{const d=await r.json();if(r.status===401){await reauthenticate();throw Error("Please sign in again to access HQ.");}if(!r.ok){if(r.status===403)hqAccess.current=null;throw Error(d.error);}return d;}).then(d=>{if(!controller.signal.aborted){hqAccess.current={userId:session.user.id,password:submittedPassword};setData(d);}}).catch(e=>{if(!controller.signal.aborted)setError(e.message);}).finally(()=>{if(!controller.signal.aborted)setBusy(false);});
  return()=>controller.abort();
 },[session,revision,submittedPassword,reauthenticate,hqAccess]);
 return <>
 <div className="page-heading"><span className="eyebrow">PRIVATE WORKSPACE</span><h1>Vail HQ.</h1><p>Your audience, your data, and the runs behind it.</p></div>
 {!session?<div className="empty-state"><h2>Owner access only</h2><button className="button primary" disabled={loading} onClick={openSignIn}>Sign in</button></div>:<>
 {!data && (!submittedPassword || error) && <form className="hq-stat" style={{maxWidth:420}} onSubmit={e=>{e.preventDefault();setSubmittedPassword(password);setPassword("");setRevision(n=>n+1);}}>
 <label htmlFor="hq-password">HQ password</label><input id="hq-password" type="password" autoComplete="current-password" maxLength={128} required value={password} onChange={e=>setPassword(e.target.value)} />
 <button className="button primary" disabled={busy}>Unlock HQ</button></form>}
 <div className="ranking-controls"><span>{data?`Updated ${new Date(data.generatedAt).toLocaleString()}`:'Private dashboard'}</span><button className="button secondary" disabled={busy || !submittedPassword} onClick={()=>setRevision(n=>n+1)}>{busy?'Refreshing…':'Refresh data'}</button></div>
 {error&&<p className="error" role="alert">{error}</p>}
 {busy&&<p role="status">Loading HQ…</p>}
 {data&&<>
 <nav className="ranking-controls" aria-label="HQ sections">{["analytics","marketing","newsletter"].map(item=><button key={item} className={`button ${section===item?"primary":"secondary"}`} aria-pressed={section===item} onClick={()=>setSection(item)}>{item=== "marketing"?"Marketing · Twitter":item[0].toUpperCase()+item.slice(1)}</button>)}</nav>
 {section==="marketing"&&<HqMarketing token={session.access_token} password={submittedPassword}/>}
 {section==="newsletter"&&<section className="hq-section"><h2>Sunday Brief.</h2><p>Your weekly editorial home. Review the current edition and subscriber experience.</p><a className="button secondary" href="/sunday-brief">Open Sunday Brief</a><p className="fine-print">Scheduling, delivery analytics, and the newsletter editor are not connected here yet.</p></section>}
 {section==="analytics"&&<>
 <section className="hq-section"><h2>Accounts</h2><div className="hq-grid">{[['Total accounts',data.audience?.accounts.total],['Created last calendar month (UTC)',data.audience?.accounts.lastMonth],['Created in the last 30 days',data.audience?.accounts.last30]].map(([label,value])=><div className="hq-stat" key={label as string}><span>{label}</span><strong>{number(value as number|undefined)}</strong></div>)}</div><p className="fine-print">Existing Supabase accounts, including unconfirmed sign-ups. Deleted accounts are excluded.</p></section>
 <section className="hq-section"><h2>Audience & active time</h2>{data.audience?<><div className="hq-grid">{data.audience.engagement.map(e=><div className="hq-stat" key={e.days}><h3>Last {e.days} days</h3><span>Unique browser visitors measured</span><strong>{number(e.browsers)}</strong><span>Distinct signed-in accounts</span><strong>{number(e.accounts)}</strong><span>Measured active time</span><strong>{Math.round(e.seconds/60).toLocaleString()} min</strong><span>Average per measured page visit: {e.average_seconds}s</span></div>)}</div><p className="fine-print">{data.audience.trackingSince?`Collecting since ${new Date(data.audience.trackingSince).toLocaleString()}.`:'No engagement samples collected yet.'} Visible-tab time sampled every 15 seconds, with a 60-second inactivity cutoff. Local previews and privacy opt-outs are excluded. Account counts here mean signed-in visitors, not all visitors who may own an account. Windows use page-visit start time.</p></>:<p>Audience metrics unavailable.</p>}</section>
 <section className="hq-section"><h2>Visitors · last 30 days</h2><div className="hq-grid">{[['Unique browser visitors',data.analytics?.summary.visitors],['Page views',data.analytics?.summary.views],['Sessions',data.analytics?.summary.sessions]].map(([label,value])=><div className="hq-stat" key={label as string}><span>{label}</span><strong>{number(value as number|undefined)}</strong></div>)}</div><p className="fine-print">Browser identifiers estimate visitors, not verified people. Privacy opt-outs and blocked tracking are not counted. Tracking gaps cannot be reconstructed.</p></section>
 <section className="hq-section"><h2>Records added by scrapers</h2><p className="muted">Based on when records entered our database, including historical backfills. These are retained records, not trade-date totals. Year to date uses UTC.</p><div className="hq-grid">{data.counts.map(p=><div className="hq-stat" key={p.label}><h3>{p.label}</h3><span>Politician records</span><strong>{number(p.politicians)}</strong><span>Insider records</span><strong>{number(p.insiders)}</strong></div>)}</div></section>
 <section className="hq-section"><h2>Latest scraper runs</h2><p className="muted">Latest 40 recorded runs across all sources. A running status can be stale if a worker stopped without recording completion.</p>{data.runs===null?<p role="alert">Run history is unavailable.</p>:!data.runs.length?<p>No recorded runs.</p>:<div className="hq-table"><table><thead><tr>{['Scraper','Status','Started','Duration','Seen','Added','Updated','Errors'].map(h=><th key={h}>{h}</th>)}</tr></thead><tbody>{data.runs.map(r=><tr key={r.id}><td>{r.scraper_name}<small>{r.source_name}</small></td><td>{r.status}</td><td>{new Date(r.started_at).toLocaleString()}</td><td>{r.duration_ms==null?'—':`${Math.round(r.duration_ms/1000)}s`}</td><td>{number(r.records_seen)}</td><td>{number(r.records_inserted)}</td><td>{number(r.records_updated)}</td><td>{number(r.error_count)}</td></tr>)}</tbody></table></div>}</section>
 </>}
 </>}
 </>}
 </>;
}
