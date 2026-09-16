'use client';
import { useEffect, useRef, useState } from 'react';
import { useAccount } from './account-provider';
import { summarizeScreen, type ScreenFilters, type ScreenCatalog, type ScreenResult } from '@/lib/research-screen';
import { ResearchBrief } from './research-brief';

type Turn={question:string;answer:string;result?:ScreenResult};
type Saved={id:string;name:string;filters:ScreenFilters};
export function ResearchScreen(){
 const {session}=useAccount();
 return <ResearchScreenContent key={session?.user.id||"signed-out"}/>;
}
function ResearchScreenContent(){
 const {session,openSignIn}=useAccount();
 const [catalog,setCatalog]=useState<ScreenCatalog|null>(null),[available,setAvailable]=useState(false),[context,setContext]=useState<ScreenFilters|null>(null),[question,setQuestion]=useState(''),[error,setError]=useState(''),[busy,setBusy]=useState(false),[result,setResult]=useState<ScreenResult|null>(null),[saved,setSaved]=useState<Saved[]>([]),[name,setName]=useState(''),[saving,setSaving]=useState(false);
 const [history,setHistory]=useState<Turn[]>([]);
 const questionInput=useRef<HTMLTextAreaElement>(null);
 const sequence=useRef(0);
 function newSearch(){sequence.current++;setBusy(false);setContext(null);setQuestion('');setHistory([]);setResult(null);setError('');setName('');requestAnimationFrame(()=>questionInput.current?.focus());}
 useEffect(()=>{const c=new AbortController();fetch('/api/research',{signal:c.signal}).then(async r=>{const d=await r.json();if(!r.ok)throw Error(d.error);setCatalog(d.catalog);setAvailable(d.assistantAvailable);}).catch(e=>{if(!c.signal.aborted)setError(e.message);});return()=>c.abort();},[]);
 useEffect(()=>{if(!session)return;const c=new AbortController();fetch('/api/account/research-screens',{signal:c.signal,headers:{Authorization:`Bearer ${session.access_token}`}}).then(async r=>{const d=await r.json();if(!r.ok)throw Error(d.error);setSaved(d.screens);}).catch(e=>{if(!c.signal.aborted)setError(e.message);});return()=>c.abort();},[session]);
 async function screen(f:ScreenFilters,version:number){const r=await fetch('/api/research',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'screen',filters:f})});const d=await r.json();if(!r.ok)throw Error(d.error);if(sequence.current===version){setResult(d);setHistory(h=>h.map((t,i)=>i===h.length-1?{...t,result:d,answer:summarizeScreen(d)}:t));}}
 async function run(f:ScreenFilters){const version=++sequence.current;setContext(f);setBusy(true);setError('');setResult(null);try{await screen(f,version);}catch(e){if(version===sequence.current){const failure=e instanceof Error?e.message:'Research failed.';setHistory(h=>h.map((t,i)=>i===h.length-1?{...t,answer:failure}:t));}}finally{if(version===sequence.current)setBusy(false);}}
 async function ask(){
 if(!session){openSignIn();return;}
 const prompt=question.trim();if(prompt.length<3)return;
 const version=++sequence.current;setBusy(true);setError('');setResult(null);
 const conversation=history.slice(-3).map(t=>({question:t.question,answer:t.answer.slice(0,700)}));
 setHistory(h=>[...h,{question:prompt,answer:'Researching…'}]);setQuestion('');
 try{
 const r=await fetch('/api/research',{method:'POST',headers:{'Content-Type':'application/json',Authorization:`Bearer ${session.access_token}`},body:JSON.stringify({action:'interpret',question:prompt,mode:context?'followup':'new',previous:context,conversation})});const d=await r.json();if(!r.ok)throw Error(d.error);if(sequence.current!==version)return;
 setHistory(h=>h.map((t,i)=>i===h.length-1?{...t,answer:d.message}:t));
 if(d.supported){setContext(d.filters);await screen(d.filters,version);}
 }catch(e){if(version===sequence.current){const failure=e instanceof Error?e.message:'Research failed. Please try again.';setHistory(h=>h.map((t,i)=>i===h.length-1?{...t,answer:failure}:t));}}finally{if(version===sequence.current){setBusy(false);requestAnimationFrame(()=>questionInput.current?.focus({preventScroll:true}));}}}
 return <section className="research-panel" aria-labelledby="research-heading">
 <div className="research-heading"><span className="eyebrow">RESEARCH · BETA</span><h1 id="research-heading">Research screener.</h1>{(history.length>0||result)&&<button type="button" className="button secondary small research-new" onClick={newSearch}>New search</button>}<p>Screen companies using disclosed congressional activity and current committee assignments.</p></div>
 <p className="fine-print">{!catalog&&!error?'Loading research…':available?(session?'Up to 20 questions per day.':'Sign in to ask up to 20 questions per day.'):'Research chat is temporarily unavailable. Please try again later.'}</p>
 {history.length>0&&<div className="research-conversation" aria-label="Research conversation">{history.map((turn,index)=><div className="research-turn" key={index}><p className="research-user"><span>You</span>{turn.question}</p><div className="research-answer">{turn.result?<ResearchBrief result={turn.result}/>:<p role="status">{turn.answer}</p>}</div></div>)}</div>}
 <form className="research-question research-chat-composer" onSubmit={e=>{e.preventDefault();void ask();}}><label htmlFor="research-question">{history.length?'Reply or ask another question':'What would you like to research?'}</label><textarea ref={questionInput} id="research-question" maxLength={1000} value={question} onChange={e=>setQuestion(e.target.value)} placeholder={history.length?'Ask a follow-up…':'Which stock was most popular in Congress over the last 3 months, and why?'} rows={2}/><button className="button primary" disabled={busy||!catalog||!available||question.trim().length<3}>{busy?'Researching…':'Send'}</button></form>
 {history.length===0&&!result&&<div className="research-examples">{['Which stock was most popular over the last 3 months, and why?','What stocks has Congress disclosed buying in the last week?','What has Ro Khanna disclosed this year?'].map(q=><button key={q} type="button" onClick={()=>{setQuestion(q);questionInput.current?.focus();}}>{q}</button>)}</div>}
 {error&&<p role="alert" className="error">{error}</p>}
 {busy&&<p role="status">Checking the available records…</p>}
 {result&&<details className="research-save-details"><summary>Save this research</summary> <form className="research-save" onSubmit={async e=>{e.preventDefault();if(!session){openSignIn();return;}setSaving(true);setError('');try{const r=await fetch('/api/account/research-screens',{method:'POST',headers:{'Content-Type':'application/json',Authorization:`Bearer ${session.access_token}`},body:JSON.stringify({name,filters:result.filters})});const d=await r.json();if(!r.ok)throw Error(d.error);setSaved(s=>[d.screen,...s]);setName('');}catch(e){setError(e instanceof Error?e.message:'Could not save.');}finally{setSaving(false);}}}><label>Name this screen<input value={name} onChange={e=>setName(e.target.value)} placeholder="My congressional research" required maxLength={100}/></label><button className="button secondary" disabled={saving}>{saving?'Saving…':'Save screen'}</button><p className="fine-print">Save these rules to rerun with a fresh rolling date range. Saved-screen email updates are not enabled in this beta.</p></form>
 </details>}
 {session&&saved.length>0&&<div className="research-saved"><h3>Your saved screens</h3>{saved.map(s=><div key={s.id}><button type="button" disabled={busy} onClick={()=>{setHistory([{question:`Saved screen: ${s.name}`,answer:'Checking the available records…'}]);setQuestion('');void run(s.filters);}}>{s.name} <span>Run →</span></button><button type="button" aria-label={`Delete ${s.name}`} onClick={async()=>{try{const r=await fetch('/api/account/research-screens?id='+s.id,{method:'DELETE',headers:{Authorization:`Bearer ${session.access_token}`}});if(!r.ok)throw Error('Could not delete screen.');setSaved(rows=>rows.filter(x=>x.id!==s.id));}catch(e){setError(e instanceof Error?e.message:'Could not delete.');}}}>Delete</button></div>)}</div>}
 </section>;
}
