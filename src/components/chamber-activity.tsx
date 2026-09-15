"use client";
import {useEffect,useState} from 'react';
export function ChamberActivity(){
 const [data,setData]=useState<{senate:number;house:number;purchases:number}|null>(null);
 const [index,setIndex]=useState(0),[paused,setPaused]=useState(false),[hover,setHover]=useState(false),[reduced,setReduced]=useState(false);
 useEffect(()=>{
  const c=new AbortController();let pending=false;
  const refresh=async()=>{if(document.hidden||pending)return;pending=true;try{const r=await fetch('/api/chamber-activity',{signal:c.signal});if(!r.ok)throw Error();const d=await r.json();if(!c.signal.aborted)setData(d);}catch{if(!c.signal.aborted)setData(null);}finally{pending=false;}};
  const media=matchMedia('(prefers-reduced-motion: reduce)');const motion=()=>setReduced(media.matches);motion();media.addEventListener('change',motion);
  void refresh();const timer=setInterval(refresh,300000);document.addEventListener('visibilitychange',refresh);
  return()=>{c.abort();clearInterval(timer);media.removeEventListener('change',motion);document.removeEventListener('visibilitychange',refresh);};
 },[]);
 useEffect(()=>{if(paused||hover||reduced)return;const t=setInterval(()=>{if(!document.hidden)setIndex(i=>(i+1)%3);},6000);return()=>clearInterval(t);},[paused,hover,reduced]);
 const labels=data?[`${data.senate} senators disclosed trades`,`${data.house} House members disclosed trades`,`${data.purchases} purchases disclosed`]:[];
 return <div className="politician-activity" onMouseEnter={()=>setHover(true)} onMouseLeave={()=>setHover(false)} onFocusCapture={()=>setHover(true)} onBlurCapture={e=>{if(!e.currentTarget.contains(e.relatedTarget))setHover(false);}}>
 <div className="chamber-caption" title="Available stock and ETF disclosures. People are counted once per chamber; purchases count transactions.">{data?<><span key={index} className={reduced?'':'chamber-caption-enter'}>{labels[index]}<small>Last 7 days · Stocks & ETFs</small></span><button className="activity-pause" aria-label={paused?'Resume chamber activity':'Pause chamber activity'} onClick={()=>setPaused(p=>!p)}>{paused?'▶':'Ⅱ'}</button></>:<span>Explore congressional disclosures below.</span>}</div>
 </div>;
}
