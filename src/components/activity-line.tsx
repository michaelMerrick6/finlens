"use client";
import Link from 'next/link';
import { useEffect, useState } from 'react';
import { Avatar, CompanyLogo } from './identity-images';
import { dateLabel } from '@/lib/ui-format';
import type { AnalysisTrade } from '@/lib/congress-analysis';
type Item={key:string;text:string;href:string;ticker?:string;name?:string;memberId?:string};
export function ActivityLine(){
 const [items,setItems]=useState<Item[]>([]),[paused,setPaused]=useState(false),[hover,setHover]=useState(false),[focused,setFocused]=useState(false),[reduced,setReduced]=useState(false);
 useEffect(()=>{
  const media=window.matchMedia('(prefers-reduced-motion: reduce)');
  const change=()=>setReduced(media.matches);change();media.addEventListener('change',change);
  const controller=new AbortController();let pending=false;
  async function refresh(){
   if(pending||document.hidden)return;pending=true;
   try{
    const r=await fetch('/api/analysis?period=30',{signal:controller.signal});if(!r.ok)throw Error();
    const data=await r.json();
    const trades:AnalysisTrade[]=data.stocks.flatMap((s:{trades:AnalysisTrade[]})=>s.trades);
    const now=Date.now();const recent=trades.filter(t=>t.published_date&&now-Date.parse(t.published_date)<30*86400000&&Date.parse(t.published_date)<=now);
    const next:Item[]=[];
    const weekStart=new Date(`${data.end}T00:00:00Z`);weekStart.setUTCDate(weekStart.getUTCDate()-6);
    const week=recent.filter(t=>(t.published_date||'')>=weekStart.toISOString().slice(0,10));
    for(const [days,records] of [[7,week],[30,recent]] as [number,AnalysisTrade[]][]){
      const buys=records.filter(t=>t.transaction_type==='buy');
      const stockCount=new Set(buys.map(t=>t.ticker)).size;
      next.push({key:`count-${days}`,text:`${buys.length.toLocaleString()} stock & ETF purchases disclosed · last ${days} days`,href:'/analysis'});
      next.push({key:`stocks-${days}`,text:`${stockCount} different stocks & ETFs in disclosed purchases · last ${days} days`,href:'/analysis'});
      const people=new Map<string,{name:string;count:number}>();
      for(const t of buys)if(t.member_id){const person=people.get(t.member_id)||{name:t.politician_name||'Politician',count:0};person.count++;people.set(t.member_id,person);}
      for(const [id,person] of [...people].sort((a,b)=>b[1].count-a[1].count||a[0].localeCompare(b[0])).slice(0,2))next.push({key:`person-${days}-${id}`,memberId:id,name:person.name,text:`${person.name} · ${person.count} purchases disclosed in ${days} days`,href:`/politicians/${encodeURIComponent(id)}`});
    }
    for(const stock of [...data.stocks].filter((s:{buyers:number})=>s.buyers>=3).sort((a:{buyers:number;ticker:string},b:{buyers:number;ticker:string})=>b.buyers-a.buyers||a.ticker.localeCompare(b.ticker)).slice(0,4))next.push({key:stock.ticker,ticker:stock.ticker,text:`${stock.buyers} politicians disclosed ${stock.ticker} purchases · last 30 days`,href:`/ticker/${encodeURIComponent(stock.ticker)}`});
    const latest=[...recent].sort((a,b)=>(b.published_date||'').localeCompare(a.published_date||''))[0];
    if(latest)next.push({key:'freshness',text:`Latest available filing · ${dateLabel(latest.published_date)}`,href:'/'});
    if(!controller.signal.aborted)setItems(previous => { const updated = recent.length ? next : []; return JSON.stringify(previous) === JSON.stringify(updated) ? previous : updated; });
   }catch{if(!controller.signal.aborted)setItems([]);}finally{pending=false;}
  }
  void refresh();const timer=setInterval(()=>void refresh(),300000);
  const visible=()=>{if(!document.hidden)void refresh();};document.addEventListener('visibilitychange',visible);
  return()=>{controller.abort();clearInterval(timer);media.removeEventListener('change',change);document.removeEventListener('visibilitychange',visible);};
 },[]);
 const stopped=paused||hover||focused||reduced;
 const renderItem=(item:Item,copy:boolean)=><Link tabIndex={copy?-1:undefined} className="activity-ticker-item" href={item.href} key={item.key}>{item.ticker?<CompanyLogo ticker={item.ticker}/>:item.memberId?<Avatar name={item.name||''} memberId={item.memberId}/>:<span className="activity-dot" aria-hidden="true"/>}<span>{item.text}</span></Link>;
 return <div className={`activity-ticker ${reduced?'is-reduced':''}`} onMouseEnter={()=>setHover(true)} onMouseLeave={()=>setHover(false)} onFocusCapture={()=>setFocused(true)} onBlurCapture={e=>{if(!e.currentTarget.contains(e.relatedTarget))setFocused(false);}}>
 <div className="activity-ticker-window">
 {items.length?<div className="activity-ticker-track" style={{animationPlayState:stopped?'paused':'running',animationDuration:`${items.length*12}s`}}><div className="activity-ticker-group">{items.map(i=>renderItem(i,false))}</div>{!reduced&&<div className="activity-ticker-group" aria-hidden="true">{items.map(i=>renderItem(i,true))}</div>}</div>:<span className="activity-line-fallback">Explore recent disclosures below.</span>}
 </div>
 {!!items.length&&!reduced&&<button type="button" className="activity-pause" onClick={()=>setPaused(p=>!p)} aria-label={paused?'Resume activity ticker':'Pause activity ticker'}>{paused?'▶':'Ⅱ'}</button>}
 </div>;
}
