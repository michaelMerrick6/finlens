import { reviewPoliticianTrade } from './reviewed-politician-trades';
export type AnalysisTrade = {chamber?:string;id:string;doc_id:string;member_id:string|null;politician_name:string|null;ticker:string|null;asset_type:string|null;amount_range:string|null;asset_name:string|null;transaction_type:string;transaction_date:string|null;published_date:string|null;source_url:string|null};
function bounds(raw:string|null){
 const standard:Record<number,number>={1001:15000,15001:50000,50001:100000,100001:250000,250001:500000,500001:1000000,1000001:5000000,5000001:25000000,25000001:50000000};
 const bare=(raw||'').match(/^\$([\d,]+)$/);
 if(bare){const min=Number(bare[1].replaceAll(',',''));if(standard[min])return {min,max:standard[min],inferred:true};}

 const match=(raw||'').match(/^\$([\d,]+)\s*-\s*\$([\d,]+)$/);
 if(!match)return null;
 const min=Number(match[1].replaceAll(',','')),max=Number(match[2].replaceAll(',',''));
 return min>0&&max>=min?{min,max,inferred:false}:null;
}
export function aggregateAnalysis(rows:AnalysisTrade[],instrument:string){
 const groups=new Map<string,{ticker:string;buyers:Set<string>;sellers:Set<string>;purchaseMin:number;purchaseMax:number;saleMin:number;saleMax:number;unknownAmounts:number;inferredAmounts:number;trades:AnalysisTrade[];latest:string}>();
 const seen=new Set<string>();let excluded=0;
 for(const raw of rows){
 const t=reviewPoliticianTrade(raw),ticker=t.ticker?.trim().toUpperCase();
 const type=(t.asset_type||'').toUpperCase();
 if(t.exclude_from_totals||t.is_contribution||!t.member_id||!ticker||['NA','N/A','UNKNOWN','MULTI','US-TREAS'].includes(ticker)||!(/^[A-Z][A-Z0-9.-]{0,11}$/).test(ticker)||!(instrument==='options'?type==='OP':['ST','STOCK','ETF','ET'].includes(type))||!['buy','sell'].includes(t.transaction_type)){excluded++;continue;}
 const key=t.source_url&&t.doc_id?`${t.source_url}|${t.doc_id}`:t.id;
 if(seen.has(key)){excluded++;continue;}seen.add(key);
 const g=groups.get(ticker)||{ticker,buyers:new Set<string>(),sellers:new Set<string>(),purchaseMin:0,purchaseMax:0,saleMin:0,saleMax:0,unknownAmounts:0,inferredAmounts:0,trades:[],latest:''};
 (t.transaction_type==='buy'?g.buyers:g.sellers).add(t.member_id.toUpperCase());
 const range=bounds(t.amount_range);
 if(range){if(range.inferred)g.inferredAmounts++;if(t.transaction_type==='buy'){g.purchaseMin+=range.min;g.purchaseMax+=range.max;}else{g.saleMin+=range.min;g.saleMax+=range.max;}}else g.unknownAmounts++;
 g.trades.push(t);g.latest=[g.latest,t.published_date||''].sort().at(-1)!;groups.set(ticker,g);
 }
 return {excluded,stocks:[...groups.values()].map(g=>{
 const buys=g.trades.filter(t=>t.transaction_type==='buy'&&t.transaction_date).sort((a,b)=>a.transaction_date!.localeCompare(b.transaction_date!));
 let together=0;
 for(let i=0;i<buys.length;i++){const start=Date.parse(buys[i].transaction_date!);const ids=new Set<string>();for(let j=i;j<buys.length&&Date.parse(buys[j].transaction_date!)-start<=13*86400000;j++)ids.add(buys[j].member_id!.toUpperCase());together=Math.max(together,ids.size);}
 return {...g,buyers:g.buyers.size,sellers:g.sellers.size,together,trades:g.trades.sort((a,b)=>(b.published_date||'').localeCompare(a.published_date||'')||a.id.localeCompare(b.id))};})};
}
