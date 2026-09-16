import { aggregateAnalysis, type AnalysisTrade } from './congress-analysis';
export type ScreenFilters = { rank?: 'politicians'|'purchase_amount'; days: number; basis: 'disclosure'|'trade'; activity: 'buy'|'sell'|'all'; minPoliticians: number; ticker: string|null; industry: string|null; memberId: string|null; committeeId: string|null; chamber: 'all'|'House'|'Senate' };
export const DEFAULT_SCREEN: ScreenFilters = {rank:'politicians',days:30,basis:'disclosure',activity:'all',minPoliticians:1,ticker:null,industry:null,memberId:null,committeeId:null,chamber:'all'};
export type Classification = {ticker:string;company_name:string;industry:string|null;source_url:string;verified_at:string};
export type ScreenCatalog = {industries:string[];committees:{id:string;name:string;snapshotId:string;verifiedAt:string}[];members:{id:string;name:string;chamber:string}[]};
export function validateScreen(value: unknown): ScreenFilters {
 if(!value || typeof value!=='object' || Array.isArray(value)) throw Error('Invalid screening filters.');
 const f=value as ScreenFilters;
 const keys=Object.keys(DEFAULT_SCREEN);
 if(Object.keys(f).some(k=>!keys.includes(k)) || keys.some(k=>k!=='rank'&&!(k in f))) throw Error('Unsupported or missing screening filters.');
 if(!Number.isInteger(f.days)||f.days<1||f.days>366||!Number.isInteger(f.minPoliticians)||f.minPoliticians<1||f.minPoliticians>550) throw Error('Use 1–366 days and 1–550 politicians.');
 if(!['disclosure','trade'].includes(f.basis)||!['buy','sell','all'].includes(f.activity)||!['all','House','Senate'].includes(f.chamber))throw Error('Invalid screening choice.');
 for(const k of ['ticker','industry','memberId','committeeId'] as const) if(f[k]!==null&&(typeof f[k]!=='string'||!f[k]!.trim()||f[k]!.length>180))throw Error('Invalid screening value.');
 if(f.ticker&&!/^[A-Z][A-Z0-9.-]{0,11}$/.test(f.ticker))throw Error('Enter a valid uppercase ticker.');
 if(f.memberId&&!/^[A-Z]\d{6}$/.test(f.memberId))throw Error('Choose a politician from the list.');
 if(f.rank!==undefined&&!['politicians','purchase_amount'].includes(f.rank))throw Error('Invalid ranking choice.');
 if(f.rank==='purchase_amount'&&f.activity!=='buy')throw Error('Purchase amount ranking requires purchases.');
 return {...f,rank:f.rank||'politicians'};
}
export function screenDates(days:number, now=new Date()) {
 const end=now.toISOString().slice(0,10);const start=new Date(end+'T00:00:00Z');start.setUTCDate(start.getUTCDate()-days+1);
 return {start:start.toISOString().slice(0,10),end};
}
export function evaluateScreen(rows:AnalysisTrade[], f:ScreenFilters, classifications:Classification[], memberIds:Set<string>|null, chamberIds:Set<string>|null){
 const groups=aggregateAnalysis(rows,'stocks');
 const classificationByTicker=new Map(classifications.map(c=>[c.ticker,c]));
 const classified=groups.stocks.filter(s=>Boolean(classificationByTicker.get(s.ticker)?.industry)).length;
 const filtered=rows.filter(t=>(f.activity==='all'||t.transaction_type===f.activity)&&(!f.ticker||t.ticker?.toUpperCase()===f.ticker)&&(!f.memberId||t.member_id===f.memberId)&&(!memberIds||memberIds.has(t.member_id||''))&&(f.chamber==='all'||t.chamber===f.chamber||(!t.chamber&&Boolean(chamberIds?.has(t.member_id||''))))&&(!f.industry||classificationByTicker.get(t.ticker?.toUpperCase()||'')?.industry===f.industry));
 const result=aggregateAnalysis(filtered,'stocks');
 const stocks=result.stocks.map(s=>({...s,disclosedPurchases:purchaseAmounts(s.trades),politicians:new Set(s.trades.map(t=>t.member_id)).size,company:classificationByTicker.get(s.ticker)||null}))
 .filter(s=>s.politicians>=f.minPoliticians).sort((a,b)=>(f.rank==='purchase_amount'?b.disclosedPurchases.min-a.disclosedPurchases.min:0)||b.politicians-a.politicians||b.trades.length-a.trades.length||a.ticker.localeCompare(b.ticker));
 return {stocks,coverage:{eligibleTickers:groups.stocks.length,classifiedTickers:classified,unclassifiedTickers:groups.stocks.length-classified},excluded:groups.excluded};
}
export type ScreenResult = ReturnType<typeof evaluateScreen> & {filters:ScreenFilters;start:string;end:string;scanned:number;computedAt:string;committeeVerifiedAt:string|null};

// Explain the deterministic ranking, including ties, without inferring trade motives.
export function summarizeScreen(result: ScreenResult): string {
 const stocks=result.stocks;
 if(!stocks.length)return 'No matching companies were found in the available records for this question.';
 if(result.filters.rank==='purchase_amount'){const max=Math.max(...stocks.map(s=>s.disclosedPurchases.min));if(!max)return 'Purchase amounts are unavailable for ranking in these matching records.';const leaders=stocks.filter(s=>s.disclosedPurchases.min===max);return `${leaders.map(s=>s.ticker).join(', ')} ${leaders.length===1?'ranks first':'tie for first'} by the summed lower bound of disclosed purchase ranges (${formatPurchaseRange(leaders[0].disclosedPurchases)}). Missing amounts are excluded; ranges can overlap, so actual dollar ordering is uncertain.`;}
 const max=Math.max(...stocks.map(s=>s.politicians));
 const leaders=stocks.filter(s=>s.politicians===max);
 const names=leaders.slice(0,5).map(s=>s.ticker).join(', ')+(leaders.length>5?` and ${leaders.length-5} others`:'');
 const action=result.filters.activity==='buy'?'purchasing':result.filters.activity==='sell'?'selling':'trading';
 return `${names} ${leaders.length===1?'ranks first':'tie for first'} by distinct politicians ${action}, with ${max} ${max===1?'politician':'politicians'}${leaders.length>1?' each':''} in the matching records from ${result.start} to ${result.end} (${result.filters.basis==='disclosure'?'disclosure':'transaction'} dates). This measures reported activity, not investment merit or the reasons for a trade.`;
}

export function purchaseAmounts(trades:AnalysisTrade[]){
 let min=0,max=0,known=0,missing=0;
 for(const t of trades){if(t.transaction_type!=='buy')continue;
 const match=(t.amount_range||'').match(/^\$([\d,]+)\s*[-–—]\s*\$([\d,]+)$/);
 const lower=match?Number(match[1].replaceAll(',','')):NaN,upper=match?Number(match[2].replaceAll(',','')):NaN;
 if(!Number.isSafeInteger(lower)||!Number.isSafeInteger(upper)||lower<=0||upper<lower){missing++;continue;}
 min+=lower;max+=upper;known++;
 }return {min,max,known,missing};
}
export function formatPurchaseRange(amount:ReturnType<typeof purchaseAmounts>){return amount.known?`$${amount.min.toLocaleString('en-US')}–$${amount.max.toLocaleString('en-US')}${amount.missing?' (partial)':''}`:'Amount unavailable';}
