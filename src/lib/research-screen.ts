import { aggregateAnalysis, type AnalysisTrade } from './congress-analysis';
export type ScreenFilters = { days: number; basis: 'disclosure'|'trade'; activity: 'buy'|'sell'|'all'; minPoliticians: number; ticker: string|null; industry: string|null; memberId: string|null; committeeId: string|null; chamber: 'all'|'House'|'Senate' };
export const DEFAULT_SCREEN: ScreenFilters = {days:30,basis:'disclosure',activity:'all',minPoliticians:1,ticker:null,industry:null,memberId:null,committeeId:null,chamber:'all'};
export type Classification = {ticker:string;company_name:string;industry:string|null;source_url:string;verified_at:string};
export type ScreenCatalog = {industries:string[];committees:{id:string;name:string;snapshotId:string;verifiedAt:string}[];members:{id:string;name:string;chamber:string}[]};
export function validateScreen(value: unknown): ScreenFilters {
 if(!value || typeof value!=='object' || Array.isArray(value)) throw Error('Invalid screening filters.');
 const f=value as ScreenFilters;
 const keys=Object.keys(DEFAULT_SCREEN);
 if(Object.keys(f).some(k=>!keys.includes(k)) || keys.some(k=>!(k in f))) throw Error('Unsupported or missing screening filters.');
 if(!Number.isInteger(f.days)||f.days<1||f.days>366||!Number.isInteger(f.minPoliticians)||f.minPoliticians<1||f.minPoliticians>550) throw Error('Use 1–366 days and 1–550 politicians.');
 if(!['disclosure','trade'].includes(f.basis)||!['buy','sell','all'].includes(f.activity)||!['all','House','Senate'].includes(f.chamber))throw Error('Invalid screening choice.');
 for(const k of ['ticker','industry','memberId','committeeId'] as const) if(f[k]!==null&&(typeof f[k]!=='string'||!f[k]!.trim()||f[k]!.length>180))throw Error('Invalid screening value.');
 if(f.ticker&&!/^[A-Z][A-Z0-9.-]{0,11}$/.test(f.ticker))throw Error('Enter a valid uppercase ticker.');
 if(f.memberId&&!/^[A-Z]\d{6}$/.test(f.memberId))throw Error('Choose a politician from the list.');
 return {...f};
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
 const stocks=result.stocks.map(s=>({...s,politicians:new Set(s.trades.map(t=>t.member_id)).size,company:classificationByTicker.get(s.ticker)||null}))
 .filter(s=>s.politicians>=f.minPoliticians).sort((a,b)=>b.politicians-a.politicians||b.trades.length-a.trades.length||a.ticker.localeCompare(b.ticker));
 return {stocks,coverage:{eligibleTickers:groups.stocks.length,classifiedTickers:classified,unclassifiedTickers:groups.stocks.length-classified},excluded:groups.excluded};
}
export type ScreenResult = ReturnType<typeof evaluateScreen> & {filters:ScreenFilters;start:string;end:string;scanned:number;computedAt:string;committeeVerifiedAt:string|null};
