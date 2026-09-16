import { formatPurchaseRange, type ScreenResult } from './research-screen';

export type ResearchBriefSection = {
 ticker:string; name:string; headline:string; paragraphs:string[];
 members:{id:string;name:string}[]; dates:string;
};
export type ResearchBrief = {title:string;lead:string;sections:ResearchBriefSection[]};

// All narrative facts are composed from the same filtered records as the evidence.
export function buildResearchBrief(result:ScreenResult):ResearchBrief {
 const {stocks,filters}=result;
 const activity=filters.activity==='buy'?'buyers':filters.activity==='sell'?'sellers':'traders';
 const action=filters.activity==='buy'?'purchases':filters.activity==='sell'?'sales':'purchases and sales';
 if(!stocks.length)return {title:'No matches in this disclosure window.',lead:'The available records do not match this question. Try asking about a longer period or a broader group of companies.',sections:[]};
 const money=filters.rank==='purchase_amount';
 const score=(s:typeof stocks[number])=>money?s.disclosedPurchases.min:s.politicians;
 const maximum=Math.max(...stocks.map(score));
 const leaders=stocks.filter(s=>score(s)===maximum);
 const first=stocks[0];
 const name=(s:typeof first)=>s.company?.company_name||s.trades[0]?.asset_name||s.ticker;
 const title=money?(maximum===0?'Purchase amounts are unavailable for ranking.':leaders.length===1?`${name(stocks[0])} leads by disclosed purchase minimum.`:`${leaders.length} companies share the highest disclosed purchase minimum.`):stocks.length===1?`${name(first)}: the disclosed activity.`:leaders.length===1?`${name(first)} leads by distinct ${activity}.`:`${leaders.length} companies share the lead by distinct ${activity}.`;
 const lead=money?`Companies are ordered by the summed lower bounds of their reported purchase ranges. ${maximum?`${first.ticker} has a disclosed purchase total of ${formatPurchaseRange(first.disclosedPurchases)}. `:''}Ranges can overlap, so this is not a definitive ranking of actual money invested. Missing and unbounded amounts are excluded; partial totals are labeled. Purchases do not necessarily represent money paid to the company itself.`:stocks.length===1?`${first.politicians} distinct ${first.politicians===1?'politician appears':'politicians appear'} in ${first.trades.length} reported ${action} for ${first.ticker} in this screen.`:
 `${leaders.length===1?name(first):leaders.slice(0,3).map(s=>s.ticker).join(', ')+(leaders.length>3?` and ${leaders.length-3} others`:'')} ${leaders.length===1?'has':'each have'} ${maximum} distinct ${activity} in the matching records. ${stocks.length} companies match overall. This ranking measures how widely a company appears across politicians, rather than the number or dollar size of their trades.`;
 return {title,lead,sections:stocks.slice(0,3).map((s,index)=>{
 const members=[...new Map(s.trades.filter(t=>t.member_id).map(t=>[t.member_id!,{id:t.member_id!,name:t.politician_name||t.member_id!}])).values()].sort((a,b)=>a.name.localeCompare(b.name));
 const buys=s.trades.filter(t=>t.transaction_type==='buy').length;
 const sells=s.trades.filter(t=>t.transaction_type==='sell').length;
 const dates=[...new Set(s.trades.map(t=>t.transaction_date).filter((d):d is string=>Boolean(d)))].sort();
 const missingDates=s.trades.filter(t=>!t.transaction_date).length;
 const count=`${s.politicians} distinct ${s.politicians===1?'politician':'politicians'}`;
 const paragraphs=[`${name(s)} appears in ${s.trades.length} matching ${s.trades.length===1?'transaction':'transactions'} from ${count}. ${buys&&sells?`${buys} are purchases and ${sells} are sales. Activity on both sides should not be read as agreement on the company.`:buys?`These records report purchases${filters.activity==='buy'?'; sales are outside this purchase-only screen':''}.`:`These records report sales${filters.activity==='sell'?'; purchases are outside this sale-only screen':''}.`}`];
 if(money){paragraphs.push(`${s.ticker}'s reported purchase ranges total ${formatPurchaseRange(s.disclosedPurchases)}, across ${s.disclosedPurchases.known} transactions with complete ranges. ${s.disclosedPurchases.missing?`${s.disclosedPurchases.missing} purchase amounts are missing, unbounded, or incomplete and excluded.`:'All matching purchase records have complete ranges.'}`);}
 else if(index===0&&stocks.length>1){const next=stocks[1];paragraphs.push(s.politicians===next.politicians?`${s.ticker} is tied on politician count. Transaction count determines display order within a tie, followed by ticker; it does not make this company a unique leader.`:`For comparison, ${next.ticker} appears for ${next.politicians} distinct ${activity} across ${next.trades.length} transactions in the same screen.`);}
 else paragraphs.push(`${s.trades.length>s.politicians?'Some politicians reported multiple transactions. Each politician still counts once in this ranking.':'Each matching transaction is attributed to a different politician.'} Household transactions are attributed to the reporting politician.`);
 return {ticker:s.ticker,name:name(s),headline:money?(s.disclosedPurchases.known?`${formatPurchaseRange(s.disclosedPurchases)} in disclosed purchases.`:'Purchase amounts unavailable.'):stocks.length===1?'What the filings show.':score(s)===maximum?(leaders.length>1?'A shared lead in reported activity.':filters.activity==='buy'?'The widest buying interest in this window.':filters.activity==='sell'?'The widest selling activity in this window.':'The widest participation in this window.'):buys&&sells?'Activity on both sides of the trade.':s.trades.length>first.trades.length?`More transactions, fewer distinct ${activity}.`:s.trades.length>s.politicians?`Repeat ${action} within a smaller group.`:'A narrower footprint in the filings.',paragraphs,members,dates:dates.length?`Reported transaction dates: ${dates[0]}${dates.length>1?` to ${dates.at(-1)}`:''}.${missingDates?` ${missingDates} records have no transaction date.`:''}`:'Transaction dates are unavailable.'};
 })};
}
