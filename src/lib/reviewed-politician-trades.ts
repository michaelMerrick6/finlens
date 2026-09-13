// Source-reviewed presentation corrections. Preserve the original database records.
// Evidence: House PTR PDFs 20033337, 20033725, 20034836, 20035143.
const upperBounds: Record<number, number> = {1001:15000,15001:50000,50001:100000,100001:250000,250001:500000,500001:1000000,1000001:5000000,5000001:25000000};
const reviewedFilings = new Set(['house-2025-20033337','house-2026-20033725','house-2026-20034836','house-2026-20035143']);
const donations = new Set(['house-2025-20033337-0','house-2026-20033725-3','house-2026-20033725-9']);
const exercises: Record<string,string> = {'1':'Alphabet (GOOGL)','6':'Amazon (AMZN)','12':'NVIDIA (NVDA)','14':'Tempus AI (TEM)','16':'Vistra (VST)'};
type Reviewable = { doc_id?: string | null; member_id?: string | null; source_url?: string | null; amount_range?: string | null; asset_name?: string | null; asset_type?: string | null; description?: string | null; filing_status?: string | null };
function applyReviewedCorrections<T extends Reviewable>(trade:T): T & { activity_note?: string; is_contribution?: boolean } {
  const id=trade.doc_id || '';
  const filing=id.replace(/-\d+$/, '');
  const match=filing.match(/^house-(\d{4})-(\d+)$/);
  if(trade.member_id!=='P000197'||!reviewedFilings.has(filing)||!match||trade.source_url!==`https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/${match[1]}/${match[2]}.pdf`)return trade;
  const result = { ...trade, activity_note: 'Reported owner: spouse.' };
  const min=Number((trade.amount_range || '').replace(/[$,]/g,''));
  if(upperBounds[min]) result.amount_range=`$${min.toLocaleString('en-US')} - $${upperBounds[min].toLocaleString('en-US')}`;
  const row=id.split('-').at(-1)!;
  if(filing==='house-2026-20033725' && exercises[row]) {
    result.asset_type='ST';
    result.asset_name=`${exercises[row]} common stock — acquired by exercising 50 call options (5,000 shares)`;
    result.activity_note+=' Option exercise resulting in stock acquisition; not a new option purchase.';
  }
  if(id==='house-2026-20033725-0')result.activity_note+=' Publicly traded AllianceBernstein partnership units; source asset code AB.';
  if(id==='house-2026-20033725-17')result.asset_name='Walt Disney Company (DIS) common stock';
  if(donations.has(id))return {...result, is_contribution:true,activity_note:'Reported owner: spouse. Charitable contribution, reported in the filing under sale/partial sale; not an open-market sale.'};
  return result;
}
export function isReviewedPublicPartnership(trade: Reviewable) {
  return trade.doc_id==='house-2026-20033725-0' && trade.member_id==='P000197' && trade.source_url==='https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/20033725.pdf';
}


export function reviewPoliticianTrade<T extends Reviewable>(raw:T): T & {activity_note?:string;activity_label?:string;is_contribution?:boolean;exclude_from_totals?:boolean} {
 const trade=applyReviewedCorrections(raw);
 const text=[trade.description,trade.asset_name,trade.activity_note].filter(Boolean).join(' ');
 const aesId=trade.doc_id?.match(/^house-2026-(20034375|20034528)-0$/)?.[1];
 if(aesId && trade.member_id==='R000614' && trade.source_url===`https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2026/${aesId}.pdf`){
  return {...trade,activity_label:aesId==='20034528'?'Stock award · amended':'Stock award',exclude_from_totals:true,
   activity_note:'Spouse received restricted stock units as compensation; not an open-market purchase. '+(aesId==='20034528'?'This filing is marked amended. Its relationship to the earlier award requires reconciliation.':'A later amended award filing reports a different amount and date. Do not treat this original amount as confirmed.')};
 }
 if(trade.is_contribution || /\b(donated|charitable contribution|donor-advised fund)\b/i.test(text))return {...trade,is_contribution:true,exclude_from_totals:true,activity_label:'Charitable contribution',activity_note:trade.activity_note||'Shares contributed rather than sold on the open market.'};
 if(/\b(awarded RSUs?|stock[- ]based compensation|as (?:part of|compensation)|restricted stock units? awarded)\b/i.test(text))return {...trade,activity_label:'Stock award',exclude_from_totals:true,activity_note:'Shares or restricted stock units received as compensation; excluded from buying and selling totals.'};
 if(/\b(exercised? \d+ call options?|exercising .*call options?|option exercise)\b/i.test(text))return {...trade,activity_label:'Option exercise',activity_note:trade.activity_note||'Shares acquired through an existing option. This is not a new option purchase.'};
 if(/amended/i.test(trade.filing_status||'') || /\bfiling status:\s*amended\b/i.test(text))return {...trade,activity_label:'Amended filing',exclude_from_totals:true,activity_note:'Corrected filing. Excluded from totals until reconciled with the original.'};
 if((trade.asset_type||'').toUpperCase()==='RS')return {...trade,activity_label:'Activity needs review',exclude_from_totals:true,activity_note:'Restricted stock record. The available description does not establish an open-market purchase; excluded from totals.'};
 return trade;
}
