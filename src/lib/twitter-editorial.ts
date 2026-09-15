type Row = {status:string;draft_text:string;payload:Record<string,unknown>|null};
export function twitterEditorial(row:Row): string | null {
 const p=row.payload||{};
 const signal=String(p.signal_type||'');
 if(!['politician_trade','politician_trade_grouped','congress_cluster'].includes(signal))return null;
 const amount=Number(p.cluster_combined_lower_bound||p.group_combined_lower_bound||String(p.amount_range||'').match(/[\d,]+/)?.[0]?.replaceAll(',','')||0);
 const clusteredBuy=signal==='congress_cluster'&&p.direction==='buy'&&Number(p.cluster_actor_count)>=2&&Number(p.cluster_window_days)>0&&Number(p.cluster_window_days)<=14;
 if(!Number.isFinite(amount)||(amount<25000&&!clusteredBuy))return null;
 if(row.status!=='pending_review')return row.draft_text;
 // Preserve editorial work; only replace the old machine-generated layout.
 if(!/Congress cluster:|Combined floor:|Disclosed ranges:|^Congress filing:|^Congress trade:/.test(row.draft_text))return row.draft_text;
 const direction=p.direction==='buy'?'purchases':p.direction==='sell'?'sales':null;
 if(!direction)return null;
 const ticker=String(p.ticker||'');
 if(!ticker)return null;
 const subject=signal==='congress_cluster'?`${p.cluster_actor_count} members of Congress`:String(p.actor_name||'A member of Congress');
 return `${subject} disclosed ${direction} of $${ticker}.\n\nAt least $${amount.toLocaleString('en-US')} in reported ${direction}${signal==='politician_trade'?'':' combined'}.\n\nExplore the disclosures: https://www.vail.finance/analysis`;
}
