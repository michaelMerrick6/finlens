import { dateLabel } from '@/lib/ui-format';
export function OptionLabel({trade}:{trade:{asset_type?:string|null;asset_name?:string|null}}){
 const name=trade.asset_name||'';
 if(trade.asset_type?.toUpperCase()!=='OP'&&!/\[OP\]|\b(?:call|put) options?\b/i.test(name))return null;
 const side=/\bcall\b/i.test(name)?'Call option':/\bput\b/i.test(name)?'Put option':'Options';
 const strike=name.match(/\bstrike(?: price)?(?: of)?\s*[:;]?\s*\$?([\d,]+(?:\.\d+)?)/i)?.[1];
 const expiry=name.match(/\b(?:expires|expiration(?: date)?(?: of)?)\s*[:;]?\s*(\d{4}-\d{2}-\d{2}|\d{1,2}\/\d{1,2}\/\d{2,4})/i)?.[1];
 let iso=expiry;
 if(expiry?.includes('/')){const [m,d,y]=expiry.split('/');iso=`${y.length===2?'20'+y:y}-${m.padStart(2,'0')}-${d.padStart(2,'0')}`;}
 const details=`Strike price: ${strike?'$'+strike:'Not available in our data'}. Expiration: ${iso?dateLabel(iso):'Not available in our data'}.`;
 return <span className="option-label" tabIndex={0} aria-label={`${side}. ${details}`}>{side} <span aria-hidden="true">ⓘ</span><span className="option-tooltip" role="tooltip"><span className="option-term"><span>Strike</span><b>{strike?'$'+strike:'Not available'}</b></span><span className="option-term"><span>Expires</span><b>{iso?dateLabel(iso):'Not available'}</b></span>{(!strike||!expiry)&&<small>Check the original filing for missing terms.</small>}</span></span>;
}
