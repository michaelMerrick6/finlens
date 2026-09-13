"use client";
import Link from 'next/link';
import { useAccount } from './account-provider';
import { Avatar } from './identity-images';
import { Icon } from './icon';
export function TrackingPromo() {
  const {session,account}=useAccount();
  const politicians=account?.follows.actors.filter(a=>a.actorType==='politician') || [];
  const personal=Boolean(session&&politicians.length);
  return <aside className="tracking-promo tracking-promo-simple">
    <h2>{personal?'Your watchlist.':<>Your watch on<br/>Washington.</>}</h2>
    {personal?<>
      <div className="watchlist-portraits" aria-label="Politicians you track">
        {politicians.slice(0,5).map(p=><span key={p.id} title={p.actorName} aria-label={p.actorName}><Avatar name={p.actorName} memberId={typeof p.metadata.member_id==='string'?p.metadata.member_id:p.actorKey}/></span>)}
        {politicians.length>5&&<span className="watchlist-more">+{politicians.length-5}</span>}
      </div>
      <p>{politicians.length} {politicians.length===1?'politician':'politicians'} on your list.<br/>See their latest disclosed activity.</p>
    </>:<p>Track politicians. Choose email alerts for newly disclosed activity.</p>}
    <Link href="/tracking">{personal?'View tracked activity':'Start tracking'}<Icon name="arrow" size={18}/></Link>
  </aside>;
}
