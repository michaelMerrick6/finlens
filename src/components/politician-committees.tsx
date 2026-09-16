import { getMemberCommittees } from '@/lib/committee-assignments';

const date = (value: string) => new Date(value).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' });
export async function PoliticianCommittees({ memberId }: { memberId: string }) {
  const data = await getMemberCommittees(memberId);
  return <section className="committee-section" aria-labelledby="committee-heading">
    <div className="section-heading"><div><span className="eyebrow">CONGRESSIONAL RESPONSIBILITIES</span>
      <h2 id="committee-heading">Committee assignments</h2></div></div>
    <p className="committee-note">Current assignments from official House and Senate rosters.
      {data.verifiedAt && <> Last verified {date(data.verifiedAt)}.</>}
    </p>
    {data.stale && <p className="committee-status" role="status">Verification is overdue. Showing the last successfully verified roster.</p>}
    {data.status === 'unavailable' ? <p>Committee data is temporarily unavailable.</p> : data.status === 'not-covered' ?
      <p>This politician is not listed in the available current member rosters.</p> : data.current.length === 0 ?
      <p>No committee assignments are listed in this member’s current official roster.</p> :
      <ul className="committee-list">{data.current.map(c => <li key={`${c.source}-${c.name}-${c.parent}`}>
        <div>{c.parent && <span className="committee-parent">{c.parent}</span>}
          <a href={c.source} target="_blank" rel="noreferrer">{c.name}</a></div>
        <span className="committee-role">{c.role}</span>
      </li>)}</ul>}
    {data.history.length > 0 && <details className="committee-history"><summary>Archived assignments · 2024 onward</summary>
      <p className="committee-note">Dated snapshots from the community-maintained Congress Legislators archive, including the preceding baseline. These are archive observation dates, not appointment dates. Coverage between snapshots and during congressional transitions may be incomplete.</p>
      <div className="committee-history-list">{data.history.map(h => <details key={`${h.observedAt}-${h.source}`}>
        <summary>Archive observed {date(h.observedAt)}</summary>
        <ul>{h.names.sort().map(name => <li key={name}>{name}</li>)}</ul>
        <a href={h.source.replace('committees-current.yaml', 'committee-membership-current.yaml')} target="_blank" rel="noreferrer">View archived roster ↗</a>
      </details>)}</div>
    </details>}
    <p className="committee-note">Current membership does not establish membership at the time of an earlier trade.</p>
  </section>;
}
