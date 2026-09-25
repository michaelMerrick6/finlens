import type { ReactNode } from 'react';

export function Skeleton({ className = '' }: { className?: string }) {
  return <span aria-hidden="true" className={`skeleton ${className}`} />;
}

export function LoadingRegion({ label, children, className = '' }: {
  label: string; children: ReactNode; className?: string;
}) {
  return <div className={`loading-region ${className}`} role="status" aria-label={label}>
    <span className="sr-only">{label}</span>
    <div aria-hidden="true">{children}</div>
  </div>;
}

export function DirectorySkeleton({ controls = false }: { controls?: boolean }) {
  return <LoadingRegion label="Loading politicians…">
    {controls && <div className="directory-controls-skeleton">
      <div className="directory-tools"><Skeleton className="skeleton-search"/><Skeleton className="skeleton-tabs"/></div>
      <div className="ranking-controls"><Skeleton className="skeleton-line"/><Skeleton className="skeleton-line"/></div>
      <div className="fine-print"><Skeleton/><Skeleton/></div>
    </div>}
    <div className="directory-grid">
      {Array.from({ length: 8 }, (_, index) => <div className="member-card member-card-skeleton" key={index}>
        <div className="member-card-top"><Skeleton className="skeleton-avatar"/><Skeleton className="skeleton-short"/></div>
        <Skeleton className="skeleton-name"/><Skeleton className="skeleton-line"/>
        <div className="member-metrics"><Skeleton className="skeleton-name"/><Skeleton/><Skeleton/></div>
        <div className="member-card-link"><Skeleton className="skeleton-line"/></div>
      </div>)}
    </div>
  </LoadingRegion>;
}

// Keep the real table columns and row heights while the first page is loading.
export function DisclosureRowsSkeleton() {
  return <>{Array.from({ length: 8 }, (_, index) => <tr className="disclosure-row-skeleton" aria-hidden="true" key={index}>
    <td><div className="person-cell"><Skeleton className="skeleton-avatar"/><div><Skeleton className="skeleton-name"/><Skeleton className="skeleton-short"/></div></div></td>
    <td><Skeleton className="skeleton-short"/><Skeleton className="skeleton-line"/></td>
    <td><Skeleton className="skeleton-badge"/></td>
    <td><Skeleton className="skeleton-line"/></td>
    <td><Skeleton className="skeleton-line"/><Skeleton className="skeleton-short"/></td>
    <td><Skeleton className="skeleton-icon"/></td>
  </tr>)}</>;
}

export function FeedSkeleton({ heading = 'Latest disclosures', showFilters = true }: { heading?: string; showFilters?: boolean }) {
  return <section className="feed-section" aria-label={heading}>
    <div className="section-heading"><div><div className="eyebrow">THE PUBLIC RECORD</div><h2>{heading}</h2></div></div>
    <LoadingRegion label="Loading disclosures…">
      {showFilters && <div className="feed-toolbar"><Skeleton className="skeleton-tabs"/><Skeleton className="skeleton-search"/></div>}
      <div className="table-scroll"><table className="trade-table"><thead><tr>
        <th>Politician</th><th>Asset</th><th>Activity</th><th>Disclosed amount</th><th>Filed</th><th/>
      </tr></thead><tbody><DisclosureRowsSkeleton/></tbody></table></div>
    </LoadingRegion>
  </section>;
}

export function DashboardSkeleton({ label = 'Loading activity…' }: { label?: string }) {
  return <LoadingRegion label={label}>
    <div className="dashboard-skeleton">
      <div><Skeleton className="skeleton-name"/><Skeleton className="skeleton-total"/><Skeleton/><Skeleton className="skeleton-chart-bar"/><Skeleton className="skeleton-line"/></div>
      <div><Skeleton className="skeleton-name"/>{Array.from({ length: 5 }, (_, index) => <div className="skeleton-sector" key={index}><Skeleton className="skeleton-line"/><Skeleton/></div>)}</div>
    </div>
    <div className="skeleton-list"><Skeleton className="skeleton-name"/>{Array.from({ length: 5 }, (_, index) => <div className="skeleton-list-row" key={index}><Skeleton className="skeleton-line"/><Skeleton className="skeleton-short"/><Skeleton className="skeleton-short"/></div>)}</div>
  </LoadingRegion>;
}

export function PageSkeleton({ label = 'Loading page…', profile = false }: { label?: string; profile?: boolean }) {
  return <main id="main" className="container loading-page" aria-busy="true">
    <div className="loading-region">
      <div aria-hidden="true" className={`page-skeleton-heading ${profile ? 'profile-skeleton-heading' : ''}`}>
        {profile && <Skeleton className="skeleton-avatar large"/>}
        <div><Skeleton className="skeleton-line"/><Skeleton className="skeleton-title"/><Skeleton className="skeleton-line"/></div>
      </div>
    </div>
    {profile ? <><div className="profile-context" aria-hidden="true"><Skeleton className="skeleton-line"/></div><CommitteeSkeleton/><FeedSkeleton heading="Disclosed activity"/></> : <DashboardSkeleton label={label}/>}
  </main>;
}

export function CommitteeSkeleton() {
  return <section className="committee-section">
    <div className="section-heading"><div><span className="eyebrow">CONGRESSIONAL RESPONSIBILITIES</span><h2>Committee assignments</h2></div></div>
    <LoadingRegion label="Loading committee assignments…">
      <Skeleton className="skeleton-line"/>
      <div className="skeleton-list-row"><Skeleton/><Skeleton className="skeleton-short"/></div>
      <div className="skeleton-list-row"><Skeleton/><Skeleton className="skeleton-short"/></div>
    </LoadingRegion>
  </section>;
}
