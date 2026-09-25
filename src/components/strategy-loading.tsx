import Link from 'next/link';
import { Avatar } from './identity-images';
import { LoadingRegion, Skeleton } from './loading-state';
import styles from './strategy-dashboard.module.css';

export function StrategyLoading({ name, memberId, src }: { name: string; memberId?: string; src?: string }) {
  const title = `${name.split(' ').at(-1)} strategy`;
  return <main id="main" className={`container ${styles.page}`} aria-busy="true">
    <Link href="/analysis/strategies" className={styles.back}>← All strategies</Link>
    <header className={styles.header}>
      <div className={styles.title}><Avatar name={name} memberId={memberId} src={src}/><div><div className={styles.eyebrow}>DISCLOSURE STRATEGY</div><h1>{title}</h1></div></div>
    </header>
    <p className={styles.intro}>The stocks, sectors and disclosed activity behind the {memberId === 'P000197' ? 'household’s estimated portfolio.' : 'reviewed portfolio.'}</p>
    <LoadingRegion label={`Loading ${title.toLowerCase()}…`}>
      <div className={styles.summaryStrip}>{[0, 1, 2, 3].map(index => <div key={index}><Skeleton className="skeleton-line"/><Skeleton className="skeleton-name"/></div>)}</div>
    </LoadingRegion>
    <LoadingRegion label="Loading holdings and activity…">
      <div className={styles.overview}>
        <div className={styles.card}>
          <Skeleton className="skeleton-name"/><Skeleton className="skeleton-line"/>
          <div className={styles.mixBody}>
            <div className={`${styles.donut} skeleton-donut`}/>
            <div>{[0, 1, 2, 3, 4].map(index => <div className="skeleton-sector" key={index}><Skeleton/><Skeleton className="skeleton-short"/></div>)}</div>
          </div>
          <Skeleton/><Skeleton className="skeleton-line"/>
        </div>
        <div className={styles.card}>
          <Skeleton className="skeleton-name"/><Skeleton className="skeleton-line"/>
          <div className={styles.sectors}>{[0, 1, 2, 3, 4].map(index => <div className="skeleton-sector" key={index}><Skeleton className="skeleton-line"/><Skeleton/></div>)}</div>
        </div>
      </div>
      <div className={styles.recent}>
        <div className={styles.sectionHeading}><Skeleton className="skeleton-name"/></div>
        <div className={styles.tradeCards}>{[0, 1, 2].map(index => <div className={styles.tradeCard} key={index}><Skeleton className="skeleton-name"/><Skeleton className="skeleton-total"/><Skeleton className="skeleton-line"/></div>)}</div>
      </div>
      <div className="skeleton-list">{[0, 1, 2, 3].map(index => <div className="skeleton-list-row" key={index}><Skeleton className="skeleton-line"/><Skeleton className="skeleton-short"/></div>)}</div>
    </LoadingRegion>
  </main>;
}
