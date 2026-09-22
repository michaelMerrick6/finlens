import Link from 'next/link';
import { notFound } from 'next/navigation';
import { partyStrategies } from '@/lib/party-strategies';
import styles from '../strategies.module.css';

function getStrategy(slug: string) {
  return partyStrategies.find(strategy => strategy.slug === slug);
}

export function generateStaticParams() {
  return partyStrategies.map(strategy => ({ party: strategy.slug }));
}

export async function generateMetadata({ params }: { params: Promise<{ party: string }> }) {
  const strategy = getStrategy((await params).party);
  if (!strategy) notFound();
  return { title: strategy.name };
}

export default async function PartyStrategyPage({ params }: { params: Promise<{ party: string }> }) {
  const strategy = getStrategy((await params).party);
  if (!strategy) notFound();
  return <main id="main" className={`container ${styles.page}`}>
    <Link href="/analysis/strategies" className="breadcrumb">← All strategies</Link>
    <div className={styles.heading}>
      <span className={`${styles.partySymbol} ${styles[strategy.tone]}`} aria-hidden="true">{strategy.symbol}</span>
      <div><span className="eyebrow">STRATEGIES · {strategy.members.toUpperCase()}</span><h1>{strategy.name}.</h1></div>
    </div>
    <p className={styles.intro}>A model portfolio built around what {strategy.members} in Congress are buying, after subtracting their reported sales.</p>
    <p className={styles.preview}>Planned strategy. Portfolio weights, performance tracking and scheduled rebalancing are not yet available.</p>
    <div className={styles.stats}>
      <div><strong>{strategy.members}</strong><span>House and Senate members</span></div>
      <div><strong>Net buying</strong><span>Estimated purchases minus sales</span></div>
      <div><strong>Weekly</strong><span>Proposed rebalance schedule</span></div>
    </div>
    <div className={styles.layout}>
      <section>
        <span className="eyebrow">PROPOSED APPROACH</span>
        <h2>Follow the party’s net buying.</h2>
        <p className={styles.intro}>For each stock, add the estimated value of purchases disclosed by {strategy.party} members of Congress and subtract their estimated sales. Stocks with positive net buying form the model portfolio.</p>
        <h2>Let net buying set the weights.</h2>
        <p className={styles.intro}>A stock accounting for 30% of the party’s total positive estimated net buying would receive a 30% model weight. Recalculate weekly as disclosures become available; zero or negative net buying would remove a stock.</p>
        <Link className="button secondary" href="/analysis">Explore congressional activity →</Link>
      </section>
      <aside className={styles.rules}>
        <span className="eyebrow">BEFORE THE STRATEGY LAUNCHES</span>
        <h3>Historical coverage</h3>
        <p>Validate disclosures from the proposed January 2023 start and use each member’s party affiliation at the time of the transaction. Independents are excluded.</p>
        <h3>Estimated amounts</h3>
        <p>Reported dollar ranges need a consistent valuation rule. Net buying measures disclosed activity, not the party’s actual combined holdings.</p>
        <h3>Public information only</h3>
        <p>Portfolio changes will use disclosures available before each rebalance. Historical returns will not assume advance knowledge of a trade.</p>
      </aside>
    </div>
  </main>;
}
