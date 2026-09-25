import Link from 'next/link';
import { Avatar } from '@/components/identity-images';
import styles from './strategies.module.css';
import { partyStrategies } from '@/lib/party-strategies';

export const metadata = { title: 'Strategies' };

export default function StrategiesPage() {
  return <main id="main" className={`container ${styles.page}`}>
    <span className="eyebrow">ANALYSIS · STRATEGIES</span>
    <h1>Follow the disclosures.</h1>
    <p className={styles.intro}>Explore model portfolios built from public financial disclosures. Inspect the holdings, sources and coverage behind each strategy.</p>
    <div className={styles.cards}>
      <article className={styles.card}>
        <div className={styles.cardTop}><Avatar name="Nancy Pelosi" memberId="P000197"/><span className={styles.badge}>Allocation preview</span></div>
        <h2>Pelosi strategy</h2>
        <p>A model stock allocation based on the Pelosi household’s estimated holdings, with the data behind each position.</p>
        <Link href="/analysis/strategies/pelosi" className="button">Explore Pelosi strategy →</Link>
      </article>
      <article className={styles.card}>
        <div className={styles.cardTop}><Avatar name="Donald Trump" src="/images/politicians/donald-trump.png"/><span className={styles.badge}>Disclosed holdings</span></div>
        <h2>Trump strategy</h2>
        <p>Explore Trump’s latest reviewed public stock holdings, including DJT, with reported amounts, disclosure dates and original sources.</p>
        <Link href="/analysis/strategies/trump" className="button secondary">Explore Trump strategy →</Link>
      </article>
      <article className={styles.card}>
        <div className={styles.cardTop}><span className={styles.symbol} aria-hidden="true">C</span><span className={styles.badge}>Planned</span></div>
        <h2>Congress net buying</h2>
        <p>Congressional purchases minus sales, with portfolio weights proportional to positive estimated net buying.</p>
      </article>
      {partyStrategies.map(strategy => <article className={styles.card} key={strategy.slug}>
        <div className={styles.cardTop}><span className={`${styles.partySymbol} ${styles[strategy.tone]}`} aria-hidden="true">{strategy.symbol}</span><span className={styles.badge}>Planned</span></div>
        <h2>{strategy.name}</h2>
        <p>{strategy.party} congressional purchases minus sales, with weights based on positive estimated net buying.</p>
        <Link href={`/analysis/strategies/${strategy.slug}`} className="button secondary">Explore {strategy.name} →</Link>
      </article>)}
    </div>
  </main>;
}
