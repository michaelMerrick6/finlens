import { CapitolIllustration } from "@/components/capitol-illustration";
import { TrackingPromo } from "@/components/tracking-promo";
import { ActivityLine } from "@/components/activity-line";
import Link from "next/link";
import { DisclosureFeed } from "@/components/disclosure-feed";
import { Icon } from "@/components/icon";
export default function Home() {
  return (
    <main id="main" className="container">
      <ActivityLine />
      <section className="home-hero home-hero-capitol">
        <CapitolIllustration />
        <div className="hero-copy">
          <div className="eyebrow">
            <span className="accent-line" />
            PUBLIC DISCLOSURES. OPEN TO EVERYONE.
          </div>
          <h1>
            A clearer view of
            <br />
            <em>Congress in the market.</em>
          </h1>
          <p>
            See what politicians disclose. Explore the original filings.
            <br className="desktop-break" /> Keep track of the people and
            activity that matter to you.
          </p>
          <div className="hero-links">
            <Link className="text-link" href="/politicians">
              Find a politician
              <Icon name="arrow" size={17} />
            </Link>
            <span>No account needed to explore</span>
          </div>
        </div>
        <TrackingPromo />
      </section>
      <div className="disclosure-context">
        <span className="context-label">A note on timing</span>
        <p>
          These are newly <strong>disclosed</strong> trades. The transactions
          may have happened weeks earlier.
        </p>
        <Link href="/about" aria-label="Learn about disclosure timing">
          <Icon name="arrow" size={18} />
        </Link>
      </div>
      <DisclosureFeed />
      <section className="bottom-guide">
        <span className="eyebrow">FOLLOW THE SOURCE</span>
        <h2>A little context goes a long way.</h2>
        <div>
          <p>
            We organize public congressional disclosures into a readable record.
            Every transaction has context: who reported it, when it happened,
            and what the filing says.
          </p>
          <Link className="text-link" href="/about">
            Get to know the data
            <Icon name="arrow" size={17} />
          </Link>
        </div>
      </section>
    </main>
  );
}
