'use client';

import Link from 'next/link';
import Image, { type ImageLoaderProps } from 'next/image';
import type { CSSProperties, ReactNode } from 'react';
import { useEffect, useRef, useState } from 'react';

import { getTickerLogoUrl } from '@/lib/company-logos';
import { getPoliticianPhotoUrl } from '@/lib/politician-photos';

/* ─── scroll observer helper ───────────────────────────────────────────────── */

function useReveal(threshold = 0.15) {
  const ref = useRef<HTMLDivElement>(null);
  const [visible, setVisible] = useState(false);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const obs = new IntersectionObserver(
      ([e]) => { if (e.isIntersecting) { setVisible(true); obs.disconnect(); } },
      { threshold },
    );
    obs.observe(el);
    return () => obs.disconnect();
  }, [threshold]);
  return { ref, visible };
}


/* ─── page ─────────────────────────────────────────────────────────────────── */

export default function LandingPage() {

  return (
    <>
      <style>{CSS}</style>
      <div className="lp">
        {/* ── NAV ── */}
        <header className="lp-nav">
          <Link className="lp-brand" href="/landing">
            <Image
              src="/vail-logo-mark.svg"
              alt=""
              width={34}
              height={34}
              priority
              className="lp-logo"
            />
            <span className="lp-brand-text">Vail</span>
          </Link>
          <nav>
            <Link href="/politicians">Politicians</Link>
            <Link href="/clusters">Clusters</Link>
            <Link href="/alerts">Alerts</Link>
            <Link className="lp-open" href="/">Open App</Link>
          </nav>
        </header>

        {/* ── HERO ── */}
        <section className="lp-hero">
          <div className="lp-hero-backdrop" aria-hidden="true">
            <div className="lp-hero-orb lp-hero-orb-a" />
            <div className="lp-hero-orb lp-hero-orb-b" />
            <div className="lp-hero-mesh" />
          </div>

          <div className="lp-hero-content">
            <p className="lp-eyebrow">Smart money signals</p>
            <h1 className="lp-h1">
              Invest like<br />an insider.
            </h1>
            <p className="lp-sub">
              Track Congress trades, SEC insider filings, and hedge fund 13Fs — all from official government sources.
              See what the smart money is buying before the rest of the market catches on.
            </p>
            <div className="lp-hero-ctas">
              <Link className="lp-btn-primary" href="/auth?mode=signup">Start alerts</Link>
              <Link className="lp-btn-ghost" href="/">View dashboard</Link>
            </div>
          </div>

          <div className="lp-scroll-hint">
            <span>↓</span>
            Scroll to explore
          </div>
        </section>

        {/* ── HOW IT WORKS: THREE SOURCES → CLUSTER ── */}
        <section className="lp-converge">
          <div className="lp-converge-in">
            <div className="lp-converge-header">
              <span className="lp-tag">How It Works</span>
              <h2 className="lp-h2">Three signals. One conviction.</h2>
              <p className="lp-converge-sub">
                Vail monitors three independent data streams from public filings. When they converge on the same stock, that&apos;s a cluster — the strongest signal we surface.
              </p>
            </div>

            {/* Source cards */}
            <ConvergeCards />

            {/* Connector lines → cluster */}
            <div className="lp-flow">
              <svg className="lp-flow-svg" viewBox="0 0 900 80" fill="none" preserveAspectRatio="none" aria-hidden="true">
                <path d="M150 0 L150 20 Q150 40 300 50 Q450 60 450 80" stroke="var(--blue)" strokeWidth="1" strokeDasharray="4 4" opacity="0.3" />
                <path d="M450 0 L450 80" stroke="var(--green)" strokeWidth="1" strokeDasharray="4 4" opacity="0.3" />
                <path d="M750 0 L750 20 Q750 40 600 50 Q450 60 450 80" stroke="var(--green)" strokeWidth="1" strokeDasharray="4 4" opacity="0.3" />
              </svg>
            </div>

            {/* Cluster card */}
            <ClusterCard />
          </div>
        </section>

        {/* ── FOR THE SELF-DIRECTED INVESTOR ── */}
        <SelfDirectedSection />

        {/* ── CTA ── */}
        <section className="lp-cta">
          <div className="lp-cta-in">
            <h2 className="lp-cta-h2">
              They file. You know.<br />
              <span className="lp-green">Instantly.</span>
            </h2>
            <p className="lp-cta-sub">
              Set up alerts in 30 seconds. No credit card required.
            </p>
            <div className="lp-cta-btns">
              <Link className="lp-btn-primary lp-btn-lg" href="/auth?mode=signup">Start tracking</Link>
              <Link className="lp-btn-ghost lp-btn-lg" href="/">Browse the data</Link>
            </div>
            <p className="lp-fine">
              Data sourced from official government and regulatory filings. Third-party datasets are validator-only. Not investment advice.
            </p>
          </div>
        </section>

        {/* ── FOOTER ── */}
        <footer className="lp-footer">
          <div className="lp-footer-in">
            <span className="lp-footer-brand">Vail</span>
            <nav>
              <Link href="/politicians">Politicians</Link>
              <Link href="/insiders">Insiders</Link>
              <Link href="/hedge-funds">Hedge Funds</Link>
              <Link href="/pricing">Pricing</Link>
            </nav>
            <span className="lp-footer-copy">© 2026 Vail</span>
          </div>
        </footer>
      </div>
    </>
  );
}

function ConvergeCards() {
  const { ref, visible } = useReveal(0.15);
  const pelosiUrl = getPoliticianPhotoUrl('P000197', '225x275', 'Nancy Pelosi');

  return (
    <div ref={ref} className={`lp-sources-grid ${visible ? 'revealed' : ''}`}>
      {/* Congress */}
      <div className="lp-src-card" style={{ transitionDelay: '0ms' }}>
        <div className="lp-src-accent" style={{ background: 'var(--blue)' }} />
        <div className="lp-src-top">
          <span className="lp-src-tag" style={{ color: 'var(--blue)', borderColor: 'rgba(94,143,244,0.25)', background: 'rgba(94,143,244,0.08)' }}>Congress</span>
          <span className="lp-src-badge">PTR</span>
        </div>
        <h3>Congressional trades</h3>
        <p>STOCK Act disclosures from the House and Senate, parsed and surfaced within minutes of filing.</p>
        <div className="lp-src-example">
          <div className="lp-src-avatar circle" style={{ backgroundImage: pelosiUrl ? `url("${pelosiUrl}")` : undefined }} />
          <div className="lp-src-ex-text">
            <strong>Nancy Pelosi</strong>
            <span>Purchased NVDA calls · $1M–$5M</span>
          </div>
          <span className="lp-src-dir buy">Buy</span>
        </div>
      </div>

      {/* Insiders */}
      <div className="lp-src-card" style={{ transitionDelay: '100ms' }}>
        <div className="lp-src-accent" style={{ background: 'var(--green)' }} />
        <div className="lp-src-top">
          <span className="lp-src-tag" style={{ color: 'var(--green)', borderColor: 'rgba(117,230,173,0.25)', background: 'rgba(117,230,173,0.08)' }}>Insiders</span>
          <span className="lp-src-badge">SEC</span>
        </div>
        <h3>SEC Form 4 filings</h3>
        <p>CEO, CFO, and director trades — open-market buys are the strongest insider signal.</p>
        <div className="lp-src-example">
          <div className="lp-src-avatar circle" style={{ backgroundImage: 'url("/jensen-huang.png")' }} />
          <div className="lp-src-ex-text">
            <strong>Jensen Huang</strong>
            <span>CEO acquired 28,000 shares NVDA</span>
          </div>
          <span className="lp-src-dir buy">Buy</span>
        </div>
      </div>

      {/* Hedge Funds */}
      <div className="lp-src-card" style={{ transitionDelay: '200ms' }}>
        <div className="lp-src-accent" style={{ background: 'var(--green)' }} />
        <div className="lp-src-top">
          <span className="lp-src-tag" style={{ color: 'var(--green)', borderColor: 'rgba(117,230,173,0.25)', background: 'rgba(117,230,173,0.08)' }}>Hedge Funds</span>
          <span className="lp-src-badge">13F</span>
        </div>
        <h3>13F-HR position changes</h3>
        <p>Quarterly snapshots from the largest institutional investors — new positions, increases, and exits.</p>
        <div className="lp-src-example">
          <div className="lp-src-avatar circle" style={{ backgroundImage: 'url("/citadel-logo.png")' }} />
          <div className="lp-src-ex-text">
            <strong>Citadel Advisors</strong>
            <span>Added 8.2M shares NVDA (+34%)</span>
          </div>
          <span className="lp-src-dir new">New</span>
        </div>
      </div>
    </div>
  );
}

function ClusterCard() {
  const { ref, visible } = useReveal(0.3);
  const pelosiUrl = getPoliticianPhotoUrl('P000197', '225x275', 'Nancy Pelosi');

  return (
    <div ref={ref} className={`lp-cluster-wrap ${visible ? 'revealed' : ''}`}>
      <div className="lp-cluster">
        <div className="lp-cluster-glow" />
        <div className="lp-cluster-head">
          <div className="lp-cluster-pulse" />
          <span className="lp-cluster-badge-main">Cluster Detected</span>
          <span className="lp-cluster-ticker">NVDA</span>
        </div>
        <div className="lp-cluster-body">
          <div className="lp-cluster-row">
            <div className="lp-cluster-dot" style={{ background: 'var(--blue)' }} />
            <div className="lp-cluster-av circle" style={{ backgroundImage: pelosiUrl ? `url("${pelosiUrl}")` : undefined }} />
            <div className="lp-cluster-info">
              <strong>Nancy Pelosi</strong>
              <span>Purchased NVDA calls · $1M–$5M</span>
            </div>
            <span className="lp-cluster-type" style={{ color: 'var(--blue)' }}>Congress</span>
          </div>
          <div className="lp-cluster-divider" />
          <div className="lp-cluster-row">
            <div className="lp-cluster-dot" style={{ background: 'var(--green)' }} />
            <div className="lp-cluster-av circle" style={{ backgroundImage: 'url("/jensen-huang.png")' }} />
            <div className="lp-cluster-info">
              <strong>Jensen Huang</strong>
              <span>CEO acquired 28,000 shares at $142</span>
            </div>
            <span className="lp-cluster-type" style={{ color: 'var(--green)' }}>Insider</span>
          </div>
          <div className="lp-cluster-divider" />
          <div className="lp-cluster-row">
            <div className="lp-cluster-dot" style={{ background: 'var(--green)' }} />
            <div className="lp-cluster-av circle" style={{ backgroundImage: 'url("/citadel-logo.png")' }} />
            <div className="lp-cluster-info">
              <strong>Citadel Advisors</strong>
              <span>Added 8.2M shares (+34%) in Q4 13F</span>
            </div>
            <span className="lp-cluster-type" style={{ color: 'var(--green)' }}>Fund</span>
          </div>
        </div>
        <div className="lp-cluster-foot">
          3 independent sources converged on NVDA within 10 days — high conviction cluster.
        </div>
      </div>
    </div>
  );
}

/* ─── Self-Directed Investor Section ──────────────────────────────────────── */

const LANDING_STATS = [
  { value: '1K', label: 'Congress 7d' },
  { value: '1K', label: 'Insiders 7d' },
  { value: '1K', label: '13Fs 7d' },
  { value: '619', label: 'Politicians' },
];

const CONGRESS_SPOTLIGHT = [
  { name: 'Nancy Pelosi', memberId: 'P000197', detail: 'House' },
  { name: 'Ro Khanna', memberId: 'K000389', detail: 'CA' },
  { name: 'Tommy Tuberville', memberId: 'T000278', detail: 'Senate' },
];

const STOCK_SPOTLIGHT = [
  { ticker: 'NVDA', name: 'Nvidia' },
  { ticker: 'AMZN', name: 'Amazon' },
  { ticker: 'MSFT', name: 'Microsoft' },
];

const FUND_SPOTLIGHT = [
  {
    name: 'Leopold Aschenbrenner',
    detail: 'Situational Awareness',
    imageUrl: '/leopold-aschenbrenner.png',
  },
  {
    name: 'Warren Buffett',
    detail: 'Berkshire Hathaway',
    imageUrl: 'https://upload.wikimedia.org/wikipedia/commons/d/df/Warren_Buffett_in_2010_%28cropped%29.jpg',
  },
  {
    name: 'Bill Ackman',
    detail: 'Pershing Square',
    imageUrl: 'https://upload.wikimedia.org/wikipedia/commons/d/d8/Bill_Ackman_%2826410186110%29_%28cropped%29.jpg',
  },
];

const RECENT_POLITICIAN_BUYS = [
  {
    name: 'Ro Khanna',
    memberId: 'K000389',
    ticker: 'KMB',
    amount: '$1,001 - $15,000',
    date: 'Jun 9',
  },
  {
    name: 'Josh Gottheimer',
    memberId: 'G000583',
    ticker: 'AMD',
    amount: '$15,001 - $50,000',
    date: 'Jun 8',
  },
  {
    name: 'Michael McCaul',
    memberId: 'M001157',
    ticker: 'GOOGL',
    amount: '$1,001 - $15,000',
    date: 'Jun 12',
  },
  {
    name: 'Gilbert Cisneros',
    memberId: 'C001123',
    ticker: 'MSFT',
    amount: '$1,001 - $15,000',
    date: 'Jun 8',
  },
  {
    name: 'Tim Moore',
    memberId: 'M001235',
    ticker: 'T',
    amount: '$15,001 - $50,000',
    date: 'Jun 12',
  },
];

const passthroughImageLoader = ({ src }: ImageLoaderProps) => src;

function SelfDirectedSection() {
  const { ref, visible } = useReveal(0.15);

  return (
    <section className="lp-sdi">
      <div className="lp-sdi-in">
        <div ref={ref} className={`lp-signal-card ${visible ? 'revealed' : ''}`}>
          <div className="lp-signal-card-copy">
            <span>Your edge</span>
          </div>
          <h2 className="lp-signal-h2">For the self-directed investor.</h2>
          <p className="lp-signal-sub">
            Build a focused feed around the people, companies, and funds you care about.
            Catch meaningful moves before they become obvious.
          </p>

          <div className="lp-signal-stats">
            {LANDING_STATS.map((item) => (
              <div key={item.label} className="lp-signal-stat">
                <strong>{item.value}</strong>
                <span>{item.label}</span>
              </div>
            ))}
          </div>

          <div className="lp-signal-rows">
            <LandingContextRow
              title="Congress Trades"
              description="Track Pelosi, Khanna, and key House or Senate disclosures."
              label="Congress"
              accent="#5e8ff4"
            >
              <CongressFaces />
            </LandingContextRow>

            <LandingContextRow
              title="Insider Filings"
              description="Follow C-suite Form 4 activity around the stocks you care about."
              label="Insiders"
              accent="#d8b04c"
            >
              <StockLogos />
            </LandingContextRow>

            <LandingContextRow
              title="Hedge Fund 13Fs"
              description="Watch Leopold, Buffett, Ackman, and quarterly 13F changes."
              label="Funds"
              accent="#75e6ad"
            >
              <FundFaces />
            </LandingContextRow>

            <LandingContextRow
              title="Cross-Source Clusters"
              description="Spot buy pressure when Congress, insiders, and funds align."
              label="Clusters"
              accent="#8b5cf6"
            >
              <ClusterChips />
            </LandingContextRow>
          </div>

          <div className="lp-sdi-foot">
            Fast alerts across Congress, SEC Form 4, 13F filings, and quality clusters.
          </div>
        </div>

        <RecentPoliticianBuys />
      </div>
    </section>
  );
}

function LandingContextRow({
  title,
  description,
  label,
  accent,
  children,
}: {
  title: string;
  description: string;
  label: string;
  accent: string;
  children: ReactNode;
}) {
  return (
    <div className="lp-context-row" style={{ '--row-accent': accent } as CSSProperties}>
      <div className="lp-context-copy">
        <div className="lp-context-label">{label}</div>
        <div className="lp-context-title">{title}</div>
        <div className="lp-context-desc">{description}</div>
      </div>
      <div className="lp-context-visual">{children}</div>
    </div>
  );
}

function CongressFace({ person, index }: { person: (typeof CONGRESS_SPOTLIGHT)[number]; index: number }) {
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  const resolvedPhotoUrl = getPoliticianPhotoUrl(person.memberId, '225x275', person.name);
  const photoUrl = resolvedPhotoUrl && failedUrl !== resolvedPhotoUrl ? resolvedPhotoUrl : null;

  return (
    <div className="lp-mini-face" style={{ zIndex: CONGRESS_SPOTLIGHT.length - index }} title={`${person.name} · ${person.detail}`}>
      {photoUrl ? (
        <Image
          loader={passthroughImageLoader}
          unoptimized
          src={photoUrl}
          alt={person.name}
          width={38}
          height={38}
          className="lp-mini-face-img"
          onError={() => setFailedUrl(photoUrl)}
        />
      ) : (
        <span>{person.name.slice(0, 2)}</span>
      )}
    </div>
  );
}

function CongressFaces() {
  return (
    <div className="lp-face-stack">
      {CONGRESS_SPOTLIGHT.map((person, index) => (
        <CongressFace key={person.memberId} person={person} index={index} />
      ))}
    </div>
  );
}

function StockLogo({ stock }: { stock: (typeof STOCK_SPOTLIGHT)[number] }) {
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  const resolvedLogoUrl = getTickerLogoUrl(stock.ticker, 96);
  const logoUrl = resolvedLogoUrl && failedUrl !== resolvedLogoUrl ? resolvedLogoUrl : null;

  return (
    <div className="lp-logo-tile" title={`${stock.name} · ${stock.ticker}`}>
      {logoUrl ? (
        <Image
          loader={passthroughImageLoader}
          unoptimized
          src={logoUrl}
          alt={`${stock.name} logo`}
          width={24}
          height={24}
          className="lp-logo-tile-img"
          onError={() => setFailedUrl(logoUrl)}
        />
      ) : (
        <span>{stock.ticker}</span>
      )}
    </div>
  );
}

function StockLogos() {
  return (
    <div className="lp-logo-stack">
      {STOCK_SPOTLIGHT.map((stock) => (
        <StockLogo key={stock.ticker} stock={stock} />
      ))}
    </div>
  );
}

function FundFace({ fund, index }: { fund: (typeof FUND_SPOTLIGHT)[number]; index: number }) {
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  const initials = fund.name
    .split(/\s+/)
    .map((part) => part.charAt(0))
    .join('')
    .slice(0, 2);

  return (
    <div className="lp-mini-face" style={{ zIndex: FUND_SPOTLIGHT.length - index }} title={`${fund.name} · ${fund.detail}`}>
      {failedUrl !== fund.imageUrl ? (
        <Image
          loader={passthroughImageLoader}
          unoptimized
          src={fund.imageUrl}
          alt={fund.name}
          width={38}
          height={38}
          className="lp-mini-face-img"
          onError={() => setFailedUrl(fund.imageUrl)}
        />
      ) : (
        <span>{initials || 'HF'}</span>
      )}
    </div>
  );
}

function FundFaces() {
  return (
    <div className="lp-face-stack">
      {FUND_SPOTLIGHT.map((fund, index) => (
        <FundFace key={fund.name} fund={fund} index={index} />
      ))}
    </div>
  );
}

function ClusterChips() {
  return (
    <div className="lp-chip-stack">
      {['CON', 'SEC', '13F'].map((label) => (
        <span key={label}>{label}</span>
      ))}
    </div>
  );
}

function LandingTickerLogo({ ticker }: { ticker: string }) {
  const [failed, setFailed] = useState(false);
  const logoUrl = getTickerLogoUrl(ticker, 32);

  if (logoUrl && !failed) {
    return (
      <span className="lp-buy-logo">
        <Image
          loader={passthroughImageLoader}
          unoptimized
          src={logoUrl}
          alt={ticker}
          width={32}
          height={32}
          className="lp-buy-logo-img"
          onError={() => setFailed(true)}
        />
      </span>
    );
  }

  return <span className="lp-buy-logo lp-buy-logo-fallback">{ticker.slice(0, 2)}</span>;
}

function LandingPoliticianPhoto({ memberId, name }: { memberId: string; name: string }) {
  const [failed, setFailed] = useState(false);
  const photoUrl = getPoliticianPhotoUrl(memberId, '225x275', name);

  if (photoUrl && !failed) {
    return (
      <span className="lp-buy-person">
        <Image
          loader={passthroughImageLoader}
          unoptimized
          src={photoUrl}
          alt={name}
          width={36}
          height={36}
          className="lp-buy-person-img"
          onError={() => setFailed(true)}
        />
      </span>
    );
  }

  return <span className="lp-buy-person lp-buy-person-fallback">{name.split(' ').map((part) => part[0]).join('').slice(0, 2)}</span>;
}

function RecentPoliticianBuys() {
  return (
    <section className="lp-recent-buys">
      <div className="lp-recent-head">
        <div>
          <div className="lp-recent-title">Recent politician buys</div>
          <div className="lp-recent-sub">Public filings, simplified.</div>
        </div>
        <div className="lp-recent-pill">Live feed preview</div>
      </div>
      <div className="lp-buy-strip">
        <div className="lp-buy-track">
          {[...RECENT_POLITICIAN_BUYS, ...RECENT_POLITICIAN_BUYS].map((trade, index) => (
            <div key={`${trade.name}-${trade.ticker}-${index}`} className="lp-buy-card">
              <LandingPoliticianPhoto memberId={trade.memberId} name={trade.name} />
              <div className="lp-buy-person-copy">
                <strong>{trade.name}</strong>
                <span>Buy · {trade.date}</span>
              </div>
              <span className="lp-buy-divider" aria-hidden="true" />
              <div className="lp-buy-ticker-copy">
                <strong>{trade.ticker}</strong>
                <span>{trade.amount}</span>
              </div>
              <LandingTickerLogo ticker={trade.ticker} />
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

/* ═══════════════════════════════════════════════════════════════════════════ */
/* CSS                                                                       */
/* ═══════════════════════════════════════════════════════════════════════════ */

const CSS = `
@import url('https://fonts.googleapis.com/css2?family=Instrument+Serif:ital@0;1&display=swap');

:root {
  --bg: #030403;
  --panel: #0a0c0b;
  --line: rgba(255,255,255,0.07);
  --text: #f0f1ee;
  --muted: rgba(240,241,238,0.5);
  --faint: rgba(240,241,238,0.28);
  --green: #75e6ad;
  --green-dim: rgba(117,230,173,0.1);
  --blue: #5e8ff4;
  --red: #ef6f64;
}

/* ── base ── */
.lp {
  background: var(--bg);
  color: var(--text);
  font-family: "SF Pro Display", "Inter", ui-sans-serif, system-ui, -apple-system, sans-serif;
  overflow-x: hidden;
  -webkit-font-smoothing: antialiased;
}

/* subtle grid overlay */
.lp::before {
  content: "";
  position: fixed; inset: 0;
  pointer-events: none;
  background-image:
    linear-gradient(rgba(255,255,255,0.014) 1px, transparent 1px),
    linear-gradient(90deg, rgba(255,255,255,0.014) 1px, transparent 1px);
  background-size: 80px 80px;
  mask-image: radial-gradient(circle at 50% 30%, black 0%, transparent 60%);
}

/* ── nav ── */
.lp-nav {
  position: fixed; top: 0; left: 0; right: 0; z-index: 100;
  display: flex; align-items: center; justify-content: space-between;
  max-width: 1100px; margin: 0 auto;
  padding: 0 28px; height: 64px;
}
.lp-nav::after {
  content: ""; position: absolute; inset: 0; z-index: -1;
  background: rgba(3,4,3,0.8);
  backdrop-filter: blur(14px); -webkit-backdrop-filter: blur(14px);
  border-bottom: 1px solid var(--line);
  opacity: 1;
}

.lp-brand {
  display: flex; align-items: center; gap: 10px;
  text-decoration: none; color: var(--text);
}
.lp-logo { height: 32px; width: 32px; object-fit: contain; }
.lp-brand-text { font-size: 18px; font-weight: 700; letter-spacing: -0.03em; }

.lp-nav nav {
  display: flex; align-items: center; gap: 22px;
}
.lp-nav nav a {
  color: var(--muted); text-decoration: none;
  font-size: 13px; font-weight: 600;
  transition: color 0.15s;
}
.lp-nav nav a:hover { color: var(--text); }

.lp-open {
  padding: 8px 16px !important;
  border: 1px solid rgba(117,230,173,0.2) !important;
  border-radius: 10px;
  background: var(--green-dim) !important;
  color: #dfffee !important;
}
.lp-open:hover {
  border-color: rgba(117,230,173,0.35) !important;
  background: rgba(117,230,173,0.15) !important;
}

/* ── hero ── */
.lp-hero {
  position: relative;
  min-height: 100vh;
  display: flex; flex-direction: column;
  align-items: center; justify-content: center;
  padding: 96px 24px 48px;
  overflow: hidden;
}

.lp-hero-backdrop {
  position: absolute; inset: 0;
  pointer-events: none;
  z-index: 0;
}
.lp-hero-backdrop::after {
  content: "";
  position: absolute; inset: 0;
  background:
    linear-gradient(180deg, rgba(3,4,3,0.2) 0%, rgba(3,4,3,0.72) 65%, rgba(3,4,3,0.95) 100%);
}
.lp-hero-orb {
  position: absolute;
  border-radius: 999px;
  filter: blur(20px);
  opacity: 0.95;
  animation: lp-drift 18s ease-in-out infinite;
}
.lp-hero-orb-a {
  top: 12%;
  left: 50%;
  width: min(58vw, 720px);
  height: min(58vw, 720px);
  transform: translateX(-62%);
  background: radial-gradient(circle, rgba(117,230,173,0.2) 0%, rgba(117,230,173,0.06) 48%, transparent 72%);
}
.lp-hero-orb-b {
  right: 10%;
  bottom: 14%;
  width: min(34vw, 420px);
  height: min(34vw, 420px);
  background: radial-gradient(circle, rgba(94,143,244,0.18) 0%, rgba(94,143,244,0.04) 52%, transparent 74%);
  animation-delay: -6s;
}
.lp-hero-mesh {
  position: absolute;
  inset: 0;
  background:
    radial-gradient(circle at 50% 28%, rgba(255,255,255,0.06), transparent 26%),
    linear-gradient(180deg, rgba(255,255,255,0.02), transparent 30%);
  mix-blend-mode: screen;
}
@keyframes lp-drift {
  0%, 100% { transform: translateX(-62%) translateY(0); }
  50% { transform: translateX(-58%) translateY(18px); }
}

.lp-hero-content {
  position: relative; z-index: 1;
  text-align: center;
  max-width: 700px;
  display: flex; flex-direction: column; align-items: center;
  gap: 0;
}

.lp-eyebrow {
  margin: 0 0 20px;
  color: var(--green);
  font-size: 11px; font-weight: 700;
  letter-spacing: 0.2em; text-transform: uppercase;
}

.lp-h1 {
  margin: 0;
  font-family: 'Instrument Serif', Georgia, 'Times New Roman', serif;
  font-size: clamp(56px, 9vw, 110px);
  font-weight: 400;
  font-style: italic;
  letter-spacing: -0.04em;
  line-height: 0.92;
  color: var(--text);
}

.lp-sub {
  max-width: 480px;
  margin: 28px 0 0;
  color: var(--muted);
  font-size: 16px; line-height: 1.7;
}

.lp-hero-ctas {
  display: flex; align-items: center; gap: 10px;
  margin-top: 32px;
}

.lp-hero-trust {
  display: flex;
  flex-wrap: wrap;
  justify-content: center;
  gap: 10px;
  margin-top: 22px;
}

.lp-trust-chip {
  display: inline-flex;
  align-items: center;
  padding: 8px 12px;
  border-radius: 999px;
  border: 1px solid rgba(255,255,255,0.08);
  background: rgba(255,255,255,0.04);
  color: var(--muted);
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}

.lp-scroll-hint {
  position: absolute; bottom: 32px;
  display: flex; flex-direction: column; align-items: center; gap: 6px;
  color: var(--faint); font-size: 11px; font-weight: 600;
  letter-spacing: 0.1em; text-transform: uppercase;
  animation: lp-float 2.4s ease-in-out infinite;
}
.lp-scroll-hint span {
  font-size: 18px; animation: lp-float 2.4s ease-in-out infinite;
}
@keyframes lp-float {
  0%, 100% { transform: translateY(0); }
  50% { transform: translateY(6px); }
}

/* ── buttons ── */
.lp-btn-primary, .lp-btn-ghost {
  display: inline-flex; align-items: center; justify-content: center;
  min-width: 140px; min-height: 46px; padding: 0 22px;
  border-radius: 11px; text-decoration: none;
  font-size: 14px; font-weight: 700;
  transition: transform 0.15s, background 0.15s, border-color 0.15s;
}
.lp-btn-primary {
  border: 1px solid var(--green);
  background: var(--green); color: #03100a;
}
.lp-btn-primary:hover { transform: translateY(-1px); }

.lp-btn-ghost {
  border: 1px solid rgba(255,255,255,0.1);
  background: rgba(255,255,255,0.03); color: var(--muted);
}
.lp-btn-ghost:hover { background: rgba(255,255,255,0.06); color: var(--text); transform: translateY(-1px); }

.lp-btn-lg { min-height: 52px; padding: 0 28px; font-size: 15px; }

/* ── convergence section ── */
.lp-converge {
  position: relative;
  padding: 120px 24px 80px;
  border-top: 1px solid var(--line);
}
.lp-converge::before {
  content: "";
  position: absolute; top: 0; left: 50%; transform: translateX(-50%);
  width: 800px; height: 500px;
  background: radial-gradient(ellipse, rgba(117,230,173,0.04), transparent 65%);
  pointer-events: none;
}
.lp-converge-in {
  max-width: 1060px; margin: 0 auto;
}
.lp-converge-header {
  text-align: center; margin-bottom: 56px;
}
.lp-converge-sub {
  margin: 14px auto 0; max-width: 520px;
  color: var(--muted); font-size: 16px; line-height: 1.65;
}

/* source cards grid */
.lp-sources-grid {
  display: grid; grid-template-columns: repeat(3, 1fr); gap: 18px;
}
.lp-sources-grid .lp-src-card {
  opacity: 0; transform: translateY(32px);
  transition: opacity 0.6s cubic-bezier(0.16,1,0.3,1), transform 0.6s cubic-bezier(0.16,1,0.3,1);
}
.lp-sources-grid.revealed .lp-src-card {
  opacity: 1; transform: translateY(0);
}

.lp-src-card {
  position: relative; overflow: hidden;
  padding: 24px 22px 20px;
  border-radius: 16px;
  border: 1px solid var(--line);
  background: rgba(255,255,255,0.018);
  transition: border-color 0.25s;
}
.lp-src-card:hover { border-color: rgba(255,255,255,0.14); }

.lp-src-accent {
  position: absolute; top: 0; left: 0; right: 0; height: 2px; opacity: 0.5;
  transition: opacity 0.25s;
}
.lp-src-card:hover .lp-src-accent { opacity: 1; }

.lp-src-top {
  display: flex; align-items: center; justify-content: space-between;
  margin-bottom: 18px;
}
.lp-src-tag {
  font-size: 10px; font-weight: 700; letter-spacing: 0.1em; text-transform: uppercase;
  padding: 4px 10px; border-radius: 999px; border: 1px solid;
}
.lp-src-badge {
  font-size: 9px; font-weight: 800; letter-spacing: 0.12em;
  color: var(--faint); background: rgba(255,255,255,0.04);
  border: 1px solid rgba(255,255,255,0.06);
  padding: 3px 8px; border-radius: 6px;
}

.lp-src-card h3 {
  margin: 0 0 8px;
  font-size: 17px; font-weight: 700; letter-spacing: -0.02em; color: var(--text);
}
.lp-src-card p {
  margin: 0 0 18px;
  font-size: 13px; line-height: 1.6; color: var(--muted);
}

.lp-src-example {
  display: flex; align-items: center; gap: 10px;
  padding: 12px;
  border-radius: 12px;
  background: rgba(255,255,255,0.025);
  border: 1px solid rgba(255,255,255,0.04);
}
.lp-src-avatar {
  width: 32px; height: 32px; flex-shrink: 0;
  background-size: cover; background-position: center;
  border: 1px solid rgba(255,255,255,0.1);
}
.lp-src-avatar.circle { border-radius: 50%; }
.lp-src-avatar.square { border-radius: 8px; background-size: 20px 20px; background-repeat: no-repeat; background-color: #111; }
.lp-src-initials {
  display: flex; align-items: center; justify-content: center;
  font-size: 10px; font-weight: 700; color: #fff;
  background: linear-gradient(135deg, #10b981, #059669) !important;
  border: none !important;
}

.lp-src-ex-text {
  flex: 1; min-width: 0; display: flex; flex-direction: column; gap: 1px;
}
.lp-src-ex-text strong { font-size: 12px; font-weight: 600; color: var(--text); }
.lp-src-ex-text span { font-size: 11px; color: var(--faint); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }

.lp-src-dir {
  font-size: 9px; font-weight: 800; letter-spacing: 0.08em; text-transform: uppercase;
  padding: 3px 8px; border-radius: 6px; flex-shrink: 0;
}
.lp-src-dir.buy { color: #34d399; background: rgba(52,211,153,0.1); }
.lp-src-dir.new { color: #60a5fa; background: rgba(96,165,250,0.1); }

/* connector lines */
.lp-flow {
  display: flex; justify-content: center;
  padding: 8px 0;
}
.lp-flow-svg {
  width: 100%; max-width: 900px; height: 60px;
}

/* cluster card */
.lp-cluster-wrap {
  max-width: 580px; margin: 0 auto;
  opacity: 0; transform: translateY(24px) scale(0.97);
  transition: opacity 0.7s cubic-bezier(0.16,1,0.3,1), transform 0.7s cubic-bezier(0.16,1,0.3,1);
}
.lp-cluster-wrap.revealed {
  opacity: 1; transform: translateY(0) scale(1);
}

.lp-cluster {
  position: relative; overflow: hidden;
  border-radius: 18px;
  border: 1px solid rgba(245,158,11,0.15);
  background: rgba(255,255,255,0.02);
  box-shadow: 0 0 60px rgba(245,158,11,0.04), 0 20px 60px rgba(0,0,0,0.3);
}
.lp-cluster-glow {
  position: absolute; top: -40px; left: 50%; transform: translateX(-50%);
  width: 300px; height: 120px;
  background: radial-gradient(ellipse, rgba(245,158,11,0.08), transparent 70%);
  pointer-events: none;
}

.lp-cluster-head {
  display: flex; align-items: center; gap: 10px;
  padding: 14px 20px;
  border-bottom: 1px solid rgba(255,255,255,0.05);
}
.lp-cluster-pulse {
  width: 8px; height: 8px; border-radius: 50%;
  background: #f59e0b;
  box-shadow: 0 0 0 0 rgba(245,158,11,0.5);
  animation: lp-cpulse 2s ease-out infinite;
}
@keyframes lp-cpulse {
  0% { box-shadow: 0 0 0 0 rgba(245,158,11,0.5); }
  70% { box-shadow: 0 0 0 8px rgba(245,158,11,0); }
  100% { box-shadow: 0 0 0 0 rgba(245,158,11,0); }
}
.lp-cluster-badge-main {
  font-size: 10px; font-weight: 800; letter-spacing: 0.12em; text-transform: uppercase;
  color: #f59e0b; background: rgba(245,158,11,0.1);
  border: 1px solid rgba(245,158,11,0.2);
  padding: 4px 10px; border-radius: 999px;
}
.lp-cluster-ticker {
  margin-left: auto;
  font-size: 15px; font-weight: 800; color: #fff; letter-spacing: 0.04em;
}

.lp-cluster-body { padding: 6px 0; }

.lp-cluster-row {
  display: flex; align-items: center; gap: 10px;
  padding: 12px 20px;
}
.lp-cluster-dot {
  width: 6px; height: 6px; border-radius: 50%; flex-shrink: 0;
}
.lp-cluster-av {
  width: 36px; height: 36px; flex-shrink: 0;
  background-size: cover; background-position: center;
  border: 1px solid rgba(255,255,255,0.1);
}
.lp-cluster-av.circle { border-radius: 50%; }
.lp-cluster-initials {
  display: flex; align-items: center; justify-content: center;
  font-size: 11px; font-weight: 700; color: #fff;
  background: linear-gradient(135deg, #2563eb, #0f766e) !important;
  border: none !important;
}

.lp-cluster-info {
  flex: 1; min-width: 0; display: flex; flex-direction: column; gap: 2px;
}
.lp-cluster-info strong { font-size: 13px; font-weight: 600; color: var(--text); }
.lp-cluster-info span { font-size: 12px; color: var(--faint); }

.lp-cluster-type {
  font-size: 10px; font-weight: 700; letter-spacing: 0.06em;
  flex-shrink: 0;
}

.lp-cluster-divider {
  height: 1px; margin: 0 20px;
  background: linear-gradient(90deg, transparent, rgba(245,158,11,0.1), transparent);
}

.lp-cluster-foot {
  padding: 14px 20px;
  border-top: 1px solid rgba(255,255,255,0.04);
  font-size: 12px; color: var(--faint); line-height: 1.5;
}

/* ── shared ── */
.lp-tag {
  display: inline-block;
  margin-bottom: 12px;
  color: var(--green); font-size: 10px; font-weight: 800;
  letter-spacing: 0.2em; text-transform: uppercase;
}
.lp-h2 {
  margin: 0;
  font-size: clamp(24px, 3.5vw, 38px); font-weight: 700;
  letter-spacing: -0.03em; color: var(--text);
}

/* ── self-directed investor ── */
.lp-sdi {
  position: relative;
  padding: 110px 24px 86px;
  border-top: 1px solid var(--line);
}
.lp-sdi::before {
  content: none;
}
.lp-sdi-in {
  position: relative;
  max-width: 820px; margin: 0 auto;
}

.lp-signal-card {
  position: relative;
  overflow: visible;
  border-radius: 0;
  border: 0;
  background: transparent;
  padding: 0;
  box-shadow: none;
  opacity: 0;
  transform: translateY(24px);
  transition: opacity 0.55s cubic-bezier(0.16,1,0.3,1), transform 0.55s cubic-bezier(0.16,1,0.3,1);
}
.lp-signal-card.revealed {
  opacity: 1;
  transform: translateY(0);
}
.lp-signal-card::before {
  content: none;
}
.lp-signal-card-copy,
.lp-signal-h2,
.lp-signal-sub,
.lp-signal-stats,
.lp-signal-rows,
.lp-sdi-foot {
  position: relative;
}
.lp-signal-card-copy {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  width: 100%;
  color: rgba(240,241,238,0.52);
  font-size: 10px;
  font-weight: 800;
  letter-spacing: 0.2em;
  text-transform: uppercase;
}
.lp-live-dot {
  width: 7px;
  height: 7px;
  border-radius: 999px;
  background: var(--green);
  box-shadow: 0 0 16px rgba(117,230,173,0.8);
}
.lp-signal-h2 {
  max-width: 720px;
  margin: 22px auto 0;
  text-align: center;
  font-size: clamp(32px, 4vw, 46px);
  line-height: 1.03;
  letter-spacing: -0.045em;
  color: var(--text);
}
.lp-signal-sub {
  max-width: 620px;
  margin: 20px auto 0;
  text-align: center;
  color: rgba(240,241,238,0.54);
  font-size: 15px;
  line-height: 1.65;
}

.lp-signal-stats {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 8px;
  margin-top: 34px;
  padding: 8px;
  border-radius: 18px;
  border: 1px solid rgba(255,255,255,0.055);
  background: rgba(255,255,255,0.014);
}
.lp-signal-stat {
  display: grid;
  gap: 2px;
  min-width: 0;
  border-radius: 14px;
  border: 1px solid rgba(255,255,255,0.055);
  background: rgba(255,255,255,0.024);
  padding: 11px 12px;
}
.lp-signal-stat strong {
  color: var(--text);
  font-size: 15px;
  line-height: 1;
}
.lp-signal-stat span {
  overflow: hidden;
  text-overflow: ellipsis;
  color: rgba(240,241,238,0.38);
  font-size: 10px;
  font-weight: 800;
  letter-spacing: 0.12em;
  text-transform: uppercase;
  white-space: nowrap;
}

.lp-signal-rows {
  display: grid;
  gap: 10px;
  margin-top: 30px;
}
.lp-context-row {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 148px;
  min-height: 92px;
  align-items: center;
  gap: 22px;
  border: 1px solid rgba(255,255,255,0.06);
  border-left-color: color-mix(in srgb, var(--row-accent), transparent 24%);
  border-radius: 20px;
  background: rgba(255,255,255,0.014);
  padding: 17px 20px;
  box-shadow: inset 2px 0 0 color-mix(in srgb, var(--row-accent), transparent 62%);
}
.lp-context-label {
  color: var(--row-accent);
  font-size: 10px;
  font-weight: 800;
  letter-spacing: 0.2em;
  text-transform: uppercase;
}
.lp-context-title {
  margin-top: 7px;
  color: var(--text);
  font-size: 15px;
  font-weight: 750;
  letter-spacing: -0.015em;
}
.lp-context-desc {
  margin-top: 5px;
  color: rgba(240,241,238,0.42);
  font-size: 13px;
  line-height: 1.45;
}
.lp-context-visual {
  display: flex;
  justify-content: flex-end;
  align-items: center;
}
.lp-face-stack {
  display: flex;
  justify-content: flex-end;
  margin-right: 8px;
}
.lp-mini-face {
  width: 38px;
  height: 38px;
  margin-left: -9px;
  overflow: hidden;
  border-radius: 999px;
  border: 1px solid rgba(255,255,255,0.12);
  background: #111113;
  box-shadow: 0 0 0 2px #080808;
}
.lp-mini-face:first-child { margin-left: 0; }
.lp-mini-face-img {
  width: 100%;
  height: 100%;
  object-fit: cover;
}
.lp-mini-face span {
  display: flex;
  width: 100%;
  height: 100%;
  align-items: center;
  justify-content: center;
  color: var(--text);
  font-size: 10px;
  font-weight: 800;
}
.lp-logo-stack,
.lp-chip-stack {
  display: flex;
  justify-content: flex-end;
  gap: 9px;
}
.lp-logo-tile {
  display: flex;
  width: 40px;
  height: 40px;
  align-items: center;
  justify-content: center;
  border-radius: 13px;
  border: 1px solid rgba(255,255,255,0.075);
  background: rgba(255,255,255,0.035);
}
.lp-logo-tile-img {
  width: 24px;
  height: 24px;
  object-fit: contain;
  border-radius: 6px;
}
.lp-logo-tile span,
.lp-chip-stack span {
  color: rgba(240,241,238,0.78);
  font-size: 10px;
  font-weight: 850;
  letter-spacing: 0.1em;
}
.lp-chip-stack span {
  display: inline-flex;
  min-width: 44px;
  height: 34px;
  align-items: center;
  justify-content: center;
  border-radius: 999px;
  border: 1px solid rgba(255,255,255,0.075);
  background: rgba(255,255,255,0.035);
}

.lp-sdi-foot {
  margin-top: 24px;
  padding: 13px 16px;
  border-radius: 16px;
  border: 1px solid rgba(255,255,255,0.055);
  background: rgba(255,255,255,0.018);
  color: rgba(240,241,238,0.5);
  font-size: 13px;
  line-height: 1.5;
}

.lp-recent-buys {
  margin-top: 28px;
  overflow: hidden;
  border-radius: 20px;
  border: 1px solid rgba(255,255,255,0.065);
  background: rgba(255,255,255,0.018);
}
.lp-recent-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 14px;
  border-bottom: 1px solid rgba(255,255,255,0.055);
  padding: 14px 18px;
}
.lp-recent-title {
  color: var(--text);
  font-size: 14px;
  font-weight: 700;
}
.lp-recent-sub {
  margin-top: 2px;
  color: rgba(240,241,238,0.35);
  font-size: 12px;
}
.lp-recent-pill {
  flex-shrink: 0;
  border-radius: 999px;
  border: 1px solid rgba(255,255,255,0.08);
  padding: 6px 12px;
  color: rgba(240,241,238,0.36);
  font-size: 12px;
}
@keyframes lp-buy-marquee {
  from { transform: translateX(0); }
  to { transform: translateX(-50%); }
}
.lp-buy-strip {
  overflow: hidden;
  mask-image: linear-gradient(90deg, transparent, black 10%, black 90%, transparent);
}
.lp-buy-track {
  display: flex;
  width: max-content;
  gap: 8px;
  padding: 10px;
  animation: lp-buy-marquee 34s linear infinite;
}
.lp-buy-strip:hover .lp-buy-track {
  animation-play-state: paused;
}
.lp-buy-card {
  display: flex;
  min-width: 350px;
  height: 58px;
  align-items: center;
  gap: 10px;
  border-radius: 16px;
  border: 1px solid rgba(255,255,255,0.065);
  background: rgba(255,255,255,0.025);
  padding: 9px 12px;
}
.lp-buy-person,
.lp-buy-logo {
  display: flex;
  flex-shrink: 0;
  align-items: center;
  justify-content: center;
  overflow: hidden;
  border-radius: 999px;
  border: 1px solid rgba(255,255,255,0.1);
}
.lp-buy-person {
  width: 36px;
  height: 36px;
  background: rgba(255,255,255,0.04);
}
.lp-buy-logo {
  width: 32px;
  height: 32px;
  background: #fff;
}
.lp-buy-person-img,
.lp-buy-logo-img {
  width: 100%;
  height: 100%;
}
.lp-buy-person-img { object-fit: cover; }
.lp-buy-logo-img {
  object-fit: contain;
  padding: 2px;
}
.lp-buy-person-fallback,
.lp-buy-logo-fallback {
  color: var(--text);
  font-size: 11px;
  font-weight: 800;
  background: rgba(255,255,255,0.04);
}
.lp-buy-person-copy,
.lp-buy-ticker-copy {
  min-width: 0;
  display: grid;
  gap: 2px;
}
.lp-buy-person-copy {
  flex: 1;
}
.lp-buy-person-copy strong,
.lp-buy-ticker-copy strong {
  overflow: hidden;
  text-overflow: ellipsis;
  color: var(--text);
  font-size: 13px;
  font-weight: 700;
  white-space: nowrap;
}
.lp-buy-person-copy span,
.lp-buy-ticker-copy span {
  overflow: hidden;
  text-overflow: ellipsis;
  color: rgba(240,241,238,0.36);
  font-size: 12px;
  white-space: nowrap;
}
.lp-buy-divider {
  width: 1px;
  height: 30px;
  flex: 0 0 auto;
  border-radius: 999px;
  background: linear-gradient(180deg, transparent, rgba(255,255,255,0.2), transparent);
}
.lp-buy-ticker-copy {
  min-width: 92px;
  text-align: right;
}

/* ── CTA ── */
.lp-cta {
  position: relative; padding: 100px 24px;
  text-align: center; border-top: 1px solid var(--line);
  overflow: hidden;
}
.lp-cta::before {
  content: "";
  position: absolute; top: 0; left: 50%; transform: translateX(-50%);
  width: 700px; height: 500px;
  background: radial-gradient(ellipse, rgba(117,230,173,0.06), transparent 65%);
  pointer-events: none;
}
.lp-cta-in {
  position: relative; z-index: 1;
  max-width: 580px; margin: 0 auto;
  display: flex; flex-direction: column; align-items: center; gap: 16px;
}
.lp-cta-h2 {
  margin: 0;
  font-size: clamp(28px, 5vw, 48px); font-weight: 800;
  letter-spacing: -0.035em; line-height: 1.1;
}
.lp-green { color: var(--green); }
.lp-cta-sub {
  margin: 0; color: var(--muted); font-size: 16px;
}
.lp-cta-btns {
  display: flex; gap: 10px; margin-top: 8px;
}
.lp-fine {
  margin: 8px 0 0; color: var(--faint); font-size: 11px;
}

/* ── footer ── */
.lp-footer {
  border-top: 1px solid var(--line); padding: 24px;
}
.lp-footer-in {
  max-width: 1060px; margin: 0 auto;
  display: flex; align-items: center; justify-content: space-between;
  flex-wrap: wrap; gap: 16px;
}
.lp-footer-brand { font-size: 15px; font-weight: 700; color: var(--faint); }
.lp-footer-in nav { display: flex; gap: 20px; }
.lp-footer-in nav a {
  font-size: 13px; color: var(--faint); text-decoration: none; transition: color 0.15s;
}
.lp-footer-in nav a:hover { color: var(--muted); }
.lp-footer-copy { font-size: 12px; color: rgba(255,255,255,0.12); }

/* ── responsive ── */
@media (max-width: 768px) {
  .lp-sources-grid { grid-template-columns: 1fr; max-width: 440px; margin: 0 auto; }
  .lp-flow { display: none; }
  .lp-cluster-wrap { margin-top: 24px; }
  .lp-signal-card { padding: 24px; border-radius: 24px; }
  .lp-context-row { grid-template-columns: 1fr; gap: 14px; }
  .lp-context-visual { justify-content: flex-start; }
  .lp-signal-stats { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .lp-recent-pill { display: none; }
  .lp-nav nav a:not(.lp-open) { display: none; }
}
@media (max-width: 480px) {
  .lp-hero-ctas, .lp-cta-btns { flex-direction: column; align-items: stretch; text-align: center; }
  .lp-footer-in { flex-direction: column; align-items: flex-start; }
  .lp-signal-card { padding: 20px; }
  .lp-signal-h2 { font-size: 30px; }
  .lp-buy-card { min-width: 320px; }
}
`;
