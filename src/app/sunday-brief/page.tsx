import Link from 'next/link';
import { BriefSignup } from '@/components/brief-signup';
import { MonumentIllustration } from '@/components/monument-illustration';
export const metadata = { title: 'Sunday Brief', description: 'The week’s noteworthy congressional trades, with the context behind them. A short Sunday read from Vail.' };
export default function SundayBrief() {
 return <main id="main" className="container brief-page brief-landing">
  <header className="brief-hero">
   <div className="brief-hero-copy"><span className="eyebrow">A WEEKLY LETTER FROM VAIL</span><h1>Sunday Brief.</h1><p>The week’s noteworthy congressional trades, with the context behind them.</p><span className="brief-meta">EVERY SUNDAY · 3-MINUTE READ · FREE</span></div>
   <MonumentIllustration />
  </header>
  <BriefSignup />
  <section className="brief-latest" aria-labelledby="brief-latest-title"><span className="eyebrow">FIRST EDITION · SEPTEMBER 13, 2026 · PREVIEW</span><h2 id="brief-latest-title">The stocks behind the AI buildout.</h2><p>Broadcom, Microsoft, and the infrastructure connecting them. A closer look at the stocks appearing in congressional disclosures.</p><Link href="/sunday-brief/2026-09-13" className="text-link">Read preview <span aria-hidden="true">→</span></Link></section>
 </main>;
}
