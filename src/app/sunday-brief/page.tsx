import Link from 'next/link';
import { BriefSignup } from '@/components/brief-signup';
export const metadata = { title: 'Sunday Brief', description: 'The week in congressional disclosures. A short Sunday read from Vail.' };
export default function SundayBrief() {
  return <main id="main" className="container brief-page">
    <header className="brief-heading"><span className="eyebrow">A WEEKLY LETTER FROM VAIL</span><h1>Sunday Brief.</h1><p>The week in congressional disclosures.<br /><em>A little context. A clearer picture.</em></p><span className="brief-meta">EVERY SUNDAY · ABOUT 3 MINUTES · FREE</span></header>
    <Link className="brief-preview-link" href="/sunday-brief/2026-09-13"><span className="eyebrow">PREVIEW THE FIRST EDITION · SEPTEMBER 13</span><h2>The stocks behind the AI buildout.</h2><p>Broadcom, Microsoft, and the infrastructure angle. Read the draft →</p></Link>
    <BriefSignup />
    <article className="brief-introduction">
      <div className="brief-edition"><span>THE FIRST EDITION IS COMING</span><span>What to expect</span></div>
      <h2>Less searching.<br />More understanding.</h2>
      <p>Congressional disclosures tell a story, but the details matter. Each Sunday, we’ll bring together the week’s noteworthy filings and explain the context behind the numbers.</p>
      <div className="brief-outline">
        <section><span>01</span><div><h3>The week at a glance</h3><p>A short look at newly disclosed purchases, sales, and the politicians reporting them.</p></div></section>
        <section><span>02</span><div><h3>Three things worth your attention</h3><p>Notable transactions and stocks appearing in multiple politicians’ disclosures. Clear reporting, with links to the records.</p></div></section>
        <section><span>03</span><div><h3>The detail behind the headline</h3><p>An option exercise, a compensation award, or a filing delay: the context that changes how a transaction should be read.</p></div></section>
      </div>
      <p className="brief-footnote">“Disclosed this week” doesn’t necessarily mean traded this week. We’ll distinguish filing dates from transaction dates and preserve reported dollar ranges.</p>
      <Link href="/analysis" className="text-link">Explore the disclosures while you wait →</Link>
    </article>
  </main>;
}
