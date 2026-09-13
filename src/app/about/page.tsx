import Link from "next/link";
export const metadata = { title: "About the data" };
export default function Page() {
  return (
    <main id="main" className="container reading-page">
      <Link className="breadcrumb" href="/">
        ← Back to disclosures
      </Link>
      <span className="eyebrow">CONTEXT BEFORE CONCLUSIONS</span>
      <h1>
        A clearer record.
        <br />
        With the context intact.
      </h1>
      <p className="lead">
        Vail makes public congressional trading disclosures easier to explore.
        The original filing remains the source of record.
      </p>
      <section>
        <h2>A disclosure is not a live trade.</h2>
        <p>
          The transaction date tells you when a trade was reported to have
          happened. The filing date tells you when it was disclosed. Those dates
          can be weeks apart. Our feed is ordered by filing date.
        </p>
      </section>
      <section>
        <h2>Amounts are ranges.</h2>
        <p>
          A disclosed range such as $15,001–$50,000 is not an exact transaction
          value. We show the reported range without turning it into a precise
          estimate.
        </p>
      </section>
      <section>
        <h2>Available records, not complete portfolios.</h2>
        <p>
          Historical coverage and politician records may be incomplete.
          Amendments, unsupported documents and records requiring review can
          affect what appears. Unusual scans stop for review instead of being
          published as partial transactions. An empty result does not establish
          that no trading occurred.
        </p>
        <p>
          The public feed focuses on transactions with identifiable securities.
          It does not represent every asset in every filing. Members’ filings
          may include transactions for spouses or dependent children; view the
          original source for ownership details.
        </p>
      </section>
      <section>
        <h2>Track first. Choose alerts separately.</h2>
        <p>
          Tracking keeps selected politicians in your personal feed. Email
          alerts are optional and cover new disclosed activity detected by our
          scheduled processing. They are not instant trade notifications and
          currently include purchases and sales.
        </p>
      </section>
      <section>
        <h2>Go back to the source.</h2>
        <p>
          Open a disclosure’s details to find the original House or Senate
          filing. Public disclosures provide context for research; they are not
          investment recommendations.
        </p>
      </section>
      <Link className="button primary" href="/">
        Explore the disclosures →
      </Link>
    </main>
  );
}
