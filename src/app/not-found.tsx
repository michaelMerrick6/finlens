import Link from "next/link";
export default function NotFound() {
  return (
    <main id="main" className="container empty-page">
      <span className="eyebrow">404 / PAGE NOT FOUND</span>
      <h1>
        Let’s get you
        <br />
        back to the record.
      </h1>
      <p>This page may have moved, or the record isn’t available.</p>
      <Link className="button primary" href="/">
        Explore disclosures →
      </Link>
    </main>
  );
}
