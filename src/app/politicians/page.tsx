import type { Metadata } from "next";
import { PoliticianDirectory } from "@/components/politician-directory";
export const metadata: Metadata = { title: "Politicians" };
export default function Page() {
  return (
    <main id="main" className="container">
      <div className="page-heading">
        <span className="eyebrow">THE PEOPLE BEHIND THE DISCLOSURES</span>
        <h1>Find someone to track.</h1>
        <p>
          Explore politicians, read their disclosed activity, and keep the
          people
          <br className="desktop-break" /> you’re interested in close at hand.
        </p>
      </div>
      <PoliticianDirectory />
    </main>
  );
}
