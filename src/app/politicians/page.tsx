import type { Metadata } from "next";
import { PoliticianDirectory } from "@/components/politician-directory";
import { ChamberActivity } from "@/components/chamber-activity";
import { ChamberIllustration } from "@/components/chamber-illustration";
export const metadata: Metadata = { title: "Politicians" };
export default function Page() {
  return (
    <main id="main" className="container">
      <div className="page-heading politician-heading">
        <div className="politician-heading-copy">
        <span className="eyebrow">THE PEOPLE BEHIND THE DISCLOSURES</span>
        <h1>Find someone to track.</h1>
        <p>
          Explore politicians, read their disclosed activity, and keep the
          people
          <br className="desktop-break" /> you’re interested in close at hand.
        </p>
        <ChamberActivity />
        </div>
        <ChamberIllustration />
      </div>
      <PoliticianDirectory />
    </main>
  );
}
