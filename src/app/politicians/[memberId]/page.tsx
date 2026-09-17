import Link from "next/link";
import { getReviewedBaseline } from "@/lib/reviewed-holdings";
import { Suspense } from "react";
import { PoliticianCommittees } from "@/components/politician-committees";
import { cache } from "react";
import { notFound } from "next/navigation";
import { getPoliticianWorkspaceData } from "@/lib/politician-workspace-server";
import { partyLabel } from "@/lib/ui-format";
import { Avatar, DisclosureFeed } from "@/components/disclosure-feed";
import { TrackButton } from "@/components/account-provider";
import { Icon } from "@/components/icon";
export const dynamic = "force-dynamic";
const loadProfile = cache((id: string) =>
  getPoliticianWorkspaceData(id, { limit: 4 }),
);
export async function generateMetadata({
  params,
}: {
  params: Promise<{ memberId: string }>;
}) {
  const { memberId } = await params;
  if (!/^[A-Za-z0-9-]{1,70}$/.test(memberId))
    return { title: "Politician not found" };
  const data = await loadProfile(memberId);
  return { title: data?.summary.displayName || "Politician not found" };
}
export default async function Page({
  params,
}: {
  params: Promise<{ memberId: string }>;
}) {
  const { memberId } = await params;
  if (!/^[A-Za-z0-9-]{1,70}$/.test(memberId)) notFound();
  const data = await loadProfile(memberId);
  if (!data) notFound();
  const { summary } = data;
  return (
    <main id="main" className="container">
      <Link className="breadcrumb" href="/politicians">
        ← All politicians
      </Link>
      <section className="profile-hero">
        <Avatar name={summary.displayName} memberId={memberId} large />
        <div className="profile-copy">
          <span className="eyebrow">CONGRESSIONAL DISCLOSURES</span>
          <h1>{summary.displayName}</h1>
          <p>
            <span className={`party-dot ${summary.party?.toLowerCase()}`} />
            {partyLabel(summary.party)}
            <span className="separator">/</span>
            {summary.chamber}
            <span className="separator">/</span>
            {summary.state || "State not listed"}
          </p>
        </div>
        <div className="profile-actions">
          <TrackButton id={memberId} name={summary.displayName} />
          {(memberId === "P000197" || getReviewedBaseline(memberId)) && <Link className="button secondary" href={`/politicians/${memberId}/holdings`}>View estimated holdings</Link>}
        </div>
      </section>
      <div className="profile-context">
        <div>
          <Icon name="bookmark" />
          <p>
            <strong>A public record, in one place.</strong>
            <br />
            Available disclosed transactions. This is not a complete portfolio.
          </p>
        </div>
        <Link className="text-link" href="/about">
          About the data
          <Icon name="arrow" size={16} />
        </Link>
      </div>
      <Suspense fallback={<p>Loading committee assignments…</p>}>
        <PoliticianCommittees memberId={memberId} />
      </Suspense>

      <DisclosureFeed
        key={memberId}
        memberId={memberId}
        heading="Disclosed activity"
      />
    </main>
  );
}
