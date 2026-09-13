import { CompanyLogo } from "@/components/identity-images";
import { notFound } from "next/navigation";
import Link from "next/link";
import { DisclosureFeed } from "@/components/disclosure-feed";
export async function generateMetadata({
  params,
}: {
  params: Promise<{ symbol: string }>;
}) {
  const { symbol } = await params;
  return { title: `${symbol.toUpperCase()} disclosures` };
}
export default async function Page({
  params,
}: {
  params: Promise<{ symbol: string }>;
}) {
  const { symbol } = await params;
  const ticker = symbol.toUpperCase();
  if (!/^[A-Z0-9.-]{1,12}$/.test(ticker)) notFound();
  return (
    <main id="main" className="container">
      <Link href="/" className="breadcrumb">
        ← Latest disclosures
      </Link>
      <div className="page-heading stock-heading">
        <span className="eyebrow">CONGRESSIONAL ACTIVITY BY SECURITY</span>
        <h1>
          <CompanyLogo ticker={ticker} large /> {ticker}
          <span className="ticker-tag">Public disclosures</span>
        </h1>
        <p>
          See which politicians reported transactions in {ticker}.<br />
          Reported trades are historical disclosures, not live market activity.
        </p>
      </div>
      <DisclosureFeed
        key={ticker}
        ticker={ticker}
        heading={`${ticker} disclosures`}
      />
    </main>
  );
}
