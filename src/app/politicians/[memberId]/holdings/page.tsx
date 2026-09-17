import Link from 'next/link';
import { Suspense } from 'react';
import { notFound } from 'next/navigation';
import { PelosiHoldings } from '@/components/pelosi-holdings';
import { getReviewedBaseline } from '@/lib/reviewed-holdings';
import { ReviewedHoldings } from '@/components/reviewed-holdings';

export const dynamic = 'force-dynamic';
export async function generateMetadata({ params }: { params: Promise<{ memberId: string }> }) {
  const { memberId } = await params;
  return { title: `${getReviewedBaseline(memberId)?.name ?? (memberId === 'P000197' ? 'Nancy Pelosi' : 'Politician')} · Estimated holdings` };
}

export default async function HoldingsPage({ params }: { params: Promise<{ memberId: string }> }) {
  const { memberId } = await params;
  if (memberId !== 'P000197' && !getReviewedBaseline(memberId)) notFound();
  return <main id="main" className="container">
    <Link className="breadcrumb" href={`/politicians/${memberId}`}>← {getReviewedBaseline(memberId)?.name ?? (memberId === 'P000197' ? 'Nancy Pelosi' : 'Politician')} · Disclosed activity</Link>
    <Suspense fallback={<p>Loading estimated holdings…</p>}>{memberId === 'P000197' ? <PelosiHoldings /> : <ReviewedHoldings memberId={memberId} />}</Suspense>
  </main>;
}
