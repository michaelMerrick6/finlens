import Link from 'next/link';
import { Suspense } from 'react';
import { notFound } from 'next/navigation';
import { PelosiHoldings } from '@/components/pelosi-holdings';

export const dynamic = 'force-dynamic';
export const metadata = { title: 'Nancy Pelosi · Estimated holdings' };

export default async function HoldingsPage({ params }: { params: Promise<{ memberId: string }> }) {
  const { memberId } = await params;
  if (memberId !== 'P000197') notFound();
  return <main id="main" className="container">
    <Link className="breadcrumb" href={`/politicians/${memberId}`}>← Nancy Pelosi · Disclosed activity</Link>
    <Suspense fallback={<p>Loading estimated holdings…</p>}><PelosiHoldings /></Suspense>
  </main>;
}
