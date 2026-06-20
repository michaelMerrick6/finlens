import { redirect } from 'next/navigation';

export const dynamic = 'force-dynamic';

export default async function PoliticianProfilePage({
  params,
}: {
  params: Promise<{ memberId: string }>;
}) {
  const { memberId } = await params;
  redirect(`/politicians?profile=${encodeURIComponent(memberId)}`);
}
