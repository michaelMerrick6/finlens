import { redirect } from 'next/navigation';

export default async function SearchResultsPage({
  searchParams,
}: {
  searchParams: Promise<{ q?: string }>;
}) {
  await searchParams;
  redirect('/dashboard');
}
