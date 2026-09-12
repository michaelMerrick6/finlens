import { NextRequest, NextResponse } from 'next/server';
import { getTickerLogoUrl } from '@/lib/company-logos';
import { searchCompaniesDetailed, searchPoliticiansDetailed } from '@/lib/entity-search';

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url);
  const q = (searchParams.get('q') ?? '').trim();

  if (q.length < 1) {
    return NextResponse.json({ results: [] });
  }

  if (q.length > 200) {
    return NextResponse.json({ error: 'Search query must be 200 characters or fewer.' }, { status: 400 });
  }
  const [companyResult, politicianResult] = await Promise.allSettled([
    searchCompaniesDetailed(q, 6),
    searchPoliticiansDetailed(q, 4),
  ]);
  const companies = companyResult.status === 'fulfilled' ? companyResult.value : [];
  const politicians = politicianResult.status === 'fulfilled' ? politicianResult.value : [];
  const failedSources = [
    ...(companyResult.status === 'rejected' ? ['companies'] : []),
    ...(politicianResult.status === 'rejected' ? ['politicians'] : []),
  ];

  const companiesWithLogos = companies.map((company) => ({
    type: 'company' as const,
    id: company.ticker,
    ticker: company.ticker,
    name: company.name,
    logoUrl: getTickerLogoUrl(company.ticker),
    score: company.score,
    exactMatch: company.exactMatch,
    strongMatch: company.strongMatch,
  }));

  const politicianResults = politicians.map((p) => ({
    type: 'politician' as const,
    id: p.id,
    fullName: p.fullName,
    party: p.party,
    chamber: p.chamber,
    state: p.state,
    score: p.score,
    exactMatch: p.exactMatch,
    strongMatch: p.strongMatch,
  }));

  // Interleave: strong company matches first, then politicians, then rest
  const strongCompanies = companiesWithLogos.filter((c) => c.strongMatch);
  const weakCompanies = companiesWithLogos.filter((c) => !c.strongMatch);
  const strongPoliticians = politicianResults.filter((p) => p.strongMatch);
  const weakPoliticians = politicianResults.filter((p) => !p.strongMatch);

  const results = [
    ...strongCompanies,
    ...strongPoliticians,
    ...weakCompanies,
    ...weakPoliticians,
  ].slice(0, 8);

  return NextResponse.json({ results, partial: failedSources.length > 0, failedSources },
    { status: failedSources.length === 2 ? 503 : 200 });
}
