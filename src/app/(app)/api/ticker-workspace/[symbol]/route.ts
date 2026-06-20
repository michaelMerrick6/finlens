import { NextResponse } from 'next/server';

import { getTickerIntelligence, TICKER_INTELLIGENCE_REVALIDATE } from '@/lib/ticker-intelligence';

export async function GET(
  _request: Request,
  { params }: { params: Promise<{ symbol: string }> },
) {
  const { symbol } = await params;
  const payload = await getTickerIntelligence(symbol);

  if (!payload) {
    return NextResponse.json({ error: 'Ticker not found.' }, { status: 404 });
  }

  return NextResponse.json(payload, {
    headers: {
      'Cache-Control': `s-maxage=${TICKER_INTELLIGENCE_REVALIDATE}, stale-while-revalidate=600`,
    },
  });
}
