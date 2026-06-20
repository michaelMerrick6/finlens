import { NextResponse } from 'next/server';

import {
  getTickerPoliticianTransactions,
  TICKER_INTELLIGENCE_REVALIDATE,
  TICKER_POLITICIAN_TRANSACTION_PAGE_SIZE,
} from '@/lib/ticker-intelligence';

export async function GET(
  request: Request,
  { params }: { params: Promise<{ symbol: string }> },
) {
  const { symbol } = await params;
  const { searchParams } = new URL(request.url);
  const offset = Math.max(0, Math.floor(Number(searchParams.get('offset') || 0) || 0));
  const limit = Math.min(50, Math.max(1, Math.floor(Number(searchParams.get('limit') || TICKER_POLITICIAN_TRANSACTION_PAGE_SIZE) || TICKER_POLITICIAN_TRANSACTION_PAGE_SIZE)));
  const payload = await getTickerPoliticianTransactions(symbol, { offset, limit });

  if (!payload) {
    return NextResponse.json({ error: 'Ticker not found.' }, { status: 404 });
  }

  return NextResponse.json(payload, {
    headers: {
      'Cache-Control': `s-maxage=${TICKER_INTELLIGENCE_REVALIDATE}, stale-while-revalidate=600`,
    },
  });
}
