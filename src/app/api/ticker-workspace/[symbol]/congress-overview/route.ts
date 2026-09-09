import { NextResponse } from 'next/server';

import { routeErrorMessage } from '@/lib/api-errors';
import { getTickerCongressOverview } from '@/lib/ticker-workspace-server';

export const dynamic = 'force-dynamic';

export async function GET(request: Request, { params }: { params: Promise<{ symbol: string }> }) {
  const { symbol } = await params;
  const range = new URL(request.url).searchParams.get('range');

  try {
    const data = await getTickerCongressOverview(symbol, range);
    if (!data) {
      return NextResponse.json({ error: 'Congress overview not found.' }, { status: 404 });
    }

    return NextResponse.json(data, {
      headers: {
        'Cache-Control': 'private, max-age=60, stale-while-revalidate=300',
      },
    });
  } catch (error) {
    const message = routeErrorMessage(error, 'Failed to load Congress overview.', 'ticker-congress-overview');
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
