import { NextResponse } from 'next/server';

import { routeErrorMessage } from '@/lib/api-errors';
import { getTickerWorkspaceData } from '@/lib/ticker-workspace-server';

export const dynamic = 'force-dynamic';

function parsePositiveInt(value: string | null, fallback: number, maximum = 50) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.min(maximum, Math.max(0, Math.floor(parsed))) : fallback;
}

export async function GET(request: Request, { params }: { params: Promise<{ symbol: string }> }) {
  const { symbol } = await params;
  const url = new URL(request.url);
  const offset = parsePositiveInt(url.searchParams.get('offset'), 0);
  const limit = parsePositiveInt(url.searchParams.get('limit'), 10);
  const source = url.searchParams.get('source');

  try {
    const data = await getTickerWorkspaceData(symbol, { offset, limit, source });
    if (!data) {
      return NextResponse.json({ error: 'Stock workspace not found.' }, { status: 404 });
    }

    return NextResponse.json(data, {
      headers: {
        'Cache-Control': 'no-store',
      },
    });
  } catch (error) {
    const message = routeErrorMessage(error, 'Failed to load stock workspace.', 'ticker-workspace-lite');
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
