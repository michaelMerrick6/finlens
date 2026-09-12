import { NextResponse } from 'next/server';

import { routeErrorMessage } from '@/lib/api-errors';
import { getCachedPoliticianProfileData } from '@/lib/public-data';

export const revalidate = 300;

export async function GET(_: Request, { params }: { params: Promise<{ memberId: string }> }) {
  const { memberId } = await params;

  try {
    const profile = await getCachedPoliticianProfileData(memberId);
    if (!profile) {
      return NextResponse.json({ error: 'Politician profile not found.' }, { status: 404 });
    }

    return NextResponse.json({
      memberId: profile.memberId,
      summary: profile.summary,
      history: profile.history,
      tradesTruncated: profile.trades.length > 120 || profile.history.hasMore,
      livePortfolio: profile.livePortfolio,
      trades: profile.trades.slice(0, 120),
    });
  } catch (error) {
    const message = routeErrorMessage(error, 'Failed to load politician profile.', 'politician-profile');
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
