import { NextResponse } from 'next/server';

import { routeErrorMessage } from '@/lib/api-errors';
import { getDashboardRecentClusterSignals } from '@/lib/public-data';

export const revalidate = 60;

export async function GET() {
  try {
    const clusters = await getDashboardRecentClusterSignals();
    return NextResponse.json({ clusters });
  } catch (error) {
    return NextResponse.json(
      { clusters: [], error: routeErrorMessage(error, 'Failed to load dashboard clusters.', 'dashboard-clusters') },
      { status: 200 },
    );
  }
}
