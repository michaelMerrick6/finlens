import { NextRequest, NextResponse } from 'next/server';

import { getDashboardClusterDetail } from '@/lib/dashboard-cluster-detail';

export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url);
  const candidateKey = (searchParams.get('key') || '').trim();

  if (!candidateKey) {
    return NextResponse.json({ error: 'Missing cluster key.' }, { status: 400 });
  }

  try {
    const detail = await getDashboardClusterDetail(candidateKey);
    if (!detail) {
      return NextResponse.json({ error: 'Cluster not found.' }, { status: 404 });
    }
    return NextResponse.json(detail);
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : 'Failed to load cluster detail.' },
      { status: 500 },
    );
  }
}
