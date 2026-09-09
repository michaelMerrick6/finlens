import { NextResponse } from 'next/server';

import { routeErrorMessage } from '@/lib/api-errors';
import { getPoliticianWorkspaceData } from '@/lib/politician-workspace-server';

export const dynamic = 'force-dynamic';

function parsePositiveInt(value: string | null, fallback: number, maximum = 50) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.min(maximum, Math.max(0, Math.floor(parsed))) : fallback;
}

export async function GET(request: Request, { params }: { params: Promise<{ memberId: string }> }) {
  const { memberId } = await params;
  const url = new URL(request.url);
  const offset = parsePositiveInt(url.searchParams.get('offset'), 0);
  const limit = parsePositiveInt(url.searchParams.get('limit'), 8);

  try {
    const data = await getPoliticianWorkspaceData(memberId, { offset, limit });
    if (!data) {
      return NextResponse.json({ error: 'Politician workspace not found.' }, { status: 404 });
    }

    return NextResponse.json(data);
  } catch (error) {
    const message = routeErrorMessage(error, 'Failed to load politician workspace.', 'politician-workspace');
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
