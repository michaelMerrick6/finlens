import { NextResponse } from 'next/server';
import { analysisRequest } from '@/lib/analysis-request';
import { loadAnalysisSummary } from '@/lib/analysis-server';
export const dynamic = 'force-dynamic';

export async function GET(request: Request) {
  let scope;
  try { scope = analysisRequest(new URL(request.url).searchParams); }
  catch { return NextResponse.json({ error: 'Choose a supported period.' }, { status: 400 }); }
  try {
    return NextResponse.json(await loadAnalysisSummary(scope.start, scope.end, scope.basis, scope.instrument));
  } catch {
    return NextResponse.json({ error: 'Analysis is temporarily unavailable. No partial results are shown.' }, { status: 503 });
  }
}
