import { NextResponse } from 'next/server';
import { analysisRequest } from '@/lib/analysis-request';
import { loadAnalysisTrades } from '@/lib/analysis-server';
export const dynamic = 'force-dynamic';

export async function GET(request: Request) {
  const params = new URL(request.url).searchParams;
  const ticker = (params.get('ticker') || '').trim().toUpperCase();
  const offset = Number(params.get('offset') || 0);
  let scope;
  try {
    scope = analysisRequest(params, true);
    if (!/^[A-Z][A-Z0-9.-]{0,11}$/.test(ticker) || !Number.isInteger(offset) || offset < 0 || offset >= 50_000) throw new Error('Invalid page');
  } catch { return NextResponse.json({ error: 'Choose a valid stock, period and page.' }, { status: 400 }); }
  try {
    return NextResponse.json(await loadAnalysisTrades(scope.start, scope.end, scope.basis, scope.instrument, ticker, offset));
  } catch {
    return NextResponse.json({ error: 'Trade details are temporarily unavailable.' }, { status: 503 });
  }
}
