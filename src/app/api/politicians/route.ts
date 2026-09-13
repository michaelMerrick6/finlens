import { NextResponse } from 'next/server';
import { loadPoliticianRanking } from '@/lib/politician-ranking';
export const dynamic = 'force-dynamic';
export async function GET(request: Request) {
  const params = new URL(request.url).searchParams;
  const terms = (params.get('q') || '').trim().slice(0, 100).toLowerCase().replace(/[^\p{L}\p{N}\s'-]/gu, ' ').split(/\s+/).filter(Boolean).slice(0, 6);
  const chamber = params.get('chamber') || 'All';
  const sort = ['volume', 'trades', 'name'].includes(params.get('sort') || '') ? params.get('sort')! : 'volume';
  const parsed = Number(params.get('offset') || 0);
  const offset = Number.isFinite(parsed) ? Math.min(2000, Math.max(0, Math.floor(parsed))) : 0;
  try {
    const data = await loadPoliticianRanking(new Date().toISOString().slice(0, 10));
    const members = data.members.filter(m => (chamber !== 'House' && chamber !== 'Senate' || m.chamber === chamber)
      && terms.every(t => `${m.first_name} ${m.last_name}`.toLowerCase().includes(t)))
      .sort((a, b) => (sort === 'volume' ? b.volume - a.volume : sort === 'trades' ? b.tradeCount - a.tradeCount : 0)
        || (a.last_name || '').localeCompare(b.last_name || '') || a.id.localeCompare(b.id));
    return NextResponse.json({ members: members.slice(offset, offset + 24).map((m, i) => ({ ...m, rank: offset + i + 1 })),
      nextOffset: offset + 24 < members.length ? offset + 24 : null, period: { start: data.start, end: data.end } });
  } catch (error) {
    console.error("Politician ranking failed", error instanceof Error ? error.message : error);
    return NextResponse.json({ error: 'The politician directory is temporarily unavailable.' }, { status: 503 });
  }
}
