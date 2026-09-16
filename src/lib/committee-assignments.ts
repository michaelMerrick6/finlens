import 'server-only';
import { getAdminSupabase } from '@/lib/supabase-admin';

type Committee = { id: string; name: string; parent_id: string | null; source_url: string };
type Assignment = { committee_id: string; role: string };
type Snapshot = {
  id: string; source_key: string; observed_at: string; verified_at: string; congress: number;
  committees: Committee[]; members: { id: string; chamber: string }[];
};
export type MemberCommittees = {
  status: 'available' | 'unavailable' | 'not-covered';
  verifiedAt: string | null; stale: boolean;
  current: { name: string; parent: string | null; role: string; source: string }[];
  history: { observedAt: string; names: string[]; source: string }[];
};

export async function getMemberCommittees(memberId: string): Promise<MemberCommittees> {
  const empty: MemberCommittees = { status: 'unavailable', verifiedAt: null, stale: false, current: [], history: [] };
  try {
    const db = getAdminSupabase();
    // Read each source separately: frequent House changes cannot crowd Senate out.
    const snapshots = await Promise.all(['official_house', 'official_senate'].map(async source => {
      const { data, error } = await db.from('committee_snapshots')
        .select('id,source_key,observed_at,verified_at,congress,committees,members')
        .eq('source_key', source).order('verified_at', { ascending: false }).limit(1);
      if (error) throw error;
      return (data?.[0] as Snapshot | undefined);
    }));
    const snapshot = snapshots.find(s => s?.members.some(m => m.id === memberId));
    const result: MemberCommittees = { ...empty, status: snapshots.every(Boolean) ? 'not-covered' : 'unavailable' };
    if (snapshot) {
      const { data, error } = await db.from('committee_assignments').select('committee_id,role')
        .eq('snapshot_id', snapshot.id).eq('member_id', memberId);
      if (error) throw error;
      result.status = 'available';
      result.verifiedAt = snapshot.verified_at;
      const now = new Date();
      const year = now.getUTCFullYear() - Number(now.getUTCMonth() === 0 && now.getUTCDate() < 3);
      const currentCongress = Math.floor((year - 1789) / 2) + 1;
      result.stale = snapshot.congress !== currentCongress || Date.now() - Date.parse(snapshot.verified_at) > 72 * 3600 * 1000;
      result.current = ((data || []) as Assignment[]).map(a => {
        const c = snapshot.committees.find(c => c.id === a.committee_id);
        if (!c) throw new Error('Unresolved committee');
        return { name: c.name, parent: snapshot.committees.find(p => p.id === c.parent_id)?.name || null,
          role: a.role, source: c.source_url };
      }).sort((a, b) => Number(Boolean(a.parent)) - Number(Boolean(b.parent)) || a.name.localeCompare(b.name));
    }
    // Archive snapshots are not merged into current membership or effective intervals.
    const { data: archives, error: archiveError } = await db.rpc('member_committee_archive', { p_member: memberId });
    if (archiveError) throw archiveError;
    result.history = (archives || []).map((row: { observed_at: string; names: string[]; source_url: string }) => ({
      observedAt: row.observed_at, names: row.names, source: row.source_url,
    }));
    return result;
  } catch (error) {
    console.error('Committee profile unavailable', error instanceof Error ? error.message : 'Data query failed');
    return empty;
  }
}
