import { supabase } from '@/lib/supabase';
import { Building2 } from 'lucide-react';
import PoliticiansFeed from '@/components/PoliticiansFeed';

export default async function PoliticiansPage() {
    const { data: trades } = await supabase
        .from('politician_trades')
        .select(`
      *,
      congress_members (
        first_name,
        last_name,
        party,
        chamber,
        state
      )
    `)
        .order('published_date', { ascending: false })
        .order('created_at', { ascending: false })
        .order('transaction_date', { ascending: false })
        .limit(500);

    return (
        <div className="space-y-6 animate-in fade-in duration-500">
            <section className="py-8 flex flex-col items-center text-center space-y-4">
                <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full glass-panel text-sm text-blue-400 font-medium mb-2 border border-blue-500/20">
                    <Building2 size={16} />
                    Capitol Hill Trades
                </div>
                <h1 className="text-4xl md:text-5xl font-extrabold tracking-tight">
                    Congressional Trading Feed
                </h1>
                <p className="text-xl text-[var(--text-secondary)] max-w-2xl mx-auto">
                    Live tracking of all Periodic Transaction Reports (PTR) filed by active United States Congress members.
                </p>
            </section>

            <PoliticiansFeed initialTrades={trades || []} />
        </div>
    );
}
