import { supabase } from '@/lib/supabase';
import { ShieldAlert } from 'lucide-react';
import InsidersFeed from '@/components/InsidersFeed';

export default async function InsidersPage() {
    const { data: trades } = await supabase
        .from('insider_trades')
        .select('*')
        .order('transaction_date', { ascending: false })
        .limit(500);

    return (
        <div className="space-y-6 animate-in fade-in duration-500">
            <section className="py-8 flex flex-col items-center text-center space-y-4">
                <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full glass-panel text-sm text-amber-400 font-medium mb-2 border border-amber-500/20">
                    <ShieldAlert size={16} />
                    SEC Form 4 Filings
                </div>
                <h1 className="text-4xl md:text-5xl font-extrabold tracking-tight">
                    Insider Trading Feed
                </h1>
                <p className="text-xl text-[var(--text-secondary)] max-w-2xl mx-auto">
                    Real-time tracking of all Form 4 insider transactions filed with the SEC — executives, directors, and 10% owners.
                </p>
            </section>

            <InsidersFeed initialTrades={trades || []} />
        </div>
    );
}
