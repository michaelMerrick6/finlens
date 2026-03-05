import { Building2, UserCircle2, TrendingUp, AlertCircle } from 'lucide-react';
import { supabase } from '@/lib/supabase';

export const revalidate = 60; // Revalidate cache every 60 seconds

export default async function TickerPage({ params }: { params: Promise<{ symbol: string }> }) {
    const resolvedParams = await params;
    const symbol = resolvedParams.symbol.toUpperCase();

    // Fetch live data from the database
    const [polTrades, inTrades, fundHoldings] = await Promise.all([
        supabase.from('politician_trades').select('*').eq('ticker', symbol).order('transaction_date', { ascending: false }),
        supabase.from('insider_trades').select('*').eq('ticker', symbol).order('transaction_date', { ascending: false }),
        supabase.from('institutional_holdings').select('*').eq('ticker', symbol).order('report_period', { ascending: false })
    ]);

    // Aggregate into unified timeline array
    const rawTimeline = [
        ...(polTrades.data || []).map((trade: any) => ({
            date: trade.transaction_date,
            type: 'politician',
            actor: trade.politician_name,
            action: `${trade.transaction_type} ${trade.amount_range}`,
            impact: trade.transaction_type.includes('Purchase') ? 'Bullish' : 'Bearish'
        })),
        ...(inTrades.data || []).map((trade: any) => ({
            date: trade.transaction_date,
            type: 'insider',
            actor: trade.filer_name,
            action: `${trade.transaction_code === 'P' ? 'Bought' : 'Sold'} $${trade.value.toLocaleString(undefined, { maximumFractionDigits: 0 })}`,
            impact: trade.transaction_code === 'P' ? 'Bullish' : 'Bearish'
        })),
        ...(fundHoldings.data || []).map((fund: any) => ({
            date: fund.published_date,
            type: 'fund',
            actor: fund.fund_name,
            action: fund.qoq_change_percent > 0 ? `Increased position by ${fund.qoq_change_percent}%` : `Decreased position by ${Math.abs(fund.qoq_change_percent)}%`,
            impact: fund.qoq_change_percent > 0 ? 'Bullish' : 'Bearish'
        }))
    ];

    // Sort by date descending and take top 20
    const unifiedTimeline = rawTimeline
        .sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime())
        .slice(0, 20);

    return (
        <div className="space-y-8 animate-in fade-in slide-in-from-bottom-8 duration-500">

            {/* Header */}
            <div className="flex flex-col md:flex-row md:items-end justify-between gap-6 pb-6 border-b border-[rgba(255,255,255,0.08)]">
                <div>
                    <div className="flex items-center gap-3 mb-2">
                        <h1 className="text-4xl font-extrabold tracking-tight text-white">{symbol}</h1>
                        <span className="px-3 py-1 rounded-full bg-blue-500/10 text-blue-400 text-sm font-semibold border border-blue-500/20">Technology</span>
                    </div>
                    <p className="text-xl text-[var(--text-secondary)] font-medium">NVIDIA Corporation</p>
                </div>
                <div className="text-right">
                    <p className="text-3xl font-bold text-white">$842.10</p>
                    <p className="text-sm font-semibold text-green-400">+2.45 (0.29%) Today</p>
                </div>
            </div>

            {/* Grid Overview */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <div className="glass-panel p-6 rounded-3xl">
                    <h3 className="text-[var(--text-secondary)] text-sm font-semibold uppercase tracking-wider mb-2">Politician Sentiment</h3>
                    <div className="flex items-center gap-3">
                        <div className="h-10 w-10 rounded-full bg-green-500/20 flex items-center justify-center text-green-400">
                            <TrendingUp size={20} />
                        </div>
                        <div>
                            <p className="text-2xl font-bold">Strong Buy</p>
                            <p className="text-xs text-gray-500">14 buys, 2 sells in LTM</p>
                        </div>
                    </div>
                </div>

                <div className="glass-panel p-6 rounded-3xl">
                    <h3 className="text-[var(--text-secondary)] text-sm font-semibold uppercase tracking-wider mb-2">Insider Sentiment</h3>
                    <div className="flex items-center gap-3">
                        <div className="h-10 w-10 rounded-full bg-red-500/20 flex items-center justify-center text-red-400">
                            <TrendingUp size={20} className="rotate-180" />
                        </div>
                        <div>
                            <p className="text-2xl font-bold">Heavy Selling</p>
                            <p className="text-xs text-gray-500">0 buys, $142M sold in LTM</p>
                        </div>
                    </div>
                </div>

                <div className="glass-panel p-6 rounded-3xl">
                    <h3 className="text-[var(--text-secondary)] text-sm font-semibold uppercase tracking-wider mb-2">Hedge Fund Flow</h3>
                    <div className="flex items-center gap-3">
                        <div className="h-10 w-10 rounded-full bg-blue-500/20 flex items-center justify-center text-blue-400">
                            <Building2 size={20} />
                        </div>
                        <div>
                            <p className="text-2xl font-bold">+12% Inflow</p>
                            <p className="text-xs text-gray-500">QoQ Institutional Growth</p>
                        </div>
                    </div>
                </div>
            </div>

            {/* Unified Timeline / Chart Area */}
            <div className="glass-panel p-8 rounded-[2rem]">
                <h2 className="text-2xl font-bold mb-6 text-white flex items-center gap-3">
                    <AlertCircle className="text-[var(--color-primary)]" /> Unified Convergence Timeline
                </h2>

                <div className="relative border-l-2 border-[rgba(255,255,255,0.1)] ml-4 space-y-8 pl-8 py-4">
                    {unifiedTimeline.map((item: any, idx: number) => (
                        <div key={idx} className="relative group">
                            <div className={`absolute -left-[41px] h-6 w-6 rounded-full border-4 border-[var(--bg-dark)] ${item.type === 'politician' ? 'bg-blue-500' :
                                item.type === 'insider' ? 'bg-violet-500' : 'bg-emerald-500'
                                }`} />

                            <div className="bg-[rgba(255,255,255,0.03)] hover:bg-[rgba(255,255,255,0.06)] border border-[rgba(255,255,255,0.05)] rounded-2xl p-5 transition-all">
                                <div className="flex justify-between items-start">
                                    <div>
                                        <span className="text-xs font-mono text-[var(--text-secondary)]">{item.date}</span>
                                        <h4 className="text-lg font-bold text-white mt-1 display-flex items-center gap-2">
                                            {item.type === 'politician' && <Building2 size={16} className="inline mr-2 text-blue-400" />}
                                            {item.type === 'insider' && <UserCircle2 size={16} className="inline mr-2 text-violet-400" />}
                                            {item.type === 'fund' && <Building2 size={16} className="inline mr-2 text-emerald-400" />}
                                            {item.actor}
                                        </h4>
                                        <p className="text-gray-300 mt-1">{item.action}</p>
                                    </div>
                                    <span className={`px-3 py-1 rounded-md text-xs font-bold ${item.impact === 'Bullish' ? 'bg-green-500/20 text-green-400' : 'bg-red-500/20 text-red-400'
                                        }`}>
                                        {item.impact}
                                    </span>
                                </div>
                            </div>
                        </div>
                    ))}
                </div>
            </div>
        </div>
    );
}
