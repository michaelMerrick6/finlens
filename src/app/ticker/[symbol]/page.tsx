'use client';

import { useParams } from 'next/navigation';
import { Building2, UserCircle2, TrendingUp, AlertCircle } from 'lucide-react';

export default function TickerPage() {
    const params = useParams();
    const symbol = (params.symbol as string).toUpperCase();

    // In a real app, we would fetch data from Supabase aggregating all three tables.
    const mockTimeline = [
        { date: '2026-03-01', type: 'politician', actor: 'Nancy Pelosi (D)', action: 'Bought $1M-$5M', impact: 'Bullish' },
        { date: '2026-02-15', type: 'fund', actor: 'Renaissance Tech', action: 'Increased position by 150%', impact: 'Bullish' },
        { date: '2026-01-20', type: 'insider', actor: 'Jensen Huang (CEO)', action: 'Sold $14M', impact: 'Bearish' },
        { date: '2026-01-10', type: 'insider', actor: 'Colette Kress (CFO)', action: 'Sold $2M', impact: 'Bearish' },
    ];

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
                    {mockTimeline.map((item, idx) => (
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
