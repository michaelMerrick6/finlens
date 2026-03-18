'use client';

import { useState, useEffect, useRef } from 'react';
import { Search, TrendingUp, TrendingDown, ExternalLink, Loader2 } from 'lucide-react';

interface InsiderTrade {
    id: string;
    ticker: string;
    filer_name: string;
    filer_relation: string;
    transaction_date: string;
    published_date: string;
    transaction_code: string;
    amount: number;
    price: number;
    value: number;
    source_url: string;
}

function formatNumber(n: number): string {
    if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`;
    if (n >= 1_000) return `$${(n / 1_000).toFixed(0)}K`;
    return `$${n.toLocaleString()}`;
}

function formatShares(n: number): string {
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
    if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
    return n.toLocaleString();
}

export default function InsidersFeed({ initialTrades }: { initialTrades: InsiderTrade[] }) {
    const [trades, setTrades] = useState<InsiderTrade[]>(initialTrades);
    const [searchQuery, setSearchQuery] = useState('');
    const [directionFilter, setDirectionFilter] = useState('All');
    const [isSearching, setIsSearching] = useState(false);
    const debounceRef = useRef<NodeJS.Timeout | null>(null);

    useEffect(() => {
        const hasFilters = searchQuery.trim() || directionFilter !== 'All';

        if (!hasFilters) {
            setTrades(initialTrades);
            return;
        }

        if (debounceRef.current) clearTimeout(debounceRef.current);

        debounceRef.current = setTimeout(async () => {
            setIsSearching(true);
            try {
                const params = new URLSearchParams();
                if (searchQuery.trim()) params.set('q', searchQuery.trim());
                if (directionFilter !== 'All') params.set('direction', directionFilter);

                const res = await fetch(`/api/search-insider-trades?${params.toString()}`);
                const json = await res.json();
                setTrades(json.trades || []);
            } catch (err) {
                console.error('Search failed:', err);
            }
            setIsSearching(false);
        }, 400);

        return () => {
            if (debounceRef.current) clearTimeout(debounceRef.current);
        };
    }, [searchQuery, directionFilter, initialTrades]);

    const directionOptions = [
        { value: 'All', label: 'All' },
        { value: 'buy', label: 'Buy' },
        { value: 'sell', label: 'Sell' },
    ];

    return (
        <>
            {/* Search + Filters */}
            <div
                style={{
                    background: 'rgba(255,255,255,0.03)',
                    backdropFilter: 'blur(12px)',
                    WebkitBackdropFilter: 'blur(12px)',
                    border: '1px solid rgba(255,255,255,0.08)',
                    borderRadius: '16px',
                    padding: '20px',
                }}
            >
                <div style={{ position: 'relative', marginBottom: '14px' }}>
                    <Search
                        size={18}
                        style={{
                            position: 'absolute',
                            left: '14px',
                            top: '50%',
                            transform: 'translateY(-50%)',
                            color: '#6b7280',
                            pointerEvents: 'none',
                        }}
                    />
                    {isSearching && (
                        <Loader2
                            size={18}
                            className="animate-spin"
                            style={{
                                position: 'absolute',
                                right: '14px',
                                top: '50%',
                                transform: 'translateY(-50%)',
                                color: '#f59e0b',
                            }}
                        />
                    )}
                    <input
                        type="text"
                        placeholder="Search insider name or ticker (e.g. Tim Cook, AAPL)..."
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        style={{
                            width: '100%',
                            padding: '13px 44px 13px 42px',
                            background: 'rgba(255,255,255,0.06)',
                            border: '1px solid rgba(255,255,255,0.12)',
                            borderRadius: '12px',
                            color: '#ffffff',
                            fontSize: '15px',
                            outline: 'none',
                            fontFamily: 'inherit',
                            boxSizing: 'border-box',
                        }}
                    />
                </div>

                <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: '10px' }}>
                    <span style={{ fontSize: '11px', color: '#6b7280', textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: 600 }}>
                        Direction
                    </span>
                    <div style={{ display: 'inline-flex', gap: '4px', background: 'rgba(255,255,255,0.04)', borderRadius: '10px', padding: '3px', border: '1px solid rgba(255,255,255,0.07)' }}>
                        {directionOptions.map((opt) => {
                            const isActive = directionFilter === opt.value;
                            const colors: Record<string, { fg: string; bg: string; border: string }> = {
                                buy: { fg: '#34d399', bg: 'rgba(16,185,129,0.15)', border: 'rgba(16,185,129,0.5)' },
                                sell: { fg: '#f87171', bg: 'rgba(239,68,68,0.15)', border: 'rgba(239,68,68,0.5)' },
                                All: { fg: '#f59e0b', bg: 'rgba(245,158,11,0.15)', border: 'rgba(245,158,11,0.5)' },
                            };
                            const c = colors[opt.value] || colors.All;
                            return (
                                <button
                                    key={opt.value}
                                    onClick={() => setDirectionFilter(opt.value)}
                                    style={{
                                        padding: '6px 16px',
                                        borderRadius: '8px',
                                        fontSize: '13px',
                                        fontWeight: 500,
                                        cursor: 'pointer',
                                        transition: 'all 0.15s ease',
                                        border: isActive ? `1px solid ${c.border}` : '1px solid transparent',
                                        background: isActive ? c.bg : 'transparent',
                                        color: isActive ? c.fg : '#9ca3af',
                                        fontFamily: 'inherit',
                                    }}
                                >
                                    {opt.label}
                                </button>
                            );
                        })}
                    </div>
                </div>
            </div>

            {/* Results Count */}
            <div style={{ fontSize: '14px', color: '#6b7280', padding: '0 4px' }}>
                {isSearching ? (
                    <span style={{ display: 'inline-flex', alignItems: 'center', gap: '6px' }}>
                        <Loader2 size={14} className="animate-spin" style={{ color: '#f59e0b' }} />
                        Searching...
                    </span>
                ) : (
                    <>
                        Showing <span style={{ color: '#d1d5db', fontWeight: 600 }}>{trades.length}</span> trades
                        {searchQuery.trim() && (
                            <span> matching &ldquo;<span style={{ color: '#f59e0b' }}>{searchQuery.trim()}</span>&rdquo;</span>
                        )}
                        {directionFilter !== 'All' && (
                            <span> — <span style={{ color: directionFilter === 'buy' ? '#34d399' : '#f87171' }}>
                                {directionFilter === 'buy' ? 'Buys' : 'Sells'}
                            </span> only</span>
                        )}
                    </>
                )}
            </div>

            {/* Trades Table */}
            <div className="glass-panel" style={{ borderRadius: '20px', overflow: 'hidden', border: '1px solid rgba(255,255,255,0.08)' }}>
                <div style={{ overflowX: 'auto' }}>
                    <table style={{ width: '100%', textAlign: 'left', borderCollapse: 'collapse' }}>
                        <thead>
                            <tr style={{ background: 'rgba(255,255,255,0.04)', borderBottom: '1px solid rgba(255,255,255,0.08)' }}>
                                <th style={{ padding: '14px 16px 14px 20px', fontSize: '13px', fontWeight: 600, color: '#9ca3af' }}>Insider</th>
                                <th style={{ padding: '14px 16px', fontSize: '13px', fontWeight: 600, color: '#9ca3af' }}>Ticker</th>
                                <th style={{ padding: '14px 16px', fontSize: '13px', fontWeight: 600, color: '#9ca3af' }}>Type</th>
                                <th style={{ padding: '14px 16px', fontSize: '13px', fontWeight: 600, color: '#9ca3af' }}>Shares</th>
                                <th style={{ padding: '14px 16px', fontSize: '13px', fontWeight: 600, color: '#9ca3af' }}>Price</th>
                                <th style={{ padding: '14px 16px', fontSize: '13px', fontWeight: 600, color: '#9ca3af' }}>Value</th>
                                <th style={{ padding: '14px 16px', fontSize: '13px', fontWeight: 600, color: '#9ca3af' }}>Tx Date</th>
                                <th style={{ padding: '14px 16px 14px 16px', fontSize: '13px', fontWeight: 600, color: '#9ca3af', textAlign: 'right' }}>Filing</th>
                            </tr>
                        </thead>
                        <tbody>
                            {trades.map((trade, i) => {
                                const isBuy = trade.transaction_code === 'buy';

                                return (
                                    <tr
                                        key={trade.id || i}
                                        style={{ borderBottom: '1px solid rgba(255,255,255,0.04)' }}
                                        className="hover:bg-white/5 transition-colors group"
                                    >
                                        <td style={{ padding: '14px 16px 14px 20px' }}>
                                            <div style={{ fontWeight: 600, color: '#fff', fontSize: '14px' }}>
                                                {trade.filer_name}
                                            </div>
                                            <div style={{ fontSize: '12px', color: '#6b7280', marginTop: '3px' }}>
                                                {trade.filer_relation || 'Insider'}
                                            </div>
                                        </td>
                                        <td style={{ padding: '14px 16px' }}>
                                            <span style={{
                                                display: 'inline-flex',
                                                alignItems: 'center',
                                                gap: '5px',
                                                padding: '4px 10px',
                                                background: 'rgba(255,255,255,0.08)',
                                                borderRadius: '8px',
                                                fontWeight: 700,
                                                color: '#fff',
                                                fontSize: '13px',
                                            }}>
                                                {trade.ticker}
                                            </span>
                                        </td>
                                        <td style={{ padding: '14px 16px' }}>
                                            <span style={{
                                                display: 'inline-flex',
                                                alignItems: 'center',
                                                gap: '5px',
                                                padding: '4px 10px',
                                                borderRadius: '6px',
                                                fontSize: '13px',
                                                fontWeight: 600,
                                                background: isBuy ? 'rgba(16,185,129,0.1)' : 'rgba(239,68,68,0.1)',
                                                color: isBuy ? '#34d399' : '#f87171',
                                            }}>
                                                {isBuy ? <TrendingUp size={14} /> : <TrendingDown size={14} />}
                                                {isBuy ? 'Buy' : 'Sell'}
                                            </span>
                                        </td>
                                        <td style={{ padding: '14px 16px', fontSize: '14px', color: '#d1d5db', whiteSpace: 'nowrap', fontWeight: 500 }}>
                                            {formatShares(trade.amount)}
                                        </td>
                                        <td style={{ padding: '14px 16px', fontSize: '14px', color: '#9ca3af', whiteSpace: 'nowrap' }}>
                                            {trade.price > 0 ? `$${trade.price.toFixed(2)}` : '—'}
                                        </td>
                                        <td style={{ padding: '14px 16px', fontSize: '14px', whiteSpace: 'nowrap', fontWeight: 600, color: isBuy ? '#34d399' : '#f87171' }}>
                                            {trade.value > 0 ? formatNumber(trade.value) : '—'}
                                        </td>
                                        <td style={{ padding: '14px 16px', fontSize: '14px', color: '#fff', fontWeight: 500, whiteSpace: 'nowrap' }}>
                                            {trade.transaction_date ? new Date(trade.transaction_date + 'T12:00:00').toLocaleDateString() : 'N/A'}
                                        </td>
                                        <td style={{ padding: '14px 16px', textAlign: 'right' }}>
                                            {trade.source_url ? (
                                                <a
                                                    href={trade.source_url}
                                                    target="_blank"
                                                    rel="noopener noreferrer"
                                                    style={{ display: 'inline-flex', alignItems: 'center', gap: '5px', fontSize: '13px', color: '#f59e0b', textDecoration: 'none' }}
                                                >
                                                    SEC Filing
                                                    <ExternalLink size={13} />
                                                </a>
                                            ) : (
                                                <span style={{ color: '#4b5563', fontSize: '13px' }}>N/A</span>
                                            )}
                                        </td>
                                    </tr>
                                );
                            })}
                        </tbody>
                    </table>

                    {trades.length === 0 && !isSearching && (
                        <div style={{ textAlign: 'center', padding: '48px 16px', color: '#6b7280', fontSize: '15px' }}>
                            No insider trades match your search. Try a different name or ticker.
                        </div>
                    )}
                </div>
            </div>
        </>
    );
}
