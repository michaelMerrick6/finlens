'use client';

import { useState, useEffect, useRef } from 'react';
import Link from 'next/link';
import { Search, ArrowUpRight, TrendingUp, TrendingDown, ExternalLink, Loader2 } from 'lucide-react';

interface Trade {
    id: string;
    member_id: string;
    politician_name: string;
    ticker: string;
    transaction_type: string;
    amount_range: string;
    published_date: string;
    transaction_date: string;
    source_url: string;
    chamber: string;
    congress_members: {
        first_name: string;
        last_name: string;
        party: string;
        chamber: string;
        state: string;
    } | null;
}

export default function PoliticiansFeed({ initialTrades }: { initialTrades: Trade[] }) {
    const [trades, setTrades] = useState<Trade[]>(initialTrades);
    const [searchQuery, setSearchQuery] = useState('');
    const [chamberFilter, setChamberFilter] = useState('All');
    const [directionFilter, setDirectionFilter] = useState('All');
    const [isSearching, setIsSearching] = useState(false);
    const debounceRef = useRef<NodeJS.Timeout | null>(null);

    // Fetch from API route when filters change
    useEffect(() => {
        const hasFilters = searchQuery.trim() || chamberFilter !== 'All' || directionFilter !== 'All';

        if (!hasFilters) {
            setTrades(initialTrades);
            return;
        }

        // Debounce search input
        if (debounceRef.current) clearTimeout(debounceRef.current);

        debounceRef.current = setTimeout(async () => {
            setIsSearching(true);
            try {
                const params = new URLSearchParams();
                if (searchQuery.trim()) params.set('q', searchQuery.trim());
                if (chamberFilter !== 'All') params.set('chamber', chamberFilter);
                if (directionFilter !== 'All') params.set('direction', directionFilter);

                const res = await fetch(`/api/search-trades?${params.toString()}`);
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
    }, [searchQuery, chamberFilter, directionFilter, initialTrades]);

    const chamberOptions = ['All', 'House', 'Senate'];
    const directionOptions = [
        { value: 'All', label: 'All' },
        { value: 'buy', label: 'Buy' },
        { value: 'sell', label: 'Sell' },
    ];

    return (
        <>
            {/* Search + Filters Bar */}
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
                {/* Search Input */}
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
                                color: '#60a5fa',
                            }}
                        />
                    )}
                    <input
                        type="text"
                        placeholder="Search politician name or ticker (e.g. Pelosi, AAPL)..."
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

                {/* Filter Row */}
                <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: '10px' }}>
                    <span style={{ fontSize: '11px', color: '#6b7280', textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: 600 }}>
                        Chamber
                    </span>
                    <div style={{ display: 'inline-flex', gap: '4px', background: 'rgba(255,255,255,0.04)', borderRadius: '10px', padding: '3px', border: '1px solid rgba(255,255,255,0.07)' }}>
                        {chamberOptions.map((opt) => (
                            <button
                                key={opt}
                                onClick={() => setChamberFilter(opt)}
                                style={{
                                    padding: '6px 16px',
                                    borderRadius: '8px',
                                    fontSize: '13px',
                                    fontWeight: 500,
                                    cursor: 'pointer',
                                    transition: 'all 0.15s ease',
                                    border: chamberFilter === opt ? '1px solid rgba(59,130,246,0.5)' : '1px solid transparent',
                                    background: chamberFilter === opt ? 'rgba(59,130,246,0.15)' : 'transparent',
                                    color: chamberFilter === opt ? '#60a5fa' : '#9ca3af',
                                    fontFamily: 'inherit',
                                }}
                            >
                                {opt}
                            </button>
                        ))}
                    </div>

                    <div style={{ width: '1px', height: '22px', background: 'rgba(255,255,255,0.1)', margin: '0 4px' }} />

                    <span style={{ fontSize: '11px', color: '#6b7280', textTransform: 'uppercase', letterSpacing: '0.08em', fontWeight: 600 }}>
                        Direction
                    </span>
                    <div style={{ display: 'inline-flex', gap: '4px', background: 'rgba(255,255,255,0.04)', borderRadius: '10px', padding: '3px', border: '1px solid rgba(255,255,255,0.07)' }}>
                        {directionOptions.map((opt) => {
                            const isActive = directionFilter === opt.value;
                            const colors: Record<string, { fg: string; bg: string; border: string }> = {
                                buy: { fg: '#34d399', bg: 'rgba(16,185,129,0.15)', border: 'rgba(16,185,129,0.5)' },
                                sell: { fg: '#f87171', bg: 'rgba(239,68,68,0.15)', border: 'rgba(239,68,68,0.5)' },
                                All: { fg: '#60a5fa', bg: 'rgba(59,130,246,0.15)', border: 'rgba(59,130,246,0.5)' },
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
                        <Loader2 size={14} className="animate-spin" style={{ color: '#60a5fa' }} />
                        Searching...
                    </span>
                ) : (
                    <>
                        Showing <span style={{ color: '#d1d5db', fontWeight: 600 }}>{trades.length}</span> trades
                        {searchQuery.trim() && (
                            <span> matching &ldquo;<span style={{ color: '#60a5fa' }}>{searchQuery.trim()}</span>&rdquo;</span>
                        )}
                        {chamberFilter !== 'All' && (
                            <span> in <span style={{ color: '#60a5fa' }}>{chamberFilter}</span></span>
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
                                <th style={{ padding: '14px 16px 14px 20px', fontSize: '13px', fontWeight: 600, color: '#9ca3af' }}>Politician</th>
                                <th style={{ padding: '14px 16px', fontSize: '13px', fontWeight: 600, color: '#9ca3af' }}>Ticker</th>
                                <th style={{ padding: '14px 16px', fontSize: '13px', fontWeight: 600, color: '#9ca3af' }}>Type</th>
                                <th style={{ padding: '14px 16px', fontSize: '13px', fontWeight: 600, color: '#9ca3af' }}>Amount Range</th>
                                <th style={{ padding: '14px 16px', fontSize: '13px', fontWeight: 600, color: '#9ca3af' }}>Filed Date</th>
                                <th style={{ padding: '14px 16px', fontSize: '13px', fontWeight: 600, color: '#9ca3af' }}>Tx Date</th>
                                <th style={{ padding: '14px 16px 14px 16px', fontSize: '13px', fontWeight: 600, color: '#9ca3af', textAlign: 'right' }}>Source DOC</th>
                            </tr>
                        </thead>
                        <tbody>
                            {trades.map((trade, i) => {
                                const member = trade.congress_members;
                                const isBuy = trade.transaction_type === 'buy';

                                return (
                                    <tr
                                        key={trade.id || i}
                                        style={{ borderBottom: '1px solid rgba(255,255,255,0.04)' }}
                                        className="hover:bg-white/5 transition-colors group"
                                    >
                                        <td style={{ padding: '14px 16px 14px 20px' }}>
                                            <Link href={`/politician/${trade.member_id}`} className="block">
                                                <div style={{ fontWeight: 600, color: '#fff', fontSize: '14px' }} className="group-hover:text-blue-400 transition-colors">
                                                    {member ? `${member.first_name} ${member.last_name}` : trade.politician_name}
                                                </div>
                                                {member && (
                                                    <div style={{ fontSize: '12px', color: '#6b7280', marginTop: '3px', display: 'flex', alignItems: 'center', gap: '5px' }}>
                                                        <span style={{ color: member.party === 'Democrat' ? '#3b82f6' : member.party === 'Republican' ? '#ef4444' : '#9ca3af' }}>
                                                            {member.party?.charAt(0)}
                                                        </span>
                                                        <span>•</span>
                                                        <span>{member.chamber}</span>
                                                        <span>•</span>
                                                        <span>{member.state}</span>
                                                    </div>
                                                )}
                                            </Link>
                                        </td>
                                        <td style={{ padding: '14px 16px' }}>
                                            <Link href={`/ticker/${trade.ticker}`}>
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
                                                    cursor: 'pointer',
                                                }}>
                                                    {trade.ticker}
                                                    <ArrowUpRight size={13} style={{ color: '#9ca3af' }} />
                                                </span>
                                            </Link>
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
                                        <td style={{ padding: '14px 16px', fontSize: '14px', color: '#d1d5db', whiteSpace: 'nowrap' }}>
                                            {trade.amount_range || 'Unknown'}
                                        </td>
                                        <td style={{ padding: '14px 16px', fontSize: '14px', color: '#fff', fontWeight: 500, whiteSpace: 'nowrap' }}>
                                            {trade.published_date ? new Date(trade.published_date).toLocaleDateString() : 'N/A'}
                                        </td>
                                        <td style={{ padding: '14px 16px', fontSize: '14px', color: '#9ca3af', whiteSpace: 'nowrap' }}>
                                            {trade.transaction_date ? new Date(trade.transaction_date).toLocaleDateString() : 'N/A'}
                                        </td>
                                        <td style={{ padding: '14px 16px', textAlign: 'right' }}>
                                            {trade.source_url ? (
                                                <a
                                                    href={trade.source_url}
                                                    target="_blank"
                                                    rel="noopener noreferrer"
                                                    style={{ display: 'inline-flex', alignItems: 'center', gap: '5px', fontSize: '13px', color: '#60a5fa', textDecoration: 'none' }}
                                                >
                                                    {trade.source_url.includes('efdsearch.senate.gov') ? 'View Filing' : 'View PDF'}
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
                            No trades match your search. Try a different name or ticker.
                        </div>
                    )}
                </div>
            </div>
        </>
    );
}
