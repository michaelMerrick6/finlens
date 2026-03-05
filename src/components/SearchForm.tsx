'use client';

import { useState } from 'react';
import { useRouter } from 'next/navigation';
import { Search } from 'lucide-react';

export default function SearchForm() {
    const [query, setQuery] = useState('');
    const router = useRouter();

    const handleSearch = (e: React.FormEvent) => {
        e.preventDefault();
        if (query.trim()) {
            router.push(`/ticker/${query.trim().toUpperCase()}`);
        }
    };

    return (
        <form onSubmit={handleSearch} className="w-full max-w-2xl relative mt-8 group">
            <div className="absolute inset-y-0 left-0 pl-6 flex items-center pointer-events-none">
                <Search className="h-6 w-6 text-gray-400 group-focus-within:text-[var(--color-primary)] transition-colors" />
            </div>
            <input
                type="text"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Search by Ticker (e.g. AAPL, NVDA)..."
                className="w-full bg-[rgba(255,255,255,0.05)] border border-[rgba(255,255,255,0.1)] rounded-2xl py-5 pl-16 pr-16 text-xl text-white placeholder-gray-500 focus:outline-none focus:border-[var(--color-primary)] focus:ring-4 focus:ring-blue-500/20 transition-all shadow-[0_0_30px_rgba(59,130,246,0.1)] focus:shadow-[0_0_40px_rgba(59,130,246,0.2)]"
            />
            <button
                type="submit"
                disabled={!query.trim()}
                className="absolute right-3 top-3 bottom-3 px-6 bg-[var(--color-primary)] hover:bg-blue-600 disabled:opacity-50 disabled:cursor-not-allowed rounded-xl transition-all font-semibold text-white shadow-lg shadow-blue-500/30"
            >
                Analyze
            </button>
        </form>
    );
}
