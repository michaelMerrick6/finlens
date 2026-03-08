import { supabase } from '@/lib/supabase';
import Link from 'next/link';
import { Building2, UserCircle2, ArrowRight, Search } from 'lucide-react';
import SearchForm from '@/components/SearchForm';

export default async function SearchResultsPage({
    searchParams,
}: {
    searchParams: Promise<{ q?: string }>;
}) {
    const params = await searchParams;
    const query = params.q || '';

    // 1. Query Companies Table (Tickers & Company Names)
    let companies: any[] = [];
    if (query) {
        const { data: cData } = await supabase
            .from('companies')
            .select('*')
            .or(`ticker.ilike.%${query}%,name.ilike.%${query}%`)
            .limit(10);
        if (cData) companies = cData;
    }

    // 2. Query Politicians Table (First & Last Names)
    let politicians: any[] = [];
    if (query) {
        const { data: pData } = await supabase
            .from('congress_members')
            .select('*')
            .or(`first_name.ilike.%${query}%,last_name.ilike.%${query}%`)
            .limit(10);
        if (pData) politicians = pData;
    }

    return (
        <div className="space-y-8 animate-in fade-in duration-500">
            <section className="py-8 flex flex-col items-center text-center space-y-4">
                <h1 className="text-4xl font-bold tracking-tight">Search Results</h1>
                <div className="w-full max-w-2xl">
                    <SearchForm />
                </div>
            </section>

            {query ? (
                <div className="grid grid-cols-1 md:grid-cols-2 gap-8">

                    {/* Companies Column */}
                    <div className="glass-panel p-6 rounded-3xl flex flex-col gap-4">
                        <div className="flex items-center gap-3 pb-4 border-b border-[rgba(255,255,255,0.08)]">
                            <div className="p-2 bg-blue-500/20 rounded-lg text-blue-400">
                                <Building2 size={24} />
                            </div>
                            <h2 className="text-2xl font-bold">Tickers & Companies</h2>
                        </div>

                        <div className="space-y-3 mt-4">
                            {companies.length > 0 ? (
                                companies.map((company) => (
                                    <Link href={`/ticker/${company.ticker}`} key={company.ticker}>
                                        <div className="group flex items-center justify-between p-4 bg-white/5 rounded-2xl hover:bg-white/10 transition cursor-pointer border border-transparent hover:border-blue-500/30">
                                            <div>
                                                <h3 className="font-bold text-lg text-blue-400 mb-1">{company.ticker}</h3>
                                                <p className="text-gray-300">{company.name}</p>
                                            </div>
                                            <ArrowRight className="text-gray-500 group-hover:text-blue-400 transition-colors" />
                                        </div>
                                    </Link>
                                ))
                            ) : (
                                <p className="text-gray-500 text-center py-8">No companies found matching "{query}".</p>
                            )}
                        </div>
                    </div>

                    {/* Politicians Column */}
                    <div className="glass-panel p-6 rounded-3xl flex flex-col gap-4">
                        <div className="flex items-center gap-3 pb-4 border-b border-[rgba(255,255,255,0.08)]">
                            <div className="p-2 bg-violet-500/20 rounded-lg text-violet-400">
                                <UserCircle2 size={24} />
                            </div>
                            <h2 className="text-2xl font-bold">Congress Members</h2>
                        </div>

                        <div className="space-y-3 mt-4">
                            {politicians.length > 0 ? (
                                politicians.map((pol) => (
                                    <Link href={`/politician/${pol.id}`} key={pol.id}>
                                        <div className="group flex items-center justify-between p-4 bg-white/5 rounded-2xl hover:bg-white/10 transition cursor-pointer border border-transparent hover:border-violet-500/30">
                                            <div>
                                                <h3 className="font-bold text-lg text-violet-400 mb-1">
                                                    {pol.first_name} {pol.last_name}
                                                </h3>
                                                <div className="flex items-center gap-2 text-sm text-gray-400">
                                                    <span className={`${pol.party === 'Democrat' ? 'text-blue-400' : pol.party === 'Republican' ? 'text-red-400' : 'text-gray-300'}`}>
                                                        {pol.party}
                                                    </span>
                                                    <span>•</span>
                                                    <span>{pol.chamber}</span>
                                                    <span>•</span>
                                                    <span>{pol.state}</span>
                                                </div>
                                            </div>
                                            <ArrowRight className="text-gray-500 group-hover:text-violet-400 transition-colors" />
                                        </div>
                                    </Link>
                                ))
                            ) : (
                                <p className="text-gray-500 text-center py-8">No politicians found matching "{query}".</p>
                            )}
                        </div>
                    </div>

                </div>
            ) : (
                <div className="text-center py-12 text-gray-500">
                    <Search size={48} className="mx-auto mb-4 opacity-20" />
                    <p>Enter a query above to search.</p>
                </div>
            )}
        </div>
    );
}
