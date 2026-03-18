import SearchForm from '@/components/SearchForm';
import { ArrowRight, TrendingUp, AlertCircle, Building2, UserCircle2 } from 'lucide-react';
import { supabase } from '@/lib/supabase';
import Link from 'next/link';

export default async function Home() {
  // Fetch real data from the database
  const { data: politicians } = await supabase
    .from('politician_trades')
    .select('*')
    .order('published_date', { ascending: false })
    .limit(2);

  const { data: insiders } = await supabase
    .from('insider_trades')
    .select('*')
    .order('transaction_date', { ascending: false })
    .limit(2);

  const { data: funds } = await supabase
    .from('institutional_holdings')
    .select('*')
    .order('value_held', { ascending: false })
    .limit(2);

  return (
    <div className="space-y-8 animate-in fade-in duration-500">

      {/* Hero Section */}
      <section className="py-12 flex flex-col items-center text-center space-y-6">
        <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full glass-panel text-sm text-[var(--color-primary)] font-medium mb-4">
          <span className="relative flex h-2 w-2">
            <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-blue-400 opacity-75"></span>
            <span className="relative inline-flex rounded-full h-2 w-2 bg-blue-500"></span>
          </span>
          Live Monitoring Active
        </div>

        <h1 className="text-5xl md:text-7xl font-extrabold tracking-tight">
          Track the <span className="text-gradient">Smart Money</span>.
        </h1>
        <p className="text-xl text-[var(--text-secondary)] max-w-2xl mx-auto">
          Unify congressional trades, corporate insider maneuvers, and hedge fund holding shifts into one powerful search engine.
        </p>

        <SearchForm />
      </section>

      {/* Market Overview Dashboard */}
      <section className="grid grid-cols-1 md:grid-cols-3 gap-6">

        {/* Politicians Column */}
        <div className="glass-panel p-6 rounded-3xl flex flex-col gap-4 group">
          <div className="flex items-center justify-between pb-2 border-b border-[rgba(255,255,255,0.08)]">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-blue-500/20 rounded-lg text-blue-400">
                <Building2 size={20} />
              </div>
              <h2 className="text-lg font-semibold tracking-tight">Capitol Hill</h2>
            </div>
            <span className="text-xs text-blue-400 font-medium bg-blue-900/40 px-2 py-1 rounded-md">Real-time</span>
          </div>

          <div className="space-y-4 mt-2">
            {politicians?.map((trade) => (
              <Link href={`/ticker/${trade.ticker}`} key={trade.id}>
                <div className="flex items-start justify-between p-3 bg-white/5 rounded-xl hover:bg-white/10 transition cursor-pointer mb-2">
                  <div>
                    <p className="font-semibold text-white truncate max-w-[150px]">{trade.politician_name}</p>
                    <p className="text-sm text-gray-400">Traded {trade.ticker}</p>
                  </div>
                  <span className={`text-sm font-bold px-2 py-1 rounded ${trade.transaction_type === 'buy' ? 'text-green-400 bg-green-400/10' : 'text-red-400 bg-red-400/10'}`}>
                    {trade.transaction_type === 'buy' ? 'Buy' : 'Sell'}
                  </span>
                </div>
              </Link>
            ))}
            {(!politicians || politicians.length === 0) && <p className="text-gray-500 text-sm">No recent trades found.</p>}
          </div>
          <Link href="/politicians" className="mt-auto pt-4 flex items-center justify-center gap-2 text-sm text-[var(--color-primary)] font-medium group-hover:text-blue-400 transition-colors w-full">
            View All Trades <ArrowRight size={16} />
          </Link>
        </div>

        {/* Insiders Column */}
        <div className="glass-panel p-6 rounded-3xl flex flex-col gap-4 group">
          <div className="flex items-center justify-between pb-2 border-b border-[rgba(255,255,255,0.08)]">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-violet-500/20 rounded-lg text-violet-400">
                <UserCircle2 size={20} />
              </div>
              <h2 className="text-lg font-semibold tracking-tight">Corporate Insiders</h2>
            </div>
            <span className="text-xs text-violet-400 font-medium bg-violet-900/40 px-2 py-1 rounded-md">Form 4</span>
          </div>

          <div className="space-y-4 mt-2">
            {insiders?.map((trade) => (
              <Link href={`/ticker/${trade.ticker}`} key={trade.id}>
                <div className="flex items-start justify-between p-3 bg-white/5 rounded-xl hover:bg-white/10 transition cursor-pointer mb-2">
                  <div>
                    <p className="font-semibold text-white truncate max-w-[150px]">{trade.filer_name}</p>
                    <p className="text-sm text-gray-400">Valued at ${(trade.value / 1000000).toFixed(1)}M</p>
                  </div>
                  <span className={`text-sm font-bold px-2 py-1 rounded ${trade.transaction_code === 'buy' ? 'text-green-400 bg-green-400/10' : 'text-red-400 bg-red-400/10'}`}>
                    {trade.transaction_code === 'buy' ? 'Buy' : 'Sell'}
                  </span>
                </div>
              </Link>
            ))}
            {(!insiders || insiders.length === 0) && <p className="text-gray-500 text-sm">No recent filings found.</p>}
          </div>
          <button className="mt-auto pt-4 flex items-center justify-center gap-2 text-sm text-[var(--color-accent)] font-medium group-hover:text-violet-400 transition-colors">
            View All Form 4s <ArrowRight size={16} />
          </button>
        </div>

        {/* Hedge Funds Column */}
        <div className="glass-panel p-6 rounded-3xl flex flex-col gap-4 group">
          <div className="flex items-center justify-between pb-2 border-b border-[rgba(255,255,255,0.08)]">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-emerald-500/20 rounded-lg text-emerald-400">
                <TrendingUp size={20} />
              </div>
              <h2 className="text-lg font-semibold tracking-tight">Hedge Funds</h2>
            </div>
            <span className="text-xs text-emerald-400 font-medium bg-emerald-900/40 px-2 py-1 rounded-md">13F-HR Q4</span>
          </div>

          <div className="space-y-4 mt-2">
            {funds?.map((fund) => (
              <Link href={`/ticker/${fund.ticker}`} key={fund.id}>
                <div className="flex items-start justify-between p-3 bg-white/5 rounded-xl hover:bg-white/10 transition cursor-pointer mb-2">
                  <div>
                    <p className="font-semibold text-white truncate max-w-[150px]">{fund.fund_name.replace(', L.P.', '').replace(' LLC', '')}</p>
                    <p className="text-sm text-gray-400">Hold {(fund.value_held / 1000000).toFixed(1)}M in {fund.ticker}</p>
                  </div>
                  <span className="text-emerald-400 text-sm font-bold bg-emerald-400/10 px-2 py-1 rounded">Hold</span>
                </div>
              </Link>
            ))}
            {(!funds || funds.length === 0) && <p className="text-gray-500 text-sm">No recent holdings found.</p>}
          </div>
          <button className="mt-auto pt-4 flex items-center justify-center gap-2 text-sm text-emerald-500 font-medium group-hover:text-emerald-400 transition-colors">
            View All 13Fs <ArrowRight size={16} />
          </button>
        </div>

      </section>

      {/* Top Trending Section (Placeholder) */}
      <section className="mt-12">
        <div className="flex items-center gap-2 mb-6">
          <AlertCircle className="text-orange-500" size={24} />
          <h2 className="text-2xl font-bold">Highly Traded This Week</h2>
        </div>
        <div className="glass-panel p-8 rounded-3xl flex items-center justify-center min-h-[200px] border border-orange-500/20">
          <p className="text-gray-400 text-center max-w-lg">
            Once enough daily data is ingested via the Python scraper cron job, this section will automatically surface stocks that highlight convergence between Politicians, Insiders, and Funds.
          </p>
        </div>
      </section>

    </div>
  );
}
