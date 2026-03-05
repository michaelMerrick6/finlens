import SearchForm from '@/components/SearchForm';
import { ArrowRight, TrendingUp, AlertCircle, Building2, UserCircle2 } from 'lucide-react';

export default function Home() {
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
            <div className="flex items-start justify-between p-3 bg-white/5 rounded-xl hover:bg-white/10 transition cursor-pointer">
              <div>
                <p className="font-semibold text-white">Nancy Pelosi (D)</p>
                <p className="text-sm text-gray-400 text-sm">Bought $1M-$5M of NVDA</p>
              </div>
              <span className="text-green-400 text-sm font-bold bg-green-400/10 px-2 py-1 rounded">Buy</span>
            </div>
            <div className="flex items-start justify-between p-3 bg-white/5 rounded-xl hover:bg-white/10 transition cursor-pointer">
              <div>
                <p className="font-semibold text-white">Tommy Tuberville (R)</p>
                <p className="text-sm text-gray-400 text-sm">Sold $500k of TSLA</p>
              </div>
              <span className="text-red-400 text-sm font-bold bg-red-400/10 px-2 py-1 rounded">Sell</span>
            </div>
          </div>
          <button className="mt-auto pt-4 flex items-center justify-center gap-2 text-sm text-[var(--color-primary)] font-medium group-hover:text-blue-400 transition-colors">
            View All Trades <ArrowRight size={16} />
          </button>
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
            <div className="flex items-start justify-between p-3 bg-white/5 rounded-xl hover:bg-white/10 transition cursor-pointer">
              <div>
                <p className="font-semibold text-white">Mark Zuckerberg (CEO)</p>
                <p className="text-sm text-gray-400 text-sm">Sold $42M of META</p>
              </div>
              <span className="text-red-400 text-sm font-bold bg-red-400/10 px-2 py-1 rounded">Sell</span>
            </div>
            <div className="flex items-start justify-between p-3 bg-white/5 rounded-xl hover:bg-white/10 transition cursor-pointer">
              <div>
                <p className="font-semibold text-white">Jamie Dimon (CEO)</p>
                <p className="text-sm text-gray-400 text-sm">Sold $150M of JPM</p>
              </div>
              <span className="text-red-400 text-sm font-bold bg-red-400/10 px-2 py-1 rounded">Sell</span>
            </div>
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
            <div className="flex items-start justify-between p-3 bg-white/5 rounded-xl hover:bg-white/10 transition cursor-pointer">
              <div>
                <p className="font-semibold text-white">Bridgewater Assoc.</p>
                <p className="text-sm text-gray-400 text-sm">+340% increase in MSFT</p>
              </div>
              <span className="text-green-400 text-sm font-bold bg-green-400/10 px-2 py-1 rounded">+340%</span>
            </div>
            <div className="flex items-start justify-between p-3 bg-white/5 rounded-xl hover:bg-white/10 transition cursor-pointer">
              <div>
                <p className="font-semibold text-white">Renaissance Tech</p>
                <p className="text-sm text-gray-400 text-sm">-100% (Liquidated) AMC</p>
              </div>
              <span className="text-red-400 text-sm font-bold bg-red-400/10 px-2 py-1 rounded">-100%</span>
            </div>
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
