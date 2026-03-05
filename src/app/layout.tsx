import type { Metadata } from 'next';
import { Inter } from 'next/font/google';
import './globals.css';

const inter = Inter({ subsets: ['latin'] });

export const metadata: Metadata = {
  title: 'FinLens - Insider, Politician and 13F SEC Trade Tracking',
  description: 'Monitor politician stocks, insider trades, and hedge fund 13f data all in one unified dashboard.',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className={`${inter.className} min-h-screen antialiased`}>
        <div className="flex flex-col min-h-screen bg-[var(--bg-dark)]">
          <nav className="glass-panel sticky top-0 z-50 px-6 py-4 flex items-center justify-between border-b-0">
            <div className="flex items-center gap-2">
              <div className="w-8 h-8 rounded-full bg-gradient-to-tr from-blue-500 to-violet-500 flex items-center justify-center font-bold text-white shadow-lg">
                F
              </div>
              <span className="text-xl font-bold tracking-tight text-white">FinLens</span>
            </div>

            <div className="flex items-center gap-4">
              <a href="/" className="text-sm font-medium text-gray-300 hover:text-white transition-colors">Dashboard</a>
              <a href="/politicians" className="text-sm font-medium text-gray-300 hover:text-white transition-colors">Politicians</a>
              <a href="/insiders" className="text-sm font-medium text-gray-300 hover:text-white transition-colors">Insiders</a>
              <a href="/hedge-funds" className="text-sm font-medium text-gray-300 hover:text-white transition-colors">Hedge Funds</a>
              <button className="btn-primary text-sm px-4 py-2">Get Alerts</button>
            </div>
          </nav>

          <main className="flex-1 w-full max-w-7xl mx-auto p-6 md:p-8">
            {children}
          </main>

          <footer className="mt-auto py-8 text-center text-sm text-[var(--text-secondary)] border-t border-[var(--border-glass)]">
            <p>© 2026 FinLens. Smart money tracking.</p>
          </footer>
        </div>
      </body>
    </html>
  );
}
