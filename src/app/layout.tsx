import type { Metadata } from 'next';
import Link from 'next/link';
import { Inter } from 'next/font/google';
import './globals.css';
import { HeaderAccountLink } from '@/components/HeaderAccountLink';

const inter = Inter({ subsets: ['latin'] });

export const metadata: Metadata = {
  title: 'Vail - Insider, Politician and 13F Signal Tracking',
  description: 'Monitor politician trades, insider filings, and fund positioning with private alerts and curated signal logic.',
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
                V
              </div>
              <span className="text-xl font-bold tracking-tight text-white">Vail</span>
            </div>

            <div className="flex items-center gap-4">
              <Link href="/" className="text-sm font-medium text-gray-300 hover:text-white transition-colors">Dashboard</Link>
              <Link href="/politicians" className="text-sm font-medium text-gray-300 hover:text-white transition-colors">Politicians</Link>
              <Link href="/insiders" className="text-sm font-medium text-gray-300 hover:text-white transition-colors">Insiders</Link>
              <Link href="/alerts" className="text-sm font-medium text-gray-300 hover:text-white transition-colors">Alerts</Link>
              <Link href="/ops" className="text-sm font-medium text-gray-300 hover:text-white transition-colors">Ops</Link>
              <Link href="/hedge-funds" className="text-sm font-medium text-gray-300 hover:text-white transition-colors">Hedge Funds</Link>
              <HeaderAccountLink />
            </div>
          </nav>

          <main className="flex-1 w-full max-w-7xl mx-auto p-6 md:p-8">
            {children}
          </main>

          <footer className="mt-auto py-8 text-center text-sm text-[var(--text-secondary)] border-t border-[var(--border-glass)]">
            <p>© 2026 Vail. Trustworthy public-market signals.</p>
          </footer>
        </div>
      </body>
    </html>
  );
}
