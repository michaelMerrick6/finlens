import type { Metadata } from 'next';
import { Inter } from 'next/font/google';
import './globals.css';

const inter = Inter({ subsets: ['latin'] });

export const metadata: Metadata = {
  title: 'Vail — Smart Money Signal Intelligence',
  description: 'Track politician trades, insider filings, and hedge fund positioning. Get notified the moment smart money moves.',
  manifest: '/site.webmanifest',
  icons: {
    icon: [
      { url: '/favicon.ico?v=vail-2', sizes: 'any' },
      { url: '/favicon-32.png?v=vail-2', sizes: '32x32', type: 'image/png' },
      { url: '/favicon-16.png?v=vail-2', sizes: '16x16', type: 'image/png' },
      { url: '/icon.svg?v=vail-2', type: 'image/svg+xml' },
      { url: '/icon-192.png?v=vail-2', sizes: '192x192', type: 'image/png' },
      { url: '/icon-512.png?v=vail-2', sizes: '512x512', type: 'image/png' },
    ],
    shortcut: '/favicon.ico?v=vail-2',
    apple: [{ url: '/apple-touch-icon.png?v=vail-2', sizes: '180x180', type: 'image/png' }],
  },
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className={`${inter.className} min-h-screen antialiased`}>
        {children}
      </body>
    </html>
  );
}
