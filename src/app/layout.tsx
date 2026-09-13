import SiteAnalytics from "@/components/site-analytics";
import type { Metadata } from "next";
import { DM_Sans, Newsreader } from "next/font/google";
import { AccountProvider } from "@/components/account-provider";
import { SiteHeader, SiteFooter } from "@/components/site-shell";
import "./globals.css";
const sans = DM_Sans({
  subsets: ["latin"],
  display: "swap",
  variable: "--font-sans",
});
const serif = Newsreader({
  subsets: ["latin"],
  style: ["normal", "italic"],
  display: "swap",
  variable: "--font-serif",
});
export const metadata: Metadata = {
  title: {
    default: "Vail — A clearer view of congressional trades",
    template: "%s · Vail",
  },
  description:
    "Explore congressional trading disclosures, track politicians and stay informed when new activity is disclosed.",
};
export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className={`${sans.variable} ${serif.variable}`}>
      <body>
        <a className="skip-link" href="#main">
          Skip to content
        </a>
        <AccountProvider
          url={
            process.env.NEXT_PUBLIC_SUPABASE_URL ||
            process.env.SUPABASE_URL ||
            ""
          }
          publicKey={
            process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ||
            process.env.SUPABASE_ANON_KEY ||
            ""
          }
        >
          <SiteHeader />
          <SiteAnalytics />
            {children}
          <SiteFooter />
        </AccountProvider>
      </body>
    </html>
  );
}
