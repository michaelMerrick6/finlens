const LOGO_DEV_TOKEN = process.env.NEXT_PUBLIC_LOGO_DEV_TOKEN?.trim() || '';

export const COMPANY_LOGO_PROVIDER_NAME = 'Logo.dev';
export const COMPANY_LOGO_PROVIDER_URL = 'https://logo.dev';

export function hasCompanyLogoSupport() {
  return Boolean(LOGO_DEV_TOKEN);
}

export function getTickerLogoUrl(ticker: string, size = 64) {
  const normalizedTicker = ticker.trim().toUpperCase();
  if (!normalizedTicker || !LOGO_DEV_TOKEN) {
    return null;
  }

  const url = new URL(`https://img.logo.dev/ticker/${encodeURIComponent(normalizedTicker)}`);
  url.searchParams.set('token', LOGO_DEV_TOKEN);
  url.searchParams.set('size', String(size));
  url.searchParams.set('format', 'png');
  url.searchParams.set('fallback', '404');
  url.searchParams.set('retina', 'true');
  return url.toString();
}
