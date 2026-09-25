import { analysisWindow, type AnalysisPeriod } from './analysis-overview';

export function analysisRequest(params: URLSearchParams, snapshot = false) {
  const period = params.get('period') || '30';
  if (!['7', '30', 'ytd', 'year'].includes(period)) throw new Error('Choose a supported period.');
  const today = new Date().toISOString().slice(0, 10);
  const end = snapshot ? params.get('end') || today : today;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(end) || !Number.isFinite(Date.parse(end)) || new Date(end).toISOString().slice(0, 10) !== end || end > today) {
    throw new Error('Choose a valid end date.');
  }
  return { ...analysisWindow(period as AnalysisPeriod, new Date(`${end}T00:00:00Z`)),
    basis: params.get('basis') === 'trade' ? 'transaction_date' : 'published_date',
    instrument: params.get('instrument') === 'options' ? 'options' : 'stocks' };
}
