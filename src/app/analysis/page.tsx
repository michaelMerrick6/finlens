import { AnalysisPage } from '@/components/analysis-page';
import { analysisWindow } from '@/lib/analysis-overview';
import { loadAnalysisSummary } from '@/lib/analysis-server';
export const metadata={title:'Activity Overview'};
export const revalidate = 300;
export default async function Page() {
  const { start, end } = analysisWindow('30');
  const initialData = await loadAnalysisSummary(start, end, 'published_date', 'stocks');
  return <main id="main" className="container"><AnalysisPage initialData={initialData}/></main>;
}
