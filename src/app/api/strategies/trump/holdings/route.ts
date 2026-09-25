import { identifiedStockHoldings, trumpActivity, trumpAnnual, trumpDjt, trumpFirstDisclosure, trumpModel, trumpStockReview } from '@/lib/strategies/trump';

export async function GET(request: Request) {
  const filing = new URL(request.url).searchParams.get('filing');
  if (filing && filing !== '2015' && filing !== '2026') {
    return Response.json({ error: 'Available filing years are 2015 and 2026.' }, { status: 400 });
  }
  if (filing === '2015') {
    return Response.json({ ...trumpFirstDisclosure, coverage: {
      currentHoldingsVerified: false, allocationAvailable: false,
      scope: trumpModel.scope, historicalReturnsAvailable: false,
      note: 'Historical Part 6 evidence only. Stock candidates require instrument and price review. No current holdings, DJT stake, or portfolio allocation is inferred for 2015.',
    } }, { headers: {
      'Content-Disposition': 'attachment; filename="trump-disclosure-2015.json"',
      'Cache-Control': 'public, max-age=3600',
    } });
  }
  return Response.json({ ...trumpAnnual, stockIndex: {
    complete: trumpStockReview.complete, holdings: identifiedStockHoldings(),
    estimateMethod: 'estimatedReportedValue is the midpoint of the combined bounded disclosure range in USD. It is an estimate at the annual snapshot date, not an exact balance or current market value. Open-ended ranges return null.',
  }, reviewedActivity: trumpActivity, djtOwnershipEvidence: trumpDjt, coverage: {
    currentHoldingsVerified: false,
    scope: trumpModel.scope,
    includesDjt: trumpModel.includesDjt,
    note: 'Annual source rows, a partial stock identity index, and selected reviewed transactions. Subsequent transaction reports have not been reconciled. Not a current model allocation.',
  } }, { headers: {
    'Content-Disposition': 'attachment; filename="trump-disclosed-holdings-2025.json"',
    'Cache-Control': 'public, max-age=3600',
  } });
}
