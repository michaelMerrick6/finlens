import { NextResponse } from 'next/server';

import { constructStripeEvent, syncBillingFromStripeEvent } from '@/lib/billing-server';

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

export async function POST(request: Request) {
  try {
    const payload = await request.text();
    const signature = request.headers.get('stripe-signature');
    const event = await constructStripeEvent(signature, payload);
    const result = await syncBillingFromStripeEvent(event);
    return NextResponse.json({ ok: true, ...result });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Stripe webhook failed.';
    return NextResponse.json({ ok: false, error: message }, { status: 400 });
  }
}
