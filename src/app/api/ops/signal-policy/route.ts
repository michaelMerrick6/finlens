import { NextResponse } from 'next/server';

import { readSignalPolicy, writeSignalPolicyFromText } from '@/lib/signal-policy';

export const dynamic = 'force-dynamic';

export async function GET() {
  try {
    const policy = await readSignalPolicy();
    return NextResponse.json({ ok: true, policy });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown signal policy error.';
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}

export async function POST(request: Request) {
  try {
    const body = (await request.json()) as { rawText?: string };
    const rawText = body?.rawText;

    if (!rawText || typeof rawText !== 'string') {
      return NextResponse.json({ ok: false, error: 'Missing rawText payload.' }, { status: 400 });
    }

    const policy = await writeSignalPolicyFromText(rawText);
    return NextResponse.json({ ok: true, policy });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown signal policy error.';
    return NextResponse.json({ ok: false, error: message }, { status: 400 });
  }
}
