import { NextResponse } from 'next/server';

import { updateTweetCandidate } from '@/lib/tweet-candidates';

export const dynamic = 'force-dynamic';

export async function PATCH(request: Request, context: { params: Promise<{ id: string }> }) {
  try {
    const { id } = await context.params;
    const body = (await request.json()) as {
      status?: 'pending_review' | 'approved' | 'rejected' | 'posted';
      review_notes?: string;
      reviewed_by?: string;
      external_post_id?: string;
    };

    if (!body?.status) {
      return NextResponse.json({ ok: false, error: 'Missing status.' }, { status: 400 });
    }

    const row = await updateTweetCandidate(id, {
      status: body.status,
      review_notes: body.review_notes,
      reviewed_by: body.reviewed_by,
      external_post_id: body.external_post_id,
    });

    return NextResponse.json({ ok: true, row });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown tweet review error.';
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
