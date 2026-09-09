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
      draft_text?: string;
      title?: string;
    };

    if (
      !body?.status &&
      !Object.prototype.hasOwnProperty.call(body || {}, 'review_notes') &&
      !Object.prototype.hasOwnProperty.call(body || {}, 'draft_text') &&
      !Object.prototype.hasOwnProperty.call(body || {}, 'title')
    ) {
      return NextResponse.json({ ok: false, error: 'Missing candidate update fields.' }, { status: 400 });
    }

    const row = await updateTweetCandidate(id, {
      status: body.status,
      review_notes: body.review_notes,
      reviewed_by: body.reviewed_by,
      external_post_id: body.external_post_id,
      draft_text: body.draft_text,
      title: body.title,
    });

    return NextResponse.json({ ok: true, row });
  } catch (error) {
    const message =
      error instanceof Error
        ? error.message
        : typeof error === 'object' && error !== null
          ? JSON.stringify(error)
          : 'Unknown tweet review error.';
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
