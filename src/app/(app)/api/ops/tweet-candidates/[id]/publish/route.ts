import { execFile } from 'node:child_process';
import path from 'node:path';
import { promisify } from 'node:util';

import { NextResponse } from 'next/server';

import { fetchTweetCandidateById } from '@/lib/tweet-candidates';

const execFileAsync = promisify(execFile);

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

export async function POST(_request: Request, context: { params: Promise<{ id: string }> }) {
  try {
    const { id } = await context.params;
    const candidate = await fetchTweetCandidateById(id);
    if (!candidate) {
      return NextResponse.json({ ok: false, error: 'Broadcast candidate not found.' }, { status: 404 });
    }

    let scriptPath = '';
    let args: string[] = [];
    let blockedConfigError = '';
    if (candidate.channel === 'twitter') {
      scriptPath = path.join(process.cwd(), 'scripts', 'dispatch_twitter_posts.py');
      args = [scriptPath, '--id', id, '--batch-size', '1'];
      blockedConfigError = 'X posting is not configured.';
    } else if (candidate.channel === 'discord_premium') {
      scriptPath = path.join(process.cwd(), 'scripts', 'dispatch_discord_broadcasts.py');
      args = [scriptPath, '--id', id];
      blockedConfigError = 'Discord premium broadcast is not configured.';
    } else {
      return NextResponse.json({ ok: false, error: `Unsupported broadcast channel: ${candidate.channel}` }, { status: 400 });
    }

    const { stdout, stderr } = await execFileAsync('python3', args, {
      cwd: process.cwd(),
      env: process.env,
      maxBuffer: 1024 * 1024 * 4,
    });

    const summaryLine = stdout
      .split('\n')
      .find((line) => line.startsWith('SUMMARY_JSON:'))
      ?.replace('SUMMARY_JSON:', '')
      .trim();
    const summary = summaryLine ? JSON.parse(summaryLine) : null;
    const failureMessage =
      summary?.failure_details?.[0]?.error ||
      summary?.reason ||
      stderr.trim() ||
      'Broadcast publish failed.';

    if (candidate.channel === 'twitter' && summary?.x_posting_enabled === false) {
      return NextResponse.json({ ok: false, error: 'X posting is disabled. Enable TWITTER_POSTING_ENABLED first.', summary }, { status: 503 });
    }
    if (candidate.channel === 'twitter' && summary?.x_configured === false) {
      return NextResponse.json({ ok: false, error: 'X posting is not configured. Add your X credentials first.', summary }, { status: 503 });
    }
    if (candidate.channel === 'discord_premium' && summary?.discord_broadcast_configured === false) {
      return NextResponse.json({ ok: false, error: blockedConfigError, summary }, { status: 503 });
    }
    if ((summary?.candidates_seen ?? 0) === 0) {
      return NextResponse.json({ ok: false, error: 'No approved broadcast candidate found for that id.', summary }, { status: 404 });
    }
    if ((summary?.candidates_posted ?? 0) < 1) {
      return NextResponse.json({ ok: false, error: failureMessage, summary, stderr: stderr.trim() || null }, { status: 500 });
    }

    return NextResponse.json({
      ok: true,
      channel: candidate.channel,
      summary,
      stdout,
      stderr: stderr.trim() || null,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown X publish error.';
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
