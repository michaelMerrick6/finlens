import { execFile } from 'node:child_process';
import path from 'node:path';
import { promisify } from 'node:util';

import { NextResponse } from 'next/server';

const execFileAsync = promisify(execFile);

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

export async function POST() {
  try {
    const scriptPath = path.join(process.cwd(), 'scripts', 'queue_tweet_candidates.py');
    const { stdout, stderr } = await execFileAsync('python3', [scriptPath], {
      cwd: process.cwd(),
      env: process.env,
      maxBuffer: 1024 * 1024 * 4,
    });

    const summaryLine = stdout
      .split('\n')
      .find((line) => line.startsWith('SUMMARY_JSON:'))
      ?.replace('SUMMARY_JSON:', '')
      .trim();

    return NextResponse.json({
      ok: true,
      summary: summaryLine ? JSON.parse(summaryLine) : null,
      stdout,
      stderr: stderr.trim() || null,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown tweet queue error.';
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
