import { execFile } from 'node:child_process';
import path from 'node:path';
import { promisify } from 'node:util';

import { NextResponse } from 'next/server';

const execFileAsync = promisify(execFile);

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

const VALID_MODES = new Set(['activity', 'unusual', 'both']);

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const ticker = (searchParams.get('ticker') || '').trim();
    const politician = (searchParams.get('politician') || '').trim();
    const insider = (searchParams.get('insider') || '').trim();
    const mode = (searchParams.get('mode') || 'unusual').trim().toLowerCase();

    if (!ticker && !politician && !insider) {
      return NextResponse.json(
        { ok: false, error: 'Provide at least one of ticker, politician, or insider.' },
        { status: 400 }
      );
    }

    if (!VALID_MODES.has(mode)) {
      return NextResponse.json({ ok: false, error: 'Invalid mode.' }, { status: 400 });
    }

    const scriptPath = path.join(process.cwd(), 'scripts', 'preview_follow_matches.py');
    const args = [scriptPath, '--mode', mode, '--match-limit', '12'];

    if (ticker) {
      args.push('--ticker', ticker);
    }
    if (politician) {
      args.push('--politician', politician);
    }
    if (insider) {
      args.push('--insider', insider);
    }

    const { stdout, stderr } = await execFileAsync('python3', args, {
      cwd: process.cwd(),
      env: process.env,
      maxBuffer: 1024 * 1024 * 4,
    });

    const payload = JSON.parse(stdout) as Record<string, unknown>;
    return NextResponse.json({
      ok: true,
      preview: payload,
      stderr: stderr.trim() || null,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown follow preview error.';
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
