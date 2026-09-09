import { execFile } from 'node:child_process';
import path from 'node:path';
import { promisify } from 'node:util';

import { NextResponse } from 'next/server';

const execFileAsync = promisify(execFile);

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

export async function POST() {
  try {
    const scriptPath = path.join(process.cwd(), 'scripts', 'send_discord_test.py');
    const { stdout, stderr } = await execFileAsync('python3', [scriptPath], {
      cwd: process.cwd(),
      env: process.env,
      maxBuffer: 1024 * 1024 * 4,
    });

    return NextResponse.json({
      ok: true,
      stdout,
      stderr: stderr.trim() || null,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown Discord test error.';
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
