import { spawn } from 'node:child_process';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

import { NextResponse } from 'next/server';

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

const QUEUE_STATE_PATH = path.join(process.cwd(), 'artifacts', 'ops', 'tweet_candidate_queue_status.json');
const QUEUE_LOG_DIR = path.join(process.cwd(), 'artifacts', 'ops');

type QueueState = {
  status: 'idle' | 'running' | 'completed' | 'failed';
  started_at?: string;
  finished_at?: string;
  pid?: number;
  log_path?: string;
  exit_code?: number;
  current_step?: string;
  progress_percent?: number;
  stderr?: string | null;
  summary?: Record<string, unknown> | null;
};

async function readQueueState(): Promise<QueueState> {
  try {
    const raw = await readFile(QUEUE_STATE_PATH, 'utf8');
    return JSON.parse(raw) as QueueState;
  } catch {
    return { status: 'idle' };
  }
}

function processAlive(pid: number | undefined) {
  if (!pid) {
    return false;
  }
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

async function normalizeQueueState() {
  const state = await readQueueState();
  if (state.status === 'running' && !processAlive(state.pid)) {
    return {
      ...state,
      status: 'failed' as const,
      finished_at: state.finished_at || new Date().toISOString(),
      stderr: state.stderr || 'Queue runner exited without writing a final state.',
    };
  }
  return state;
}

export async function GET() {
  const state = await normalizeQueueState();
  return NextResponse.json({
    ok: true,
    state,
    running: state.status === 'running',
  });
}

export async function POST() {
  try {
    const currentState = await normalizeQueueState();
    if (currentState.status === 'running') {
      return NextResponse.json({
        ok: true,
        started: false,
        running: true,
        state: currentState,
      });
    }

    await mkdir(QUEUE_LOG_DIR, { recursive: true });
    const stamp = new Date().toISOString().replace(/[:.]/g, '-');
    const logPath = path.join(QUEUE_LOG_DIR, `tweet_candidate_queue_${stamp}.log`);
    const wrapperPath = path.join(process.cwd(), 'scripts', 'run_queue_tweet_candidates_background.py');

    const child = spawn(
      'python3',
      [wrapperPath, '--state-path', QUEUE_STATE_PATH, '--log-path', logPath, '--repo-root', process.cwd()],
      {
        cwd: process.cwd(),
        env: process.env,
        detached: true,
        stdio: 'ignore',
      }
    );
    child.unref();

    const optimisticState: QueueState = {
      status: 'running',
      started_at: new Date().toISOString(),
      pid: child.pid,
      log_path: logPath,
      current_step: 'Starting queue worker',
      progress_percent: 0,
      summary: null,
      stderr: null,
    };
    await writeFile(QUEUE_STATE_PATH, JSON.stringify(optimisticState, null, 2));

    return NextResponse.json({
      ok: true,
      started: true,
      running: true,
      state: optimisticState,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown tweet queue error.';
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
