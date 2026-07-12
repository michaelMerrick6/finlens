import { NextRequest, NextResponse } from 'next/server';

import { secretsMatch } from '@/lib/ops-access';

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

const WORKFLOW_FILES = {
  'capture-13f': 'capture-13f.yml',
  'capture-congress': 'capture-congress.yml',
  'capture-insider': 'capture-insider.yml',
  'daily-scraper': 'daily-scraper.yml',
  'process-signals': 'process-signals.yml',
} as const;

type WorkflowSlug = keyof typeof WORKFLOW_FILES;

type GitHubWorkflow = {
  id: number;
  name: string;
  path: string;
  state: string;
};

type GitHubWorkflowRun = {
  id: number;
  status: string;
  conclusion: string | null;
  html_url: string;
  created_at: string;
  updated_at: string;
  run_started_at: string | null;
};

function normalizeSecret(value: string | null) {
  return value?.trim() || '';
}

function authorized(request: NextRequest) {
  const expected = normalizeSecret(process.env.CRON_SECRET || process.env.VAIL_CRON_SECRET || null);
  if (!expected) {
    return process.env.NODE_ENV !== 'production';
  }

  const authHeader = request.headers.get('authorization') || '';
  const bearer = authHeader.startsWith('Bearer ') ? authHeader.slice('Bearer '.length).trim() : '';
  const headerSecret = request.headers.get('x-cron-secret')?.trim() || '';
  return secretsMatch(bearer, expected) || secretsMatch(headerSecret, expected);
}

function repoFullName() {
  const explicit = normalizeSecret(process.env.GITHUB_ACTIONS_REPO || null);
  if (explicit) {
    return explicit;
  }

  const owner = normalizeSecret(process.env.VERCEL_GIT_REPO_OWNER || null);
  const slug = normalizeSecret(process.env.VERCEL_GIT_REPO_SLUG || null);
  if (owner && slug) {
    return `${owner}/${slug}`;
  }

  return '';
}

function workflowRef() {
  return normalizeSecret(process.env.GITHUB_ACTIONS_REF || null) || 'main';
}

function githubToken() {
  return normalizeSecret(process.env.GITHUB_ACTIONS_DISPATCH_TOKEN || null);
}

function githubApiBaseUrl() {
  return normalizeSecret(process.env.GITHUB_ACTIONS_API_BASE_URL || null) || 'https://api.github.com';
}

async function githubRequest<T>(
  path: string,
  init?: RequestInit & { allowEmpty?: boolean },
): Promise<T | null> {
  const token = githubToken();
  const response = await fetch(`${githubApiBaseUrl()}${path}`, {
    ...init,
    headers: {
      Accept: 'application/vnd.github+json',
      Authorization: `Bearer ${token}`,
      'User-Agent': 'vail-cron-dispatch',
      ...(init?.headers || {}),
    },
    cache: 'no-store',
  });

  if (response.status === 204 && init?.allowEmpty) {
    return null;
  }

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`GitHub API ${response.status}: ${text.slice(0, 400)}`);
  }

  return (await response.json()) as T;
}

async function fetchWorkflow(repo: string, workflowFile: string) {
  return githubRequest<GitHubWorkflow>(`/repos/${repo}/actions/workflows/${workflowFile}`);
}

async function enableWorkflow(repo: string, workflowFile: string) {
  await githubRequest(`/repos/${repo}/actions/workflows/${workflowFile}/enable`, {
    method: 'PUT',
    allowEmpty: true,
  });
}

async function fetchRecentRuns(repo: string, workflowFile: string) {
  const payload = await githubRequest<{ workflow_runs: GitHubWorkflowRun[] }>(
    `/repos/${repo}/actions/workflows/${workflowFile}/runs?event=workflow_dispatch&per_page=5`,
  );
  return payload?.workflow_runs || [];
}

async function dispatchWorkflow(repo: string, workflowFile: string) {
  await githubRequest(`/repos/${repo}/actions/workflows/${workflowFile}/dispatches`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ ref: workflowRef() }),
    allowEmpty: true,
  });
}

function activeRun(runs: GitHubWorkflowRun[]) {
  return runs.find((run) => run.status !== 'completed') || null;
}

async function handleRequest(request: NextRequest, workflow: string) {
  if (!authorized(request)) {
    return NextResponse.json({ ok: false, error: 'Unauthorized.' }, { status: 401 });
  }

  const repo = repoFullName();
  const token = githubToken();
  if (!repo || !token) {
    return NextResponse.json(
      {
        ok: false,
        error: 'Missing GitHub dispatch configuration.',
        missing: {
          repo: !repo,
          token: !token,
        },
      },
      { status: 500 },
    );
  }

  if (!Object.prototype.hasOwnProperty.call(WORKFLOW_FILES, workflow)) {
    return NextResponse.json(
      {
        ok: false,
        error: 'Unsupported workflow.',
        supportedWorkflows: Object.keys(WORKFLOW_FILES),
      },
      { status: 404 },
    );
  }

  const workflowFile = WORKFLOW_FILES[workflow as WorkflowSlug];
  const before = await fetchWorkflow(repo, workflowFile);
  const previousState = before?.state || 'unknown';
  let enabled = false;

  if (before && before.state !== 'active') {
    await enableWorkflow(repo, workflowFile);
    enabled = true;
  }

  const runsBeforeDispatch = await fetchRecentRuns(repo, workflowFile);
  const inflight = activeRun(runsBeforeDispatch);
  if (inflight) {
    return NextResponse.json({
      ok: true,
      workflow,
      workflowFile,
      repo,
      previousState,
      enabled,
      dispatched: false,
      reason: 'active_run_exists',
      activeRun: inflight,
    });
  }

  await dispatchWorkflow(repo, workflowFile);

  return NextResponse.json({
    ok: true,
    workflow,
    workflowFile,
    repo,
    previousState,
    enabled,
    dispatched: true,
    ref: workflowRef(),
  });
}

export async function GET(
  request: NextRequest,
  context: { params: Promise<{ workflow: string }> },
) {
  const { workflow } = await context.params;
  return handleRequest(request, workflow);
}

export async function POST(
  request: NextRequest,
  context: { params: Promise<{ workflow: string }> },
) {
  const { workflow } = await context.params;
  return handleRequest(request, workflow);
}
