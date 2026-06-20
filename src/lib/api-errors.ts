import 'server-only';

export function routeErrorMessage(error: unknown, fallback: string, context: string) {
  if (process.env.NODE_ENV === 'production') {
    console.error(`[${context}]`, error);
    return fallback;
  }

  return error instanceof Error ? error.message : fallback;
}
