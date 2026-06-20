const enablePipelineCrons = (process.env.ENABLE_VERCEL_PIPELINE_CRONS || '').trim() === '1';

const config = {
  $schema: 'https://openapi.vercel.sh/vercel.json',
  crons: enablePipelineCrons
    ? [
        { path: '/api/cron/github-dispatch/capture-congress', schedule: '10 * * * 1-5' },
        { path: '/api/cron/github-dispatch/capture-congress', schedule: '10 */6 * * 0,6' },
        { path: '/api/cron/github-dispatch/capture-insider', schedule: '20 * * * 1-5' },
        { path: '/api/cron/github-dispatch/capture-insider', schedule: '20 */6 * * 0,6' },
        { path: '/api/cron/github-dispatch/process-signals', schedule: '*/30 * * * 1-5' },
        { path: '/api/cron/github-dispatch/process-signals', schedule: '40 */4 * * 0,6' },
        { path: '/api/cron/github-dispatch/capture-13f', schedule: '50 6,18 * * *' },
        { path: '/api/cron/github-dispatch/daily-scraper', schedule: '0 5 * * *' },
      ]
    : [],
};

export default config;
