import 'server-only';

const RESEND_API_URL = 'https://api.resend.com/emails';

function resendApiKey() {
  return (process.env.RESEND_API_KEY || '').trim();
}

function resendFromEmail() {
  return (process.env.RESEND_FROM_EMAIL || '').trim();
}

function resendFromName() {
  return (process.env.RESEND_FROM_NAME || 'Vail').trim();
}

function resendReplyTo() {
  return (process.env.RESEND_REPLY_TO || '').trim();
}

export async function sendEmailMessage(input: {
  to: string;
  subject: string;
  text: string;
  html?: string;
}) {
  const apiKey = resendApiKey();
  const fromEmail = resendFromEmail();

  if (!apiKey) {
    throw new Error('Missing RESEND_API_KEY.');
  }
  if (!fromEmail) {
    throw new Error('Missing RESEND_FROM_EMAIL.');
  }

  const payload: Record<string, unknown> = {
    from: `${resendFromName()} <${fromEmail}>`,
    to: [input.to],
    subject: input.subject,
    text: input.text,
  };

  if (input.html) {
    payload.html = input.html;
  }

  const replyTo = resendReplyTo();
  if (replyTo) {
    payload.reply_to = replyTo;
  }

  const response = await fetch(RESEND_API_URL, {
    method: 'POST',
    cache: 'no-store',
    headers: {
      Authorization: `Bearer ${apiKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const body = (await response.json().catch(() => null)) as { message?: string; error?: { message?: string } } | null;
  if (!response.ok) {
    throw new Error(body?.message || body?.error?.message || `Resend send failed with status ${response.status}.`);
  }

  return body;
}
