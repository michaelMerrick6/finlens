import 'server-only';

const TWILIO_API_BASE = 'https://api.twilio.com/2010-04-01';

function twilioAccountSid() {
  return (process.env.TWILIO_ACCOUNT_SID || '').trim();
}

function twilioAuthToken() {
  return (process.env.TWILIO_AUTH_TOKEN || '').trim();
}

function twilioFromPhone() {
  return normalizePhoneNumber(process.env.TWILIO_FROM_PHONE || '');
}

export function normalizePhoneNumber(value: string | null | undefined) {
  const raw = String(value || '').trim();
  if (!raw) {
    return '';
  }
  if (raw.startsWith('+')) {
    const digits = raw.slice(1).replace(/\D/g, '');
    return digits ? `+${digits}` : '';
  }
  const digits = raw.replace(/\D/g, '');
  if (digits.length === 10) {
    return `+1${digits}`;
  }
  if (digits.length === 11 && digits.startsWith('1')) {
    return `+${digits}`;
  }
  return digits ? `+${digits}` : '';
}

export function isSmsConfigured() {
  return Boolean(twilioAccountSid() && twilioAuthToken() && twilioFromPhone());
}

export async function sendSmsMessage(to: string, body: string) {
  const accountSid = twilioAccountSid();
  const authToken = twilioAuthToken();
  const from = twilioFromPhone();
  const normalizedTo = normalizePhoneNumber(to);

  if (!accountSid || !authToken || !from) {
    throw new Error('Missing TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, or TWILIO_FROM_PHONE.');
  }
  if (!normalizedTo) {
    throw new Error('Missing SMS destination.');
  }

  const params = new URLSearchParams({
    From: from,
    To: normalizedTo,
    Body: body,
  });

  const response = await fetch(`${TWILIO_API_BASE}/Accounts/${accountSid}/Messages.json`, {
    method: 'POST',
    headers: {
      Authorization: `Basic ${Buffer.from(`${accountSid}:${authToken}`).toString('base64')}`,
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body: params,
  });

  if (!response.ok) {
    const text = await response.text().catch(() => '');
    throw new Error(`Twilio SMS failed with status ${response.status}: ${text}`);
  }
}
