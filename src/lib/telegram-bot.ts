import 'server-only';

const TELEGRAM_BASE_URL = 'https://api.telegram.org';

type TelegramChat = {
  id?: number;
  username?: string;
};

type TelegramUpdate = {
  message?: {
    chat?: TelegramChat;
    from?: {
      username?: string;
    };
  };
  edited_message?: {
    chat?: TelegramChat;
  };
  my_chat_member?: {
    chat?: TelegramChat;
  };
  callback_query?: {
    message?: {
      chat?: TelegramChat;
    };
  };
};

function telegramBotToken() {
  return (process.env.TELEGRAM_BOT_TOKEN || '').trim();
}

export function normalizeTelegramUsername(value: string | null | undefined) {
  const candidate = (value || '').trim().toLowerCase();
  return candidate.replace(/^@/, '').replace(/[^a-z0-9_]/g, '');
}

async function telegramApi(method: string, init?: RequestInit & { params?: URLSearchParams }) {
  const token = telegramBotToken();
  if (!token) {
    throw new Error('Missing TELEGRAM_BOT_TOKEN.');
  }

  const url = new URL(`${TELEGRAM_BASE_URL}/bot${token}/${method}`);
  if (init?.params) {
    url.search = init.params.toString();
  }

  const response = await fetch(url, {
    method: 'POST',
    cache: 'no-store',
    headers: init?.body ? { 'Content-Type': 'application/json' } : undefined,
    body: init?.body,
  });

  if (!response.ok) {
    throw new Error(`Telegram API ${method} failed with status ${response.status}.`);
  }

  const payload = (await response.json()) as { ok?: boolean; result?: unknown; description?: string };
  if (!payload.ok) {
    throw new Error(payload.description || `Telegram API ${method} failed.`);
  }

  return payload.result;
}

export async function getTelegramBotUsername() {
  try {
    const result = (await telegramApi('getMe')) as { username?: string } | null;
    return result?.username || null;
  } catch {
    return null;
  }
}

export async function sendTelegramMessage(chatId: string, text: string) {
  const normalizedChatId = (chatId || '').trim();
  if (!normalizedChatId) {
    throw new Error('Missing Telegram chat id.');
  }

  await telegramApi('sendMessage', {
    body: JSON.stringify({
      chat_id: normalizedChatId,
      text,
      disable_web_page_preview: true,
    }),
  });
}

async function fetchRecentUpdates(limit = 100) {
  const params = new URLSearchParams({
    limit: String(limit),
    timeout: '0',
  });
  const result = (await telegramApi('getUpdates', { params })) as TelegramUpdate[];
  return result || [];
}

function iterUpdateChats(update: TelegramUpdate) {
  const chats: TelegramChat[] = [];
  if (update.message?.chat) chats.push(update.message.chat);
  if (update.edited_message?.chat) chats.push(update.edited_message.chat);
  if (update.my_chat_member?.chat) chats.push(update.my_chat_member.chat);
  if (update.callback_query?.message?.chat) chats.push(update.callback_query.message.chat);
  return chats;
}

export async function resolveTelegramChatId(destinationHint: string) {
  const candidate = (destinationHint || '').trim();
  if (!candidate) {
    throw new Error('Missing Telegram destination.');
  }

  if (/^-?\d+$/.test(candidate)) {
    return candidate;
  }

  const username = normalizeTelegramUsername(candidate);
  if (!username) {
    throw new Error('Invalid Telegram username.');
  }

  const updates = await fetchRecentUpdates();
  for (const update of [...updates].reverse()) {
    const fromUsername = normalizeTelegramUsername(update.message?.from?.username);
    const fromChatId = update.message?.chat?.id;
    if (fromUsername === username && typeof fromChatId === 'number') {
      return String(fromChatId);
    }

    for (const chat of iterUpdateChats(update)) {
      const chatUsername = normalizeTelegramUsername(chat.username);
      if (chatUsername === username && typeof chat.id === 'number') {
        return String(chat.id);
      }
    }
  }

  throw new Error(`Could not resolve Telegram user '${destinationHint}'. Start the bot and send it a message first.`);
}
