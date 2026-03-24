import os
import re

import requests


def telegram_bot_token() -> str:
    return os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()


def normalize_telegram_username(value: str | None) -> str:
    candidate = (value or "").strip().lower()
    if candidate.startswith("@"):
        candidate = candidate[1:]
    return re.sub(r"[^a-z0-9_]", "", candidate)


def telegram_api(method: str, *, json: dict | None = None, params: dict | None = None, timeout: int = 15) -> dict:
    token = telegram_bot_token()
    if not token:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN.")

    response = requests.post(
        f"https://api.telegram.org/bot{token}/{method}",
        json=json,
        params=params,
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    if not payload.get("ok"):
        raise RuntimeError(f"Telegram API error: {payload}")
    return payload


def fetch_recent_updates(limit: int = 100) -> list[dict]:
    payload = telegram_api("getUpdates", params={"limit": limit, "timeout": 0})
    return payload.get("result") or []


def iter_update_chats(update: dict):
    message = update.get("message") or {}
    if message.get("chat"):
        yield message["chat"]

    edited_message = update.get("edited_message") or {}
    if edited_message.get("chat"):
        yield edited_message["chat"]

    chat_member = update.get("my_chat_member") or {}
    if chat_member.get("chat"):
        yield chat_member["chat"]

    callback_query = update.get("callback_query") or {}
    callback_message = callback_query.get("message") or {}
    if callback_message.get("chat"):
        yield callback_message["chat"]


def resolve_chat_id(destination_hint: str) -> str:
    candidate = (destination_hint or "").strip()
    if not candidate:
        raise RuntimeError("Missing Telegram destination.")
    if re.fullmatch(r"-?\d+", candidate):
        return candidate

    username = normalize_telegram_username(candidate)
    if not username:
        raise RuntimeError("Invalid Telegram username.")

    for update in reversed(fetch_recent_updates()):
        message = update.get("message") or {}
        from_user = message.get("from") or {}
        from_username = normalize_telegram_username(from_user.get("username"))
        if from_username == username and message.get("chat", {}).get("id") is not None:
            return str(message["chat"]["id"])

        for chat in iter_update_chats(update):
            chat_username = normalize_telegram_username(chat.get("username"))
            if chat_username == username and chat.get("id") is not None:
                return str(chat["id"])

    raise RuntimeError(
        f"Could not resolve Telegram user '{destination_hint}'. Have them start the bot and send a message first."
    )


def send_message(chat_id: str, text: str) -> None:
    telegram_api(
        "sendMessage",
        json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
        },
    )
