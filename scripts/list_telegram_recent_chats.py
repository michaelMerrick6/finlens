from telegram_support import fetch_recent_updates, iter_update_chats, normalize_telegram_username


def main():
    seen: set[str] = set()
    rows: list[dict] = []

    for update in reversed(fetch_recent_updates()):
        message = update.get("message") or {}
        from_user = message.get("from") or {}
        if from_user.get("id") is not None:
            key = f"user:{from_user['id']}"
            if key not in seen:
                seen.add(key)
                rows.append(
                    {
                        "chat_id": message.get("chat", {}).get("id"),
                        "telegram_user_id": from_user.get("id"),
                        "username": normalize_telegram_username(from_user.get("username")),
                        "display_name": " ".join(
                            part for part in [from_user.get("first_name"), from_user.get("last_name")] if part
                        ).strip(),
                        "type": message.get("chat", {}).get("type"),
                    }
                )

        for chat in iter_update_chats(update):
            if chat.get("id") is None:
                continue
            key = f"chat:{chat['id']}"
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                {
                    "chat_id": chat.get("id"),
                    "telegram_user_id": None,
                    "username": normalize_telegram_username(chat.get("username")),
                    "display_name": chat.get("title") or chat.get("first_name") or "",
                    "type": chat.get("type"),
                }
            )

    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
