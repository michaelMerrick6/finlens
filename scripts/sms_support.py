import base64
import os
import re

import requests


TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "").strip()
TWILIO_FROM_PHONE = os.environ.get("TWILIO_FROM_PHONE", "").strip()


def normalize_phone_number(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.startswith("+"):
        digits = re.sub(r"\D", "", raw[1:])
        return f"+{digits}" if digits else ""
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return f"+{digits}" if digits else ""


def sms_configured() -> bool:
    return bool(TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and normalize_phone_number(TWILIO_FROM_PHONE))


def send_sms(destination: str, body: str) -> None:
    to_phone = normalize_phone_number(destination)
    from_phone = normalize_phone_number(TWILIO_FROM_PHONE)
    if not TWILIO_ACCOUNT_SID or not TWILIO_AUTH_TOKEN or not from_phone:
        raise RuntimeError("Missing TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, or TWILIO_FROM_PHONE.")
    if not to_phone:
        raise RuntimeError("Missing SMS destination.")

    auth = base64.b64encode(f"{TWILIO_ACCOUNT_SID}:{TWILIO_AUTH_TOKEN}".encode("utf-8")).decode("ascii")
    response = requests.post(
        f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Messages.json",
        headers={"Authorization": f"Basic {auth}"},
        data={"From": from_phone, "To": to_phone, "Body": body},
        timeout=20,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Twilio SMS error {response.status_code}: {response.text}")
