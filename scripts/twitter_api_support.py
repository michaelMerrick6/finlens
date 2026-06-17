import os
from typing import Any

import requests
from requests_oauthlib import OAuth1


API_BASE_URL = os.environ.get("X_API_BASE_URL", "https://api.x.com").strip().rstrip("/")
REQUEST_TIMEOUT_SECONDS = int(os.environ.get("X_API_TIMEOUT_SECONDS", "30"))
DEFAULT_USER_AGENT = os.environ.get("X_API_USER_AGENT", "Vail Signals/1.0").strip() or "Vail Signals/1.0"


class TwitterApiError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, response_body: Any | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body


def _env(*names: str) -> str:
    for name in names:
        value = str(os.environ.get(name) or "").strip()
        if value:
            return value
    return ""


def twitter_posting_enabled() -> bool:
    return _env("TWITTER_POSTING_ENABLED", "X_POSTING_ENABLED").lower() in {"1", "true", "yes", "on"}


def oauth1_configured() -> bool:
    return all(
        [
            _env("X_API_KEY", "TWITTER_API_KEY"),
            _env("X_API_SECRET", "TWITTER_API_SECRET"),
            _env("X_ACCESS_TOKEN", "TWITTER_ACCESS_TOKEN"),
            _env("X_ACCESS_TOKEN_SECRET", "TWITTER_ACCESS_TOKEN_SECRET"),
        ]
    )


def oauth2_user_configured() -> bool:
    return bool(_env("X_USER_ACCESS_TOKEN", "TWITTER_USER_ACCESS_TOKEN"))


def configured_auth_mode() -> str | None:
    if oauth2_user_configured():
        return "oauth2_user"
    if oauth1_configured():
        return "oauth1_user"
    return None


def twitter_posting_config() -> dict[str, Any]:
    auth_mode = configured_auth_mode()
    return {
        "enabled": twitter_posting_enabled(),
        "configured": bool(auth_mode),
        "auth_mode": auth_mode,
    }


def _default_headers() -> dict[str, str]:
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": DEFAULT_USER_AGENT,
    }


def _build_request_kwargs() -> dict[str, Any]:
    auth_mode = configured_auth_mode()
    if auth_mode == "oauth2_user":
        return {
            "headers": {
                **_default_headers(),
                "Authorization": f"Bearer {_env('X_USER_ACCESS_TOKEN', 'TWITTER_USER_ACCESS_TOKEN')}",
            }
        }
    if auth_mode == "oauth1_user":
        return {
            "headers": _default_headers(),
            "auth": OAuth1(
                _env("X_API_KEY", "TWITTER_API_KEY"),
                _env("X_API_SECRET", "TWITTER_API_SECRET"),
                _env("X_ACCESS_TOKEN", "TWITTER_ACCESS_TOKEN"),
                _env("X_ACCESS_TOKEN_SECRET", "TWITTER_ACCESS_TOKEN_SECRET"),
            ),
        }
    raise TwitterApiError(
        "X posting credentials are missing. Configure either X_USER_ACCESS_TOKEN or the X OAuth 1.0a credential set."
    )


def _error_message(response: requests.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        text = (response.text or "").strip()
        return text or f"HTTP {response.status_code}"

    if isinstance(payload, dict):
        errors = payload.get("errors") or []
        if isinstance(errors, list) and errors:
            first = errors[0]
            if isinstance(first, dict):
                detail = str(first.get("detail") or first.get("message") or "").strip()
                if detail:
                    return detail
        detail = str(payload.get("detail") or payload.get("title") or "").strip()
        if detail:
            return detail
    return f"HTTP {response.status_code}"


def _request(method: str, path: str, *, json_body: dict[str, Any] | None = None) -> dict[str, Any]:
    request_kwargs = _build_request_kwargs()
    response = requests.request(
        method,
        f"{API_BASE_URL}{path}",
        json=json_body,
        timeout=REQUEST_TIMEOUT_SECONDS,
        **request_kwargs,
    )
    if not response.ok:
        raise TwitterApiError(
            f"X API request failed: {_error_message(response)}",
            status_code=response.status_code,
            response_body=response.text,
        )
    try:
        return response.json()
    except ValueError as exc:
        raise TwitterApiError("X API returned a non-JSON response.", status_code=response.status_code) from exc


def get_authenticated_user() -> dict[str, Any]:
    payload = _request("GET", "/2/users/me")
    data = payload.get("data") or {}
    if not isinstance(data, dict) or not data:
        raise TwitterApiError("X API did not return an authenticated user.")
    return data


def create_post(text: str) -> dict[str, Any]:
    clean_text = str(text or "").strip()
    if not clean_text:
        raise TwitterApiError("Cannot publish an empty post.")
    payload = _request("POST", "/2/tweets", json_body={"text": clean_text})
    data = payload.get("data") or {}
    if not isinstance(data, dict) or not str(data.get("id") or "").strip():
        raise TwitterApiError("X API did not return a post id.")
    return data
