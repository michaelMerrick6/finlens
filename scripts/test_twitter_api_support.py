import os
from contextlib import contextmanager

from twitter_api_support import configured_auth_mode, twitter_posting_config, twitter_posting_enabled


@contextmanager
def patch_env(values: dict[str, str | None]):
    original = {key: os.environ.get(key) for key in values}
    try:
        for key, value in values.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        yield
    finally:
        for key, value in original.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def test_oauth2_user_token_preferred():
    with patch_env(
        {
            "X_USER_ACCESS_TOKEN": "user-token",
            "X_API_KEY": "api-key",
            "X_API_SECRET": "api-secret",
            "X_ACCESS_TOKEN": "access-token",
            "X_ACCESS_TOKEN_SECRET": "access-secret",
        }
    ):
        assert configured_auth_mode() == "oauth2_user"


def test_oauth1_selected_when_user_bearer_missing():
    with patch_env(
        {
            "X_USER_ACCESS_TOKEN": None,
            "X_API_KEY": "api-key",
            "X_API_SECRET": "api-secret",
            "X_ACCESS_TOKEN": "access-token",
            "X_ACCESS_TOKEN_SECRET": "access-secret",
        }
    ):
        assert configured_auth_mode() == "oauth1_user"


def test_posting_enable_flag_and_config_summary():
    with patch_env(
        {
            "TWITTER_POSTING_ENABLED": "1",
            "X_USER_ACCESS_TOKEN": "user-token",
            "X_API_KEY": None,
            "X_API_SECRET": None,
            "X_ACCESS_TOKEN": None,
            "X_ACCESS_TOKEN_SECRET": None,
        }
    ):
        assert twitter_posting_enabled() is True
        summary = twitter_posting_config()
        assert summary["enabled"] is True
        assert summary["configured"] is True
        assert summary["auth_mode"] == "oauth2_user"


if __name__ == "__main__":
    test_oauth2_user_token_preferred()
    test_oauth1_selected_when_user_bearer_missing()
    test_posting_enable_flag_and_config_summary()
    print("twitter api support tests passed")
