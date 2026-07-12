import os
from unittest.mock import patch

from shared_utils import positive_int_env


def test_positive_int_env() -> None:
    with patch.dict(os.environ, {"TEST_LIMIT": "42"}):
        assert positive_int_env("TEST_LIMIT", 10) == 42

    for invalid in ("", "invalid", "0", "-3"):
        with patch.dict(os.environ, {"TEST_LIMIT": invalid}):
            assert positive_int_env("TEST_LIMIT", 10) == 10


if __name__ == "__main__":
    test_positive_int_env()
    print("shared utility tests passed")
