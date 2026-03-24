import os
from datetime import date, datetime
from zoneinfo import ZoneInfo


CONGRESS_TIMEZONE = os.environ.get("VAIL_CONGRESS_TIMEZONE", "America/New_York")


def congress_now() -> datetime:
    return datetime.now(ZoneInfo(CONGRESS_TIMEZONE))


def congress_today() -> date:
    return congress_now().date()
