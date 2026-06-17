import os
from datetime import date, datetime, timedelta

from pipeline_support import emit_summary, get_supabase_client, utc_now
from sec_13f_support import TRACKED_13F_FUNDS
from shared_utils import stable_id


REMINDER_DAYS = {
    7: "one_week_before",
    1: "one_day_before",
    0: "due_today",
}
REMINDER_IMPORTANCE = {
    7: 0.58,
    1: 0.64,
    0: 0.72,
}


def parse_iso_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def current_date() -> date:
    override = os.environ.get("SEC_13F_REMINDER_TODAY", "").strip()
    if override:
        return parse_iso_date(override)
    return utc_now().date()


def quarter_end_dates_for_year(year: int) -> list[date]:
    return [
        date(year, 3, 31),
        date(year, 6, 30),
        date(year, 9, 30),
        date(year, 12, 31),
    ]


def observed_fixed_holiday(year: int, month: int, day: int) -> date:
    holiday = date(year, month, day)
    if holiday.weekday() == 5:
        return holiday - timedelta(days=1)
    if holiday.weekday() == 6:
        return holiday + timedelta(days=1)
    return holiday


def nth_weekday(year: int, month: int, weekday: int, occurrence: int) -> date:
    day = date(year, month, 1)
    offset = (weekday - day.weekday()) % 7
    return day + timedelta(days=offset + (occurrence - 1) * 7)


def last_weekday(year: int, month: int, weekday: int) -> date:
    next_month = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    day = next_month - timedelta(days=1)
    return day - timedelta(days=(day.weekday() - weekday) % 7)


def federal_holidays(year: int) -> set[date]:
    return {
        observed_fixed_holiday(year, 1, 1),
        nth_weekday(year, 1, 0, 3),
        nth_weekday(year, 2, 0, 3),
        last_weekday(year, 5, 0),
        observed_fixed_holiday(year, 6, 19),
        observed_fixed_holiday(year, 7, 4),
        nth_weekday(year, 9, 0, 1),
        nth_weekday(year, 10, 0, 2),
        observed_fixed_holiday(year, 11, 11),
        nth_weekday(year, 11, 3, 4),
        observed_fixed_holiday(year, 12, 25),
    }


def next_business_day(day: date) -> date:
    while day.weekday() >= 5 or day in federal_holidays(day.year):
        day += timedelta(days=1)
    return day


def expected_13f_due_date(report_period: date) -> date:
    return next_business_day(report_period + timedelta(days=45))


def quarter_label(report_period: date) -> str:
    return f"Q{((report_period.month - 1) // 3) + 1} {report_period.year}"


def candidate_deadlines(today: date) -> list[tuple[date, date, int]]:
    candidates: list[tuple[date, date, int]] = []
    for year in range(today.year - 1, today.year + 2):
        for report_period in quarter_end_dates_for_year(year):
            due_date = expected_13f_due_date(report_period)
            days_before = (due_date - today).days
            if days_before in REMINDER_DAYS:
                candidates.append((report_period, due_date, days_before))
    return sorted(candidates, key=lambda item: item[1])


def sec_fund_url(cik: str) -> str:
    return f"https://www.sec.gov/edgar/browse/?CIK={int(cik)}&owner=exclude"


def build_deadline_events(today: date, funds: list[dict[str, str]]) -> list[dict]:
    events: list[dict] = []
    for report_period, due_date, days_before in candidate_deadlines(today):
        label = quarter_label(report_period)
        reminder_key = REMINDER_DAYS[days_before]
        if days_before == 7:
            timing = "in one week"
        elif days_before == 1:
            timing = "tomorrow"
        else:
            timing = "today"

        for fund in funds:
            fund_name = str(fund.get("name") or "").strip()
            cik = str(fund.get("cik") or "").strip()
            if not fund_name or not cik:
                continue

            source_document_id = stable_id(
                [
                    "13f_deadline",
                    cik,
                    report_period.isoformat(),
                    reminder_key,
                ]
            )
            payload = {
                "fund_name": fund_name,
                "cik": cik,
                "filing_type": "13F-HR",
                "report_period": report_period.isoformat(),
                "report_period_label": label,
                "expected_filing_due_date": due_date.isoformat(),
                "reminder_days_before": days_before,
                "reminder_key": reminder_key,
                "deadline_rule": "13F-HR is expected within 45 days after quarter end.",
            }
            events.append(
                {
                    "source": "hedge_fund",
                    "signal_type": "fund_filing_deadline_reminder",
                    "source_document_id": source_document_id,
                    "ticker": "13F",
                    "actor_name": fund_name,
                    "actor_type": "fund",
                    "direction": "reminder",
                    "occurred_at": due_date.isoformat(),
                    "published_at": today.isoformat(),
                    "importance_score": REMINDER_IMPORTANCE[days_before],
                    "title": f"{fund_name} 13F expected {timing}",
                    "summary": f"{fund_name} is expected to file its {label} 13F-HR {timing}.",
                    "source_url": sec_fund_url(cik),
                    "payload": payload,
                }
            )
    return events


def filing_already_ingested(supabase, *, fund_name: str, report_period: str) -> bool:
    response = (
        supabase.table("institutional_holdings")
        .select("id")
        .eq("fund_name", fund_name)
        .eq("report_period", report_period)
        .limit(1)
        .execute()
    )
    return bool(response.data)


def filter_unfiled_events(supabase, events: list[dict]) -> list[dict]:
    filtered: list[dict] = []
    seen_periods: dict[tuple[str, str], bool] = {}
    for event in events:
        payload = event.get("payload") or {}
        key = (str(payload.get("fund_name") or ""), str(payload.get("report_period") or ""))
        if not key[0] or not key[1]:
            continue
        if key not in seen_periods:
            seen_periods[key] = filing_already_ingested(supabase, fund_name=key[0], report_period=key[1])
        if not seen_periods[key]:
            filtered.append(event)
    return filtered


def main() -> None:
    today = current_date()
    candidates = build_deadline_events(today, TRACKED_13F_FUNDS)
    dry_run = os.environ.get("SEC_13F_REMINDER_DRY_RUN", "0").strip() == "1"

    if not candidates:
        emit_summary(
            {
                "date": today.isoformat(),
                "candidate_events": 0,
                "signal_events_created": 0,
                "dry_run": dry_run,
            }
        )
        print("No 13F deadline reminders due today.")
        return

    supabase = get_supabase_client()
    events = filter_unfiled_events(supabase, candidates)

    if events and not dry_run:
        supabase.table("signal_events").upsert(events, on_conflict="source,source_document_id").execute()

    emit_summary(
        {
            "date": today.isoformat(),
            "candidate_events": len(candidates),
            "signal_events_created": len(events),
            "dry_run": dry_run,
        }
    )
    print(f"Prepared {len(events)} 13F deadline reminder events from {len(candidates)} candidates.")


if __name__ == "__main__":
    main()
