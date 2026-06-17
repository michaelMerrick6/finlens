from datetime import date

from emit_13f_deadline_events import (
    build_deadline_events,
    candidate_deadlines,
    expected_13f_due_date,
    quarter_label,
)


def test_expected_13f_due_date_rolls_weekend_forward() -> None:
    assert expected_13f_due_date(date(2024, 3, 31)) == date(2024, 5, 15)
    assert expected_13f_due_date(date(2025, 12, 31)) == date(2026, 2, 17)


def test_candidate_deadlines_for_one_week_before_q2_2026() -> None:
    candidates = candidate_deadlines(date(2026, 8, 7))
    assert candidates == [(date(2026, 6, 30), date(2026, 8, 14), 7)]


def test_build_deadline_events() -> None:
    events = build_deadline_events(
        date(2026, 8, 13),
        [{"cik": "0002045724", "name": "Situational Awareness LP"}],
    )
    assert len(events) == 1
    event = events[0]
    assert event["signal_type"] == "fund_filing_deadline_reminder"
    assert event["ticker"] == "13F"
    assert event["actor_type"] == "fund"
    assert event["actor_name"] == "Situational Awareness LP"
    assert event["importance_score"] == 0.64
    assert event["payload"]["reminder_days_before"] == 1
    assert event["payload"]["report_period_label"] == quarter_label(date(2026, 6, 30))


def main() -> None:
    test_expected_13f_due_date_rolls_weekend_forward()
    test_candidate_deadlines_for_one_week_before_q2_2026()
    test_build_deadline_events()
    print("13F deadline reminder tests passed")


if __name__ == "__main__":
    main()
