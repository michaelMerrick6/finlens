from insider_holdings import summarize_buy_transactions, summarize_sell_transactions


def test_summarize_sell_transactions() -> None:
    metrics = summarize_sell_transactions(
        [
            {"shares": 40, "value": 4000, "following_shares": 60},
            {"shares": 10, "value": 1000, "following_shares": 50},
        ]
    )
    assert metrics is not None
    assert metrics["insider_total_sell_shares"] == 50
    assert metrics["insider_shares_before_sale"] == 100
    assert metrics["insider_shares_after_sale"] == 50
    assert metrics["insider_holding_reduction_pct"] == 0.5


def test_summarize_buy_transactions() -> None:
    metrics = summarize_buy_transactions(
        [
            {"shares": 30, "value": 3000, "following_shares": 90},
            {"shares": 10, "value": 1000, "following_shares": 100},
        ]
    )
    assert metrics is not None
    assert metrics["insider_total_buy_shares"] == 40
    assert metrics["insider_shares_before_buy"] == 60
    assert metrics["insider_shares_after_buy"] == 100
    assert metrics["insider_holding_increase_pct"] == 0.666667


def main() -> None:
    test_summarize_sell_transactions()
    test_summarize_buy_transactions()
    print("insider holdings tests passed")


if __name__ == "__main__":
    main()
