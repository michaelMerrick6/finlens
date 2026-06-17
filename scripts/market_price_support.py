from __future__ import annotations

from datetime import datetime, timedelta, timezone

import requests


YAHOO_CHART_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0 Safari/537.36"
    )
}
NASDAQ_API_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.nasdaq.com",
    "Referer": "https://www.nasdaq.com/",
}
MARKET_DATA_TIMEOUT_SECONDS = 15
_SERIES_CACHE: dict[str, dict | None] = {}
YAHOO_CHART_BASE_URLS = (
    "https://query2.finance.yahoo.com/v8/finance/chart/{ticker}",
    "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}",
)


def _normalize_ticker(value: str) -> str:
    return str(value or "").strip().upper()


def _iso_date(timestamp_seconds: int) -> str:
    return datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc).date().isoformat()


def _nasdaq_iso_date(value: str | None) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%m/%d/%Y").date().isoformat()
    except ValueError:
        return None


def _money_number(value: str | None) -> float | None:
    raw = str(value or "").strip().replace("$", "").replace(",", "")
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _range_for_earliest_date(earliest_date: str | None) -> str:
    if not earliest_date:
        return "1y"
    try:
        start = datetime.fromisoformat(f"{str(earliest_date)[:10]}T00:00:00+00:00")
    except ValueError:
        return "1y"
    now = datetime.now(timezone.utc)
    age_days = max((now - start).days, 0)
    if age_days <= 90:
        return "3mo"
    if age_days <= 180:
        return "6mo"
    if age_days <= 370:
        return "1y"
    if age_days <= 740:
        return "2y"
    if age_days <= 1850:
        return "5y"
    return "10y"


def _fetch_nasdaq_price_series(normalized_ticker: str, earliest_date: str | None) -> dict | None:
    start = None
    if earliest_date:
        try:
            start = datetime.fromisoformat(f"{str(earliest_date)[:10]}T00:00:00+00:00") - timedelta(days=7)
        except ValueError:
            start = None
    if start is None:
        start = datetime.now(timezone.utc) - timedelta(days=370)
    end = datetime.now(timezone.utc)
    url = f"https://api.nasdaq.com/api/quote/{requests.utils.quote(normalized_ticker)}/historical"
    response = requests.get(
        url,
        params={
            "assetclass": "stocks",
            "fromdate": start.date().isoformat(),
            "todate": end.date().isoformat(),
            "limit": "9999",
        },
        headers=NASDAQ_API_HEADERS,
        timeout=MARKET_DATA_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    rows = ((((payload.get("data") or {}).get("tradesTable") or {}).get("rows")) or [])
    points: list[dict] = []
    for row in rows:
        point_date = _nasdaq_iso_date(row.get("date"))
        price = _money_number(row.get("close"))
        if not point_date or not isinstance(price, (int, float)):
            continue
        points.append({"date": point_date, "price": float(price)})
    if not points:
        return None
    points.sort(key=lambda point: point["date"])
    return {
        "ticker": normalized_ticker,
        "instrument_type": "EQUITY",
        "current_price": points[-1]["price"],
        "price_as_of": points[-1]["date"],
        "points": points,
        "provider": "nasdaq_historical",
    }


def fetch_market_price_series(ticker: str, earliest_date: str | None = None) -> dict | None:
    normalized_ticker = _normalize_ticker(ticker)
    if not normalized_ticker:
        return None

    cache_key = f"{normalized_ticker}::{str(earliest_date or '').strip()}"
    if cache_key in _SERIES_CACHE:
        return _SERIES_CACHE[cache_key]

    try:
        series = _fetch_nasdaq_price_series(normalized_ticker, earliest_date)
        if series:
            _SERIES_CACHE[cache_key] = series
            return series
    except Exception:
        pass

    params = {
        "interval": "1d",
        "range": _range_for_earliest_date(earliest_date),
        "includeAdjustedClose": "true",
        "events": "div,splits",
    }

    last_error: Exception | None = None
    for url_template in YAHOO_CHART_BASE_URLS:
        url = url_template.format(ticker=requests.utils.quote(normalized_ticker))
        try:
            response = requests.get(
                url,
                params=params,
                headers=YAHOO_CHART_HEADERS,
                timeout=MARKET_DATA_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            payload = response.json()
            result = (((payload.get("chart") or {}).get("result") or [None])[0]) or {}
            timestamps = result.get("timestamp") or []
            quote = ((result.get("indicators") or {}).get("quote") or [{}])[0] or {}
            adjclose = ((result.get("indicators") or {}).get("adjclose") or [{}])[0] or {}
            closes = quote.get("close") or []
            adjusted = adjclose.get("adjclose") or []

            points: list[dict] = []
            for index, timestamp in enumerate(timestamps):
                if not timestamp:
                    continue
                adjusted_price = adjusted[index] if index < len(adjusted) else None
                close_price = closes[index] if index < len(closes) else None
                price = adjusted_price if isinstance(adjusted_price, (int, float)) else close_price
                if not isinstance(price, (int, float)):
                    continue
                points.append({"date": _iso_date(int(timestamp)), "price": float(price)})

            meta = result.get("meta") or {}
            regular_market_price = meta.get("regularMarketPrice")
            current_price = float(regular_market_price) if isinstance(regular_market_price, (int, float)) else None
            if current_price is None and points:
                current_price = points[-1]["price"]

            series = {
                "ticker": normalized_ticker,
                "instrument_type": meta.get("instrumentType"),
                "current_price": current_price,
                "price_as_of": points[-1]["date"] if points else None,
                "points": points,
                "provider": "yahoo_historical",
            }
            _SERIES_CACHE[cache_key] = series
            return series
        except Exception as exc:
            last_error = exc
            continue

    _SERIES_CACHE[cache_key] = None
    return None


def get_price_on_or_before(series: dict | None, target_date: str | None) -> float | None:
    if not series or not target_date:
        return None
    points = series.get("points") or []
    if not points:
        return None

    target = str(target_date)[:10]
    left = 0
    right = len(points) - 1
    best_index = -1

    while left <= right:
        middle = (left + right) // 2
        point_date = str(points[middle].get("date") or "")
        if point_date <= target:
            best_index = middle
            left = middle + 1
        else:
            right = middle - 1

    if best_index < 0:
        return None
    price = points[best_index].get("price")
    return float(price) if isinstance(price, (int, float)) else None
