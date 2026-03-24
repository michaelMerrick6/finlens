import re

from signal_policy import load_signal_policy


THEME_DISPLAY_NAMES = {
    "quantum": "Quantum",
    "energy": "Energy",
    "nuclear": "Nuclear",
    "biotech": "Biotech",
    "ai": "AI",
    "defense": "Defense",
    "crypto": "Crypto",
}

DEFAULT_THEME_TICKERS = {
    "quantum": {
        "IONQ",
        "QBTS",
        "RGTI",
        "QUBT",
        "QMCO",
    },
    "energy": {
        "XOM",
        "CVX",
        "COP",
        "OXY",
        "FANG",
        "SLB",
        "HAL",
        "MPC",
        "VLO",
        "ET",
        "WMB",
        "KMI",
        "CEG",
        "VST",
        "SMR",
        "OKLO",
        "NNE",
        "CCJ",
    },
    "nuclear": {
        "OKLO",
        "SMR",
        "NNE",
        "CCJ",
        "BWXT",
        "UEC",
        "LEU",
        "CEG",
        "VST",
        "URA",
        "URNM",
        "NUKZ",
    },
    "biotech": {
        "SANA",
        "RXRX",
        "DNA",
        "CRSP",
        "BEAM",
        "EDIT",
        "NTLA",
        "TGTX",
        "MRNA",
        "XBI",
    },
    "ai": {
        "NVDA",
        "AMD",
        "ARM",
        "SMCI",
        "PLTR",
        "BBAI",
        "SOUN",
        "AI",
        "TSM",
        "AVGO",
    },
    "defense": {
        "LMT",
        "RTX",
        "NOC",
        "GD",
        "LHX",
        "LDOS",
        "HII",
        "KTOS",
        "AVAV",
        "MRCY",
    },
    "crypto": {
        "MSTR",
        "COIN",
        "MARA",
        "RIOT",
        "CLSK",
        "IREN",
        "HUT",
        "BTBT",
        "CIFR",
        "HIVE",
    },
}

DEFAULT_THEME_KEYWORDS = {
    "quantum": (
        "quantum",
        "quantum computing",
        "d wave",
        "dwave",
        "rigetti",
        "ionq",
    ),
    "energy": (
        "energy",
        "oil",
        "gas",
        "petroleum",
        "solar",
        "renewable",
        "power",
        "pipeline",
        "utilities",
    ),
    "nuclear": (
        "nuclear",
        "uranium",
        "reactor",
        "reactors",
        "fission",
        "fusion",
        "nuscale",
        "oklo",
    ),
    "biotech": (
        "biotech",
        "biosciences",
        "therapeutics",
        "pharmaceutical",
        "pharma",
        "genomics",
        "oncology",
    ),
    "ai": (
        "artificial intelligence",
        "machine learning",
        "ai infrastructure",
        "gpu",
        "semiconductor",
    ),
    "defense": (
        "defense",
        "defence",
        "aerospace",
        "missile",
        "drone",
        "munitions",
        "naval",
    ),
    "crypto": (
        "bitcoin",
        "crypto",
        "blockchain",
        "digital asset",
        "mining",
    ),
}

POLICY = load_signal_policy()

CURATED_THEME_TICKERS = {
    theme: {str(ticker).strip().upper() for ticker in tickers if str(ticker).strip()}
    for theme, tickers in (POLICY.get("theme_tickers") or DEFAULT_THEME_TICKERS).items()
}

THEME_KEYWORDS = {
    theme: tuple(str(keyword).strip().lower() for keyword in keywords if str(keyword).strip())
    for theme, keywords in (POLICY.get("theme_keywords") or DEFAULT_THEME_KEYWORDS).items()
}


def theme_label(theme: str) -> str:
    return THEME_DISPLAY_NAMES.get(theme, theme.replace("_", " ").title())


def theme_labels(themes: list[str] | set[str] | tuple[str, ...]) -> list[str]:
    seen = []
    for theme in themes:
        if theme not in seen:
            seen.append(theme)
    return [theme_label(theme) for theme in seen]


def normalize_text(value: str | None) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    return re.sub(r"[^a-z0-9]+", " ", raw).strip()


def candidate_tickers(event: dict) -> list[str]:
    payload = event.get("payload") or {}
    tickers = []

    ticker = (event.get("ticker") or "").strip().upper()
    if ticker and ticker != "MULTI":
        tickers.append(ticker)

    for summary_ticker in payload.get("summary_tickers") or []:
        normalized = str(summary_ticker or "").strip().upper()
        if normalized and normalized not in tickers:
            tickers.append(normalized)

    return tickers


def candidate_texts(event: dict) -> list[str]:
    payload = event.get("payload") or {}
    values = [
        payload.get("asset_name"),
        payload.get("security_title"),
        payload.get("company_name"),
        payload.get("issuer_name"),
        payload.get("name"),
        payload.get("sector"),
        payload.get("industry"),
        event.get("title"),
        event.get("summary"),
    ]
    texts = []
    for value in values:
        normalized = normalize_text(value)
        if normalized and normalized not in texts:
            texts.append(normalized)
    return texts


def infer_themes(*, tickers: list[str], texts: list[str]) -> list[str]:
    themes = set()

    for theme, theme_tickers in CURATED_THEME_TICKERS.items():
        if any(ticker in theme_tickers for ticker in tickers):
            themes.add(theme)

    for theme, keywords in THEME_KEYWORDS.items():
        for text in texts:
            if any(keyword in text for keyword in keywords):
                themes.add(theme)
                break

    return sorted(themes)


def event_signal_profile(event: dict) -> dict:
    tickers = candidate_tickers(event)
    texts = candidate_texts(event)
    themes = infer_themes(tickers=tickers, texts=texts)
    return {
        "tickers": tickers,
        "themes": themes,
        "theme_labels": theme_labels(themes),
        "is_priority_theme": bool(themes),
    }
