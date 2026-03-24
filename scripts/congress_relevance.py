from __future__ import annotations

import copy
from functools import lru_cache
from typing import Any
from xml.etree import ElementTree as ET

import requests


HOUSE_MEMBER_DATA_URL = "https://clerk.house.gov/xml/lists/MemberData.xml"
SENATE_MEMBER_DATA_URL = "https://www.senate.gov/legislative/LIS_MEMBER/cvc_member_data.xml"

# Committee code mapping is derived from the official House Member Data XML User Guide
# published by the Clerk of the House.
HOUSE_COMMITTEE_CODE_MAP = {
    "AG00": "Committee on Agriculture",
    "AP00": "Committee on Appropriations",
    "AS00": "Committee on Armed Services",
    "BU00": "Committee on the Budget",
    "ED00": "Committee on Education and Workforce",
    "FA00": "Committee on Foreign Affairs",
    "GO00": "Committee on Homeland Security",
    "HS00": "Committee on House Administration",
    "IF00": "Committee on Financial Services",
    "II00": "Committee on Natural Resources",
    "JU00": "Committee on the Judiciary",
    "OV00": "Committee on Oversight and Government Reform",
    "PW00": "Committee on Transportation and Infrastructure",
    "RU00": "Committee on Rules",
    "SM00": "Committee on Small Business",
    "SO00": "Committee on Ethics",
    "SY00": "Committee on Science, Space, and Technology",
    "VC00": "Committee on Energy and Commerce",
    "WM00": "Committee on Ways and Means",
    "IG00": "Permanent Select Committee on Intelligence",
}

COMMITTEE_THEME_KEYWORDS = {
    "energy": (
        "energy",
        "natural resources",
        "environment and public works",
        "transportation and infrastructure",
    ),
    "nuclear": (
        "energy",
        "natural resources",
        "environment and public works",
    ),
    "ai": (
        "science, space, and technology",
        "commerce",
        "intelligence",
        "technology",
    ),
    "quantum": (
        "science, space, and technology",
        "technology",
        "intelligence",
    ),
    "biotech": (
        "health",
        "energy and commerce",
        "science",
        "labor",
        "pensions",
    ),
    "defense": (
        "armed services",
        "homeland security",
        "foreign affairs",
        "intelligence",
        "veterans",
    ),
    "crypto": (
        "banking",
        "financial services",
        "finance",
        "agriculture",
    ),
}


def committee_themes(name: str | None) -> list[str]:
    lowered = str(name or "").strip().lower()
    if not lowered:
        return []
    themes = []
    for theme, keywords in COMMITTEE_THEME_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            themes.append(theme)
    return themes


def _normalize_roles(roles: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    normalized_roles = []
    theme_keys: list[str] = []

    for role in roles:
        name = str(role.get("name") or "").strip()
        code = str(role.get("code") or "").strip()
        source = str(role.get("source") or "").strip()
        if not name:
            continue
        role_themes = role.get("themes") or committee_themes(name)
        normalized_roles.append(
            {
                "name": name,
                "code": code,
                "source": source,
                "themes": role_themes,
            }
        )
        for theme in role_themes:
            if theme not in theme_keys:
                theme_keys.append(theme)

    return normalized_roles, theme_keys


def _upsert_role(role_map: dict[str, list[dict[str, Any]]], member_id: str, role: dict[str, Any]) -> None:
    member_id = str(member_id or "").strip()
    if not member_id:
        return
    role_map.setdefault(member_id, []).append(role)


def fetch_official_house_roles(timeout: int = 30) -> dict[str, dict[str, Any]]:
    response = requests.get(HOUSE_MEMBER_DATA_URL, timeout=timeout)
    response.raise_for_status()
    root = ET.fromstring(response.text)

    role_map: dict[str, list[dict[str, Any]]] = {}

    for member in root.findall(".//member"):
        member_id = (member.findtext("./member-info/bioguideID") or "").strip()
        chamber = "House"
        assignments = member.find("./committee-assignments")
        if assignments is None:
            continue

        for committee in assignments.findall("./committee"):
            code = str(committee.attrib.get("comcode") or "").strip().upper()
            name = HOUSE_COMMITTEE_CODE_MAP.get(code)
            if not name:
                continue
            _upsert_role(
                role_map,
                member_id,
                {
                    "name": name,
                    "code": code,
                    "source": chamber.lower(),
                    "themes": committee_themes(name),
                },
            )

    profiles = {}
    for member_id, roles in role_map.items():
        normalized_roles, theme_keys = _normalize_roles(roles)
        profiles[member_id] = {
            "member_id": member_id,
            "chamber": "House",
            "committee_roles": normalized_roles,
            "theme_keys": theme_keys,
            "source_name": "official_house_member_xml",
            "source_url": HOUSE_MEMBER_DATA_URL,
        }
    return profiles


def fetch_official_senate_roles(timeout: int = 30) -> dict[str, dict[str, Any]]:
    response = requests.get(SENATE_MEMBER_DATA_URL, timeout=timeout)
    response.raise_for_status()
    root = ET.fromstring(response.text)

    profiles = {}
    for senator in root.findall(".//senator"):
        member_id = (senator.findtext("./bioguideId") or "").strip()
        roles = []
        for committee in senator.findall("./committees/committee"):
            name = (committee.text or "").strip()
            code = str(committee.attrib.get("code") or "").strip().upper()
            if not name:
                continue
            roles.append(
                {
                    "name": name,
                    "code": code,
                    "source": "senate",
                    "themes": committee_themes(name),
                }
            )
        normalized_roles, theme_keys = _normalize_roles(roles)
        profiles[member_id] = {
            "member_id": member_id,
            "chamber": "Senate",
            "committee_roles": normalized_roles,
            "theme_keys": theme_keys,
            "source_name": "official_senate_member_xml",
            "source_url": SENATE_MEMBER_DATA_URL,
        }
    return profiles


@lru_cache(maxsize=1)
def fetch_live_member_relevance_profiles() -> dict[str, dict[str, Any]]:
    profiles: dict[str, dict[str, Any]] = {}
    try:
        profiles.update(fetch_official_house_roles())
    except Exception as exc:
        print(f"[congress_relevance] Warning: failed to fetch House committee roles ({exc})")
    try:
        profiles.update(fetch_official_senate_roles())
    except Exception as exc:
        print(f"[congress_relevance] Warning: failed to fetch Senate committee roles ({exc})")
    return profiles


def enrich_event_with_member_roles(event: dict, profiles: dict[str, dict[str, Any]]) -> dict:
    if str(event.get("source") or "").strip().lower() != "congress":
        return event

    payload = dict(event.get("payload") or {})
    member_id = str(payload.get("member_id") or "").strip()
    if not member_id:
        return event

    profile = profiles.get(member_id)
    if not profile:
        return event

    enriched = dict(event)
    payload["member_committee_roles"] = copy.deepcopy(profile.get("committee_roles") or [])
    payload["member_committee_themes"] = list(profile.get("theme_keys") or [])
    payload["member_role_source"] = profile.get("source_name")
    enriched["payload"] = payload
    return enriched


def enrich_events_with_member_roles(events: list[dict], profiles: dict[str, dict[str, Any]] | None = None) -> list[dict]:
    if profiles is None:
        profiles = fetch_live_member_relevance_profiles()
    if not profiles:
        return events
    return [enrich_event_with_member_roles(event, profiles) for event in events]
