import re
import unicodedata


COMMON_NAME_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}
FIRST_NAME_ALIAS_GROUPS = (
    {"bill", "billy", "will", "william"},
    {"dan", "daniel", "danny"},
    {"dave", "david"},
    {"jim", "jimmy", "james"},
    {"rick", "richard"},
    {"ted", "rafael"},
    {"tom", "tommy", "thomas"},
)
FIRST_NAME_ALIAS_MAP = {token: group for group in FIRST_NAME_ALIAS_GROUPS for token in group}


def normalize_name_tokens(value: str) -> list[str]:
    normalized = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode("ascii")
    return [token for token in re.findall(r"[a-z]+", normalized.lower()) if token not in COMMON_NAME_SUFFIXES]


def normalize_actor_key(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]", "", normalized.lower())


def alias_tokens(token: str) -> set[str]:
    return FIRST_NAME_ALIAS_MAP.get(token, {token})


def first_name_tokens_match(first_tokens: list[str], member_first_name: str) -> bool:
    member_tokens = normalize_name_tokens(member_first_name)
    if not first_tokens or not member_tokens:
        return False

    for filed_token in first_tokens:
        for member_token in member_tokens:
            if filed_token == member_token:
                return True
            if alias_tokens(filed_token) & alias_tokens(member_token):
                return True
            if len(filed_token) == 1 and member_token.startswith(filed_token):
                return True
            if len(member_token) == 1 and filed_token.startswith(member_token):
                return True
            if min(len(filed_token), len(member_token)) >= 3 and (
                filed_token.startswith(member_token) or member_token.startswith(filed_token)
            ):
                return True
    return False


def is_placeholder_member(member: dict) -> bool:
    return str(member.get("id") or "").startswith("unknown-")


def resolve_politician_target(raw_value: str, members: list[dict]) -> dict | None:
    candidate = raw_value.strip()
    if not candidate:
        return None

    for member in members:
        if str(member.get("id") or "").upper() == candidate.upper():
            return member

    tokens = normalize_name_tokens(candidate)
    if len(tokens) < 2:
        return None

    exact_candidates: list[dict] = []
    active_candidates: list[dict] = []

    for member in members:
        if is_placeholder_member(member):
            continue
        member_last_tokens = normalize_name_tokens(member.get("last_name") or "")
        if not member_last_tokens:
            continue
        if len(tokens) <= len(member_last_tokens):
            continue
        if tokens[-len(member_last_tokens) :] != member_last_tokens:
            continue

        first_tokens = tokens[: -len(member_last_tokens)]
        exact_candidates.append(member)
        if member.get("active") is not False:
            active_candidates.append(member)
        if member.get("active") is not False and first_name_tokens_match(first_tokens, member.get("first_name") or ""):
            return member

    if len(active_candidates) == 1:
        return active_candidates[0]
    if len(exact_candidates) == 1:
        return exact_candidates[0]
    return None


def event_actor_match_keys(event: dict) -> set[str]:
    actor_type = str(event.get("actor_type") or "").strip().lower()
    if not actor_type:
        return set()

    payload = event.get("payload") or {}
    keys: set[str] = set()

    def add_normalized_key(target_actor_type: str, raw_value: str | None) -> None:
        normalized = normalize_actor_key(raw_value or "")
        if normalized:
            keys.add(f"{target_actor_type}:{normalized}")

    def add_exact_key(target_actor_type: str, raw_value: str | None) -> None:
        normalized = str(raw_value or "").strip().lower()
        if normalized:
            keys.add(f"{target_actor_type}:{normalized}")

    if actor_type == "politician":
        member_id = (payload.get("member_id") or "").strip()
        if member_id:
            add_exact_key("politician", member_id)
        add_normalized_key("politician", payload.get("politician_name"))
    elif actor_type == "insider":
        add_normalized_key("insider", payload.get("filer_name"))
    elif actor_type == "fund":
        add_normalized_key("fund", payload.get("fund_name"))
    elif actor_type == "cluster":
        cluster_actors = payload.get("cluster_actors") or []
        base_signal_type = str(payload.get("base_signal_type") or "").lower()
        if base_signal_type == "politician_trade":
            for actor in cluster_actors:
                if not isinstance(actor, dict):
                    continue
                add_exact_key("politician", actor.get("member_id"))
                add_normalized_key("politician", actor.get("name"))
        elif base_signal_type == "insider_trade":
            for actor in cluster_actors:
                if not isinstance(actor, dict):
                    continue
                add_normalized_key("insider", actor.get("name"))

    if actor_type != "cluster":
        add_normalized_key(actor_type, event.get("actor_name"))
    return keys
