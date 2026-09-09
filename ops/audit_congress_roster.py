"""Read-only roster reconciliation. Never creates or updates database records."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import re
from pathlib import Path
import sys
from xml.etree import ElementTree as ET

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from pipeline_support import get_supabase_client

SOURCES = {
    'House': 'https://clerk.house.gov/xml/lists/MemberData.xml',
    'Senate': 'https://www.senate.gov/legislative/LIS_MEMBER/cvc_member_data.xml',
}


def parse_roster(content, chamber):
    root = ET.fromstring(content)
    nodes = root.findall('.//member' if chamber == 'House' else './/senator')
    members = {}
    for node in nodes:
        if chamber == 'House':
            info = node.find('member-info')
            if info is None:
                raise ValueError('House member has no member-info')
            member_id = (info.findtext('bioguideID') or '').strip()
            first = (info.findtext('firstname') or '').strip()
            last = (info.findtext('lastname') or '').strip()
            name = (info.findtext('official-name') or '').strip()
            state_node = info.find('state')
            state = state_node.get('postal-code', '') if state_node is not None else ''
            party = (info.findtext('party') or '').strip()
        else:
            member_id = (node.findtext('bioguideId') or '').strip()
            first = (node.findtext('name/first') or '').strip()
            last = (node.findtext('name/last') or '').strip()
            name = f'{first} {last}'.strip()
            state = (node.findtext('state') or '').strip()
            party = (node.findtext('party') or '').strip()
        if not member_id:
            if first or last:
                raise ValueError('Named member has no canonical ID; source review required')
            continue
        if not re.fullmatch(r'[A-Z][0-9]{6}', member_id) or not first or not last or not state:
            raise ValueError('Incomplete or invalid official member identity')
        if member_id in members:
            raise ValueError('Duplicate official identity; source review required')
        members[member_id] = {'id': member_id, 'name': name, 'first_name': first,
                              'last_name': last, 'chamber': chamber, 'state': state,
                              'party': {'D': 'Democrat', 'R': 'Republican', 'I': 'Independent'}.get(party, party),
                              'active': True, 'source_url': f'https://bioguide.congress.gov/search/bio/{member_id}'}
    if not members:
        raise ValueError(f'No identified {chamber} members; refusing an empty comparison')
    return members, len(nodes)


def load_roster():
    members, evidence = {}, {}
    for chamber, url in SOURCES.items():
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        parsed, entries = parse_roster(response.content, chamber)
        if members.keys() & parsed.keys():
            raise ValueError('Identity appears in both chamber rosters; review required')
        members.update(parsed)
        evidence[chamber] = {'url': url, 'sha256': hashlib.sha256(response.content).hexdigest(),
                             'source_entries': entries, 'identified_members': len(parsed),
                             'entries_without_id': entries - len(parsed)}
    return members, evidence


def load_stored_members(db):
    stored, offset = [], 0
    while True:
        batch = db.table('congress_members').select('id,first_name,last_name,chamber,active,state,party').order('id').range(offset, offset + 499).execute().data or []
        stored.extend(batch)
        if len(batch) < 500:
            return stored
        offset += len(batch)


def audit():
    official, evidence = load_roster()
    db = get_supabase_client()
    stored = load_stored_members(db)
    by_id = {row['id']: row for row in stored}
    missing = [row for key, row in official.items() if key not in by_id]
    inactive = [row for key, row in official.items() if key in by_id and by_id[key].get('active') is not True]
    mismatch = [dict(row, stored_chamber=by_id[key].get('chamber')) for key, row in official.items()
                if key in by_id and by_id[key].get('chamber') != row['chamber']]
    stale = [row for row in stored if row.get('active') is True and row['id'] not in official]
    return {'checked_at': datetime.now(timezone.utc).isoformat(), 'scope': 'current official federal congressional rosters; not filing completeness',
            'sources': evidence, 'stored_members': len(stored), 'official_members': len(official),
            'matched_members': len(official) - len(missing), 'stored_by_chamber': dict(Counter(row.get('chamber') for row in stored)),
            'missing_members': missing, 'current_members_not_active': inactive, 'chamber_mismatches': mismatch,
            'stored_active_not_on_current_rosters': stale,
            'current_roster_gate_passed': not (missing or inactive or mismatch or stale)}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = audit()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({key: len(value) if isinstance(value, list) else value for key, value in result.items() if key != 'sources'}, indent=2))
