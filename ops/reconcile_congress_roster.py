"""Generate a reviewable roster change proposal. No database write mode exists."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import unicodedata

from audit_congress_roster import get_supabase_client, load_roster, load_stored_members


def name_key(value):
    return ''.join(c for c in unicodedata.normalize('NFKD', value or '').casefold() if c.isalnum())


def propose_changes(official, stored):
    by_id = {row['id']: row for row in stored}
    additions, corrections, reviews = [], [], []
    for member_id, member in sorted(official.items()):
        if member_id not in by_id:
            candidates = [row['id'] for row in stored
                          if name_key(row.get('last_name')) == name_key(member['last_name'])]
            additions.append({'record': {k: v for k, v in member.items() if k != 'name'},
                              'same_surname_ids_for_review': candidates})
            continue
        existing = by_id[member_id]
        changes = {field: {'before': existing.get(field), 'after': member[field]}
                   for field in ['first_name', 'last_name', 'chamber', 'state', 'party', 'active']
                   if existing.get(field) != member[field]}
        if changes:
            corrections.append({'id': member_id, 'changes': changes})
    for row in stored:
        if row.get('active') is True and row['id'] not in official:
            reviews.append({'record': row, 'reason': 'Absent from current rosters; verify term end and aliases before changing status.'})
    return {'proposed_additions': additions, 'proposed_corrections': corrections,
            'active_status_reviews': reviews, 'automatic_merges': [], 'automatic_deletions': []}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    official, sources = load_roster()
    stored = load_stored_members(get_supabase_client())
    proposal = propose_changes(official, stored)
    result = {'generated_at': datetime.now(timezone.utc).isoformat(), 'mode': 'read-only proposal',
              'sources': sources, 'official_count': len(official), 'stored_count': len(stored), **proposal}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({key: len(value) for key, value in proposal.items()}))


if __name__ == '__main__':
    main()
