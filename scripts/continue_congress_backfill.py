"""Dispatch the next bounded historical batch after measurable, error-free progress."""
import argparse
import json
import os
from pathlib import Path

import requests


def next_batch(report, remaining):
    if not 1 <= remaining <= 100:
        raise ValueError('batches_remaining must be between 1 and 100')
    chambers = report.get('chambers', [])
    if {row.get('chamber') for row in chambers} != {'House', 'Senate'} or len(chambers) != 2:
        raise ValueError('Incomplete backfill report')
    # Document-level failures are durably queued for review; infrastructure errors stop the chain.
    if any(row.get('error') or row.get('discovery_errors') for row in chambers):
        return None
    if remaining == 1 or not sum(row.get('filings_completed', 0) for row in chambers):
        return None
    if not sum(row.get('filings_pending', 0) for row in chambers):
        return None
    return remaining - 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('report', type=Path)
    parser.add_argument('--batches-remaining', type=int, required=True)
    args = parser.parse_args()
    remaining = next_batch(json.loads(args.report.read_text()), args.batches_remaining)
    if remaining is None:
        print('Backfill chain stopped: drained, budget exhausted, no progress, or infrastructure error.')
        return
    repository = os.environ['GITHUB_REPOSITORY']
    response = requests.post(f'https://api.github.com/repos/{repository}/actions/workflows/backfill-congress.yml/dispatches',
        headers={'Authorization': 'Bearer ' + os.environ['GH_TOKEN'], 'Accept': 'application/vnd.github+json'},
        json={'ref': os.environ.get('GITHUB_REF_NAME', 'main'),
              'inputs': {'batches_remaining': str(remaining)}}, timeout=30)
    response.raise_for_status()
    print(f'Next historical batch dispatched; {remaining} batches remain in this chain.')


if __name__ == '__main__':
    main()
