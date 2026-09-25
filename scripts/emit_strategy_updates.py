"""Send new official Trump filings through the existing tracking notification queue."""
import json
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests

from pipeline_support import emit_summary, get_supabase_client, utc_now

INVENTORY = Path(__file__).resolve().parents[1] / 'src/lib/strategies/trump-sources.json'
INDEX_URL = 'https://www.whitehouse.gov/disclosures/'


def official_pdf(href):
    url = urlsplit(urljoin(INDEX_URL, href))
    if url.scheme != 'https' or url.netloc != 'www.whitehouse.gov' or not url.path.lower().endswith('.pdf'):
        return None
    return urlunsplit((url.scheme, url.netloc, url.path, '', ''))


class TrumpFilings(HTMLParser):
    def __init__(self):
        super().__init__()
        self.filings, self.href, self.title = {}, None, []

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            self.href = dict(attrs).get('href')
            self.title = []

    def handle_data(self, text):
        if self.href:
            self.title.append(text)

    def handle_endtag(self, tag):
        if tag != 'a':
            return
        title = ' '.join(''.join(self.title).split())
        url = official_pdf(self.href) if self.href else None
        if url and title.startswith('President Donald J. Trump'):
            self.filings[url] = title
        self.href = None


def build_events(html, baseline_urls, detected_at):
    parser = TrumpFilings()
    parser.feed(html)
    if not parser.filings:
        raise ValueError('No Trump filings recognized; the source may be unavailable or have changed format.')
    baseline = {official_pdf(url) for url in baseline_urls}
    return [{
        'source': 'strategy', 'source_document_id': url, 'signal_type': 'strategy_filing',
        'ticker': '', 'actor_type': 'politician', 'actor_name': 'Trump strategy',
        'direction': None, 'occurred_at': None, 'published_at': None, 'importance_score': 0,
        'title': 'Trump strategy: new financial disclosure',
        'summary': f'{title}. Newly detected on the White House website; holdings and trades still need review.',
        'source_url': url,
        'payload': {'strategy_key': 'strategy:trump', 'detected_at': detected_at},
    } for url, title in parser.filings.items() if url not in baseline]


def save_events(client, events):
    if events:
        # Preserve the first detection time, event id and delivery history on subsequent polls.
        client.table('signal_events').upsert(events, on_conflict='source,source_document_id', ignore_duplicates=True).execute()


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    inventory = json.loads(INVENTORY.read_text())
    baseline = [row['url'] for row in inventory['sources'] if row.get('notificationBaseline')]
    if not baseline:
        raise ValueError('Missing notification baseline; refusing to notify on historical filings.')
    response = requests.get(INDEX_URL, timeout=30)
    response.raise_for_status()
    events = build_events(response.text, baseline, utc_now().isoformat())
    if not args.dry_run:
        save_events(get_supabase_client(), events)
    emit_summary({'strategy': 'trump', 'new_document_candidates': len(events), 'dry_run': args.dry_run})


if __name__ == '__main__':
    main()
