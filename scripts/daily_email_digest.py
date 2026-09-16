"""One complete daily tracking email, claimed atomically per account in PostgreSQL."""
import html
import os
import time
from collections import defaultdict
from urllib.parse import urlparse

from alert_delivery_support import DeliveryRateLimited

AGGREGATE_TYPES = {'politician_filing_summary', 'insider_filing_summary',
                   'politician_trade_grouped', 'insider_trade_grouped'}


def digest_events(client, events):
    """Expand legacy summaries without their ten-row preview cap; preserve physical trades."""
    known = {str(event['id']): event for event in events}
    visiting, leaves = set(), {}

    def expand(event):
        key = str(event['id'])
        if key in visiting:
            raise ValueError('Cyclic filing summary; refusing an incomplete email')
        if event.get('signal_type') not in AGGREGATE_TYPES:
            identity = (event.get('source'), event.get('source_document_id') or key)
            leaves.setdefault(identity, event)
            return
        visiting.add(key)
        payload = event.get('payload') or {}
        ids = [str(value) for value in (payload.get('summary_event_ids') or payload.get('group_event_ids') or [])]
        if not ids:
            raise ValueError('Filing summary has no underlying transactions')
        missing = list(dict.fromkeys(value for value in ids if value not in known))
        for start in range(0, len(missing), 100):
            rows = client.table('signal_events').select('*').in_('id', missing[start:start+100]).execute().data or []
            known.update({str(row['id']): row for row in rows})
        for child in ids:
            if child not in known:
                raise ValueError('A filing summary transaction is unavailable; review required')
            expand(known[child])
        visiting.remove(key)

    for event in events:
        expand(event)
    return sorted(leaves.values(), key=lambda e: (e.get('actor_name') or '', e.get('occurred_at') or '', str(e['id'])))


def safe_url(value):
    value = str(value or '').strip()
    return value if urlparse(value).scheme in {'http', 'https'} and urlparse(value).netloc else ''


def render_digest(digest, events):
    items = []
    for event in events:
        payload = event.get('payload') or {}
        items.append(dict(eventId=str(event['id']), title=event.get('title') or 'Tracking activity',
            actor=event.get('actor_name') or 'Tracked activity', ticker=event.get('ticker') or '',
            asset=payload.get('asset_name') or event.get('ticker') or '',
            activity={'buy':'Purchase','sell':'Sale','exchange':'Exchange'}.get(event.get('direction'), event.get('direction') or ''),
            amount=payload.get('amount_range') or (f"${float(payload['value']):,.2f}" if payload.get('value') else ''),
            tradedAt=payload.get('transaction_date') or event.get('occurred_at'),
            filedAt=payload.get('published_date') or event.get('published_at'),
            summary=event.get('summary') or '', sourceUrl=safe_url(event.get('source_url'))))
    subject = f"Vail daily tracking · {len(items)} update{'s' if len(items) != 1 else ''}"
    intro = 'Your daily roundup of newly detected activity across all the people and stocks you track.'
    grouped = defaultdict(list)
    for item in items:
        grouped[item['actor']].append(item)
    text = [subject, '', intro]
    sections = []
    esc = lambda value: html.escape(str(value or ''), quote=True)
    for actor, rows in grouped.items():
        text.extend(['', f'{actor} · {len(rows)} update' + ('s' if len(rows) != 1 else '')])
        body = []
        for row in rows:
            detail = ' · '.join(str(row[k]) for k in ['activity','asset','amount'] if row[k]) or row['title']
            dates = ' · '.join(f'{label} {str(row[k])[:10]}' for k,label in [('tradedAt','Traded'),('filedAt','Filed')] if row[k])
            text.extend([detail, dates, row['summary'], row['sourceUrl']])
            source = f'<a href="{esc(row["sourceUrl"])}">Source filing</a>' if row['sourceUrl'] else ''
            body.append(f'<tr><td style="padding:14px 0;border-bottom:1px solid #e3e8df"><strong>{esc(detail)}</strong><br>'
                        f'<span style="color:#667265;font-size:13px">{esc(dates)}</span><br>{esc(row["summary"])}<br>{source}</td></tr>')
        sections.append(f'<h2 style="margin:28px 0 8px;font-size:19px">{esc(actor)} <span style="color:#667265;font-size:13px">{len(rows)} {"update" if len(rows) == 1 else "updates"}</span></h2>'
                        f'<table role="presentation" style="width:100%;border-collapse:collapse">{"".join(body)}</table>')
    base = safe_url(os.environ.get('APP_BASE_URL', 'https://vail.finance')).rstrip('/') or 'https://vail.finance'
    footer = f'View your tracking and sent emails, or turn off emails: {base}/tracking'
    text.extend(['', 'At most one tracking email per day. Late disclosures appear in a later roundup.', footer])
    markup = ('<div style="background:#f8faf6;padding:32px 16px;color:#233c33;font-family:Arial,sans-serif;line-height:1.6">'
        '<div style="max-width:680px;margin:auto"><p style="letter-spacing:2px;font-size:12px">VAIL · YOUR DAILY TRACKING</p>'
        f'<h1 style="font-size:28px">Your day in disclosures.</h1><p>{esc(intro)}</p>{"".join(sections)}'
        f'<p style="margin-top:28px;font-size:13px">At most one tracking email per day. Late disclosures appear in a later roundup.<br>'
        f'<a href="{esc(base)}/tracking">View sent emails or manage email preferences</a></p></div></div>')
    return dict(subject=subject, html=markup, text='\n'.join(line for line in text if line is not None), items=items)


def dispatch_daily_digests(client, *, send, configured, batch_size=20, min_send_interval=.6):
    if not configured:
        raise RuntimeError('Daily email dispatch blocked: provider configuration missing; queue unchanged.')
    summary = dict(digests_sent=0, digests_uncertain=0, digests_deferred=0, digests_cancelled=0)
    for _ in range(batch_size):
        digest = client.rpc('claim_daily_email_digest', {}).execute().data
        if not digest:
            break
        # Fail closed before the provider if content cannot be assembled completely.
        events = digest_events(client, digest['events'])
        events = client.rpc('daily_email_unseen_events', {'p_id':digest['id'],'p_events':events}).execute().data
        if not isinstance(events, list):
            raise ValueError('Cannot verify previously emailed activity')
        content = render_digest(digest, events)
        prepared = client.rpc('prepare_daily_email_digest', {'p_id':digest['id'],
            'p_subject':content['subject'], 'p_html':content['html'], 'p_text':content['text'],
            'p_items':content['items']}).execute().data
        if not prepared:
            summary['digests_cancelled'] += 1
            continue
        try:
            provider_id = send(digest['destination'], content, digest['id'])
        except DeliveryRateLimited:
            client.rpc('finish_daily_email_digest', {'p_id':digest['id'],'p_status':'ready'}).execute()
            summary['digests_deferred'] += 1
            break
        except Exception:
            client.rpc('finish_daily_email_digest', {'p_id':digest['id'],'p_status':'uncertain'}).execute()
            summary['digests_uncertain'] += 1
            continue
        # A DB failure leaves status=sending; never replay an accepted email automatically.
        client.rpc('finish_daily_email_digest', {'p_id':digest['id'],'p_status':'sent','p_provider_id':provider_id}).execute()
        summary['digests_sent'] += 1
        if min_send_interval:
            time.sleep(min_send_interval)
    return summary
