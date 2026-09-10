"""Apply a reviewed repair plan atomically; dry-run rolls all mutations back.

Plans and backups contain production records and belong under ignored artifacts/.
Requires psycopg[binary], DATABASE_URL, and the existing repair-write opt-in.
This command does not run notification or social delivery workers.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'scripts'))
from dotenv import load_dotenv
from legacy_congress_guard import require_repair_write_opt_in
from reviewed_congress_filings import load_reviewed_filing, reviewed_trades


def normalized(value):
    return json.loads(json.dumps(value, default=str))


def check_same(actual, expected, label):
    # Concurrent timestamp refreshes are harmless; economic data changes are not.
    def keyed(rows):
        return {str(r['id']): {k: v for k, v in normalized(r).items() if k not in {'updated_at', 'created_at'}} for r in rows}
    if keyed(actual) != keyed(expected):
        raise ValueError(f'{label} changed since plan creation; regenerate the plan')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('plan', type=Path)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    load_dotenv('.env.local')
    require_repair_write_opt_in(Path(__file__).name)
    import psycopg
    from psycopg.rows import dict_row
    from psycopg.types.json import Jsonb
    from psycopg import sql

    plan = json.loads(args.plan.read_text())
    canonical = [r for prefix in plan['prefixes'] for r in reviewed_trades(prefix, load_reviewed_filing(prefix))]
    if [{k: r[k] for k in c} for r, c in zip(plan['trades'], canonical)] != canonical or len(plan['trades']) != len(canonical):
        raise ValueError('Repair rows differ from committed reviewed transcriptions')
    patterns = [p + '-%' for p in plan['prefixes']]
    derived_ids = [r['id'] for r in plan['old_derived']]
    expected_alerts = plan['old_alerts'] + plan['old_derived_dependencies']['alert_deliveries']
    preserved = {r['signal_event_id'] for r in expected_alerts}
    replacement_ids = {r['id'] for r in plan['signal_events'] + plan['derived']}
    if not preserved <= replacement_ids:
        raise ValueError('A historical alert has no verified replacement event')
    backup = {}
    with psycopg.connect(os.environ['DATABASE_URL'], connect_timeout=15, row_factory=dict_row) as conn:
        conn.execute("SET LOCAL lock_timeout = '15s'")
        conn.execute("SET LOCAL statement_timeout = '90s'")
        # Prevent ingestion/delivery workers changing affected rows between backup
        # and replacement, including FK inserts. All locks end at commit/rollback.
        conn.execute('LOCK TABLE politician_trades, raw_filings, signal_events, alert_deliveries, tweet_candidates, cluster_alert_daily_events IN SHARE ROW EXCLUSIVE MODE')
        for table, column in [('politician_trades', 'doc_id'), ('raw_filings', 'source_document_id'), ('signal_events', 'source_document_id')]:
            rows = conn.execute(sql.SQL('SELECT * FROM {} WHERE {} LIKE ANY(%s)').format(sql.Identifier(table), sql.Identifier(column)), (patterns,)).fetchall()
            check_same(rows, plan['expected'][table], table)
            backup[table] = rows
        derived = conn.execute('SELECT * FROM signal_events WHERE id=ANY(%s::uuid[])', (derived_ids,)).fetchall()
        check_same(derived, plan['old_derived'], 'derived signals')
        backup['derived'] = derived
        affected_ids = [str(r['id']) for r in backup['signal_events']] + derived_ids
        for table in ['alert_deliveries', 'tweet_candidates', 'cluster_alert_daily_events']:
            deps = conn.execute(sql.SQL('SELECT * FROM {} WHERE signal_event_id=ANY(%s::uuid[])').format(sql.Identifier(table)), (affected_ids,)).fetchall()
            backup[table] = deps
            if table == 'alert_deliveries':
                check_same(deps, expected_alerts, table)
                if any(str(d['signal_event_id']) not in preserved or d['status'] != 'sent' for d in deps):
                    raise ValueError('Unexpected alert dependency')
            elif table == 'tweet_candidates':
                check_same(deps, plan['old_derived_dependencies'][table], table)
                if any(d['status'] != 'pending_review' or d.get('posted_at') or d.get('external_post_id') for d in deps):
                    raise ValueError('Cannot remove a published/reviewed social record')
            elif deps:
                raise ValueError('Unexpected cluster delivery dependency')
        new_keys = [r['source_document_id'] for r in plan['derived']]
        collisions = conn.execute('SELECT * FROM signal_events WHERE source_document_id=ANY(%s)', (new_keys,)).fetchall()
        replacements = [r for r in collisions if str(r['id']) not in derived_ids]
        check_same(replacements, plan['existing_derived_replacements'], 'existing derived replacements')
        backup['existing_derived_replacements'] = replacements
        stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
        backup_path = args.plan.parent / f'database-repair-backup-{stamp}.json'
        with backup_path.open('x') as f:
            json.dump(backup, f, default=str, indent=2)
            f.flush()
            os.fsync(f.fileno())
        print(f'Backup written: {backup_path}', flush=True)
        # A sent alert keeps its signal UUID. Move that UUID out of the old
        # unique-key range temporarily; restore it with the corrected CL trade.
        conn.execute("UPDATE signal_events SET source_document_id=source_document_id || '::repair-in-progress', raw_filing_id=NULL WHERE id=ANY(%s::uuid[])", (list(preserved),))
        conn.execute('DELETE FROM tweet_candidates WHERE signal_event_id=ANY(%s::uuid[])', (affected_ids,))
        remove_ids = [i for i in affected_ids if i not in preserved]
        conn.execute('DELETE FROM signal_events WHERE id=ANY(%s::uuid[])', (remove_ids,))
        conn.execute('DELETE FROM raw_filings WHERE source_document_id LIKE ANY(%s)', (patterns,))
        conn.execute('DELETE FROM politician_trades WHERE doc_id LIKE ANY(%s)', (patterns,))

        def insert_rows(table, rows, update=False):
            if not rows:
                return
            columns = list(rows[0])
            if any(set(r) != set(columns) for r in rows):
                raise ValueError(f'Inconsistent columns in {table}')
            query = sql.SQL('INSERT INTO {} ({}) VALUES ({})').format(sql.Identifier(table), sql.SQL(',').join(map(sql.Identifier, columns)), sql.SQL(',').join(sql.Placeholder() for _ in columns))
            if update:
                query += sql.SQL(' ON CONFLICT (id) DO UPDATE SET ') + sql.SQL(',').join(sql.SQL('{}=EXCLUDED.{}').format(sql.Identifier(k), sql.Identifier(k)) for k in columns if k != 'id')
            values = [[Jsonb(r[k]) if isinstance(r[k], (dict, list)) else r[k] for k in columns] for r in rows]
            with conn.cursor() as cur:
                cur.executemany(query, values)

        insert_rows('politician_trades', plan['trades'])
        insert_rows('raw_filings', plan['raw_filings'])
        insert_rows('signal_events', plan['signal_events'], update=True)
        insert_rows('signal_events', plan['derived'], update=True)
        for table, column, key in [('politician_trades', 'doc_id', 'trades'), ('raw_filings', 'source_document_id', 'raw_filings'), ('signal_events', 'source_document_id', 'signal_events')]:
            rows = conn.execute(sql.SQL('SELECT * FROM {} WHERE {} LIKE ANY(%s)').format(sql.Identifier(table), sql.Identifier(column)), (patterns,)).fetchall()
            expected = plan[key]
            fields = set(expected[0]) - {'created_at', 'updated_at'}
            def project(data):
                return {str(r['id']): {k: float(r[k]) if k == 'importance_score' else normalized(r[k]) for k in fields} for r in data}
            if project(rows) != project(expected):
                raise ValueError(f'{table} readback does not match repair plan')
        after_alerts = conn.execute('SELECT * FROM alert_deliveries WHERE signal_event_id=ANY(%s::uuid[])', (list(preserved),)).fetchall()
        check_same(after_alerts, expected_alerts, 'preserved alert history')
        result = dict(applied=args.apply, trades=len(plan['trades']), raw_filings=len(plan['raw_filings']), signals=len(plan['signal_events']), stale_derived_replaced=len(derived), derived_upserted=len(plan['derived']), existing_derived_updated=len(replacements), alerts_preserved=len(after_alerts), backup=str(backup_path))
        if args.apply:
            conn.commit()
        else:
            conn.rollback()
        result_path = args.plan.parent / ('repair-result.json' if args.apply else 'repair-dry-run.json')
        result_path.write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
