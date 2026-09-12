"""Complete, stable member lookup shared by ingestion and read-only audits."""


def load_congress_members(client):
    rows = []
    while True:
        response = (client.table('congress_members')
                    .select('id, first_name, last_name, chamber, active')
                    .order('id').range(len(rows), len(rows) + 499).execute())
        batch = response.data or []
        rows.extend(batch)
        if len(batch) < 500:
            break
    if not rows:
        raise RuntimeError('Congress member lookup is empty; refusing unresolved imports')
    return rows
