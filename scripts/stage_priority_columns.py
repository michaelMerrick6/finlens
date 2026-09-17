import json
import hashlib
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from house_annual_columns import parse_columns
from house_ptr_columns import parse_ptr_columns
ROOT=Path(__file__).resolve().parents[1]
def run(task):
    member_id,kind,path,source=task
    try:return dict(member_id=member_id,kind=kind,source=source,path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest(),**(parse_columns(path) if kind=='annual' else parse_ptr_columns(path)))
    except Exception as error:return dict(member_id=member_id,kind=kind,source=source,issues=[dict(reason=str(error))],current_holdings_eligible=False)
def main():
    inv=json.loads((ROOT/'docs/research/priority-holdings-inventory.json').read_text())['members']
    tx=json.loads((ROOT/'docs/research/priority-holdings-transactions.json').read_text())['documents']
    tasks=[]
    for m in inv:
        b=m.get('baseline',{})
        if b and 'image-or-empty-page-needs-review' not in b.get('issues',[]):
            tasks.append((m['member_id'],'annual',next((ROOT/'artifacts/congress-holdings/sources').glob('*-'+b['doc_id']+'.pdf')),b['source']))
    for d in tx:
        if d.get('extracted_pages') and d['status']=='transaction-reconciliation-required':
            tasks.append((d['member_id'],'ptr',next((ROOT/'artifacts/congress-holdings/priority-transactions').glob('*-'+d['doc_id']+'.pdf')),d['source']))
    with ProcessPoolExecutor(max_workers=3) as pool:
        rows=[]
        for i,r in enumerate(pool.map(run,tasks),1):
            rows.append(r)
            print(i,len(tasks),r['member_id'],r['kind'],len(r.get('holdings',r.get('events',[]))),len(r['issues']),flush=True)
    (ROOT/'docs/research/priority-holdings-columns.json').write_text(json.dumps(rows,indent=2)+'\n')
if __name__=='__main__':main()
