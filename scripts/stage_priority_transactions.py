"""Cache priority PTR evidence locally. No database writes or holdings promotion."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import re
import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader
from audit_priority_holdings import senate_session

ROOT=Path(__file__).resolve().parents[1]
CACHE=ROOT/'artifacts/congress-holdings/priority-transactions'


def house_document(task):
    member, doc=task
    path=CACHE/f'house-{doc["year"]}-{doc["doc_id"]}.pdf'
    result=dict(member_id=member['member_id'],name=member['name'],source=doc['source'],filed=doc['filed'],doc_id=doc['doc_id'],current_holdings_eligible=False)
    try:
        if not path.exists():
            response=requests.get(doc['source'],timeout=30);response.raise_for_status()
            if not response.content.startswith(b'%PDF'):raise ValueError('Invalid PDF')
            path.write_bytes(response.content)
        content=path.read_bytes();reader=PdfReader(io.BytesIO(content))
        pages=[page.extract_text() or '' for page in reader.pages]
        result.update(sha256=hashlib.sha256(content).hexdigest(),pages=len(pages),extracted_pages=pages,
                      status='ocr-and-review-required' if any(len(p.strip())<30 for p in pages) else 'transaction-reconciliation-required')
        # Keep complete text, including source owner, dates, corrections and footnotes.
        # Mentions are review leads, never quantities applied to positions.
        result['quantity_mentions']=re.findall(r'.{0,70}\b[\d,]+\s+(?:shares|contracts|options)\b.{0,120}', ' '.join(pages), re.I)
    except Exception as error:result.update(status='source-unavailable',error=str(error))
    return result


def main():
    CACHE.mkdir(parents=True,exist_ok=True)
    inventory=json.loads((ROOT/'docs/research/priority-holdings-inventory.json').read_text())
    tasks=[(member,doc) for member in inventory['members'] for doc in member.get('transaction_documents_to_review',[])]
    with ThreadPoolExecutor(max_workers=4) as pool:
        rows=[]
        for i,result in enumerate(pool.map(house_document,tasks),1):
            rows.append(result)
            if i%20==0:print(f'House evidence {i}/{len(tasks)}',flush=True)
    session=senate_session()
    for member in inventory['members']:
        if member['chamber']!='Senate':continue
        # Keep all PTRs from the inventory window until the baseline date is reviewed.
        for doc in member['documents']:
            if doc['kind']!='ptr':continue
            row=dict(member_id=member['member_id'],name=member['name'],source=doc['source'],filed=doc['filed'],current_holdings_eligible=False)
            try:
                response=session.get(doc['source'],timeout=30);response.raise_for_status()
                soup=BeautifulSoup(response.text,'html.parser')
                heading=' '.join(soup.h1.get_text(' ',strip=True).split()) if soup.h1 else ''
                if 'Periodic Transaction Report' not in heading:raise ValueError('Expected Senate PTR')
                tables=[[[c.get_text(' ',strip=True) for c in tr.find_all(['th','td'])] for tr in t.find_all('tr')] for t in soup.find_all('table')]
                doc_id=doc['source'].rstrip('/').split('/')[-1]
                (CACHE/f'senate-{doc_id}.html').write_text(response.text)
                row.update(doc_id=doc_id,sha256=hashlib.sha256(response.content).hexdigest(),tables=tables,
                           status='transaction-reconciliation-required' if tables else 'paper-report-review-required')
            except Exception as error:row.update(status='source-unavailable',error=str(error))
            rows.append(row)
        print(member['name'],'PTR evidence staged',flush=True)
    target=ROOT/'docs/research/priority-holdings-transactions.json'
    target.write_text(json.dumps(dict(checked_at=datetime.now(timezone.utc).isoformat(),documents=rows),indent=2)+'\n')
    print(json.dumps({'documents':len(rows),'unavailable':sum(r['status']=='source-unavailable' for r in rows)}))

if __name__=='__main__':main()
