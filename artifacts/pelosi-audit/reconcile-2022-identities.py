"""Reproduce identity reconciliation against visually reviewed annual pages 7-10."""
import json,re,hashlib
from pathlib import Path
from pypdf import PdfReader
P=Path(__file__).parent
source=P/'sources/2022-10053231.pdf'
reader=PdfReader(source)
chunks=[]
for page in range(7,11):
 text=reader.pages[page-1].extract_text().replace('\x00','')
 if page==7:text=text.split('S B: T',1)[1]
 if page==10:text=text.split('* For the complete',1)[0]
 text=re.sub(r'Asset Owner Date Tx\.\s*Type\s*Amount Cap\.\s*Gains >\s*\$200\?','',text)
 chunks.append((page,text))
text='\n'.join(t for _,t in chunks)
text=text.replace('$500,001 -\n\n(partial)$1,000,000', '$500,001 - $1,000,000')
pattern=re.compile(r'([^\n]+\[(?:OL|ST|OP|AB)\]) SP (\d{2}/\d{1,2}/2022) ([PSE])\s*(?:\(partial\)\s*)?(\$[\d,.]+(?:\s*-\s*\$[\d,]+)?)')
found=list(pattern.finditer(text))
# Source order, visually verified across all four Schedule B pages.
ids='''2023-20022260-0 2022-20020561-0 2023-20022260-2 2023-20022260-3 2023-20022260-1
2022-20021837-0 2022-20020561-1 2022-20020561-2 2022-20021142-0 2022-20021142-1 2022-20021374-0 2022-20021837-1 2022-20021142-2 2022-20021142-3 2023-20022260-4
2023-20022260-5 2022-20021374-1 2022-20021438-0 2022-20021837-2 2022-20020561-3 2023-20022260-6 2023-20022260-7 2022-20021675-0 2023-20022260-8 2023-20022260-9
2023-20022260-10 2022-20020662-0 2023-20022260-11 2022-20021374-2 2022-20022063-0 2022-20020561-4 2022-20021837-3 2023-20022260-12 2022-20020916-0'''.split()
assert len(found)==len(ids)==34,(len(found),len(ids))
db=json.loads((P/'stored-transactions.json').read_text()); bydoc={r['doc_id']:r for r in db}
rows=[]
for i,(m,key) in enumerate(zip(found,ids)):
 r=bydoc['house-'+key]; month,day,year=m[2].split('/');date=f'{year}-{month}-{int(day):02}'
 assert r['transaction_date']==date
 amount=re.sub(r'\s+',' ',m[4]);assert re.search(r'\$[\d,]+',r['amount_range'])[0]==re.search(r'\$[\d,]+',amount)[0]
 detail=text[m.end():found[i+1].start() if i+1<len(found) else len(text)].strip()
 page=7 if i<5 else 8 if i<15 else 9 if i<25 else 10
 rows.append(dict(annual_row=i+1,annual_page=page,asset=m[1],owner='SP',transaction_date=date,source_activity=m[3],amount_range=amount,source_details=detail,db_id=r['id'],doc_id=r['doc_id'],stored_ticker=r['ticker'],stored_asset_type=r['asset_type']))
originals=[r['doc_id'] for r in db if r['doc_id'].startswith('house-2022-20020515-')]
matched={r['doc_id'] for r in rows}
remaining=[r['doc_id'] for r in db if r['transaction_date'].startswith('2022') and r['doc_id'] not in matched and r['doc_id'] not in originals]
assert len(originals)==5 and not remaining
result=dict(status='34 annual transaction rows matched to 34 stored rows; five additional stored rows are previously reconciled superseded originals. Identity coverage verified against this annual report, not complete historical coverage.',source_url='https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2022/10053231.pdf',source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),visual_review='All Schedule B entries and notes reviewed on PDF pages 7-10',rows=rows,superseded_originals=originals,unmatched_stored_rows=remaining,issues=[
'REOF XX private investments on August 24 and December 27 incorrectly carry stock ticker ONE.',
'AllianceBernstein December 28 sale has missing ticker N/A.',
'Alphabet September 16 exercise is GOOG Class C in annual report but stored as GOOGL and OP; original PTR 20021837 page 1 also explicitly identifies GOOG Class C shares.',
'Option exercises must become shares and remove option contracts; raw types still include OP.',
'DIS September 16 is a worthless expiration; source $1 is not sale proceeds.',
'CRM December 20 source amount $9 conflicts with 130 contracts x 100 shares x $0.01 ($130); preserve discrepancy, do not infer exact proceeds.',
'WBD April 11 is 2419 shares received via spinoff, not cash purchase.'
])
(P/'2022-row-identity-reconciliation.json').write_text(json.dumps(result,indent=2)+'\n')
print('Verified 34 source rows, 34 distinct stored identities, 5 superseded originals, 0 unexplained stored rows.')
