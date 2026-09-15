"""Extract reported securities value ranges; never treat them as share counts."""
import json,re,hashlib
from pathlib import Path
from pypdf import PdfReader
p=Path(__file__).resolve().parent; pdf=p/'sources/2025-10075701.pdf'
rows=[]
for page_no,page in enumerate(PdfReader(pdf).pages[:7],1):
 text=page.extract_text().replace('\x00','').split('S B:')[0]
 pattern=r'\(([A-Z]+)\)\s*\[(ST|OP|OL)\]\s*SP\s*(None|\$[\d,]+\s*-\s*\$[\d,]+)'
 for m in re.finditer(pattern,text):
  ticker,kind,value=m.groups(); nums=[int(n.replace(',','')) for n in re.findall(r'\$([\d,]+)',value)]
  rows.append({'page':page_no,'ticker_as_filed':ticker,'asset_type_as_filed':kind,'owner':'SP','reported_value':value.replace('\n',' '),'value_min':nums[0] if nums else None,'value_max':nums[1] if nums else None,'shares':None,'status':'text_extracted_pending_visual_review','current_holding_verified':False})
result={'source':'https://disclosures-clerk.house.gov/public_disc/financial-pdfs/2025/10075701.pdf','sha256':hashlib.sha256(pdf.read_bytes()).hexdigest(),'reporting_year':2025,'filed':'2026-05-15','scope':'Ticker-bearing securities in Schedule A only; excludes private assets, cash and untickered funds. Not a complete portfolio.','limitations':['Value ranges do not provide exact share counts.','None is retained as reported, not converted into a numerical zero.','Current positions require subsequent disclosures and corporate-action reconciliation.','SP identifies spouse ownership, not Nancy Pelosi personal ownership.'],'rows':rows}
(p/'2025-holdings-baseline.json').write_text(json.dumps(result,indent=2)+'\n')
print('Rows',len(rows));print([(r['ticker_as_filed'],r['asset_type_as_filed'],r['reported_value']) for r in rows])
