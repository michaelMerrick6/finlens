"""Resumable local OCR evidence for scanned priority filings. OCR is not verification."""
import json
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from pypdf import PdfReader
ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'artifacts/congress-holdings/ocr'

def ocr(task):
    pdf,number=task
    prefix=OUT/pdf.stem/f'page-{number:04}'
    prefix.parent.mkdir(parents=True,exist_ok=True)
    if prefix.with_suffix('.txt').exists() and prefix.with_suffix('.tsv').exists():return 'cached'
    image=prefix.with_suffix('.png')
    env={**os.environ,'OMP_THREAD_LIMIT':'1'}
    subprocess.run(['pdftoppm','-f',str(number),'-l',str(number),'-r','170','-singlefile','-png',str(pdf),str(prefix)],check=True,capture_output=True,timeout=90)
    subprocess.run(['tesseract',str(image),str(prefix),'--psm','1','txt','tsv'],check=True,capture_output=True,timeout=90,env=env)
    image.unlink(missing_ok=True)
    return 'processed'

def main():
    inventory=json.loads((ROOT/'docs/research/priority-holdings-inventory.json').read_text())
    transactions=json.loads((ROOT/'docs/research/priority-holdings-transactions.json').read_text())['documents']
    paths=[]
    for m in inventory['members']:
        b=m.get('baseline',{})
        if 'image-or-empty-page-needs-review' in b.get('issues',[]):
            paths.append(next((ROOT/'artifacts/congress-holdings/sources').glob('*-'+b['doc_id']+'.pdf')))
    for d in transactions:
        if d['status']=='ocr-and-review-required':paths.append(next((ROOT/'artifacts/congress-holdings/priority-transactions').glob('*-'+d['doc_id']+'.pdf')))
    tasks=[(p,n+1) for p in paths for n in range(len(PdfReader(p).pages))]
    errors=[]
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures={pool.submit(ocr,t):t for t in tasks}
        for i,future in enumerate(as_completed(futures),1):
            try:future.result()
            except Exception as error:errors.append(dict(file=str(futures[future][0]),page=futures[future][1],error=str(error)))
            if i%25==0:print(f'OCR {i}/{len(tasks)} pages; failures {len(errors)}',flush=True)
    report=dict(documents=len(paths),pages=len(tasks),errors=errors,current_holdings_eligible=False)
    (ROOT/'docs/research/priority-holdings-ocr.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report))
if __name__=='__main__':main()
