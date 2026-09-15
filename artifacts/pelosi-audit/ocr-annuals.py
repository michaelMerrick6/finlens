"""Cache page-level OCR for scanned originals; never writes database."""
import json,os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
os.environ['OMP_THREAD_LIMIT']='1'
from pdf2image import convert_from_path
import pytesseract
root=Path(__file__).parent;out=root/'ocr';out.mkdir(exist_ok=True)
reports=json.loads((root/'annual-reconciliation.json').read_text())
jobs=[(r,p) for r in reports if r['status']=='needs_ocr' for p in range(1,r['pages']+1)]
def run(job):
 r,p=job;name=f"{r['index_year']}-{r['doc_id']}";dest=out/f'{name}-p{p:02}.txt'
 if not dest.exists():
  img=convert_from_path(root/'sources'/f'{name}.pdf',dpi=160,first_page=p,last_page=p)[0]
  dest.write_text(pytesseract.image_to_string(img,config='--psm 3'))
 return name,p
with ThreadPoolExecutor(max_workers=3) as pool:
 for i,result in enumerate(pool.map(run,jobs),1):
  if i%10==0:print(i,'/',len(jobs),flush=True)
print('DONE',len(jobs),flush=True)
