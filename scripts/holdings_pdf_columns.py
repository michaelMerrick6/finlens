"""Column boundaries from the form's header rectangles, independent of row shading."""
from collections import defaultdict

def header_geometry(page, kind):
    required={'Asset','Owner','Value','Income'} if kind=='annual' else {'Asset','Owner','Transaction','Date','Amount'}
    words=page.extract_words(); lines=defaultdict(list)
    for word in words:lines[round(word['top'])].append(word)
    for line in lines.values():
        if not required.issubset({w['text'] for w in line}):continue
        y=min(w['top'] for w in line); groups=defaultdict(list)
        for r in page.rects:
            if r['width']>5 and r['height']>10 and r['top']<=y<r['bottom']:
                groups[round(r['top'],1)].append(r)
        for top,rects in sorted(groups.items(),reverse=True):
            xs=[]
            for x in sorted(set(round(r[k],2) for r in rects for k in ['x0','x1'])):
                if not xs or x-xs[-1]>2:xs.append(x)
            count=7 if kind=='annual' else 9
            if len(xs)==count:return top,xs
    return None
