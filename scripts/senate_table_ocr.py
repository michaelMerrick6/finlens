"""Local extraction of the ruled, 11-amount-column Senate paper form.

No network, database, model API, or reviewed-answer imports. Coordinates are
measured from each page; unknown layouts raise rather than falling back to guesses.
"""
import re
from datetime import datetime

import cv2
import numpy as np
import pytesseract


class TableReviewRequired(ValueError):
    pass


AMOUNTS = (
    "$1,001 - $15,000", "$15,001 - $50,000", "$50,001 - $100,000",
    "$100,001 - $250,000", "$250,001 - $500,000", "$500,001 - $1,000,000",
    "Over $1,000,000", "$1,000,001 - $5,000,000", "$5,000,001 - $25,000,000",
    "$25,000,001 - $50,000,000", "Over $50,000,000",
)


def groups(positions, gap=4):
    result = []
    for p in positions:
        if not result or p - result[-1][-1] > gap:
            result.append([int(p)])
        else:
            result[-1].append(int(p))
    return [int(np.median(g)) for g in result]


def align_page(image):
    gray = np.array(image.convert("L"))
    h, w = gray.shape
    lines = cv2.HoughLinesP((gray < 170).astype('uint8') * 255, 1, np.pi / 1800,
                           threshold=200, minLineLength=w * .4, maxLineGap=25)
    angles = []
    if lines is not None:
        for x1, y1, x2, y2 in lines.reshape(-1, 4):
            angle = np.degrees(np.arctan2(y2 - y1, x2 - x1))
            if abs(angle) < 5:
                angles.append(angle)
    if angles:
        angle = float(np.median(angles))
        if abs(angle) > .03:
            gray = cv2.warpAffine(gray, cv2.getRotationMatrix2D((w / 2, h / 2), angle, 1),
                                 (w, h), borderValue=255)
    return gray


def cell_text(cell, *, numeric=False):
    if not cell.size:
        raise TableReviewRequired("Empty cell crop")
    cell = cv2.resize(cell, None, fx=2, fy=2, interpolation=cv2.INTER_CUBIC)
    cell = cv2.copyMakeBorder(cell, 10, 10, 10, 10, cv2.BORDER_CONSTANT, value=255)
    config = '--psm 7' if numeric else '--psm 6'
    if numeric:
        config += ' -c tessedit_char_whitelist=0123456789/'
    return ' '.join(pytesseract.image_to_string(cell, config=config, timeout=15).split())


def mark_scores(gray, xs, lo, hi, columns):
    # Inset all four borders to keep grid ink out of the checkbox measurement.
    scores = []
    for c in columns:
        inset = max(5, round((xs[c + 1] - xs[c]) * .12))
        crop = gray[lo + inset:hi - inset, xs[c] + inset:xs[c + 1] - inset]
        scores.append(float(np.mean(crop < 140)) if crop.size else 0)
    return scores


def selected_mark(scores):
    active = [i for i, score in enumerate(scores) if score >= .003]
    if not active:
        return None
    if len(active) != 1:
        raise TableReviewRequired("Multiple or noisy checkbox marks")
    return active[0]


def page_geometry(gray):
    h, w = gray.shape
    bw = (gray < 170).astype('uint8') * 255
    vertical = cv2.morphologyEx(bw, cv2.MORPH_OPEN, np.ones((round(h * .10), 1), np.uint8))
    xs = groups(np.where(np.count_nonzero(vertical[round(h * .45):], axis=0) > h * .25)[0])
    if len(xs) != 18:
        raise TableReviewRequired(f"Unsupported table: expected 18 boundaries, found {len(xs)}")
    widths = np.diff(xs)
    unit = float(np.median(widths[6:]))
    if not (6 < widths[1] / unit < 10 and 2 < widths[5] / unit < 4
            and all(.65 < x / unit < 1.4 for x in list(widths[2:5]) + list(widths[6:]))):
        raise TableReviewRequired("Unsupported column proportions")
    # The row-number divider begins after the example rows and ends before notes.
    pos = np.where(np.any(vertical[:, xs[1]-2:xs[1]+3] > 0, axis=1))[0]
    runs = np.split(pos, np.where(np.diff(pos) > 4)[0] + 1)
    run = max(runs, key=len)
    start, end = int(run[0]), int(run[-1])
    # Amount dividers stop at the last transaction, even if the number divider
    # continues down into a footnote spanning the other columns.
    for c in (6, 8, 10):
        positions = np.where(np.any(vertical[:, xs[c]-2:xs[c]+3] > 0, axis=1))[0]
        segments = np.split(positions, np.where(np.diff(positions) > 4)[0] + 1)
        end = min(end, int(max(segments, key=len)[-1]))
    horizontal = cv2.morphologyEx(bw, cv2.MORPH_OPEN, np.ones((1, round(w * .15)), np.uint8))
    ys = groups(np.where(np.count_nonzero(horizontal[:, xs[0]:xs[-1]], axis=1) > (xs[-1]-xs[0]) * .8)[0])
    body = [y for y in ys if start - 8 <= y <= end + 8]
    if len(body) < 3 or abs(body[0]-start) > 8 or abs(body[-1]-end) > 8:
        raise TableReviewRequired("Incomplete row grid")
    if any(not h * .02 < b-a < h * .045 for a,b in zip(body, body[1:])):
        raise TableReviewRequired("Unsupported or broken row grid")
    header_bottom = next((b for a,b in zip(ys,ys[1:]) if b <= body[0]+8 and b-a > h*.12), None)
    if header_bottom is None:
        raise TableReviewRequired("Cannot locate column headings")
    return xs, body, header_bottom


def verify_headings(gray, xs, bottom):
    # Verify the meaning/order of all checkbox columns, not just their count.
    for c, expected in [(2,'purchase'),(3,'sale'),(4,'exchange')]:
        crop = gray[bottom-500:bottom-5, xs[c]+5:xs[c+1]-5]
        text = cell_text(cv2.rotate(crop, cv2.ROTATE_90_CLOCKWISE)).lower()
        if expected not in text:
            raise TableReviewRequired(f"Unrecognized transaction heading {c}")
    for i, expected in enumerate(AMOUNTS):
        crop = gray[bottom-500:bottom-5, xs[i+6]+5:xs[i+7]-5]
        text = cell_text(cv2.rotate(crop, cv2.ROTATE_90_CLOCKWISE))
        # Punctuation varies; the digits and Over qualifier must agree exactly.
        if re.sub(r'\D','',text) != re.sub(r'\D','',expected) or ('over' in text.lower()) != expected.startswith('Over'):
            raise TableReviewRequired(f"Unrecognized amount heading {i+1}: {text}")


def extract_page(image, *, page, filed_date, account=None):
    gray = align_page(image)
    # Normalize before measuring thin borders and cropping text.
    gray = cv2.resize(gray, (3400, 4400), interpolation=cv2.INTER_CUBIC)
    xs, ys, header_bottom = page_geometry(gray)
    verify_headings(gray, xs, header_bottom)
    rows = []
    for number, (lo,hi) in enumerate(zip(ys,ys[1:]), 1):
        def crop(c):
            return gray[lo+6:hi-6, xs[c]+6:xs[c+1]-6]
        direction = selected_mark(mark_scores(gray,xs,lo,hi,range(2,5)))
        amount = selected_mark(mark_scores(gray,xs,lo,hi,range(6,17)))
        date_ink = float(np.mean(crop(5) < 140))
        asset = cell_text(crop(1))
        if direction is None and amount is None and date_ink < .001:
            # Account heading or blank spacer: no populated financial cells.
            if asset:
                ink_columns = np.where(np.any(crop(1) < 140, axis=0))[0]
                if not len(ink_columns) or ink_columns[0] > 50:
                    raise TableReviewRequired(f"Page {page}, row {number}: asset without financial fields")
                account = asset
            continue
        if direction is None or amount is None:
            raise TableReviewRequired(f"Page {page}, row {number}: missing checkbox")
        raw_date = cell_text(crop(5), numeric=True).replace(' ','')
        parsed = None
        for fmt in ('%m/%d/%Y','%m/%d/%y'):
            try:
                parsed = datetime.strptime(raw_date,fmt).date().isoformat()
                break
            except ValueError:
                pass
        if not parsed or parsed > filed_date or not asset:
            raise TableReviewRequired(f"Page {page}, row {number}: unreadable asset/date ({raw_date})")
        rows.append(dict(page=page,row=number,asset_name=asset,account=account,
                         transaction_date=parsed,transaction_type=('buy','sell','exchange')[direction],
                         amount_range=AMOUNTS[amount],bounds=[xs[0],lo,xs[-1],hi]))
    return rows, account


def is_cover_letter(image):
    """Recognize an introductory enclosure letter, never an arbitrary blank scan."""
    gray = np.array(image.convert('L'))
    h, w = gray.shape
    bw = (gray < 170).astype('uint8') * 255
    vertical = cv2.morphologyEx(bw, cv2.MORPH_OPEN, np.ones((round(h*.15),1),np.uint8))
    if np.count_nonzero(vertical[round(h*.15):]) > h*.05:
        return False
    text = pytesseract.image_to_string(gray, config='--psm 6', timeout=30).lower()
    return ('enclosed please find' in text and 'enclosures' in text
            and 'secretary of the senate' in text and 'identification of assets' not in text)


def extract_document(images, *, filed_date):
    rows, account = [], None
    if not images:
        raise TableReviewRequired('No source pages')
    for page, image in enumerate(images, 1):
        # Only the first page may be an enclosure letter. Never silently skip
        # an unsupported continuation page or publish a partial document.
        if page == 1 and is_cover_letter(image):
            continue
        found, account = extract_page(image, page=page, filed_date=filed_date, account=account)
        rows.extend(found)
    if not rows:
        raise TableReviewRequired('No transactions found in supported tables')
    return rows
