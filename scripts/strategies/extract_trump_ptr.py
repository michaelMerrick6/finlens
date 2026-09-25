#!/usr/bin/env python3
"""Create review text from scanned Trump PTRs; never publish OCR as holdings.

Horizontal and vertical table rules confuse whole-page OCR. Removing those
rules before OCR retains separate transaction lines. Original rendered pages
are retained beside the text so every correction can be checked visually.
Usage: python scripts/strategies/extract_trump_ptr.py INPUT.pdf OUTPUT_DIR
Requires Poppler, Tesseract and scripts/requirements.txt. Output is draft only.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import subprocess

import cv2
import numpy as np
from pypdf import PdfReader


def extract_page(pdf, output, number):
    stem = output / f'page-{number:03}'
    original = stem.with_suffix('.png')
    text_path = stem.with_suffix('.txt')
    if not original.exists():
        subprocess.run(['pdftoppm', '-f', str(number), '-l', str(number),
                        '-r', '250', '-singlefile', '-gray', '-png', str(pdf), str(stem)],
                       check=True, capture_output=True)
    if not text_path.exists():
        gray = cv2.imread(str(original), cv2.IMREAD_GRAYSCALE)
        ink = cv2.threshold(gray, 170, 255, cv2.THRESH_BINARY_INV)[1]
        horizontal = cv2.morphologyEx(ink, cv2.MORPH_OPEN,
                                     cv2.getStructuringElement(cv2.MORPH_RECT, (180, 1)))
        vertical = cv2.morphologyEx(ink, cv2.MORPH_OPEN,
                                   cv2.getStructuringElement(cv2.MORPH_RECT, (1, 70)))
        gray[cv2.dilate(horizontal | vertical, np.ones((3, 3), np.uint8)) > 0] = 255
        cleaned = output / f'page-{number:03}-clean.png'
        cv2.imwrite(str(cleaned), gray)
        text = subprocess.check_output(['tesseract', str(cleaned), 'stdout', '--psm', '6'],
                                       text=True, stderr=subprocess.DEVNULL)
        text_path.write_text(text)
    return number


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('pdf', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(args.pdf.read_bytes()).hexdigest()
    manifest = args.output / 'manifest.json'
    if manifest.exists() and json.loads(manifest.read_text())['sha256'] != digest:
        raise ValueError('Source changed; use a new review directory')
    pages = len(PdfReader(args.pdf).pages)
    manifest.write_text(json.dumps({'sha256': digest, 'pages': pages,
                                   'status': 'unreviewed-ocr', 'currentHoldingsEligible': False}, indent=2))
    cv2.setNumThreads(1)
    with ThreadPoolExecutor(max_workers=4) as pool:
        for number in pool.map(lambda n: extract_page(args.pdf, args.output, n), range(1, pages + 1)):
            print(f'Extracted page {number}/{pages}', flush=True)


if __name__ == '__main__':
    main()
