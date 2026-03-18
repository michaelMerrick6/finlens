import os
import requests
from pdf2image import convert_from_bytes
import pytesseract
from PIL import Image
import io

# House Paper Filing Example: Tony Wied
DOC_ID = "8221360"
YEAR = "2026"
PDF_URL = f"https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{YEAR}/{DOC_ID}.pdf"

def test_ocr():
    print(f"Downloading scanned PDF: {PDF_URL}")
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(PDF_URL, headers=headers)
    
    if r.status_code != 200:
        print(f"Failed to download: {r.status_code}")
        return

    print("Converting PDF to images...")
    # poppler_path might be needed if not in PATH, but we saw it in /opt/homebrew/bin/
    images = convert_from_bytes(r.content, dpi=300)
    
    print(f"Extracted {len(images)} pages. Running OCR on Page 1...")
    
    # Just test page 1 for now (usually where the trades are or start)
    text = pytesseract.image_to_string(images[0])
    
    print("\n--- OCR RESULTS (Page 1) ---")
    print(text)
    print("---------------------------\n")

    if len(images) > 1:
        print("Running OCR on Page 2...")
        text2 = pytesseract.image_to_string(images[1])
        print("\n--- OCR RESULTS (Page 2) ---")
        print(text2)
        print("---------------------------\n")

if __name__ == "__main__":
    test_ocr()
