#!/usr/bin/env python3
"""
Process all documents in Thue-Phi-Le-Phi (Tax-Fee-Levy) category.
Outputs structured JSON files and raw TXT files for each document.
"""

import json
import os
import sys
from pathlib import Path
from tqdm import tqdm
from bs4 import BeautifulSoup

sys.path.insert(0, '.')
from datasets import load_from_disk
from parsers import get_parser

# Output directories
OUTPUT_DIR = Path("outputs/thue_phi_le_phi")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def sanitize_filename(name: str, max_len: int = 80) -> str:
    """Create safe filename from document title."""
    invalid = '<>:"/\\|?*'
    for char in invalid:
        name = name.replace(char, '_')
    name = '_'.join(name.split())
    return name[:max_len]

def save_raw_text(html_content: str, output_path: Path):
    """Clean HTML tags and save as plain text."""
    if not html_content:
        return
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Remove scripts and styles
    for script in soup(["script", "style"]):
        script.extract()
        
    text = soup.get_text(separator='\n\n', strip=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(text)

def process_document(item: dict) -> dict:
    """Process a single document."""
    doc_type = item.get('loai_van_ban', 'Unknown')
    html = item.get('noi_dung_html', '')
    title = item.get('title', 'Untitled')
    
    if not html:
        return None
    
    parser = get_parser(doc_type)
    result = parser.parse(html, title=title)
    
    return {
        "document_info": {
            "title": title,
            "so_hieu": item.get('so_hieu', ''),
            "loai_van_ban": doc_type,
            "category": item.get('category', ''),
            "link": item.get('link', ''),
            "ngay_ban_hanh": item.get('ngay_ban_hanh', ''),
            "noi_ban_hanh": item.get('noi_ban_hanh', ''),
            "tinh_trang": item.get('tinh_trang', ''),
        },
        "parsed_result": result,
        "raw_html": html  # Return this to save as txt later if needed, but we pass item to loop
    }

def main():
    print("Loading dataset...")
    ds = load_from_disk('data_universal')
    
    print("Filtering Thue-Phi-Le-Phi documents...")
    tax_docs = [item for item in tqdm(ds['train'], desc="Scanning") 
                if item.get('category') == 'Thue-Phi-Le-Phi']
    
    print(f"\n📊 Found {len(tax_docs)} documents in Thue-Phi-Le-Phi category")
    
    # Process each document
    print(f"\n🔄 Processing {len(tax_docs)} documents (JSON + TXT)...")
    success = 0
    errors = 0
    
    for item in tqdm(tax_docs, desc="Processing"):
        try:
            # Process JSON
            result = process_document(item)
            if result:
                doc_type = item.get('loai_van_ban', 'Unknown')
                type_dir = OUTPUT_DIR / sanitize_filename(doc_type)
                type_dir.mkdir(exist_ok=True)
                
                # Base filename
                filename = sanitize_filename(item.get('so_hieu', '') or item.get('title', 'doc'))
                
                # Handle duplicate filenames (check availability based on json)
                counter = 1
                base_path = type_dir / filename
                json_path = base_path.with_suffix('.json')
                
                while json_path.exists():
                    json_path = type_dir / f"{filename}_{counter}.json"
                    counter += 1
                
                # Final paths
                final_json_path = json_path
                # TXT path corresponds to JSON path
                final_txt_path = final_json_path.with_suffix('.txt')
                
                # Save JSON
                with open(final_json_path, 'w', encoding='utf-8') as f:
                    # Don't save raw_html in JSON to keep it clean, user requested separate txt
                    if 'raw_html' in result:
                        del result['raw_html']
                    json.dump(result, f, ensure_ascii=False, indent=2)
                
                # Save Raw Text
                save_raw_text(item.get('noi_dung_html', ''), final_txt_path)
                
                success += 1
        except Exception as e:
            errors += 1
            print(f"\n⚠️ Error: {item.get('title', '')[:50]}... - {e}")
    
    print(f"\n✅ Complete!")
    print(f"   Processed: {success}")
    print(f"   Errors: {errors}")
    print(f"   Output: {OUTPUT_DIR}")

if __name__ == "__main__":
    main()
