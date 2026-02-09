import os
import re
import json
import logging
import traceback
from bs4 import BeautifulSoup, NavigableString, Tag
from datasets import load_from_disk
from tqdm import tqdm
==============================================================================================================================
# Configure logging==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ==========================================
# 1. DATA STRUCTURES
# ==========================================

class LegalNode:
    def __init__(self, level, type_name, title, content="", html_id=None):
        self.level = level         # Hierarchy level (0=Doc, 1=Part, 2=Chapter, 3=Section, 4=Article, 5=Clause)
        self.type = type_name      # 'document', 'part', 'chapter', 'section', 'article', 'clause', 'point'
        self.title = title         # e.g., "Điều 1. Phạm vi điều chỉnh"
        self.content_lines = []    # List of text segments for body content
        if content:
            self.content_lines.append(content)
        self.children = []         # Nested nodes
        self.html_id = html_id     # For linking back to source if needed

    def add_text(self, text):
        if text.strip():
            self.content_lines.append(text.strip())

    def to_dict(self):
        data = {
            "type": self.type,
            "title": self.title,
        }
        if self.html_id:
            data["html_id"] = self.html_id
            
        # Join content lines
        full_content = "\n".join(self.content_lines).strip()
        if full_content:
            data["content"] = full_content
            
        if self.children:
            data["children"] = [child.to_dict() for child in self.children]
        return data

# ==========================================
# 2. PARSER LOGIC
# ==========================================

class TVPLParser:
    def __init__(self):
        # Regex patterns for identifying headers
        self.PATTERNS = {
            # Part: PHẦN THỨ NHẤT, PHẦN I... (Allows prefix like "như sau:")
            'part': re.compile(r'(?:^|.*[\.\:\n]\s*)((?:Phần\s+(?:thứ\s+)?[IVX]+|PHẦN\s+(?:THỨ\s+)?[IVX]+|Phần\s+[A-Z]+|PHẦN\s+[A-Z]+).*)', re.IGNORECASE | re.DOTALL),
            # Chapter: CHƯƠNG I, Chương 1... OR Roman Numeral I., II.
            'chapter': re.compile(r'(?:^|.*[\.\:\n]\s*)(((?:Chương\s+[IVX0-9]+|CHƯƠNG\s+[IVX0-9]+)|(?:[IVX]+)\.\s+).*)', re.IGNORECASE | re.DOTALL),
            # Section: MỤC 1...
            'section': re.compile(r'(?:^|.*[\.\:\n]\s*)((?:Mục\s+[0-9]+|MỤC\s+[0-9]+).*)', re.IGNORECASE | re.DOTALL),
            # Article: Điều 1., Điều 1:, ĐIỀU 1...
            'article': re.compile(r'^\s*(Điều\s+\d+|ĐIỀU\s+\d+)[\.:]?\s*(.*)', re.IGNORECASE),
            # Point: a), b), đ) - usually inside Clause
            'point': re.compile(r'^\s*([a-zđ])[\)\.]\s+(.*)', re.IGNORECASE),
             # Appendix: Phụ lục... or Mẫu số...
            'appendix': re.compile(r'^\s*(Phụ lục|PHỤ LỤC|Mẫu số|MẪU SỐ)\s+[0-9IVX]*.*', re.IGNORECASE),
            # Metadata Markers
            'recipients': re.compile(r'^\s*(Nơi nhận|Nơi gửi)[:;]', re.IGNORECASE),
            'signature': re.compile(r'^\s*(TM\.|KT\.|TL\.|PP\.|CHỦ TỊCH|THỦ TƯỚNG|BỘ TRƯỞNG|THỐNG ĐỐC|GIÁM ĐỐC|TỔNG GIÁM ĐỐC|QUYỀN|KÝ THAY|Thay mặt).*', re.IGNORECASE)
        }
        # Loose numbering for Chỉ thị (e.g. "1.", "2.1.")
        self.loose_numbering = re.compile(r'^(\d+(\.\d+)*)\.?\s+(.*)')
        
    def clean_text(self, text):
        if not text: return ""
        # Remove non-breaking spaces and excessive whitespace
        return re.sub(r'\s+', ' ', text.replace('\xa0', ' ').replace('\r', '')).strip()

    def parse_html(self, html_content, doc_title, doc_type=""):
        if not html_content:
            return None, {}, []
            
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Try to find the main content body
        content_div = soup.find('div', class_='content1') or soup.find('div', id='contentBody') or soup.body or soup

        # Cleanup scripts/styles
        for tag in content_div.find_all(['script', 'style', 'iframe']):
            tag.extract()

        # Initialize Root Node
        root = LegalNode(0, "document", doc_title)
        
        # State holders
        stack = [root]
        metadata = {
            "recipients": [],
            "signers": []
        }
        attachments = []
        is_parsing_appendices = False
        is_parsing_metadata = False
        
        processed_elements = set()
        elements = content_div.find_all(['p', 'div', 'h3', 'h4', 'h5', 'table', 'span'])
        
        # Flags for loose parsing (Chỉ thị, Thông tư, Nghị quyết, Kế hoạch, Quyết định)
        loose_types = ["chỉ thị", "thông tư", "nghị quyết", "kế hoạch", "quyết định", "hướng dẫn"]
        is_loose_doc = any(t in doc_type.lower() for t in loose_types) if doc_type else False

        for el in elements:
            if el in processed_elements:
                continue
            
            text_raw = el.get_text(separator=" ", strip=True)
            text = self.clean_text(text_raw)
            if not text: 
                continue

            # Check for boldness (Header indicator)
            is_bold = bool(el.find('b') or el.find('strong'))
            if not is_bold and el.name in ['h3', 'h4', 'h5']:
                is_bold = True
                
            anchor = el.find('a', attrs={'name': True})
            html_id = anchor['name'] if anchor else None

            # --- ANCHOR-BASED STRUCTURAL DETECTION (PRIORITY) ---
            anchor_type = None
            if html_id:
                if html_id.startswith('dieu_'): anchor_type = 'article'
                elif html_id.startswith('chuong_'): anchor_type = 'chapter'
                elif html_id.startswith('phan_'): anchor_type = 'part'
                elif html_id.startswith('muc_'): anchor_type = 'section'
                elif html_id.startswith('khoan_'): anchor_type = 'clause' # Sometimes used

            # --- STOPPERS & METADATA ---
            
            # 1. Recipients (Nơi nhận)
            if self.PATTERNS['recipients'].match(text):
                is_parsing_metadata = True
                metadata['recipients'].append(text)
                continue
                
            # 2. Signatures
            if self.PATTERNS['signature'].match(text) and (len(text) < 100 or is_bold): 
                is_parsing_metadata = True
                metadata['signers'].append(text)
                continue
                
            if is_parsing_metadata:
                # Heuristic to break out of metadata if we see a clear header or appendix
                is_header = any(p.match(text) for k,p in self.PATTERNS.items() if k not in ['recipients', 'signature'])
                if is_header or anchor_type:
                    is_parsing_metadata = False 
                    # Fall through to process as content
                else:
                    if len(text) < 50 and (text[0].isupper() or text.startswith("-")):
                         metadata['signers'].append(text)
                    elif text.startswith("-"):
                         metadata['recipients'].append(text)
                    continue

            # 3. Appendix (Level 1 equivalent, acts like a Part)
            # Also catch "Mẫu số" if top-level bold
            if self.PATTERNS['appendix'].match(text) and (is_bold or len(text) < 100):
                 is_parsing_appendices = True
                 attachments.append({
                     "title": text,
                     "content": []
                 })
                 continue
            
            if is_parsing_appendices:
                # Check if we hit metadata
                if self.PATTERNS['recipients'].match(text) or self.PATTERNS['signature'].match(text):
                    is_parsing_appendices = False
                    is_parsing_metadata = True
                    # Re-evaluate this line for metadata in next iteration or handle now?
                    # Let's handle now roughly
                    if self.PATTERNS['recipients'].match(text): metadata['recipients'].append(text)
                    else: metadata['signers'].append(text)
                    continue
                
                # Check if we hit a MAIN header (Article/Chapter) -> Exit appendix mode?
                # Unlikely in law docs, Appendices usually at end. But possible.
                if self.PATTERNS['article'].match(text) or self.PATTERNS['chapter'].match(text):
                     is_parsing_appendices = False
                     # Fall through to process as main content
                else:
                    if attachments:
                        attachments[-1]["content"].append(text)
                    continue

            # --- REGULAR MATCHING ---
            
            matched_node = None
            
            # Priority 1: Anchor
            if anchor_type == 'part': matched_node = LegalNode(1, "part", text, html_id=html_id)
            elif anchor_type == 'chapter': matched_node = LegalNode(2, "chapter", text, html_id=html_id)
            elif anchor_type == 'section': matched_node = LegalNode(3, "section", text, html_id=html_id)
            elif anchor_type == 'article': matched_node = LegalNode(4, "article", text, html_id=html_id)
            elif anchor_type == 'clause': matched_node = LegalNode(5, "clause", text, html_id=html_id)
            
            # Priority 2: Text Pattern detection
            if not matched_node:
                # print(f"DEBUG: '{text[:50]}...' Bold: {is_bold}")
                if self.PATTERNS['part'].match(text):
                     # print(f"  Matches Part. Bold: {is_bold}")
                     if is_bold:
                         matched_node = LegalNode(1, "part", text, html_id=html_id)
                elif self.PATTERNS['chapter'].match(text):
                     # print(f"  Matches Chapter. Bold: {is_bold}")
                     if is_bold:
                         matched_node = LegalNode(2, "chapter", text, html_id=html_id)
                elif self.PATTERNS['section'].match(text):
                     if is_bold:
                         matched_node = LegalNode(3, "section", text, html_id=html_id)
                
                # Article: Allow non-bold for "Nghị quyết"/"Quyết định" if it clearly starts with "Điều"
                elif (match := self.PATTERNS['article'].match(text)):
                      # Strict check: bold is preferred, but for loose types we allow plain text if it looks like "Điều X."
                      if is_bold or (is_loose_doc and re.match(r'^(Điều|ĐIỀU)\s+\d+[\.\:]', text)):
                          matched_node = LegalNode(4, "article", text, html_id=html_id)
                
                # Loose Numbering / Clauses
                # If "Chỉ thị", treat "1.", "2." as Articles/Sections
                elif (match := self.loose_numbering.match(text)):
                    number = match.group(1)
                    content = match.group(3)
                    
                    # FIX: FIRST check if we're inside a clause or point
                    # If so, "2." should be a SIBLING clause (stack will pop)
                    if stack[-1].type in ('clause', 'point'):
                        matched_node = LegalNode(5, "clause", number, content=content, html_id=html_id)
                    # Standard Doc: "1." directly inside Article is a Clause
                    elif stack[-1].type == 'article':
                        matched_node = LegalNode(5, "clause", number, content=content, html_id=html_id)
                    # Loose doc logic (Chỉ thị, Thông tư): "1." at top level is an Item
                    elif is_loose_doc and stack[-1].level < 4:
                        matched_node = LegalNode(4, "item", text, content=content, html_id=html_id)
                    else:
                        stack[-1].add_text(text)
                        
                # 7. Point (a, b, c...)
                elif (match := self.PATTERNS['point'].match(text)):
                     matched_node = LegalNode(6, "point", text, html_id=html_id)

            # --- STACK UPDATE ---
            if matched_node:
                while len(stack) > 1 and stack[-1].level >= matched_node.level:
                    stack.pop()
                parent = stack[-1]
                parent.children.append(matched_node)
                stack.append(matched_node)
            else:
                stack[-1].add_text(text)

        # Post-process attachments to string
        final_attachments = []
        for att in attachments:
            final_attachments.append({
                "title": att["title"],
                "content": "\n".join(att["content"])
            })

        return root.to_dict(), metadata, final_attachments

# ==========================================
# 3. MAIN
# ==========================================

from concurrent.futures import ProcessPoolExecutor
import multiprocessing

# Global helper for multiprocessing
def slugify(value):
    if not value: return "Uncategorized"
    value = str(value)
    value = re.sub(r'[\\/*?:"<>|]', "", value)
    value = re.sub(r'\s+', "_", value)
    return value

def process_item(item):
    # Re-instantiate parser per process or rely on lightweight init
    # Since TVPLParser is light (just regex), local init is fine.
    parser = TVPLParser()
    
    title = item.get('title', 'Unknown')
    output_dir = "outputs/structured_data"
    
    raw_category = item.get('category', 'Khac')
    raw_doc_type = item.get('loai_van_ban')

    # --- INFER DOC TYPE FROM TITLE ---
    title_lower = title.strip().lower()
    inferred_type = None
    
    # Priority check for types
    targets = ["Chỉ thị", "Thông tư", "Quyết định", "Nghị quyết", "Kế hoạch", "Hướng dẫn", "Luật", "Nghị định", "Thông báo", "Công văn", "Công điện", "Lệnh công bố", "Văn bản hợp nhất"]
    
    for t in targets:
            if title_lower.startswith(t.lower()):
                inferred_type = t
                break
    
    if inferred_type:
        raw_doc_type = inferred_type
    
    # --- ORGANIZE ---
    # User request: "put them it correct category and loai_van_ban"
    # Logic: Use dataset category. If empty/'Khac', use 'Thue-Phi-Le-Phi' or 'Khac' depending on user pref.
    # User previously asked for 'Outside' -> 'Thue-Phi-Le-Phi'. Let's stick to that for 'Khac' to be safe,
    # OR trust the dataset if it has meaningful categories.
    # Given "correct category", let's use the field.
    
    if not raw_category or raw_category == 'Khac':
        # Default bucket for uncategorized
        item_category = "Thue-Phi-Le-Phi"
    else:
        item_category = raw_category

    # --- TYPE MATCHING ---
    matched_type = None
    for t in targets:
        # Check refined doc_type/title
        if t.lower() in str(raw_doc_type).lower() or t.lower() in title.lower():
            matched_type = t
            break
    
    # If not in our specific targets, just use the raw type (slugified later)
    if not matched_type:
        matched_type = raw_doc_type if raw_doc_type else "Van_ban_khac"
        
    category = slugify(item_category)
    doc_type = slugify(matched_type) 
    
    # Organize: outputs/structured_data/{Category}/{DocType}/filename.json
    save_dir = os.path.join(output_dir, category, doc_type)
    os.makedirs(save_dir, exist_ok=True)

    html = item.get('noi_dung_html', '')
    if not html: return 0
    
    try:
        # Parse
        tree, metadata, attachments = parser.parse_html(html, title, doc_type=matched_type)
        
        # Add metadata from item
        full_data = {
            "document_info": {
                "title": title,
                "url": item.get('link'),
                "category": item_category,
                "doc_type": raw_doc_type,
                "date_issued": item.get('ngay_ban_hanh'),
                "id": item.get('_id'),
                "metadata": metadata
            },
            "structure": tree,
            "attachments": attachments
        }
        
        # Save
        safe_filename = re.sub(r'[\\/*?:"<>|]', "", title).replace(" ", "_")[:150] + ".json"
        out_path = os.path.join(save_dir, safe_filename)
        
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(full_data, f, ensure_ascii=False, indent=2)
        return 1
    except Exception as e:
        # logger.error(f"Error processing {title}: {e}")
        return 0

def main():
    # Paths
    input_path = "data_universal"
    output_dir = "outputs/structured_data"
    os.makedirs(output_dir, exist_ok=True)
    
    logger.info(f"Loading dataset from {input_path}...")
    try:
        ds = load_from_disk(input_path)
    except Exception as e:
        logger.error(f"Failed to load dataset: {e}")
        return

    # Process 'train' split
    dataset = ds['train']
    logger.info(f"Total documents: {len(dataset)}")
    
    # --- SINGLE-THREADED PROCESSING (User Request) ---
    logger.info("Starting single-threaded processing...")
    
    results = []
    for item in tqdm(dataset, total=len(dataset), desc="Processing"):
        results.append(process_item(item))
        
    processed_count = sum(1 for r in results if r)
    logger.info(f"Processed {processed_count} documents.")

if __name__ == "__main__":
    main()
