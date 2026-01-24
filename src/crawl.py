import requests
from bs4 import BeautifulSoup
import re
import json
import os
import time

# --- CẤU HÌNH ---
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "outputs", "structured_data")
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
}

def clean_filename(title):
    return re.sub(r'[\\/*?:"<>|]', "", title).replace(" ", "_")[:100]

def fetch_html(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"❌ Lỗi tải URL {url}: {e}")
        return None

def is_element_bold(element):
    if element.name in ['b', 'strong', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
        return True
    if element.has_attr('style'):
        style = element['style'].lower()
        if 'bold' in style or '700' in style:
            return True
    return False

def extract_rich_lines(html):
    soup = BeautifulSoup(html, 'lxml')
    content_div = soup.find('div', class_='content1') or \
                  soup.find('div', id='contentBody') or \
                  soup.find('body')
    
    if not content_div: return "", []

    h1 = soup.find('h1')
    doc_title = h1.get_text().strip() if h1 else "Unknown_Doc"

    # Xử lý Bảng
    for table in content_div.find_all('table'):
        rows = []
        for tr in table.find_all('tr'):
            cells = [td.get_text(separator=" ", strip=True) for td in tr.find_all(['td', 'th'])]
            if cells: rows.append(f"| {' | '.join(cells)} |")
        markdown = "\n" + "\n".join(rows) + "\n" if rows else ""
        table.replace_with(f"\n{markdown}\n")

    # Đánh dấu xuống dòng
    for tag in content_div.find_all(["br", "p", "div", "li", "tr", "h1", "h2", "h3"]):
        tag.insert_before("\n")

    # Quét text
    full_text_soup = content_div.get_text() 
    raw_lines = [l for l in full_text_soup.split('\n') if l.strip()]
    
    merged_lines = []
    for line in raw_lines:
        clean_line = line.strip()
        found_el = content_div.find(string=lambda t: t and clean_line in t.replace('\xa0', ' '))
        is_bold = False
        if found_el:
            parent = found_el.parent
            while parent and parent != content_div:
                if is_element_bold(parent):
                    is_bold = True
                    break
                parent = parent.parent
        merged_lines.append({"text": clean_line, "is_bold": is_bold})

    return doc_title, merged_lines

# --- VÁ LỖI DÒNG ---
def repair_broken_lines(rich_lines):
    repaired = []
    i = 0
    while i < len(rich_lines):
        current = rich_lines[i]
        text = current['text']
        
        # Vá lỗi: "Điều" đứng một mình
        if re.match(r'^(Điều|Chương)\s*[\.:]?$', text, re.IGNORECASE) and i + 1 < len(rich_lines):
            next_line = rich_lines[i+1]
            if re.match(r'^[0-9IVX]+', next_line['text'], re.IGNORECASE):
                merged_text = text + " " + next_line['text']
                is_bold = current['is_bold'] or next_line['is_bold']
                repaired.append({"text": merged_text, "is_bold": is_bold})
                i += 2
                continue

        # Vá lỗi: "Điều 1." bị ngắt với tên điều
        if re.match(r'^Điều\s+\d+[\.:]?$', text, re.IGNORECASE) and i + 1 < len(rich_lines):
            next_line = rich_lines[i+1]
            if not re.match(r'^(\d+\.|[a-zđ][\)\.])', next_line['text'], re.IGNORECASE) and next_line['text'][0].isalpha():
                merged_text = text + " " + next_line['text']
                is_bold = current['is_bold'] or next_line['is_bold']
                repaired.append({"text": merged_text, "is_bold": is_bold})
                i += 2
                continue

        repaired.append(current)
        i += 1
    return repaired

# --- BỘ LỌC THAM CHIẾU ---
def is_reference_line(text):
    if len(text) > 300: return True
    text_lower = text.lower()
    if text_lower.endswith("thông tư này") or text_lower.endswith("nghị định này") or text_lower.endswith("luật này"): return True
    if re.search(r'(chương|điều)\s+\d+[\,\;]\s*(chương|điều)', text_lower): return True
    indicators = [r'quy định tại', r'căn cứ', r'tại chương', r'của chương', r'tại điều', r'của điều']
    for pattern in indicators:
        if re.search(pattern, text, re.IGNORECASE): return True
    return False

# --- CẤU TRÚC NODE ---
class LegalNode:
    def __init__(self, level, type_name, title, content_part=""):
        self.level = level
        self.type = type_name
        self.title = title
        self.content_lines = []
        if content_part: self.content_lines.append(content_part)
        self.children = []
    def to_dict(self):
        data = {"type": self.type, "title": self.title}
        if self.content_lines: data["content"] = "\n".join(self.content_lines)
        if self.children: data["children"] = [child.to_dict() for child in self.children]
        return data

# --- PARSER CHÍNH ---
def parse_legal_tree_v28(rich_lines):
    # Regex
    p_chapter_solo = re.compile(r'^\s*Chương\s+[IVXLCDM0-9]+[\.:]?$', re.IGNORECASE)
    p_chapter_full = re.compile(r'^\s*(Chương\s+[IVXLCDM0-9]+.*)', re.IGNORECASE)
    p_section = re.compile(r'^\s*(Mục\s+[0-9]+.*)', re.IGNORECASE)
    p_article = re.compile(r'^\s*(Điều\s+\d+)[\.:]?\s*(.*)$', re.IGNORECASE)
    p_clause = re.compile(r'^\s*(\d+)\.[\)\.]?\s*(.*)$')
    p_point = re.compile(r'^\s*([a-zđ])[\)\.]\s*(.*)$', re.IGNORECASE)
    p_appendix = re.compile(r'^\s*(Phụ lục\s+[A-Z0-9\.]+.*|Mẫu số\s+.*|Biểu mẫu\s+.*|Danh mục\s+.*)', re.IGNORECASE)
    p_recipients = re.compile(r'(?:^|\|\s*|[\.\;]\s+)(Nơi nhận\s*:.*)', re.IGNORECASE)

    # Regex đặc biệt: Bắt đầu bằng chữ "Điều" (dùng để dừng vòng lặp Chương)
    p_article_start = re.compile(r'^\s*Điều\b', re.IGNORECASE)

    root = LegalNode(0, "root", "ROOT")
    footer_node = LegalNode(1, "footer", "Footer")
    appendix_root = LegalNode(0, "appendix_root", "APPENDICES")
    stack = [root] 
    
    PHASE_BODY = 1
    PHASE_FOOTER = 2
    PHASE_APPENDIX = 3
    current_phase = PHASE_BODY

    i = 0
    while i < len(rich_lines):
        line_obj = rich_lines[i]
        text = line_obj['text']
        is_bold = line_obj['is_bold']
        
        # 0. CHUYỂN TRẠNG THÁI
        split_match = p_recipients.search(text)
        is_valid_recipient = False
        if split_match:
            if is_bold or "|" in text or text.lower().startswith("nơi nhận"): is_valid_recipient = True
        
        if current_phase == PHASE_BODY and is_valid_recipient and split_match:
            pre_content = text[:split_match.start(1)].strip()
            footer_content = split_match.group(1).strip()
            pre_content = re.sub(r'[\|\.\/]+$', '', pre_content).strip()
            if pre_content and stack[-1].level > 0: stack[-1].content_lines.append(pre_content)
            current_phase = PHASE_FOOTER
            footer_node.content_lines.append(footer_content)
            i += 1
            continue

        if (p_appendix.match(text) and (is_bold or text.isupper())) and not is_reference_line(text):
            current_phase = PHASE_APPENDIX

        # 1. XỬ LÝ THEO GIAI ĐOẠN
        matched_node = None

        if current_phase == PHASE_FOOTER:
            if p_appendix.match(text) and is_bold: current_phase = PHASE_APPENDIX
            else:
                footer_node.content_lines.append(text)
                i += 1
                continue

        if current_phase == PHASE_APPENDIX:
            if p_appendix.match(text) and (is_bold or text.isupper()):
                new_appendix = LegalNode(2, "appendix", text)
                appendix_root.children.append(new_appendix)
            else:
                if not appendix_root.children: appendix_root.children.append(LegalNode(2, "appendix", "Phụ lục chung"))
                appendix_root.children[-1].content_lines.append(text)
            i += 1
            continue

        if current_phase == PHASE_BODY:
            
            # --- CHƯƠNG ---
            if p_chapter_solo.match(text):
                if not is_reference_line(text):
                    title = text
                    # Vòng lặp ghép tên chương
                    while i + 1 < len(rich_lines):
                        next_line = rich_lines[i+1]
                        next_text = next_line['text']
                        next_bold = next_line['is_bold']
                        
                        # === PHANH KHẨN CẤP ===
                        # Nếu gặp dòng bắt đầu bằng "Điều" (kể cả không có số) hoặc "Mục"
                        # -> Dừng ngay lập tức!
                        if p_article_start.match(next_text) or p_section.match(next_text): 
                            break
                        
                        # Nếu dòng tiếp theo Viết hoa hoặc In đậm -> Ghép vào
                        if next_text.isupper() or next_bold:
                            title += "\n" + next_text
                            i += 1
                        else:
                            break
                    matched_node = LegalNode(2, "chapter", title)

            elif p_chapter_full.match(text):
                if (is_bold or text.isupper()) and not is_reference_line(text):
                    matched_node = LegalNode(2, "chapter", text)

            # --- MỤC ---
            elif p_section.match(text):
                if (is_bold or text.isupper()) and not is_reference_line(text):
                    matched_node = LegalNode(3, "section", text)
            
            # --- ĐIỀU ---
            elif (match := p_article.match(text)):
                if is_bold or not is_reference_line(text):
                    title = f"{match.group(1)}. {match.group(2)}"
                    # Nối tiêu đề Điều
                    while i + 1 < len(rich_lines):
                         next_l = rich_lines[i+1]['text']
                         is_next_bold = rich_lines[i+1]['is_bold']
                         # Dừng nếu gặp Khoản 1. hoặc Điểm a)
                         if p_clause.match(next_l) or p_point.match(next_l): break
                         
                         if (next_l[0].islower() or is_next_bold):
                             title += " " + next_l
                             i += 1
                         else: break
                    matched_node = LegalNode(4, "article", title)

            # --- KHOẢN & ĐIỂM ---
            elif (match := p_clause.match(text)):
                matched_node = LegalNode(5, "clause", match.group(1), match.group(2))
            elif (match := p_point.match(text)):
                matched_node = LegalNode(6, "point", match.group(1), match.group(2))

            # --- STACK ---
            if matched_node:
                while stack[-1].level >= matched_node.level: stack.pop()
                stack[-1].children.append(matched_node)
                stack.append(matched_node)
            else:
                if stack[-1].level > 0: stack[-1].content_lines.append(text)
            i += 1

    final_result = {
        "body": root.to_dict().get('children', []),
        "footer": footer_node.to_dict().get('content', ""),
        "appendices": appendix_root.to_dict().get('children', [])
    }
    return final_result

def process_data(input_file):
    if not os.path.exists(input_file): return
    with open(input_file, 'r', encoding='utf-8') as f:
        urls = [line.strip() for line in f if line.strip()]

    for idx, url in enumerate(urls):
        print(f"[{idx+1}/{len(urls)}] Đang cào: {url}")
        html = fetch_html(url)
        if not html: continue

        title, rich_lines = extract_rich_lines(html)
        file_name = clean_filename(title)
        
        repaired_lines = repair_broken_lines(rich_lines)
        segmented_data = parse_legal_tree_v28(repaired_lines)
        
        final_output = {
            "document_info": {"title": title, "url": url, "crawled_at": time.strftime("%Y-%m-%d")},
            **segmented_data
        }

        with open(os.path.join(OUTPUT_DIR, f"{file_name}.json"), 'w', encoding='utf-8') as f:
            json.dump(final_output, f, ensure_ascii=False, indent=4)
        
        with open(os.path.join(OUTPUT_DIR, f"{file_name}.txt"), 'w', encoding='utf-8') as f:
            raw_text = "\n".join([l['text'] for l in repaired_lines])
            f.write(raw_text)

        print(f"   ✅ JSON: {file_name}.json")
        time.sleep(1)

if __name__ == "__main__":
    input_file_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "inputs", "urls.txt")
    process_data(input_file_path)