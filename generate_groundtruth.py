#!/usr/bin/env python3
"""
Auto-generate test/groundtruth Q&A pairs from parsed Vietnamese legal documents.

Reads all JSON files in outputs/thue_phi_le_phi/, extracts articles with
substantive content, and generates grounded questions with expected answers.

Question types:
  1. factual          — asks about specific provisions/content of an article
  2. case-study       — creates a scenario and asks how the law applies
  3. reasoning        — asks to explain relationships between articles
  4. hallucination-trap — asks about things NOT in the document

Target: 300-400 questions from ~70 diverse documents (stratified by loai_van_ban)

Usage:
    python3 generate_groundtruth.py                  # generate ~350 questions (default)
    python3 generate_groundtruth.py --target 400     # aim for 400 questions
    python3 generate_groundtruth.py --all            # ALL docs (50k+ questions)
    python3 generate_groundtruth.py --stats           # just show stats (dry run)
"""

import json
import glob
import random
import re
import sys
import logging
from pathlib import Path
from collections import Counter, defaultdict
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent
PARSED_DIR = ROOT / "outputs" / "thue_phi_le_phi"
OUTPUT_FILE = ROOT / "test_groundtruth.json"

random.seed(42)  # reproducible

# ─────────────────────────────────────────────────
# 1. EXTRACT articles/clauses from parsed structure
# ─────────────────────────────────────────────────

def extract_articles(node: dict, doc_title: str = "", depth: int = 0) -> list[dict]:
    """Recursively extract articles and substantive clauses from parsed structure."""
    results = []
    ntype = node.get("type", "")
    title = (node.get("title") or "").strip()
    content = (node.get("content") or "").strip()
    children = node.get("children", [])

    # Collect child content for articles that have children but no/short own content
    children_text = ""
    child_items = []
    for ch in children:
        ch_title = (ch.get("title") or "").strip()
        ch_content = (ch.get("content") or "").strip()
        ch_type = ch.get("type", "")
        piece = f"{ch_title}: {ch_content}" if ch_content else ch_title
        if piece and len(piece) > 10:
            child_items.append({"type": ch_type, "title": ch_title, "content": ch_content})
        if ch_content:
            children_text += " " + ch_content

    full_content = (content + " " + children_text).strip()

    if ntype == "article" and len(full_content) > 60:
        results.append({
            "type": "article",
            "title": title,
            "content": content,
            "full_content": full_content[:3000],  # cap for sanity
            "children": child_items[:20],
            "num_children": len(child_items),
        })

    # Also extract standalone substantive clauses (only if not under a collected article)
    if ntype == "clause" and content and len(content) > 80 and depth >= 2:
        results.append({
            "type": "clause",
            "title": title,
            "content": content[:2000],
            "full_content": content[:2000],
            "children": [],
            "num_children": 0,
        })

    for ch in children:
        results.extend(extract_articles(ch, doc_title, depth + 1))

    return results


def load_document(filepath: str) -> Optional[dict]:
    """Load a parsed JSON document and extract key info."""
    try:
        with open(filepath, encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None

    di = data.get("document_info", {})
    struct = data.get("parsed_result", {}).get("structure", {})
    if not struct:
        return None

    articles = extract_articles(struct, di.get("title", ""))
    # Only keep articles (not bare clauses for most question types)
    article_nodes = [a for a in articles if a["type"] == "article"]

    if not article_nodes:
        return None

    return {
        "filepath": filepath,
        "title": di.get("title", ""),
        "so_hieu": di.get("so_hieu", ""),
        "loai_van_ban": di.get("loai_van_ban", ""),
        "ngay_ban_hanh": di.get("ngay_ban_hanh", ""),
        "noi_ban_hanh": di.get("noi_ban_hanh", ""),
        "tinh_trang": di.get("tinh_trang", "Đã biết"),
        "link": di.get("link", ""),
        "articles": article_nodes,
    }


# ─────────────────────────────────────────────────
# 2. QUESTION GENERATION TEMPLATES
# ─────────────────────────────────────────────────

def _clean(text: str, max_len: int = 500) -> str:
    """Clean content for use in answers: dedup lines, trim."""
    # Remove duplicate consecutive lines (parser artifact)
    lines = text.split("\n")
    cleaned = []
    for line in lines:
        line = line.strip()
        if line and (not cleaned or line != cleaned[-1]):
            cleaned.append(line)
    result = "\n".join(cleaned)
    if len(result) > max_len:
        result = result[:max_len] + "..."
    return result


def _extract_dieu_number(title: str) -> str:
    """Extract article number from title like 'Điều 7. Thuế suất'."""
    m = re.match(r"Điều\s+(\d+[\w]*)", title)
    return m.group(0) if m else title[:40]


def _has_list_content(article: dict) -> bool:
    """Check if article has enumerated items (khoản, điểm)."""
    return article["num_children"] >= 2


def _has_numbers(content: str) -> bool:
    """Check if content has specific numbers (percentages, amounts, dates)."""
    return bool(re.search(r'\d+[%,.]?\d*\s*(%|triệu|đồng|ngày|tháng|năm|m²|lần)', content))


# ── FACTUAL GENERATORS ──

def gen_factual_content(doc: dict, article: dict) -> Optional[dict]:
    """Generate a factual question about the content of an article."""
    dieu = _extract_dieu_number(article["title"])
    content = _clean(article["full_content"], 800)

    if len(content) < 60:
        return None

    so_hieu = doc["so_hieu"]
    loai = doc["loai_van_ban"]

    query = f"Theo {dieu} {loai} {so_hieu}, nội dung quy định cụ thể là gì?"

    return {
        "type": "factual",
        "source_doc": so_hieu,
        "source_title": doc["title"],
        "article_ref": article["title"],
        "query": query,
        "expected_answer": f"Theo {dieu} {loai} {so_hieu} ({doc['title']}): {content}",
    }


def gen_factual_list(doc: dict, article: dict) -> Optional[dict]:
    """Generate a factual question about a list/enumeration in an article."""
    if not _has_list_content(article):
        return None

    dieu = _extract_dieu_number(article["title"])
    so_hieu = doc["so_hieu"]
    loai = doc["loai_van_ban"]
    n = article["num_children"]

    child_summary = "; ".join(
        f"({i+1}) {_clean(ch['title'], 100)}" + (f": {_clean(ch['content'], 150)}" if ch['content'] else "")
        for i, ch in enumerate(article["children"][:15])
    )

    query = f"{dieu} {loai} {so_hieu} liệt kê bao nhiêu trường hợp/khoản và nội dung cụ thể là gì?"

    return {
        "type": "factual",
        "source_doc": so_hieu,
        "source_title": doc["title"],
        "article_ref": article["title"],
        "query": query,
        "expected_answer": f"{dieu} liệt kê {n} khoản/trường hợp: {child_summary}",
    }


def gen_factual_number(doc: dict, article: dict) -> Optional[dict]:
    """Generate a factual question about specific numbers in an article."""
    content = article["full_content"]
    if not _has_numbers(content):
        return None

    dieu = _extract_dieu_number(article["title"])
    so_hieu = doc["so_hieu"]
    loai = doc["loai_van_ban"]

    # Find the specific numbers
    numbers = re.findall(r'(\d+[.,]?\d*\s*(?:%|triệu|đồng|ngày|tháng|năm|m²|lần|giờ|phút))', content)
    if not numbers:
        return None

    query = f"Các mức/con số cụ thể được quy định tại {dieu} {loai} {so_hieu} là bao nhiêu?"

    answer_content = _clean(content, 600)
    return {
        "type": "factual",
        "source_doc": so_hieu,
        "source_title": doc["title"],
        "article_ref": article["title"],
        "query": query,
        "expected_answer": f"Theo {dieu}: {answer_content}",
    }


def gen_factual_scope(doc: dict) -> Optional[dict]:
    """Generate a question about the scope/applicability of the document."""
    # Find Điều 1 (phạm vi điều chỉnh) or Điều 2 (đối tượng áp dụng)
    for art in doc["articles"][:5]:
        title_lower = art["title"].lower()
        if any(k in title_lower for k in ["phạm vi", "điều chỉnh", "đối tượng áp dụng", "áp dụng"]):
            content = _clean(art["full_content"], 600)
            if len(content) < 40:
                continue
            so_hieu = doc["so_hieu"]
            loai = doc["loai_van_ban"]
            query = f"{loai} {so_hieu} quy định về vấn đề gì và áp dụng cho đối tượng nào?"
            return {
                "type": "factual",
                "source_doc": so_hieu,
                "source_title": doc["title"],
                "article_ref": art["title"],
                "query": query,
                "expected_answer": f"Theo {art['title']} {loai} {so_hieu} ({doc['title']}): {content}",
            }
    return None


def gen_factual_effective_date(doc: dict) -> Optional[dict]:
    """Generate a question about effective date / validity."""
    # Find the last article (usually about hiệu lực)
    for art in reversed(doc["articles"]):
        title_lower = art["title"].lower()
        if any(k in title_lower for k in ["hiệu lực", "thi hành", "điều khoản"]):
            content = _clean(art["full_content"], 400)
            if len(content) < 30:
                continue
            so_hieu = doc["so_hieu"]
            loai = doc["loai_van_ban"]

            # Build expected answer including tinh_trang
            ts = doc["tinh_trang"]
            answer = f"Theo {art['title']}: {content}"
            if ts and ts != "Đã biết":
                answer += f" Tình trạng hiện tại: {ts}."

            return {
                "type": "factual",
                "source_doc": so_hieu,
                "source_title": doc["title"],
                "article_ref": art["title"],
                "query": f"{loai} {so_hieu} có hiệu lực từ khi nào và tình trạng hiệu lực hiện tại?",
                "expected_answer": answer,
            }
    return None


# ── CASE-STUDY GENERATORS ──

CASE_TEMPLATES = [
    {
        "condition": lambda doc, art: "thuế" in art["full_content"].lower() and _has_numbers(art["full_content"]),
        "gen_case": lambda doc, art: f"Một doanh nghiệp/cá nhân cần áp dụng quy định tại {_extract_dieu_number(art['title'])} {doc['loai_van_ban']} {doc['so_hieu']}.",
        "gen_query": lambda doc, art: f"Theo {_extract_dieu_number(art['title'])}, quy định này áp dụng cụ thể như thế nào cho trường hợp nêu trên?",
    },
    {
        "condition": lambda doc, art: any(k in art["full_content"].lower() for k in ["xử phạt", "vi phạm", "phạt tiền", "cưỡng chế"]),
        "gen_case": lambda doc, art: f"Một tổ chức/cá nhân vi phạm quy định tại {_extract_dieu_number(art['title'])} {doc['loai_van_ban']} {doc['so_hieu']}.",
        "gen_query": lambda doc, art: f"Hình thức xử lý và mức phạt cụ thể theo {_extract_dieu_number(art['title'])} là gì?",
    },
    {
        "condition": lambda doc, art: any(k in art["full_content"].lower() for k in ["miễn", "giảm", "ưu đãi", "không chịu thuế", "không phải nộp"]),
        "gen_case": lambda doc, art: f"Một đối tượng muốn biết mình có thuộc diện miễn/giảm theo {_extract_dieu_number(art['title'])} {doc['loai_van_ban']} {doc['so_hieu']} không.",
        "gen_query": lambda doc, art: f"Theo {_extract_dieu_number(art['title'])}, những trường hợp nào được miễn/giảm và điều kiện cụ thể là gì?",
    },
    {
        "condition": lambda doc, art: any(k in art["full_content"].lower() for k in ["thủ tục", "hồ sơ", "trình tự", "đăng ký", "kê khai"]),
        "gen_case": lambda doc, art: f"Một người nộp thuế cần thực hiện thủ tục theo {_extract_dieu_number(art['title'])} {doc['loai_van_ban']} {doc['so_hieu']}.",
        "gen_query": lambda doc, art: f"Trình tự, thủ tục và hồ sơ cần thiết theo {_extract_dieu_number(art['title'])} bao gồm những gì?",
    },
    {
        "condition": lambda doc, art: any(k in art["full_content"].lower() for k in ["trách nhiệm", "nghĩa vụ", "quyền", "quyền hạn"]),
        "gen_case": lambda doc, art: f"Cần xác định trách nhiệm/quyền hạn của các bên theo {_extract_dieu_number(art['title'])} {doc['loai_van_ban']} {doc['so_hieu']}.",
        "gen_query": lambda doc, art: f"Theo {_extract_dieu_number(art['title'])}, trách nhiệm và quyền hạn cụ thể của các bên được quy định như thế nào?",
    },
]


def gen_case_study(doc: dict, article: dict) -> Optional[dict]:
    """Generate a case-study question from matching templates."""
    for tmpl in CASE_TEMPLATES:
        try:
            if tmpl["condition"](doc, article):
                case = tmpl["gen_case"](doc, article)
                query = tmpl["gen_query"](doc, article)
                content = _clean(article["full_content"], 800)

                return {
                    "type": "case-study",
                    "source_doc": doc["so_hieu"],
                    "source_title": doc["title"],
                    "article_ref": article["title"],
                    "case": case,
                    "query": query,
                    "expected_answer": f"Theo {_extract_dieu_number(article['title'])} {doc['loai_van_ban']} {doc['so_hieu']}: {content}",
                }
        except Exception:
            continue
    return None


# ── REASONING GENERATORS ──

def gen_reasoning_multi_article(doc: dict) -> Optional[dict]:
    """Generate a reasoning question linking multiple articles in one doc."""
    if len(doc["articles"]) < 3:
        return None

    # Pick 2-3 related articles
    arts = random.sample(doc["articles"][:min(10, len(doc["articles"]))], min(3, len(doc["articles"])))
    dieus = [_extract_dieu_number(a["title"]) for a in arts]
    so_hieu = doc["so_hieu"]
    loai = doc["loai_van_ban"]

    query = f"Giải thích mối quan hệ và logic giữa {', '.join(dieus)} trong {loai} {so_hieu}."

    parts = []
    for a in arts:
        parts.append(f"- {_extract_dieu_number(a['title'])}: {_clean(a['full_content'], 250)}")

    return {
        "type": "reasoning",
        "source_doc": so_hieu,
        "source_title": doc["title"],
        "article_ref": [a["title"] for a in arts],
        "query": query,
        "expected_answer": f"Trong {loai} {so_hieu} ({doc['title']}), các điều khoản liên hệ như sau:\n" + "\n".join(parts),
    }


def gen_reasoning_compare_status(doc: dict) -> Optional[dict]:
    """Generate a reasoning question about document status and implications."""
    ts = doc["tinh_trang"]
    if ts in ("Đã biết", "", None):
        return None

    so_hieu = doc["so_hieu"]
    loai = doc["loai_van_ban"]

    query = f"{loai} {so_hieu} hiện có tình trạng '{ts}'. Điều này có ý nghĩa gì về mặt pháp lý khi áp dụng văn bản?"

    # Find effective date article
    eff_content = ""
    for art in reversed(doc["articles"]):
        if any(k in art["title"].lower() for k in ["hiệu lực", "thi hành"]):
            eff_content = _clean(art["full_content"], 300)
            break

    answer = f"{loai} {so_hieu} ({doc['title']}) có tình trạng: {ts}."
    if ts == "Hết hiệu lực":
        answer += " Văn bản đã hết hiệu lực pháp luật, không còn được áp dụng. Các quy định trong văn bản đã được thay thế bởi văn bản mới."
    elif ts == "Còn hiệu lực":
        answer += " Văn bản đang có hiệu lực pháp luật, các quy định trong văn bản vẫn được áp dụng."
    elif ts == "Hết hiệu lực một phần":
        answer += " Một số điều/khoản trong văn bản đã bị sửa đổi, bổ sung hoặc bãi bỏ bởi văn bản khác, nhưng phần còn lại vẫn có hiệu lực."
    elif ts == "Tạm ngưng hiệu lực":
        answer += " Văn bản tạm thời không được áp dụng, chờ quyết định từ cơ quan có thẩm quyền."
    if eff_content:
        answer += f" {eff_content}"

    return {
        "type": "reasoning",
        "source_doc": so_hieu,
        "source_title": doc["title"],
        "article_ref": "document_info.tinh_trang",
        "query": query,
        "expected_answer": answer,
    }


# ── HALLUCINATION-TRAP GENERATORS ──

TRAP_TEMPLATES = [
    {
        "gen": lambda doc: {
            "query": f"Theo {doc['loai_van_ban']} {doc['so_hieu']}, mức phạt tù tối đa cho vi phạm quy định này là bao nhiêu năm?",
            "expected_answer": f"{doc['loai_van_ban']} {doc['so_hieu']} ({doc['title']}) KHÔNG quy định về hình phạt tù. Văn bản này chỉ quy định về xử lý hành chính/nội dung quản lý thuế-phí-lệ phí. Hình phạt tù thuộc phạm vi điều chỉnh của Bộ luật Hình sự.",
        },
    },
    {
        "gen": lambda doc: {
            "query": f"{doc['loai_van_ban']} {doc['so_hieu']} có quy định cụ thể về thuế suất VAT cho hàng hóa xuất khẩu qua sàn thương mại điện tử xuyên biên giới không?",
            "expected_answer": f"{doc['loai_van_ban']} {doc['so_hieu']} ({doc['title']}) KHÔNG có quy định riêng về thuế suất VAT cho thương mại điện tử xuyên biên giới. Cần tra cứu các văn bản chuyên biệt về thương mại điện tử và thuế GTGT xuất khẩu.",
        },
    },
    {
        "gen": lambda doc: {
            "query": f"{doc['loai_van_ban']} {doc['so_hieu']} được ban hành bởi Quốc hội ngày {doc.get('ngay_ban_hanh', '??')} quy định cụ thể tiêu chuẩn ISO nào phải tuân thủ?",
            "expected_answer": f"{doc['loai_van_ban']} {doc['so_hieu']} ({doc['title']}) KHÔNG đề cập đến bất kỳ tiêu chuẩn ISO cụ thể nào. Câu hỏi chứa thông tin gây hiểu nhầm. Nơi ban hành thực tế: {doc.get('noi_ban_hanh', 'không rõ')} (không nhất thiết là Quốc hội).",
        },
    },
    {
        "gen": lambda doc: {
            "query": f"Liệt kê các hình thức xử phạt hình sự mà {doc['loai_van_ban']} {doc['so_hieu']} quy định cho tội tham nhũng trong lĩnh vực thuế.",
            "expected_answer": f"{doc['loai_van_ban']} {doc['so_hieu']} ({doc['title']}) KHÔNG quy định về xử phạt hình sự hay tội tham nhũng. Đây là văn bản thuộc lĩnh vực Thuế-Phí-Lệ phí, không phải Bộ luật Hình sự. Bất kỳ câu trả lời nào liệt kê hình phạt hình sự đều là bịa đặt.",
        },
    },
    {
        "gen": lambda doc: {
            "query": f"{doc['loai_van_ban']} {doc['so_hieu']} quy định doanh nghiệp phải nộp báo cáo ESG (Environment, Social, Governance) cho cơ quan thuế như thế nào?",
            "expected_answer": f"{doc['loai_van_ban']} {doc['so_hieu']} ({doc['title']}) KHÔNG có bất kỳ quy định nào về báo cáo ESG. Đây là khái niệm thuộc lĩnh vực quản trị doanh nghiệp/chứng khoán, không liên quan đến nội dung văn bản thuế-phí-lệ phí này.",
        },
    },
]


def gen_hallucination_trap(doc: dict) -> Optional[dict]:
    """Generate a hallucination-trap question for a document."""
    tmpl = random.choice(TRAP_TEMPLATES)
    try:
        result = tmpl["gen"](doc)
        return {
            "type": "hallucination-trap",
            "source_doc": doc["so_hieu"],
            "source_title": doc["title"],
            "query": result["query"],
            "expected_answer": result["expected_answer"],
        }
    except Exception:
        return None


# ─────────────────────────────────────────────────
# 3. MAIN GENERATION PIPELINE
# ─────────────────────────────────────────────────

def generate_questions_for_doc(doc: dict, max_per_doc: int = 4) -> list[dict]:
    """Generate a balanced mix of question types for a single document.

    Produces up to max_per_doc questions. Default 4 = 2 factual + 1 case/reasoning
    + 1 hallucination-trap.
    """
    articles = doc["articles"]
    if not articles:
        return []

    candidates: dict[str, list[dict]] = {
        "factual": [],
        "case-study": [],
        "reasoning": [],
        "hallucination-trap": [],
    }

    substantial = [a for a in articles if len(a["full_content"]) > 100]

    # ── Collect FACTUAL candidates ──
    q = gen_factual_scope(doc)
    if q:
        candidates["factual"].append(q)

    if substantial:
        art = random.choice(substantial)
        q = gen_factual_content(doc, art)
        if q:
            candidates["factual"].append(q)

    list_articles = [a for a in articles if _has_list_content(a)]
    if list_articles:
        art = random.choice(list_articles)
        q = gen_factual_list(doc, art)
        if q:
            candidates["factual"].append(q)

    number_articles = [a for a in articles if _has_numbers(a["full_content"])]
    if number_articles:
        art = random.choice(number_articles)
        q = gen_factual_number(doc, art)
        if q:
            candidates["factual"].append(q)

    q = gen_factual_effective_date(doc)
    if q:
        candidates["factual"].append(q)

    # ── Collect CASE-STUDY candidates ──
    if substantial:
        art = random.choice(substantial)
        q = gen_case_study(doc, art)
        if q:
            candidates["case-study"].append(q)

    # ── Collect REASONING candidates ──
    if len(articles) >= 3:
        q = gen_reasoning_multi_article(doc)
        if q:
            candidates["reasoning"].append(q)
    q = gen_reasoning_compare_status(doc)
    if q:
        candidates["reasoning"].append(q)

    # ── Collect HALLUCINATION-TRAP candidates ──
    q = gen_hallucination_trap(doc)
    if q:
        candidates["hallucination-trap"].append(q)

    # ── Balanced selection up to max_per_doc ──
    # Target mix: 2 factual, 1 case-study OR reasoning, 1 hallucination-trap
    selected = []

    # 1. Pick up to 2 factual
    facts = candidates["factual"]
    random.shuffle(facts)
    selected.extend(facts[:2])

    # 2. Pick 1 case-study (prefer) or reasoning
    if candidates["case-study"]:
        selected.append(candidates["case-study"][0])
    elif candidates["reasoning"]:
        selected.append(candidates["reasoning"][0])

    # 3. Pick 1 hallucination-trap
    if candidates["hallucination-trap"]:
        selected.append(candidates["hallucination-trap"][0])

    # 4. If room, add 1 more (reasoning > factual > case-study)
    if len(selected) < max_per_doc:
        for pool_name in ["reasoning", "factual", "case-study"]:
            for q in candidates[pool_name]:
                if q not in selected:
                    selected.append(q)
                    break
            if len(selected) >= max_per_doc:
                break

    return selected[:max_per_doc]


# ─────────────────────────────────────────────────
# 4. STRATIFIED SAMPLING — pick diverse, high-quality docs
# ─────────────────────────────────────────────────

# Priority loai_van_ban (most legally substantive)
PRIORITY_TYPES = [
    "Luật",
    "Nghị định",
    "Thông tư",
    "Thông tư liên tịch",
    "Văn bản hợp nhất",
    "Nghị quyết",
    "Quyết định",
    "Chỉ thị",
]


def _doc_quality_score(doc: dict) -> float:
    """Score a doc for selection: more articles, enrichment, content = higher."""
    score = 0.0
    # Enriched docs are much more valuable
    if doc["tinh_trang"] not in ("Đã biết", "", None):
        score += 50
    # More articles = richer doc
    score += min(len(doc["articles"]), 20) * 3
    # Has list content
    if any(_has_list_content(a) for a in doc["articles"]):
        score += 10
    # Has numbers
    if any(_has_numbers(a["full_content"]) for a in doc["articles"]):
        score += 10
    # Total content length (proxy for substantiveness)
    total_len = sum(len(a["full_content"]) for a in doc["articles"])
    score += min(total_len / 500, 20)
    return score


def sample_documents(docs: list[dict], target_questions: int) -> list[dict]:
    """Stratified sampling: pick diverse docs across loai_van_ban to hit target.

    Avg ~5 questions/doc, so need target/5 docs.
    Allocates slots proportionally to loai_van_ban, with minimum
    representation for priority types.
    """
    n_docs_needed = max(target_questions // 5, 40)  # ~5 Q/doc
    logger.info("Need ~%d docs for ~%d questions", n_docs_needed, target_questions)

    # Group by loai_van_ban
    by_type: dict[str, list[dict]] = defaultdict(list)
    for d in docs:
        by_type[d["loai_van_ban"] or "Khác"].append(d)

    # Sort each group by quality (best first)
    for grp in by_type.values():
        grp.sort(key=_doc_quality_score, reverse=True)

    # Allocate slots: priority types get guaranteed minimums
    allocation: dict[str, int] = {}
    remaining = n_docs_needed

    # Phase 1: Guarantee at least some from each priority type that exists
    for lt in PRIORITY_TYPES:
        if lt in by_type and by_type[lt]:
            count = min(max(2, len(by_type[lt]) * n_docs_needed // len(docs)), len(by_type[lt]))
            # Boost for Luật / Nghị định / Thông tư (most important)
            if lt in ("Luật", "Nghị định", "Thông tư"):
                count = min(count + 5, len(by_type[lt]))
            allocation[lt] = count
            remaining -= count

    # Phase 2: Fill remaining with best-scoring docs from non-allocated types
    other_types = [lt for lt in by_type if lt not in allocation]
    for lt in other_types:
        if remaining <= 0:
            break
        count = min(max(1, remaining // max(len(other_types), 1)), len(by_type[lt]))
        allocation[lt] = count
        remaining -= count

    # Phase 3: If still have room, add more from top-scoring types
    if remaining > 0:
        for lt in PRIORITY_TYPES:
            if remaining <= 0:
                break
            if lt in by_type:
                can_add = len(by_type[lt]) - allocation.get(lt, 0)
                add = min(remaining, can_add)
                allocation[lt] = allocation.get(lt, 0) + add
                remaining -= add

    # Select docs
    selected = []
    for lt, count in allocation.items():
        grp = by_type[lt]
        # Pick top-quality docs, but also sprinkle a few random ones for diversity
        top_n = min(count, len(grp))
        if top_n <= 3:
            selected.extend(grp[:top_n])
        else:
            # Top 60% by quality + 40% random from the rest
            n_top = max(top_n * 3 // 5, 1)
            n_rand = top_n - n_top
            selected.extend(grp[:n_top])
            rest = grp[n_top:]
            if rest and n_rand > 0:
                selected.extend(random.sample(rest, min(n_rand, len(rest))))

    random.shuffle(selected)

    logger.info("Selected %d docs across %d loai_van_ban types:", len(selected), len(allocation))
    sel_types = Counter(d["loai_van_ban"] or "Khác" for d in selected)
    for lt, c in sel_types.most_common():
        logger.info("  %-25s: %d", lt, c)

    return selected


def main():
    target = 350
    stats_only = False
    all_docs_mode = False

    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "--stats":
            stats_only = True
        elif args[i] == "--all":
            all_docs_mode = True
        elif args[i] == "--target" and i + 1 < len(args):
            target = int(args[i + 1])
            i += 1
        i += 1

    # Load all documents
    files = sorted(glob.glob(str(PARSED_DIR / "**" / "*.json"), recursive=True))
    logger.info("Found %d JSON files", len(files))

    docs = []
    for i, fp in enumerate(files):
        doc = load_document(fp)
        if doc:
            docs.append(doc)
        if (i + 1) % 2000 == 0:
            logger.info("  Loaded %d/%d files (%d valid docs)...", i + 1, len(files), len(docs))

    logger.info("Loaded %d valid documents (with articles)", len(docs))

    # Unless --all, sample a diverse subset to hit the target
    if not all_docs_mode:
        docs = sample_documents(docs, target)

    if stats_only:
        _show_stats(docs)
        return

    # Generate questions
    all_questions = []
    type_counts = Counter()
    docs_with_q = 0

    for doc in docs:
        qs = generate_questions_for_doc(doc)
        if qs:
            docs_with_q += 1
        for q in qs:
            type_counts[q["type"]] += 1
        all_questions.extend(qs)

    # Shuffle for variety
    random.shuffle(all_questions)

    # Save
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_questions, f, ensure_ascii=False, indent=2)

    # Summary
    print()
    print("=" * 60)
    print("✅ TEST GROUNDTRUTH GENERATION COMPLETE")
    print("=" * 60)
    print(f"  Documents sampled:     {len(docs)}")
    print(f"  Documents with Q&A:    {docs_with_q}")
    print(f"  Total questions:       {len(all_questions)}")
    print(f"  Avg per doc:           {len(all_questions)/max(docs_with_q,1):.1f}")
    print()
    print("  By type:")
    for t, c in type_counts.most_common():
        print(f"    {t:25s}: {c:5d} ({c/len(all_questions)*100:.1f}%)")
    print()
    print("  By loai_van_ban:")
    lvb = Counter(q["source_doc"] for q in all_questions)
    lvb_type = Counter()
    doc_map = {d["so_hieu"]: d["loai_van_ban"] for d in docs}
    for sh, cnt in lvb.items():
        lvb_type[doc_map.get(sh, "?")] += cnt
    for lt, c in lvb_type.most_common():
        print(f"    {lt:25s}: {c:5d}")
    print()
    print(f"  Output: {OUTPUT_FILE}")
    print("=" * 60)


def _show_stats(docs):
    """Show statistics about what would be generated."""
    type_counts = Counter()
    docs_with_q = 0

    for doc in docs:
        qs = generate_questions_for_doc(doc)
        if qs:
            docs_with_q += 1
        for q in qs:
            type_counts[q["type"]] += 1

    total = sum(type_counts.values())
    print()
    print("=" * 60)
    print("📊 GENERATION STATISTICS (dry run)")
    print("=" * 60)
    print(f"  Documents:           {len(docs)}")
    print(f"  Docs with questions: {docs_with_q}")
    print(f"  Total questions:     {total}")
    print(f"  Avg per doc:         {total/max(docs_with_q,1):.1f}")
    print()
    for t, c in type_counts.most_common():
        print(f"    {t:25s}: {c:6d} ({c/total*100:.1f}%)")
    print("=" * 60)


if __name__ == "__main__":
    main()
