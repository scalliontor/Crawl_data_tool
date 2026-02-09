# Vietnamese Legal Document Crawl & Parse Pipeline

End-to-end pipeline for crawling, parsing, enriching and evaluating Vietnamese legal documents from [ThuVienPhapLuat](https://thuvienphapluat.vn) and [VBPL](https://vbpl.vn), focused on the **Thuế – Phí – Lệ phí** (Tax – Fee – Levy) domain.

## Overview

```
HuggingFace Dataset ──► Parse 11,280 docs ──► Enrich via VBPL ──► Merge ──► Generate Q&A
   (data_universal/)     (process_tax_data)    (run_tax_enrichment)  (merge)   (groundtruth)
```

**Key numbers:**
- **11,280** parsed documents (JSON + TXT) across 20 `loại văn bản`
- **754** documents enriched with legal status from vbpl.vn
- **350** grounded Q&A pairs for RAG evaluation
- **4** specialized parsers covering all 31 Vietnamese legal document types

## Project Structure

```
.
├── parsers/                      # Document parser module
│   ├── __init__.py               #   get_parser() routing by loại văn bản
│   ├── base_parser.py            #   LegalNode tree + BaseParser
│   ├── hierarchical_parser.py    #   Luật, Nghị định, Thông tư, Pháp lệnh
│   ├── decision_parser.py        #   Quyết định, Lệnh, Nghị quyết
│   ├── directive_parser.py       #   Chỉ thị, Thông báo, Công điện
│   └── plan_parser.py            #   Kế hoạch, Hướng dẫn, Quy chế
│
├── src/
│   ├── parse_law_dataset.py      # Dataset loading & HTML → structured JSON
│   └── crawlers/                 # VBPL crawler modules
│       ├── models.py             #   Data models (VBPLItem, SearchResult)
│       ├── vbpl_searcher.py      #   Job A: Search vbpl.vn for matching docs
│       ├── vbpl_status.py        #   Job B: Scrape legal status (hiệu lực)
│       ├── vbpl_crawler.py       #   Job C: Download full text from vbpl.vn
│       └── run_enrichment.py     #   Orchestrator for Jobs A→B→C
│
├── process_tax_data.py           # Parse all 11,280 docs → outputs/
├── run_tax_enrichment.py         # Run enrichment pipeline (VBPL matching)
├── merge_enrichment.py           # Merge enrichment into parsed JSONs
├── discover_new_documents.py     # Discover new docs by week range
├── generate_groundtruth.py       # Generate test Q&A dataset
│
├── test_groundtruth.json         # 350 grounded Q&A pairs (4 types)
├── requirements.txt              # Python dependencies
├── PIPELINE_DOCUMENTATION.md     # Full technical documentation
│
├── outputs/                      # ⛔ gitignored — generated data
│   └── thue_phi_le_phi/          #   11,280 JSON + TXT files by loại văn bản
├── data_universal/               # ⛔ gitignored — HuggingFace Arrow dataset
└── .venv/                        # ⛔ gitignored — Python virtual environment
```

## Quick Start

```bash
# 1. Setup
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium   # for VBPL crawler

# 2. Parse all documents from HuggingFace dataset
python3 process_tax_data.py

# 3. Enrich with legal status from vbpl.vn
python3 run_tax_enrichment.py run

# 4. Merge enrichment into parsed files
python3 merge_enrichment.py --apply

# 5. Generate test Q&A dataset (~350 questions)
python3 generate_groundtruth.py
```

## Pipeline Components

### 1. Parsing (`process_tax_data.py` + `parsers/`)

Reads the HuggingFace Arrow dataset, routes each document to the appropriate parser by `loại văn bản`, and outputs:
- **JSON** — structured tree: `document_info` + `parsed_result.structure` (chapters → articles → clauses)
- **TXT** — plain-text extraction for full-text search

```python
from parsers import get_parser

parser = get_parser("Thông tư")
result = parser.parse(html_content, title="Thông tư 80/2021/TT-BTC")
# → {"structure": {type, title, content, children: [...]}, "metadata": {...}}
```

### 2. Enrichment (`run_tax_enrichment.py`)

Three-stage pipeline using Playwright + aiohttp to match parsed docs against [vbpl.vn](https://vbpl.vn):

| Stage | Module | Purpose |
|-------|--------|---------|
| **Job A** | `vbpl_searcher.py` | Search vbpl.vn by số hiệu → candidate matches |
| **Job B** | `vbpl_status.py` | Scrape legal status page → hiệu lực, sự kiện pháp lý |
| **Job C** | `vbpl_crawler.py` | Download full-text HTML from vbpl.vn |

Results saved to `outputs/enrichment/enrichment_results.jsonl`.

### 3. Merge (`merge_enrichment.py`)

Flattens enrichment data directly into each document's `document_info`:

```
tinh_trang:       "Còn hiệu lực" | "Hết hiệu lực" | "Hết hiệu lực một phần" | ...
ngay_hieu_luc:    "01/01/2022"
su_kien_phap_ly:  [{date, event_text, related_doc, url}, ...]
do_khop_vbpl:     "exact" | "fuzzy"
```

```bash
python3 merge_enrichment.py --apply   # write to files
python3 merge_enrichment.py --stats   # show statistics only
```

### 4. Discovery (`discover_new_documents.py`)

Crawls vbpl.vn week-by-week to discover new documents not yet in the dataset.

```bash
python3 discover_new_documents.py run              # all weeks
python3 discover_new_documents.py run --weeks 5     # first 5 weeks
```

### 5. Groundtruth Generation (`generate_groundtruth.py`)

Auto-generates grounded Q&A pairs from parsed documents for RAG evaluation:

| Type | Description | % |
|------|-------------|---|
| **factual** | Direct questions about article content | ~52% |
| **hallucination-trap** | Questions about things NOT in the document | ~25% |
| **case-study** | Scenario-based application questions | ~15% |
| **reasoning** | Cross-article relationship questions | ~7% |

```bash
python3 generate_groundtruth.py                # ~350 questions (default)
python3 generate_groundtruth.py --target 400   # adjust target
python3 generate_groundtruth.py --all          # all docs (~52k questions)
python3 generate_groundtruth.py --stats        # dry run
```

## Supported Document Types

| Parser | Document Types |
|--------|---------------|
| `HierarchicalParser` | Luật, Nghị định, Thông tư, Pháp lệnh, Thông tư liên tịch, Văn bản hợp nhất |
| `DecisionParser` | Quyết định, Lệnh, Nghị quyết, Sắc lệnh |
| `DirectiveParser` | Chỉ thị, Thông báo, Công điện, Thông tri |
| `PlanParser` | Kế hoạch, Hướng dẫn, Quy chế, Quy định |

## Requirements

- Python 3.11+
- See [requirements.txt](requirements.txt) for full list
- Playwright + Chromium (for VBPL crawlers only)

## License

For academic/research use.
