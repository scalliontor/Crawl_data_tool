#!/usr/bin/env python3
"""
Pipeline for discovering and fully processing NEW legal documents
that do NOT exist in the data_universal dataset.

Flow:
  1. discover  — Browse vbpl.vn by date range / doc type → list of ItemIDs
  2. filter    — Exclude documents already in data_universal (by so_hieu)
  3. enrich    — Scrape thuộc tính + lịch sử for each new doc
  4. crawl     — Fetch toàn văn (HTML or PDF)
  5. parse     — Run appropriate parser → structured JSON tree
  6. save      — Write JSONL + individual JSON files

Usage:
    # Weekly crawl: discover all new Luật from 2024 → today (7 days/chunk)
    python3 discover_new_documents.py --from-date 01/01/2024 --doc-type Luật

    # Monthly chunks instead of weekly
    python3 discover_new_documents.py --from-date 01/01/2024 --chunk-days 30

    # Resume interrupted weekly run (auto-skips completed weeks)
    python3 discover_new_documents.py --from-date 01/01/2024 --doc-type Luật

    # Fresh start (clear checkpoints)
    python3 discover_new_documents.py --from-date 01/01/2025 --fresh

    # Limit total documents across all weeks
    python3 discover_new_documents.py --from-date 01/01/2025 --limit 10

    # Discover + skip filter (don't check existing DB)
    python3 discover_new_documents.py --from-date 01/01/2025 --skip-filter

    # Single document mode (by số hiệu)
    python3 discover_new_documents.py --so-hieu "100/2024/ND-CP"

    # Show stats from previous runs
    python3 discover_new_documents.py --stats
"""

import json
import os
import sys
import time
import random
import logging
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from collections import Counter

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from src.crawlers.vbpl_searcher import VBPLSearcher
from src.crawlers.vbpl_status import VBPLStatusScraper
from src.crawlers.vbpl_crawler import VBPLCrawler
from parsers import get_parser, PARSER_MAP

# ──────────────────────────────────────
# Output directories
# ──────────────────────────────────────
OUT_DIR = ROOT / "outputs" / "new_documents"
JSONL_FILE = OUT_DIR / "discovered_documents.jsonl"
PARSED_DIR = OUT_DIR / "parsed"
HTML_DIR = OUT_DIR / "raw_html"
PDF_DIR = OUT_DIR / "pdfs"
STATS_FILE = OUT_DIR / "discovery_stats.json"

WEEK_CHECKPOINT = OUT_DIR / "week_checkpoint.json"

for d in [OUT_DIR, PARSED_DIR, HTML_DIR, PDF_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ──────────────────────────────────────
# Config
# ──────────────────────────────────────
BASE_DELAY = 2.5
JITTER_MAX = 1.0
BACKOFF_FACTOR = 2.0
MAX_DELAY = 60.0
MAX_RETRIES = 3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────
# 1. Load existing so_hieu set (for filtering)
# ──────────────────────────────────────

def load_existing_so_hieus() -> set[str]:
    """Load so_hieu from data_universal to skip already-known docs."""
    cache = ROOT / "outputs" / "enrichment" / "tax_docs_cache.json"
    if cache.exists():
        with open(cache, encoding="utf-8") as f:
            docs = json.load(f)
        so_hieus = {d["so_hieu"].strip().lower() for d in docs if d.get("so_hieu")}
        logger.info("Loaded %d existing so_hieu from cache", len(so_hieus))
        return so_hieus

    # Fallback: try loading from dataset directly (slow on NTFS)
    dataset_path = ROOT / "data_universal"
    if dataset_path.exists():
        try:
            from datasets import load_from_disk
            logger.info("Loading dataset (may be slow)...")
            ds = load_from_disk(str(dataset_path))
            so_hieus = {
                s.strip().lower()
                for s in ds["train"]["so_hieu"]
                if s and s.strip()
            }
            logger.info("Loaded %d existing so_hieu from dataset", len(so_hieus))
            return so_hieus
        except Exception as e:
            logger.warning("Could not load dataset: %s", e)

    logger.warning("No existing DB found — will not filter duplicates")
    return set()


# ──────────────────────────────────────
# 2. Checkpoint (resume support)
# ──────────────────────────────────────

def load_checkpoint() -> set[int]:
    """Return set of already-processed ItemIDs."""
    done = set()
    if JSONL_FILE.exists():
        with open(JSONL_FILE, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                    item_id = r.get("match", {}).get("vbpl_item_id")
                    if item_id:
                        done.add(int(item_id))
                except json.JSONDecodeError:
                    pass
    return done


# ──────────────────────────────────────
# 2b. Weekly chunking utilities
# ──────────────────────────────────────

def _parse_vn_date(s: str) -> datetime:
    """Parse dd/mm/yyyy → datetime."""
    return datetime.strptime(s.strip(), "%d/%m/%Y")


def _fmt_vn_date(dt: datetime) -> str:
    """Format datetime → dd/mm/yyyy."""
    return dt.strftime("%d/%m/%Y")


def generate_weekly_chunks(
    from_date: str, to_date: str, chunk_days: int = 7
) -> list[tuple[str, str]]:
    """
    Split [from_date, to_date] into chunks of `chunk_days` days.
    Returns list of (start_dd/mm/yyyy, end_dd/mm/yyyy).

    Example:
        generate_weekly_chunks("01/01/2025", "22/01/2025", chunk_days=7)
        → [("01/01/2025", "07/01/2025"),
           ("08/01/2025", "14/01/2025"),
           ("15/01/2025", "22/01/2025")]
    """
    start = _parse_vn_date(from_date)
    end = _parse_vn_date(to_date)
    chunks = []
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=chunk_days - 1), end)
        chunks.append((_fmt_vn_date(cursor), _fmt_vn_date(chunk_end)))
        cursor = chunk_end + timedelta(days=1)
    return chunks


def load_week_checkpoint() -> dict:
    """
    Load week-level checkpoint: which weeks have been fully processed.
    Format: {"completed_weeks": ["01/01/2025__07/01/2025", ...],
             "last_run": "2026-02-08T..."}
    """
    if WEEK_CHECKPOINT.exists():
        with open(WEEK_CHECKPOINT, encoding="utf-8") as f:
            return json.load(f)
    return {"completed_weeks": [], "last_run": None}


def save_week_checkpoint(ckpt: dict):
    """Persist week checkpoint to disk."""
    ckpt["last_run"] = datetime.now().isoformat()
    with open(WEEK_CHECKPOINT, "w", encoding="utf-8") as f:
        json.dump(ckpt, f, ensure_ascii=False, indent=2)


# ──────────────────────────────────────
# 3. Main pipeline
# ──────────────────────────────────────

def process_one_document(
    match,
    scraper: VBPLStatusScraper,
    crawler: VBPLCrawler,
    current_delay: float,
) -> tuple[dict, float]:
    """
    Full pipeline for a single document:
      enrich → crawl toàn văn → parse → save files.

    Returns (record_dict, updated_delay).
    """
    item_id = match.vbpl_item_id
    so_hieu = match.so_hieu or f"ItemID_{item_id}"
    path_seg = match.path_segment or "TW"

    safe_name = so_hieu.replace("/", "_").replace("\\", "_").replace(" ", "_")

    record = {
        "match": match.to_dict(),
        "validity": None,
        "evidence": None,
        "toanvan": None,
        "parsed": None,
        "error": None,
        "processed_at": datetime.now().isoformat(),
    }

    # ── Step A: Enrich (thuộc tính + lịch sử) ──
    try:
        enriched = scraper.enrich(match)
        record["validity"] = enriched.validity.to_dict()
        record["evidence"] = enriched.evidence.to_dict()
        logger.info("  ✓ Enriched: status=%s, %d events",
                     enriched.validity.status_current,
                     len(enriched.validity.events))
    except Exception as e:
        logger.warning("  ✗ Enrich failed: %s", e)
        record["error"] = f"enrich: {e}"

    time.sleep(current_delay + random.uniform(0, JITTER_MAX))

    # ── Step B: Crawl toàn văn ──
    toanvan = None
    try:
        toanvan = crawler.crawl_toanvan(item_id, path_seg)
        record["toanvan"] = {
            "source": toanvan["source"],
            "content_text_len": len(toanvan.get("content_text") or ""),
            "content_html_len": len(toanvan.get("content_html") or ""),
            "pdf_url": toanvan.get("pdf_url"),
            "pdf_filename": toanvan.get("pdf_filename"),
        }

        # Save raw HTML
        if toanvan.get("content_html"):
            html_path = HTML_DIR / f"{safe_name}.html"
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(toanvan["content_html"])
            logger.info("  ✓ HTML saved: %d chars → %s",
                         len(toanvan["content_html"]), html_path.name)

        # Download PDF if available
        if toanvan.get("pdf_url"):
            pdf_path = PDF_DIR / (toanvan.get("pdf_filename") or f"{safe_name}.pdf")
            try:
                crawler.download_pdf(toanvan["pdf_url"], str(pdf_path))
                record["toanvan"]["pdf_local"] = str(pdf_path)
            except Exception as e:
                logger.warning("  ✗ PDF download failed: %s", e)

    except Exception as e:
        logger.warning("  ✗ Crawl failed: %s", e)
        if record["error"]:
            record["error"] += f"; crawl: {e}"
        else:
            record["error"] = f"crawl: {e}"

    time.sleep(current_delay + random.uniform(0, JITTER_MAX))

    # ── Step C: Parse (if HTML content available) ──
    if toanvan and toanvan.get("content_html"):
        try:
            # Determine doc type for parser selection
            loai_vb = "Luật"  # default
            if record.get("validity"):
                loai_vb = record["validity"].get("loai_van_ban", "Luật") or "Luật"
            # Also try from matched title
            if not loai_vb or loai_vb == "Luật":
                title = match.matched_title or ""
                for doc_type in PARSER_MAP:
                    if doc_type.lower() in title.lower():
                        loai_vb = doc_type
                        break

            parser = get_parser(loai_vb)
            title = match.matched_title or so_hieu
            parsed = parser.parse(toanvan["content_html"], title=title)

            # Save parsed JSON
            parsed_path = PARSED_DIR / f"{safe_name}.json"
            with open(parsed_path, "w", encoding="utf-8") as f:
                json.dump(parsed, f, ensure_ascii=False, indent=2)

            # Summary for JSONL
            structure = parsed.get("structure", {})
            record["parsed"] = {
                "parser": parser.__class__.__name__,
                "doc_type": loai_vb,
                "total_nodes": _count_nodes(structure),
                "node_types": _count_by_type(structure),
                "parsed_file": str(parsed_path),
            }

            logger.info("  ✓ Parsed with %s: %d nodes → %s",
                         parser.__class__.__name__,
                         record["parsed"]["total_nodes"],
                         parsed_path.name)

        except Exception as e:
            logger.warning("  ✗ Parse failed: %s", e)
            if record["error"]:
                record["error"] += f"; parse: {e}"
            else:
                record["error"] = f"parse: {e}"

    # Adaptive delay
    if record["error"]:
        current_delay = min(current_delay * BACKOFF_FACTOR, MAX_DELAY)
    else:
        current_delay = max(BASE_DELAY, current_delay * 0.85)

    return record, current_delay


def _count_nodes(node: dict) -> int:
    total = 1
    for child in node.get("children", []):
        total += _count_nodes(child)
    return total


def _count_by_type(node: dict) -> dict:
    counts: dict[str, int] = {}
    _count_by_type_recursive(node, counts)
    return counts


def _count_by_type_recursive(node: dict, counts: dict):
    t = node.get("type", "?")
    counts[t] = counts.get(t, 0) + 1
    for child in node.get("children", []):
        _count_by_type_recursive(child, counts)


# ──────────────────────────────────────
# 4. Discovery mode (browse by date range)
# ──────────────────────────────────────

def _process_matches(
    matches: list,
    scraper: VBPLStatusScraper,
    crawler: VBPLCrawler,
    stats: Counter,
    current_delay: float,
    limit: int = 0,
) -> float:
    """Process a list of matches (shared by both weekly and flat mode)."""
    from tqdm import tqdm

    if limit and limit > 0:
        matches = matches[:limit]

    if not matches:
        return current_delay

    pbar = tqdm(matches, desc="Processing", unit="doc", leave=False)
    for match in pbar:
        so_hieu = match.so_hieu or f"ID_{match.vbpl_item_id}"
        pbar.set_postfix_str(f"{so_hieu[:25]} d={current_delay:.1f}")

        logger.info("── Processing: %s (ItemID=%s) ──", so_hieu, match.vbpl_item_id)

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                record, current_delay = process_one_document(
                    match, scraper, crawler, current_delay
                )
                break
            except Exception as e:
                logger.warning("  Attempt %d/%d failed: %s", attempt, MAX_RETRIES, e)
                current_delay = min(current_delay * BACKOFF_FACTOR, MAX_DELAY)
                if attempt < MAX_RETRIES:
                    time.sleep(current_delay + random.uniform(0, JITTER_MAX))
                else:
                    record = {
                        "match": match.to_dict(),
                        "error": f"all retries failed: {e}",
                        "processed_at": datetime.now().isoformat(),
                    }

        # Write to JSONL
        with open(JSONL_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

        # Stats
        if record.get("error"):
            stats["error"] += 1
        else:
            stats["success"] += 1
        if record.get("toanvan", {}).get("source") == "html":
            stats["html"] += 1
        elif record.get("toanvan", {}).get("source") == "pdf":
            stats["pdf"] += 1
        if record.get("parsed"):
            stats["parsed"] += 1

        time.sleep(current_delay + random.uniform(0, JITTER_MAX))

    pbar.close()
    return current_delay


def run_discover(args):
    """
    Browse vbpl.vn by date range and process new documents.

    If --weekly is set (default), the date range is split into 7-day chunks.
    Each completed week is checkpointed so the pipeline can resume mid-run.
    """
    today = datetime.now().strftime("%d/%m/%Y")
    from_date = args.from_date or "01/01/2024"
    to_date = args.to_date or today
    doc_type = args.doc_type
    chunk_days = args.chunk_days

    logger.info("=" * 60)
    logger.info("DISCOVER NEW DOCUMENTS")
    logger.info("  Date range:   %s → %s", from_date, to_date)
    logger.info("  Doc type:     %s", doc_type or "ALL")
    logger.info("  Chunk size:   %d days%s", chunk_days,
                " (weekly)" if chunk_days == 7 else "")
    logger.info("=" * 60)

    # Load existing DB for filtering
    existing = set()
    if not args.skip_filter:
        existing = load_existing_so_hieus()

    # Load checkpoints
    done_ids = load_checkpoint()
    week_ckpt = load_week_checkpoint()
    completed_weeks = set(week_ckpt.get("completed_weeks", []))
    logger.info("Checkpoint: %d docs processed, %d weeks completed",
                len(done_ids), len(completed_weeks))

    # Generate weekly chunks
    chunks = generate_weekly_chunks(from_date, to_date, chunk_days=chunk_days)
    logger.info("Total chunks: %d (each ≤%d days)", len(chunks), chunk_days)

    # Filter out already-completed weeks
    remaining_chunks = [
        (s, e) for s, e in chunks
        if f"{s}__{e}" not in completed_weeks
    ]
    if len(remaining_chunks) < len(chunks):
        logger.info("Skipping %d already-completed chunks, %d remaining",
                    len(chunks) - len(remaining_chunks), len(remaining_chunks))

    if not remaining_chunks:
        logger.info("All weeks already processed! Use --fresh to restart.")
        return

    # Shared resources
    crawler = VBPLCrawler(delay=BASE_DELAY)
    scraper = VBPLStatusScraper(delay=BASE_DELAY)
    current_delay = BASE_DELAY
    total_stats = Counter()
    total_new = 0
    global_limit_remaining = args.limit if args.limit > 0 else None

    for week_idx, (w_start, w_end) in enumerate(remaining_chunks, 1):
        week_key = f"{w_start}__{w_end}"
        logger.info("")
        logger.info("━" * 60)
        logger.info("📅 WEEK %d/%d: %s → %s",
                    week_idx, len(remaining_chunks), w_start, w_end)
        logger.info("━" * 60)

        # Discover docs in this week
        try:
            matches = crawler.discover(
                from_date=w_start,
                to_date=w_end,
                loai_van_ban=doc_type,
                max_pages=args.max_pages,
            )
        except Exception as e:
            logger.error("  ✗ Discovery failed for week %s→%s: %s", w_start, w_end, e)
            total_stats["week_errors"] += 1
            time.sleep(current_delay * 2)
            continue

        logger.info("  Found %d documents in this period", len(matches))

        # Filter
        new_matches = []
        for m in matches:
            if m.vbpl_item_id in done_ids:
                continue
            if not args.skip_filter and m.so_hieu and m.so_hieu.strip().lower() in existing:
                continue
            new_matches.append(m)

        skipped = len(matches) - len(new_matches)
        if skipped:
            logger.info("  Skipped %d (already known), %d new to process",
                        skipped, len(new_matches))

        # Apply global limit
        if global_limit_remaining is not None:
            new_matches = new_matches[:global_limit_remaining]
            global_limit_remaining -= len(new_matches)

        if new_matches:
            week_stats = Counter()
            current_delay = _process_matches(
                new_matches, scraper, crawler, week_stats, current_delay,
            )
            total_stats.update(week_stats)
            total_new += len(new_matches)

            # Add processed IDs to done set (for next week's filtering)
            for m in new_matches:
                if m.vbpl_item_id:
                    done_ids.add(m.vbpl_item_id)

            logger.info("  Week result: %s",
                        ", ".join(f"{k}={v}" for k, v in sorted(week_stats.items())))
        else:
            logger.info("  Nothing new in this period.")

        # Mark week as completed
        completed_weeks.add(week_key)
        week_ckpt["completed_weeks"] = sorted(completed_weeks)
        save_week_checkpoint(week_ckpt)
        logger.info("  ✓ Week checkpoint saved.")

        # Check global limit
        if global_limit_remaining is not None and global_limit_remaining <= 0:
            logger.info("Global limit reached, stopping.")
            break

    # Final summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("🏁 ALL DONE")
    logger.info("  Weeks processed:  %d / %d", len(completed_weeks), len(chunks))
    logger.info("  Documents new:    %d", total_new)
    for k, v in sorted(total_stats.items()):
        logger.info("  %s: %d", k, v)
    logger.info("=" * 60)


# ──────────────────────────────────────
# 5. Single document mode (by so_hieu)
# ──────────────────────────────────────

def run_single(args):
    """Process a single document by so_hieu."""
    so_hieu = args.so_hieu
    logger.info("=" * 60)
    logger.info("SINGLE DOCUMENT MODE: %s", so_hieu)
    logger.info("=" * 60)

    searcher = VBPLSearcher(delay=BASE_DELAY)
    scraper = VBPLStatusScraper(delay=BASE_DELAY)
    crawler = VBPLCrawler(delay=BASE_DELAY)

    # Search
    match = searcher.search(so_hieu)
    if match.confidence == "none":
        logger.error("✗ No match found for '%s'", so_hieu)
        return

    logger.info("✓ Found: ItemID=%s, confidence=%s", match.vbpl_item_id, match.confidence)
    if match.matched_title:
        logger.info("  Title: %s", match.matched_title)

    time.sleep(BASE_DELAY + random.uniform(0, JITTER_MAX))

    # Full pipeline
    record, _ = process_one_document(match, scraper, crawler, BASE_DELAY)

    # Write to JSONL
    with open(JSONL_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

    # Pretty print summary
    print("\n" + "=" * 60)
    print(f"📄 {so_hieu}")
    print("=" * 60)

    if record.get("validity"):
        v = record["validity"]
        print(f"  Trạng thái:     {v.get('status_raw', '?')}")
        print(f"  Hiệu lực từ:    {v.get('effective_date', '?')}")
        print(f"  Ngày ban hành:  {v.get('ngay_ban_hanh', '?')}")
        print(f"  Cơ quan:        {v.get('co_quan_ban_hanh', '?')}")
        print(f"  Lĩnh vực:       {v.get('linh_vuc', '?')}")
        n_events = len(v.get("events", []))
        print(f"  Lịch sử:        {n_events} events")

    if record.get("toanvan"):
        t = record["toanvan"]
        print(f"\n  Toàn văn:       source={t['source']}")
        print(f"  HTML:           {t['content_html_len']} chars")
        print(f"  Text:           {t['content_text_len']} chars")
        if t.get("pdf_url"):
            print(f"  PDF:            {t['pdf_url']}")

    if record.get("parsed"):
        p = record["parsed"]
        print(f"\n  Parser:         {p['parser']}")
        print(f"  Total nodes:    {p['total_nodes']}")
        for ntype, count in sorted(p["node_types"].items(),
                                     key=lambda x: x[1], reverse=True):
            print(f"    {ntype:12s}: {count}")
        print(f"  Saved to:       {p['parsed_file']}")

    if record.get("error"):
        print(f"\n  ⚠ Errors: {record['error']}")

    print("=" * 60)


# ──────────────────────────────────────
# 6. Stats
# ──────────────────────────────────────

def show_stats():
    """Show statistics from previous runs."""
    if not JSONL_FILE.exists():
        print("No output file found.")
        return

    records = []
    with open(JSONL_FILE, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass

    total = len(records)
    if not total:
        print("No records found.")
        return

    errors = sum(1 for r in records if r.get("error"))
    with_validity = sum(1 for r in records if r.get("validity"))
    with_toanvan = sum(1 for r in records if r.get("toanvan"))
    with_parsed = sum(1 for r in records if r.get("parsed"))

    source_dist = Counter(
        r.get("toanvan", {}).get("source", "none") if r.get("toanvan") else "none"
        for r in records
    )
    status_dist = Counter(
        r.get("validity", {}).get("status_current", "unknown") if r.get("validity") else "no_data"
        for r in records
    )
    parser_dist = Counter(
        r.get("parsed", {}).get("parser", "?") if r.get("parsed") else "none"
        for r in records
    )
    confidence_dist = Counter(
        r.get("match", {}).get("confidence", "?")
        for r in records
    )

    print("\n" + "=" * 60)
    print("📊 DISCOVERY PIPELINE STATISTICS")
    print("=" * 60)
    print(f"  Total documents:      {total}")
    print(f"  Errors:               {errors}")
    print(f"  With validity data:   {with_validity}")
    print(f"  With toàn văn:        {with_toanvan}")
    print(f"  Successfully parsed:  {with_parsed}")

    print(f"\n  Match confidence:")
    for k, v in confidence_dist.most_common():
        print(f"    {k:12s}: {v:4d} ({v / total * 100:5.1f}%)")

    print(f"\n  Content source:")
    for k, v in source_dist.most_common():
        print(f"    {k:12s}: {v:4d} ({v / total * 100:5.1f}%)")

    print(f"\n  Status distribution:")
    for k, v in status_dist.most_common():
        print(f"    {k:20s}: {v:4d} ({v / total * 100:5.1f}%)")

    print(f"\n  Parser distribution:")
    for k, v in parser_dist.most_common():
        print(f"    {k:24s}: {v:4d}")

    print("=" * 60)


# ──────────────────────────────────────
# CLI
# ──────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Discover & process NEW legal documents from vbpl.vn",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Weekly crawl from 2024 (auto-resumes if interrupted)
  python3 discover_new_documents.py --from-date 01/01/2024 --doc-type Luật

  # Monthly chunks (fewer requests, bigger batches)
  python3 discover_new_documents.py --from-date 01/01/2024 --chunk-days 30

  # Limit to 10 total documents
  python3 discover_new_documents.py --from-date 01/01/2025 --limit 10

  # Start fresh (clear all checkpoints)
  python3 discover_new_documents.py --from-date 01/01/2024 --fresh

  # Process a single known document
  python3 discover_new_documents.py --so-hieu "100/2024/ND-CP"

  # Show stats from previous runs
  python3 discover_new_documents.py --stats

  # Available doc types:
  #   Luật, Nghị định, Thông tư, Thông tư liên tịch,
  #   Quyết định, Nghị quyết, Pháp lệnh, Lệnh,
  #   Hiến pháp, Bộ luật, Nghị quyết liên tịch
        """,
    )

    parser.add_argument("--so-hieu", type=str,
                        help="Process a single document by số hiệu (e.g. '64/2024/QH15')")
    parser.add_argument("--from-date", type=str, default="01/01/2024",
                        help="Start date dd/mm/yyyy (default: 01/01/2024)")
    parser.add_argument("--to-date", type=str, default=None,
                        help="End date dd/mm/yyyy (default: today)")
    parser.add_argument("--doc-type", type=str, default=None,
                        help="Filter by doc type (e.g. 'Luật', 'Nghị định')")
    parser.add_argument("--chunk-days", type=int, default=7,
                        help="Days per chunk (default: 7 = weekly). Use 30 for monthly.")
    parser.add_argument("--limit", type=int, default=0,
                        help="Max TOTAL documents to process across all weeks")
    parser.add_argument("--max-pages", type=int, default=100,
                        help="Max browse pages per week (50 docs/page)")
    parser.add_argument("--skip-filter", action="store_true",
                        help="Skip filtering against existing database")
    parser.add_argument("--fresh", action="store_true",
                        help="Clear week checkpoint and start over")
    parser.add_argument("--stats", action="store_true",
                        help="Show statistics from previous runs")

    args = parser.parse_args()

    if args.stats:
        show_stats()
        return

    if args.fresh:
        if WEEK_CHECKPOINT.exists():
            WEEK_CHECKPOINT.unlink()
            logger.info("Cleared week checkpoint.")
        if JSONL_FILE.exists():
            JSONL_FILE.unlink()
            logger.info("Cleared output JSONL.")

    if args.so_hieu:
        run_single(args)
    else:
        run_discover(args)


if __name__ == "__main__":
    main()
