#!/usr/bin/env python3
"""
Batch Enrichment Pipeline — search → enrich for a subset of the TVPL dataset.

Features:
  • Checkpoint-based: writes results to JSONL incrementally,
    skips already-processed so_hieu on restart.
  • Adaptive rate-limiting: backs off on HTTP 429 / 5xx.
  • Per-document error isolation: one failure doesn't kill the batch.
  • Summary stats printed at the end.

Usage:
    python -m src.crawlers.run_enrichment                     # all central-type tax docs
    python -m src.crawlers.run_enrichment --limit 20          # first 20 only
    python -m src.crawlers.run_enrichment --resume             # skip already-done
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import sys
import time
from collections import Counter
from dataclasses import asdict
from pathlib import Path

# ensure project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tqdm import tqdm

from src.crawlers.models import VBPLMatch, EnrichedDocument
from src.crawlers.vbpl_searcher import VBPLSearcher
from src.crawlers.vbpl_status import VBPLStatusScraper

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = PROJECT_ROOT / "outputs" / "enrichment"
DATASET_PATH = PROJECT_ROOT / "data_universal"

CENTRAL_TYPES = {"Luật", "Nghị định", "Thông tư", "Thông tư liên tịch", "Pháp lệnh"}

# Rate-limit settings
BASE_DELAY = 2.0          # seconds between requests
JITTER_MAX = 1.0          # random jitter 0..JITTER_MAX added to delay
BACKOFF_FACTOR = 2.0      # multiply delay on error
MAX_DELAY = 60.0           # cap on backoff delay
MAX_RETRIES = 3            # per-document retries


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def load_tax_docs(category: str = "Thue-Phi-Le-Phi") -> list[dict]:
    """Load dataset and return unique central-type docs in a category.

    Uses HuggingFace datasets with column projection for speed.
    """
    from datasets import load_from_disk

    ds = load_from_disk(str(DATASET_PATH))
    train = ds["train"]

    # Read only needed columns as batch (much faster than row-by-row)
    needed_cols = [
        "so_hieu", "title", "loai_van_ban", "category",
        "tinh_trang", "ngay_ban_hanh", "link",
    ]
    available = [c for c in needed_cols if c in train.column_names]

    # Use batch column access — returns dict of lists
    col_data = {c: train[c] for c in available}
    n_rows = len(col_data[available[0]])

    docs: list[dict] = []
    for i in range(n_rows):
        cat = col_data["category"][i] if "category" in col_data else ""
        lvb = col_data["loai_van_ban"][i] if "loai_van_ban" in col_data else ""
        sh = col_data["so_hieu"][i] if "so_hieu" in col_data else ""
        if cat == category and lvb in CENTRAL_TYPES and sh and sh.strip():
            row = {c: col_data[c][i] for c in available}
            docs.append(row)

    # Deduplicate by so_hieu (keep first occurrence)
    seen: set[str] = set()
    unique: list[dict] = []
    for d in docs:
        sh = d["so_hieu"].strip()
        if sh not in seen:
            seen.add(sh)
            unique.append(d)
    return unique


def load_checkpoint(output_path: Path) -> set[str]:
    """Return set of so_hieu already in the JSONL output."""
    done: set[str] = set()
    if output_path.exists():
        with open(output_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    sh = record.get("original", {}).get("so_hieu", "")
                    if sh:
                        done.add(sh)
                except json.JSONDecodeError:
                    continue
    return done


def append_result(output_path: Path, record: dict):
    """Append one JSON record as a line to the output JSONL."""
    with open(output_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def adaptive_sleep(current_delay: float) -> float:
    """Sleep with jitter, return the delay used."""
    jitter = random.uniform(0, JITTER_MAX)
    total = current_delay + jitter
    time.sleep(total)
    return total


# ──────────────────────────────────────────────
# Core pipeline
# ──────────────────────────────────────────────

def enrich_one(
    item: dict,
    searcher: VBPLSearcher,
    scraper: VBPLStatusScraper,
    current_delay: float,
) -> tuple[dict, float]:
    """
    Run search → enrich for a single document.

    Returns (record_dict, updated_delay).
    The delay may increase on errors (backoff) or decrease on success.
    """
    so_hieu = item["so_hieu"].strip()

    record: dict = {
        "original": {
            "so_hieu": so_hieu,
            "title": item.get("title", ""),
            "loai_van_ban": item.get("loai_van_ban", ""),
            "category": item.get("category", ""),
            "tinh_trang_tvpl": item.get("tinh_trang", ""),
            "ngay_ban_hanh": item.get("ngay_ban_hanh", ""),
            "link_tvpl": item.get("link", ""),
        },
        "match": None,
        "validity": None,
        "evidence": None,
        "error": None,
    }

    delay = current_delay
    last_error: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Step 1: Search
            match = searcher.search(so_hieu)
            record["match"] = match.to_dict()

            if match.confidence == "none":
                # No match found — skip enrichment
                return record, max(BASE_DELAY, delay * 0.9)

            # Polite pause between search and enrich
            adaptive_sleep(delay)

            # Step 2: Enrich (thuộc tính + lịch sử)
            enriched = scraper.enrich(match)
            record["validity"] = enriched.validity.to_dict()
            record["evidence"] = enriched.evidence.to_dict()

            # Success → ease delay back toward baseline
            delay = max(BASE_DELAY, delay * 0.8)
            return record, delay

        except Exception as e:
            last_error = e
            logger.warning(
                "  Attempt %d/%d for %s failed: %s",
                attempt, MAX_RETRIES, so_hieu, e,
            )
            delay = min(delay * BACKOFF_FACTOR, MAX_DELAY)
            if attempt < MAX_RETRIES:
                adaptive_sleep(delay)

    # All retries exhausted
    record["error"] = str(last_error)
    return record, delay


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Batch enrichment pipeline")
    parser.add_argument("--category", default="Thue-Phi-Le-Phi",
                        help="Dataset category to process")
    parser.add_argument("--limit", type=int, default=0,
                        help="Max documents to process (0 = all)")
    parser.add_argument("--delay", type=float, default=BASE_DELAY,
                        help="Base delay between requests (seconds)")
    parser.add_argument("--output-dir", type=str, default=str(DEFAULT_OUTPUT),
                        help="Output directory")
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Resume from checkpoint (skip already-done)")
    parser.add_argument("--no-resume", action="store_true",
                        help="Start fresh (overwrite output)")
    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    # Output paths
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    safe_cat = args.category.replace("-", "_").lower()
    output_jsonl = out_dir / f"enriched_{safe_cat}.jsonl"
    stats_json = out_dir / f"stats_{safe_cat}.json"

    # Load data
    logger.info("Loading dataset from %s ...", DATASET_PATH)
    docs = load_tax_docs(args.category)
    logger.info("Found %d unique central-type documents", len(docs))

    if args.limit > 0:
        docs = docs[: args.limit]
        logger.info("Limited to %d documents", len(docs))

    # Checkpoint
    if args.no_resume and output_jsonl.exists():
        output_jsonl.unlink()
        logger.info("Cleared previous output")

    done = load_checkpoint(output_jsonl) if not args.no_resume else set()
    remaining = [d for d in docs if d["so_hieu"].strip() not in done]
    logger.info(
        "Checkpoint: %d already done, %d remaining",
        len(done), len(remaining),
    )

    if not remaining:
        logger.info("Nothing to do!")
        print_stats(output_jsonl, stats_json)
        return

    # Init crawlers with polite delays
    searcher = VBPLSearcher(delay=args.delay)
    scraper = VBPLStatusScraper(delay=args.delay)

    # Run
    current_delay = args.delay
    stats = Counter()

    pbar = tqdm(remaining, desc="Enriching", unit="doc")
    for item in pbar:
        so_hieu = item["so_hieu"].strip()
        pbar.set_postfix_str(f"{so_hieu[:25]}  delay={current_delay:.1f}s")

        record, current_delay = enrich_one(item, searcher, scraper, current_delay)
        append_result(output_jsonl, record)

        # Track stats
        if record.get("error"):
            stats["error"] += 1
        elif record["match"] and record["match"]["confidence"] == "none":
            stats["no_match"] += 1
        elif record["match"] and record["match"]["confidence"] == "fuzzy":
            stats["fuzzy"] += 1
        else:
            stats["exact"] += 1

        if record.get("validity"):
            status = record["validity"].get("status_current", "unknown")
            stats[f"status_{status}"] += 1

        # Polite delay before next document
        adaptive_sleep(current_delay)

    pbar.close()

    # Final stats
    logger.info("="*60)
    logger.info("BATCH COMPLETE")
    logger.info("  Total processed: %d", len(remaining))
    for k, v in sorted(stats.items()):
        logger.info("  %s: %d", k, v)

    print_stats(output_jsonl, stats_json)


def print_stats(output_jsonl: Path, stats_json: Path):
    """Read the JSONL and produce summary statistics."""
    if not output_jsonl.exists():
        return

    records = []
    with open(output_jsonl, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass

    total = len(records)
    if total == 0:
        return

    # Match stats
    match_conf = Counter(
        r.get("match", {}).get("confidence", "missing") if r.get("match") else "missing"
        for r in records
    )
    # Status distribution
    status_dist = Counter(
        r.get("validity", {}).get("status_current", "unknown") if r.get("validity") else "no_data"
        for r in records
    )
    # Error count
    errors = sum(1 for r in records if r.get("error"))
    # Events stats
    event_counts = [
        len(r.get("validity", {}).get("events", []))
        for r in records
        if r.get("validity")
    ]
    avg_events = sum(event_counts) / len(event_counts) if event_counts else 0

    stats = {
        "total": total,
        "match_confidence": dict(match_conf),
        "status_distribution": dict(status_dist),
        "errors": errors,
        "avg_events": round(avg_events, 1),
        "max_events": max(event_counts) if event_counts else 0,
        "with_events": sum(1 for c in event_counts if c > 0),
    }

    with open(stats_json, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    print("\n" + "="*60)
    print("📊 ENRICHMENT STATISTICS")
    print("="*60)
    print(f"  Total documents:      {total}")
    print(f"  Errors:               {errors}")
    print(f"\n  Match confidence:")
    for k, v in match_conf.most_common():
        pct = v / total * 100
        print(f"    {k:12s}: {v:4d} ({pct:5.1f}%)")
    print(f"\n  Status distribution:")
    for k, v in status_dist.most_common():
        pct = v / total * 100
        print(f"    {k:20s}: {v:4d} ({pct:5.1f}%)")
    print(f"\n  History events:")
    print(f"    Docs with events:   {stats['with_events']}")
    print(f"    Average events/doc: {stats['avg_events']}")
    print(f"    Max events:         {stats['max_events']}")
    print("="*60)


if __name__ == "__main__":
    main()
