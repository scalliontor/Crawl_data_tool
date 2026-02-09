#!/usr/bin/env python3
"""
Step 1: Cache the so_hieu list from dataset (run once).
Step 2: Run enrichment from cached list (avoids slow dataset loading).
"""

import json
import os
import sys
import time
import random
import logging
from pathlib import Path
from collections import Counter

# Ensure project root importable
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

OUT_DIR = ROOT / "outputs" / "enrichment"
OUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_FILE = OUT_DIR / "tax_docs_cache.json"
OUTPUT_JSONL = OUT_DIR / "enriched_thue_phi_le_phi.jsonl"
STATS_JSON = OUT_DIR / "stats_thue_phi_le_phi.json"

CENTRAL_TYPES = {"Luật", "Nghị định", "Thông tư", "Thông tư liên tịch", "Pháp lệnh"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────
# Step 1: Cache
# ──────────────────────────────────────

def build_cache():
    """Read dataset once, save filtered list to JSON."""
    from datasets import load_from_disk

    logger.info("Loading dataset...")
    ds = load_from_disk(str(ROOT / "data_universal"))
    train = ds["train"]

    logger.info("Reading columns...")
    categories = train["category"]
    loai_vbs = train["loai_van_ban"]
    so_hieus = train["so_hieu"]
    titles = train["title"]
    # Optional columns
    tinh_trangs = train["tinh_trang"] if "tinh_trang" in train.column_names else [""] * len(categories)
    ngay_bhs = train["ngay_ban_hanh"] if "ngay_ban_hanh" in train.column_names else [""] * len(categories)
    links = train["link"] if "link" in train.column_names else [""] * len(categories)

    logger.info("Filtering...")
    docs = []
    for i in range(len(categories)):
        if (categories[i] == "Thue-Phi-Le-Phi"
                and loai_vbs[i] in CENTRAL_TYPES
                and so_hieus[i] and so_hieus[i].strip()):
            docs.append({
                "so_hieu": so_hieus[i].strip(),
                "title": titles[i],
                "loai_van_ban": loai_vbs[i],
                "category": categories[i],
                "tinh_trang": tinh_trangs[i] or "",
                "ngay_ban_hanh": ngay_bhs[i] or "",
                "link": links[i] or "",
            })

    # Deduplicate
    seen = set()
    unique = []
    for d in docs:
        if d["so_hieu"] not in seen:
            seen.add(d["so_hieu"])
            unique.append(d)

    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(unique, f, ensure_ascii=False, indent=2)

    logger.info("Cached %d unique docs → %s", len(unique), CACHE_FILE)
    return unique


# ──────────────────────────────────────
# Step 2: Enrichment
# ──────────────────────────────────────

BASE_DELAY = 2.5
JITTER_MAX = 1.0
BACKOFF_FACTOR = 2.0
MAX_DELAY = 60.0
MAX_RETRIES = 3


def load_checkpoint() -> set[str]:
    done = set()
    if OUTPUT_JSONL.exists():
        with open(OUTPUT_JSONL, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                    sh = r.get("original", {}).get("so_hieu", "")
                    if sh:
                        done.add(sh)
                except json.JSONDecodeError:
                    pass
    return done


def run_enrichment(docs: list[dict], fresh: bool = False):
    from src.crawlers.vbpl_searcher import VBPLSearcher
    from src.crawlers.vbpl_status import VBPLStatusScraper
    from tqdm import tqdm

    if fresh and OUTPUT_JSONL.exists():
        OUTPUT_JSONL.unlink()

    done = load_checkpoint()
    remaining = [d for d in docs if d["so_hieu"] not in done]
    logger.info("Checkpoint: %d done, %d remaining", len(done), len(remaining))

    if not remaining:
        logger.info("Nothing to do!")
        return

    searcher = VBPLSearcher(delay=BASE_DELAY)
    scraper = VBPLStatusScraper(delay=BASE_DELAY)
    current_delay = BASE_DELAY
    stats = Counter()

    pbar = tqdm(remaining, desc="Enriching", unit="doc")
    for item in pbar:
        so_hieu = item["so_hieu"]
        pbar.set_postfix_str(f"{so_hieu[:25]} d={current_delay:.1f}")

        record = {
            "original": item,
            "match": None,
            "validity": None,
            "evidence": None,
            "error": None,
        }

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                # Search
                match = searcher.search(so_hieu)
                record["match"] = match.to_dict()

                if match.confidence == "none":
                    stats["no_match"] += 1
                    current_delay = max(BASE_DELAY, current_delay * 0.9)
                    break

                # Pause between search and enrich
                time.sleep(current_delay + random.uniform(0, JITTER_MAX))

                # Enrich
                enriched = scraper.enrich(match)
                record["validity"] = enriched.validity.to_dict()
                record["evidence"] = enriched.evidence.to_dict()

                if match.confidence == "fuzzy":
                    stats["fuzzy"] += 1
                else:
                    stats["exact"] += 1

                st = enriched.validity.status_current
                stats[f"status_{st}"] += 1

                current_delay = max(BASE_DELAY, current_delay * 0.8)
                break

            except Exception as e:
                logger.warning("  Attempt %d/%d %s: %s", attempt, MAX_RETRIES, so_hieu, e)
                current_delay = min(current_delay * BACKOFF_FACTOR, MAX_DELAY)
                if attempt < MAX_RETRIES:
                    time.sleep(current_delay + random.uniform(0, JITTER_MAX))
                else:
                    record["error"] = str(e)
                    stats["error"] += 1

        # Write result
        with open(OUTPUT_JSONL, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

        # Polite delay
        time.sleep(current_delay + random.uniform(0, JITTER_MAX))

    pbar.close()
    logger.info("BATCH COMPLETE: processed %d", len(remaining))
    for k, v in sorted(stats.items()):
        logger.info("  %s: %d", k, v)


# ──────────────────────────────────────
# Stats
# ──────────────────────────────────────

def compute_stats():
    if not OUTPUT_JSONL.exists():
        print("No output file found.")
        return

    records = []
    with open(OUTPUT_JSONL, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass

    total = len(records)
    if not total:
        return

    match_conf = Counter(
        r.get("match", {}).get("confidence", "missing") if r.get("match") else "missing"
        for r in records
    )
    status_dist = Counter(
        r.get("validity", {}).get("status_current", "unknown") if r.get("validity") else "no_data"
        for r in records
    )
    errors = sum(1 for r in records if r.get("error"))
    event_counts = [
        len(r.get("validity", {}).get("events", []))
        for r in records if r.get("validity")
    ]
    avg_events = sum(event_counts) / max(len(event_counts), 1)

    stats = {
        "total": total,
        "match_confidence": dict(match_conf),
        "status_distribution": dict(status_dist),
        "errors": errors,
        "avg_events": round(avg_events, 1),
        "max_events": max(event_counts) if event_counts else 0,
        "with_events": sum(1 for c in event_counts if c > 0),
    }
    with open(STATS_JSON, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    print("\n" + "=" * 60)
    print("📊 ENRICHMENT STATISTICS")
    print("=" * 60)
    print(f"  Total documents:      {total}")
    print(f"  Errors:               {errors}")
    print(f"\n  Match confidence:")
    for k, v in match_conf.most_common():
        print(f"    {k:12s}: {v:4d} ({v/total*100:5.1f}%)")
    print(f"\n  Status distribution:")
    for k, v in status_dist.most_common():
        print(f"    {k:20s}: {v:4d} ({v/total*100:5.1f}%)")
    print(f"\n  History events:")
    print(f"    Docs with events:   {stats['with_events']}")
    print(f"    Average events/doc: {stats['avg_events']}")
    print(f"    Max events:         {stats['max_events']}")
    print("=" * 60)


# ──────────────────────────────────────
# Main
# ──────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["cache", "run", "stats", "all"],
                        help="cache=build cache, run=enrichment, stats=print stats, all=cache+run+stats")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--fresh", action="store_true", help="Start fresh (delete old JSONL)")
    args = parser.parse_args()

    if args.action in ("cache", "all"):
        build_cache()

    if args.action in ("run", "all"):
        if CACHE_FILE.exists():
            with open(CACHE_FILE, encoding="utf-8") as f:
                docs = json.load(f)
        else:
            docs = build_cache()

        if args.limit > 0:
            docs = docs[:args.limit]
            logger.info("Limited to %d docs", args.limit)

        run_enrichment(docs, fresh=args.fresh)

    if args.action in ("stats", "all"):
        compute_stats()
