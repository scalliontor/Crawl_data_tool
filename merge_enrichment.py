#!/usr/bin/env python3
"""
Merge enrichment data (validity, events, evidence) directly into
document_info of parsed JSON files in outputs/thue_phi_le_phi/.

Key changes:
- Updates `document_info.tinh_trang` from useless "Đã biết" → proper Vietnamese
  status ("Còn hiệu lực", "Hết hiệu lực", etc.)
- Adds enrichment fields (effective_date, events, vbpl_url, ...) directly into
  `document_info` — NO separate 'enrichment' block
- If old 'enrichment' block exists from previous merge, migrates it into
  document_info and removes the block
- Single authoritative document_info for downstream consumption

Usage:
    python3 merge_enrichment.py              # dry-run (default)
    python3 merge_enrichment.py --apply      # actually write files
    python3 merge_enrichment.py --stats      # show stats only
"""

import json
import os
import sys
import shutil
import logging
from pathlib import Path
from collections import Counter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent
ENRICHMENT_JSONL = ROOT / "outputs" / "enrichment" / "enriched_thue_phi_le_phi.jsonl"
PARSED_DIR = ROOT / "outputs" / "thue_phi_le_phi"


def load_enrichment() -> dict[str, dict]:
    """Load enrichment JSONL → dict keyed by so_hieu (best entry per doc)."""
    enrich_map: dict[str, dict] = {}

    with open(ENRICHMENT_JSONL, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            d = json.loads(line)
            o = d.get("original") or {}
            sh = o.get("so_hieu", "").strip()
            if not sh:
                continue
            # Keep non-error entry over error entry
            if sh not in enrich_map or (enrich_map[sh].get("error") and not d.get("error")):
                enrich_map[sh] = d

    logger.info("Loaded %d unique enrichment entries", len(enrich_map))
    return enrich_map


def build_file_index() -> dict[str, list[Path]]:
    """Scan outputs/thue_phi_le_phi/ → dict keyed by so_hieu → list of file paths."""
    file_index: dict[str, list[Path]] = {}

    for folder in sorted(PARSED_DIR.iterdir()):
        if not folder.is_dir():
            continue
        for json_file in folder.glob("*.json"):
            try:
                with open(json_file, encoding="utf-8") as f:
                    data = json.load(f)
                sh = data.get("document_info", {}).get("so_hieu", "").strip()
                if sh:
                    file_index.setdefault(sh, []).append(json_file)
            except (json.JSONDecodeError, KeyError):
                pass

    total_files = sum(len(v) for v in file_index.values())
    logger.info("Indexed %d JSON files (%d unique so_hieu) in %s",
                total_files, len(file_index), PARSED_DIR)
    return file_index


# Map English status_current → Vietnamese tinh_trang
STATUS_MAP = {
    "valid":     "Còn hiệu lực",
    "expired":   "Hết hiệu lực",
    "partial":   "Hết hiệu lực một phần",
    "suspended": "Tạm ngưng hiệu lực",
    "unknown":   "Không xác định",
}


def apply_enrichment_to_doc_info(doc_info: dict, entry: dict) -> dict:
    """Write enrichment fields directly into document_info dict.

    Updates tinh_trang and adds enrichment metadata as flat fields.
    Returns the updated doc_info (mutated in place).
    """
    match = entry.get("match") or {}
    validity = entry.get("validity") or {}
    evidence = entry.get("evidence") or {}

    status_en = validity.get("status_current", "unknown")
    tinh_trang_vn = STATUS_MAP.get(status_en, f"Đã biết ({status_en})")

    # --- Core: overwrite tinh_trang with real status ---
    doc_info["tinh_trang"] = tinh_trang_vn

    # --- Enrichment metadata (flat, no nesting) ---
    effective = validity.get("effective_date")
    if effective:
        doc_info["ngay_hieu_luc"] = effective

    events = validity.get("events", [])
    if events:
        doc_info["su_kien_phap_ly"] = events

    confidence = match.get("confidence")
    if confidence:
        doc_info["do_khop_vbpl"] = confidence

    vbpl_id = match.get("vbpl_item_id")
    if vbpl_id:
        doc_info["vbpl_item_id"] = vbpl_id

    vbpl_url = match.get("url")
    if vbpl_url:
        doc_info["vbpl_url"] = vbpl_url

    src_pages = evidence.get("source_pages", [])
    if src_pages:
        doc_info["vbpl_evidence"] = src_pages

    fetched = evidence.get("fetched_at")
    if fetched:
        doc_info["enriched_at"] = fetched

    return doc_info


def merge(apply: bool = False):
    """Main merge logic — writes enrichment directly into document_info."""
    enrich_map = load_enrichment()
    file_index = build_file_index()

    matched = set(enrich_map.keys()) & set(file_index.keys())
    mergeable = {sh for sh in matched if not enrich_map[sh].get("error")}
    error_only = matched - mergeable

    logger.info("Matched: %d, Mergeable: %d, Error-only (skip): %d",
                len(matched), len(mergeable), len(error_only))

    stats = Counter()

    for sh in sorted(mergeable):
        file_paths = file_index[sh]
        entry = enrich_map[sh]

        for fpath in file_paths:
            if not apply:
                stats["would_merge"] += 1
                continue

            # Read current file
            with open(fpath, encoding="utf-8") as f:
                data = json.load(f)

            doc_info = data.get("document_info", {})

            # Check if already merged (tinh_trang != "Đã biết" and has enriched_at)
            if doc_info.get("enriched_at") and doc_info.get("tinh_trang") != "Đã biết":
                stats["already_merged"] += 1
                continue

            # Apply enrichment directly into document_info
            apply_enrichment_to_doc_info(doc_info, entry)
            data["document_info"] = doc_info

            # Remove old separate 'enrichment' block if it exists
            had_old_block = "enrichment" in data
            if had_old_block:
                del data["enrichment"]
                stats["migrated_old_block"] += 1

            # Write back
            with open(fpath, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            stats["merged"] += 1

            if stats["merged"] % 50 == 0:
                logger.info("  Merged %d files...", stats["merged"])

    # Summary
    print()
    print("=" * 60)
    if apply:
        print("✅ MERGE COMPLETE (enrichment → document_info)")
    else:
        print("🔍 DRY RUN (use --apply to write files)")
    print("=" * 60)
    print(f"  Total enrichment entries:   {len(enrich_map)}")
    print(f"  Total parsed JSON files:    {len(file_index)}")
    print(f"  Matched by so_hieu:         {len(matched)}")
    print(f"  Mergeable (no error):       {len(mergeable)}")
    print(f"  Error-only (skipped):       {len(error_only)}")
    print()
    for k, v in sorted(stats.items()):
        print(f"  {k}: {v}")
    print("=" * 60)


def show_stats():
    """Show what enrichment data would look like after merge."""
    enrich_map = load_enrichment()
    file_index = build_file_index()

    matched = set(enrich_map.keys()) & set(file_index.keys())
    mergeable = {sh for sh in matched if not enrich_map[sh].get("error")}

    status_dist = Counter()
    event_counts = []
    for sh in mergeable:
        entry = enrich_map[sh]
        v = entry.get("validity") or {}
        status_dist[v.get("status_current", "unknown")] += 1
        event_counts.append(len(v.get("events", [])))

    print()
    print("=" * 60)
    print("📊 ENRICHMENT MERGE STATISTICS")
    print("=" * 60)
    print(f"  Files to merge: {len(mergeable)}")
    print()
    print("  Status distribution:")
    for k, v in status_dist.most_common():
        print(f"    {k:20s}: {v:4d} ({v/len(mergeable)*100:.1f}%)")
    print()
    if event_counts:
        print(f"  Events per doc:")
        print(f"    Average:  {sum(event_counts)/len(event_counts):.1f}")
        print(f"    Max:      {max(event_counts)}")
        print(f"    With ≥1:  {sum(1 for c in event_counts if c > 0)}")
    print("=" * 60)

    # Show sample merged output
    for sh in sorted(mergeable):
        entry = enrich_map[sh]
        v = entry.get("validity") or {}
        if v.get("status_current") not in (None, "unknown") and len(v.get("events", [])) >= 2:
            block = build_enrichment_block(entry)
            print()
            print(f"  Sample merge for {sh}:")
            print(json.dumps(block, indent=4, ensure_ascii=False))
            break


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Merge enrichment data into parsed JSON files")
    parser.add_argument("--apply", action="store_true", help="Actually write files (default: dry-run)")
    parser.add_argument("--stats", action="store_true", help="Show statistics only")
    args = parser.parse_args()

    if args.stats:
        show_stats()
    else:
        merge(apply=args.apply)
