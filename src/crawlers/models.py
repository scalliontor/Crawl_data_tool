"""
Data models for the VBPL enrichment pipeline.

Designed for traceability: every field links back to evidence (URL + timestamp + HTML hash).
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field, asdict
from datetime import datetime, date
from typing import Optional


# ──────────────────────────────────────────────
# Enum-like mappings (plain strings, no deps)
# ──────────────────────────────────────────────

STATUS_MAP: dict[str, str] = {
    "Còn hiệu lực":             "valid",
    "Hết hiệu lực toàn bộ":     "expired",
    "Hết hiệu lực một phần":    "partial",
    "Hết hiệu lực":             "expired",
    "Chưa có hiệu lực":         "not_yet_effective",
    "Ngưng hiệu lực":           "suspended",
    "Ngưng hiệu lực một phần":  "partial_suspended",
}

ACTION_MAP: dict[str, str] = {
    "Văn bản được ban hành":     "issued",
    "Văn bản có hiệu lực":      "effective",
    "Văn bản hết hiệu lực":     "expired",
    "Bị hết hiệu lực":          "expired_by",
    "Bị thay thế":               "replaced",
    "Bị thay thế bởi":           "replaced",
    "Bị bãi bỏ":                "abolished",
    "Bị bãi bỏ 1 phần":         "partial_abolish",
    "Bị sửa đổi 1 phần":        "partial_amend",
    "Được bổ sung":              "supplemented",
    "Được sửa đổi":             "amended",
    "Sửa đổi, bổ sung":         "amended",
}


# ──────────────────────────────────────────────
# Data classes (pure Python, JSON-serialisable)
# ──────────────────────────────────────────────

@dataclass
class VBPLMatch:
    """Result of Step 1: mapping so_hieu → VBPL internal coordinates."""
    so_hieu: str                     # From TVPL dataset, e.g. "80/2021/TT-BTC"
    vbpl_item_id: Optional[int] = None
    dvid: Optional[int] = None       # NOT always 13!  Extracted from redirect path.
    path_segment: str = ""           # e.g. "TW", "botaichinh", "dongthap"
    matched_title: str = ""
    matched_url: str = ""
    confidence: str = "none"         # "exact", "fuzzy", "none"
    search_url: str = ""             # The AJAX URL we used, for reproducibility

    def detail_url(self, page: str) -> str:
        """Build URL for thuoctinh / lichsu / toanvan pages."""
        if not self.vbpl_item_id or not self.path_segment:
            return ""
        return f"https://vbpl.vn/{self.path_segment}/Pages/{page}.aspx?ItemID={self.vbpl_item_id}"

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class HistoryEvent:
    """One row from the Lịch sử table."""
    event_date: str                  # dd/mm/yyyy
    action_raw: str                  # Original Vietnamese text
    action_type: str                 # Normalised enum from ACTION_MAP
    source_doc: str                  # Số hiệu of the doc that caused the event
    source_item_id: Optional[int] = None
    source_url: str = ""             # Link to source doc on VBPL
    scope_text: str = ""             # Content of balloon div ("Điểm c khoản 2 Điều 8…")
    detail_balloon_id: str = ""      # e.g. "balloon_40742_9"

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class DocumentValidity:
    """Enriched validity + events for a single document."""
    status_current: str = "unknown"  # valid, expired, partial, not_yet_effective, suspended, unknown
    effective_date: str = ""
    expiry_date: str = ""
    status_raw: str = ""             # Raw text from thuộc tính header

    # Structured metadata from thuộc tính table
    so_ky_hieu: str = ""
    loai_van_ban: str = ""
    ngay_ban_hanh: str = ""
    nganh: str = ""
    linh_vuc: str = ""
    co_quan_ban_hanh: str = ""
    nguoi_ky: str = ""
    chuc_danh: str = ""
    pham_vi: str = ""

    # Event log from lịch sử table
    events: list[HistoryEvent] = field(default_factory=list)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["events"] = [e.to_dict() if isinstance(e, HistoryEvent) else e for e in self.events]
        return d


@dataclass
class Evidence:
    """Audit trail: which URLs were fetched and when."""
    source_pages: list[str] = field(default_factory=list)
    fetched_at: str = ""
    html_hashes: dict[str, str] = field(default_factory=dict)  # {url: md5}

    @staticmethod
    def hash_html(html: str) -> str:
        return hashlib.md5(html.encode("utf-8", errors="replace")).hexdigest()

    def record(self, url: str, html: str):
        self.source_pages.append(url)
        self.html_hashes[url] = self.hash_html(html)
        self.fetched_at = datetime.now().isoformat()

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class EnrichedDocument:
    """Full enriched record: match + validity + evidence."""
    match: VBPLMatch
    validity: DocumentValidity = field(default_factory=DocumentValidity)
    evidence: Evidence = field(default_factory=Evidence)

    def to_dict(self) -> dict:
        return {
            "match": self.match.to_dict(),
            "validity": self.validity.to_dict(),
            "evidence": self.evidence.to_dict(),
        }
