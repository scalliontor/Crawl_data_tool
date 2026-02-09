"""
VBPL.vn Crawlers — Pipeline for enriching legal document metadata.

Job A: vbpl_searcher  — Match so_hieu → VBPL ItemID
Job B: vbpl_status    — Scrape Thuộc tính + Lịch sử → validity/events
Job C: vbpl_crawler   — Gap-fill new documents (2024→2026)
"""

from .models import (
    VBPLMatch,
    HistoryEvent,
    DocumentValidity,
    Evidence,
    EnrichedDocument,
    STATUS_MAP,
    ACTION_MAP,
)
from .vbpl_searcher import VBPLSearcher
from .vbpl_status import VBPLStatusScraper
from .vbpl_crawler import VBPLCrawler

__all__ = [
    "VBPLMatch",
    "HistoryEvent",
    "DocumentValidity",
    "Evidence",
    "EnrichedDocument",
    "STATUS_MAP",
    "ACTION_MAP",
    "VBPLSearcher",
    "VBPLStatusScraper",
    "VBPLCrawler",
]
