"""
Job A — VBPLSearcher: match `so_hieu` → VBPL ItemID.

Uses the AJAX endpoint:
  /VBQPPL_UserControls/Publishing/TimKiem/pKetQuaTimKiem.aspx
  ?dvid=13&IsVietNamese=True&type=0&s=1
  &Keyword={so_hieu}&SearchIn=Title,Title1&IsRec=1&pv=0

Two response modes:
  1. Single result → JS redirect:  window.location.href = '/{path}/Pages/vbpq-toanvan.aspx?ItemID=XXXX'
  2. Multi result  → HTML list:    <ul class="listLaw"><li>…<p class="title"><a href="…?ItemID=XXXX">…</a></p>…</li></ul>
"""

import re
import time
import logging
import unicodedata
from urllib.parse import quote, urlencode
from typing import Optional

import requests
from bs4 import BeautifulSoup

from .models import VBPLMatch

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Path segment → dvid mapping (known values)
# ──────────────────────────────────────────────
PATH_TO_DVID: dict[str, int] = {
    "TW": 13,
    "tw": 13,
    "botaichinh": 14,
    "boquocphong": 16,
    # add more as discovered — or we can extract from redirect
}

# Default dvid for initial search (Trung ương)
DEFAULT_DVID = 13


class VBPLSearcher:
    """
    Step 1: Map `so_hieu` to VBPL ItemID.

    Usage:
        searcher = VBPLSearcher()
        match = searcher.search("80/2021/TT-BTC")
        # match.vbpl_item_id == 151086
        # match.path_segment == "botaichinh"
    """

    AJAX_BASE = "https://vbpl.vn/VBQPPL_UserControls/Publishing/TimKiem/pKetQuaTimKiem.aspx"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }

    # Regex to extract redirect URL from JS
    RE_REDIRECT = re.compile(r"window\.location\.href\s*=\s*'([^']+)'")
    # Regex to extract ItemID from URL
    RE_ITEM_ID = re.compile(r"ItemID=(\d+)")

    def __init__(self, delay: float = 1.0):
        """
        Args:
            delay: Seconds to wait between requests (polite crawling).
        """
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        self.delay = delay
        self._last_request_time: float = 0

    def _rate_limit(self):
        """Enforce minimum delay between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self._last_request_time = time.time()

    @staticmethod
    def normalise_so_hieu(so_hieu: str) -> str:
        """
        Normalise so_hieu for matching:
        - Strip whitespace
        - Normalise dash variants (–, —, ‐) to ASCII hyphen
        - NFC unicode normalisation
        """
        s = unicodedata.normalize("NFC", so_hieu.strip())
        s = re.sub(r"[\u2010-\u2015\u2212\uFE58\uFE63\uFF0D]", "-", s)
        s = re.sub(r"\s+", " ", s)
        return s

    def _build_search_url(self, keyword: str, dvid: int = DEFAULT_DVID) -> str:
        """Build the AJAX search URL."""
        params = {
            "dvid": dvid,
            "IsVietNamese": "True",
            "type": 0,
            "s": 1,
            "Keyword": keyword,
            "SearchIn": "Title,Title1",
            "IsRec": 1,
            "pv": 0,
        }
        return f"{self.AJAX_BASE}?{urlencode(params)}"

    @staticmethod
    def _extract_path_segment(url: str) -> str:
        """
        Extract the path segment (dvid proxy) from redirect URL.
        E.g. "/botaichinh/Pages/vbpq-toanvan.aspx?ItemID=151086" → "botaichinh"
             "/TW/Pages/…" → "TW"
        """
        url = url.strip("/")
        parts = url.split("/")
        if len(parts) >= 2 and parts[1].lower() == "pages":
            return parts[0]
        return parts[0] if parts else "TW"

    def _parse_single_result(self, html: str, so_hieu: str, search_url: str) -> Optional[VBPLMatch]:
        """Parse a single-result response (JS redirect)."""
        m = self.RE_REDIRECT.search(html)
        if not m:
            return None

        redirect_url = m.group(1)
        item_match = self.RE_ITEM_ID.search(redirect_url)
        if not item_match:
            return None

        path_seg = self._extract_path_segment(redirect_url)
        item_id = int(item_match.group(1))

        return VBPLMatch(
            so_hieu=so_hieu,
            vbpl_item_id=item_id,
            dvid=PATH_TO_DVID.get(path_seg),
            path_segment=path_seg,
            matched_title="",  # Not available in redirect-only response
            matched_url=f"https://vbpl.vn{redirect_url}",
            confidence="exact",
            search_url=search_url,
        )

    def _parse_multi_results(self, html: str, so_hieu: str, search_url: str) -> Optional[VBPLMatch]:
        """
        Parse a multi-result response.
        Strategy:
          1. Exact match: title contains normalised so_hieu
          2. First result (fallback, lower confidence)
        """
        soup = BeautifulSoup(html, "html.parser")
        items = soup.select("ul.listLaw li")

        if not items:
            return None

        normalised = self.normalise_so_hieu(so_hieu).lower()

        # Pass 1: exact match on title text
        for item in items:
            title_a = item.select_one("p.title a")
            if not title_a:
                continue

            title_text = title_a.text.strip()
            href = title_a.get("href", "")
            title_norm = self.normalise_so_hieu(title_text).lower()

            if normalised in title_norm or normalised.replace(" ", "") in title_norm.replace(" ", ""):
                item_match = self.RE_ITEM_ID.search(href)
                if item_match:
                    path_seg = self._extract_path_segment(href)
                    return VBPLMatch(
                        so_hieu=so_hieu,
                        vbpl_item_id=int(item_match.group(1)),
                        dvid=PATH_TO_DVID.get(path_seg),
                        path_segment=path_seg,
                        matched_title=title_text,
                        matched_url=f"https://vbpl.vn{href}",
                        confidence="exact",
                        search_url=search_url,
                    )

        # Pass 2: fallback to first result
        first = items[0].select_one("p.title a")
        if first:
            href = first.get("href", "")
            item_match = self.RE_ITEM_ID.search(href)
            if item_match:
                path_seg = self._extract_path_segment(href)
                return VBPLMatch(
                    so_hieu=so_hieu,
                    vbpl_item_id=int(item_match.group(1)),
                    dvid=PATH_TO_DVID.get(path_seg),
                    path_segment=path_seg,
                    matched_title=first.text.strip(),
                    matched_url=f"https://vbpl.vn{href}",
                    confidence="fuzzy",
                    search_url=search_url,
                )

        return None

    def search(self, so_hieu: str) -> VBPLMatch:
        """
        Search vbpl.vn for a document by so_hieu.

        Returns VBPLMatch with confidence:
          - "exact"  → so_hieu matches title exactly
          - "fuzzy"  → first result (title doesn't match exactly)
          - "none"   → no results found

        Raises requests.RequestException on network errors (caller should handle retry).
        """
        normalised = self.normalise_so_hieu(so_hieu)
        search_url = self._build_search_url(normalised)

        self._rate_limit()
        logger.debug("Searching: %s", search_url)
        resp = self.session.get(search_url, timeout=30)
        resp.raise_for_status()

        html = resp.text

        # Case 1: single result → JS redirect
        if "window.location.href" in html:
            match = self._parse_single_result(html, normalised, search_url)
            if match:
                logger.info("✓ Exact match (redirect): %s → ItemID=%s", so_hieu, match.vbpl_item_id)
                return match

        # Case 2: multi results → HTML list
        if "listLaw" in html:
            match = self._parse_multi_results(html, normalised, search_url)
            if match:
                logger.info("✓ %s match (list): %s → ItemID=%s [%s]",
                            match.confidence.capitalize(), so_hieu, match.vbpl_item_id, match.matched_title)
                return match

        # Case 3: no results
        logger.warning("✗ No match: %s", so_hieu)
        return VBPLMatch(
            so_hieu=normalised,
            confidence="none",
            search_url=search_url,
        )
