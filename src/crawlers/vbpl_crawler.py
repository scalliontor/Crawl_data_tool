"""
Job C — VBPLCrawler: Gap-fill new documents from vbpl.vn (2024→2026).

Browse by date range (and optionally by loại văn bản):
  /VBQPPL_UserControls/Publishing/TimKiem/pKetQuaTimKiem.aspx
  ?dvid=13&IsVietNamese=True&type=0&s=1
  &Keyword=&SearchIn=Title,Title1
  &fromyear={dd/mm/yyyy}&toyear={dd/mm/yyyy}
  [&idLoaiVanBan={id}]
  &Page={n}&RowPerPage=50

For each new doc found:
  1. Crawl toàn văn (full HTML body)
  2. Crawl thuộc tính + lịch sử (via VBPLStatusScraper)
  3. Parse with existing parsers module
"""

import re
import time
import logging
from typing import Optional
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup

from .models import VBPLMatch

logger = logging.getLogger(__name__)

# Known loại văn bản IDs on vbpl.vn
LOAI_VAN_BAN_IDS: dict[str, int] = {
    "Hiến pháp":          15,
    "Bộ luật":            16,
    "Luật":               17,
    "Nghị quyết":         18,
    "Pháp lệnh":         19,
    "Nghị định":          20,
    "Quyết định":         21,
    "Thông tư":           22,
    "Thông tư liên tịch": 23,
    "Lệnh":              2,
    "Nghị quyết liên tịch": 3,
}


class VBPLCrawler:
    """
    Step 3: Discover and crawl new documents from vbpl.vn.

    Usage:
        crawler = VBPLCrawler()
        
        # Discover new documents by date range
        new_docs = crawler.discover(from_date="01/01/2024", to_date="08/02/2026")
        
        # Crawl full text for a specific document
        html = crawler.crawl_toanvan(item_id=151086, path_segment="TW")
    """

    AJAX_BASE = "https://vbpl.vn/VBQPPL_UserControls/Publishing/TimKiem/pKetQuaTimKiem.aspx"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }

    RE_ITEM_ID = re.compile(r"ItemID=(\d+)")
    RE_TOTAL = re.compile(r"Tìm thấy\s*<b>(\d+)</b>")

    def __init__(self, delay: float = 1.5):
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        self.delay = delay
        self._last_request_time: float = 0

    def _rate_limit(self):
        elapsed = time.time() - self._last_request_time
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self._last_request_time = time.time()

    def _fetch(self, url: str) -> str:
        self._rate_limit()
        logger.debug("Fetching: %s", url)
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.text

    def _build_browse_url(
        self,
        from_date: str,
        to_date: str,
        page: int = 1,
        rows_per_page: int = 50,
        dvid: int = 13,
        loai_van_ban_id: Optional[int] = None,
    ) -> str:
        """Build the AJAX search URL for browsing by date range."""
        params = {
            "dvid": dvid,
            "IsVietNamese": "True",
            "type": 0,
            "s": 1,
            "Keyword": "",
            "SearchIn": "Title,Title1",
            "IsRec": 1,
            "pv": 0,
            "fromyear": from_date,
            "toyear": to_date,
            "Page": page,
            "RowPerPage": rows_per_page,
        }
        if loai_van_ban_id:
            params["idLoaiVanBan"] = loai_van_ban_id

        return f"{self.AJAX_BASE}?{urlencode(params)}"

    def _parse_listing_page(self, html: str) -> list[VBPLMatch]:
        """Parse a listing page and extract document matches."""
        results: list[VBPLMatch] = []

        # Handle single-result redirect
        redirect_match = re.search(r"window\.location\.href\s*=\s*'([^']+)'", html)
        if redirect_match:
            url = redirect_match.group(1)
            id_match = self.RE_ITEM_ID.search(url)
            if id_match:
                path_seg = url.strip("/").split("/")[0] if "/" in url else "TW"
                results.append(VBPLMatch(
                    so_hieu="",
                    vbpl_item_id=int(id_match.group(1)),
                    path_segment=path_seg,
                    matched_url=f"https://vbpl.vn{url}",
                    confidence="exact",
                ))
            return results

        # Multi-result listing
        soup = BeautifulSoup(html, "html.parser")
        items = soup.select("ul.listLaw li")

        for item in items:
            title_a = item.select_one("p.title a")
            if not title_a:
                continue

            title_text = title_a.text.strip()
            href = title_a.get("href", "")
            id_match = self.RE_ITEM_ID.search(href)
            if not id_match:
                continue

            path_seg = href.strip("/").split("/")[0] if "/" in href else "TW"

            # Extract date info
            info_ps = item.select("div.right p")
            ban_hanh = ""
            hieu_luc = ""
            trang_thai = ""
            for p in info_ps:
                text = p.get_text(strip=True)
                if text.startswith("Ban hành:"):
                    ban_hanh = text.replace("Ban hành:", "").strip()
                elif text.startswith("Hiệu lực:"):
                    hieu_luc = text.replace("Hiệu lực:", "").strip()
                elif text.startswith("Trạng thái:"):
                    trang_thai = text.replace("Trạng thái:", "").strip()

            # Extract so_hieu from title (e.g. "Thông tư 80/2021/TT-BTC" → "80/2021/TT-BTC")
            so_hieu_match = re.search(r"(\d+/\d{4}/[A-ZĐa-zđ\-]+)", title_text)
            so_hieu = so_hieu_match.group(1) if so_hieu_match else title_text

            results.append(VBPLMatch(
                so_hieu=so_hieu,
                vbpl_item_id=int(id_match.group(1)),
                path_segment=path_seg,
                matched_title=title_text,
                matched_url=f"https://vbpl.vn{href}",
                confidence="exact",
            ))

        return results

    def _get_total_count(self, html: str) -> int:
        """Extract total result count from search results."""
        match = self.RE_TOTAL.search(html)
        return int(match.group(1)) if match else 0

    def discover(
        self,
        from_date: str = "01/01/2024",
        to_date: str = "08/02/2026",
        dvid: int = 13,
        loai_van_ban: Optional[str] = None,
        max_pages: int = 100,
    ) -> list[VBPLMatch]:
        """
        Discover documents in a date range.

        Args:
            from_date: Start date (dd/mm/yyyy)
            to_date:   End date (dd/mm/yyyy)
            dvid:      Database division ID (13=TW)
            loai_van_ban: Optional filter by doc type name
            max_pages: Maximum number of pages to crawl

        Returns:
            List of VBPLMatch objects for discovered documents.
        """
        loai_id = LOAI_VAN_BAN_IDS.get(loai_van_ban) if loai_van_ban else None

        all_results: list[VBPLMatch] = []
        seen_ids: set[int] = set()

        for page in range(1, max_pages + 1):
            url = self._build_browse_url(
                from_date=from_date,
                to_date=to_date,
                page=page,
                dvid=dvid,
                loai_van_ban_id=loai_id,
            )

            try:
                html = self._fetch(url)
            except requests.RequestException as e:
                logger.error("Failed to fetch page %d: %s", page, e)
                if page == 1:
                    # If the very first page fails, raise so the caller
                    # knows we couldn't browse this period at all.
                    raise
                break

            # Check total on first page
            if page == 1:
                total = self._get_total_count(html)
                logger.info("Discovered %d total documents (%s → %s, type=%s)",
                            total, from_date, to_date, loai_van_ban or "ALL")

            results = self._parse_listing_page(html)

            if not results:
                logger.info("No more results at page %d, stopping.", page)
                break

            for r in results:
                if r.vbpl_item_id and r.vbpl_item_id not in seen_ids:
                    seen_ids.add(r.vbpl_item_id)
                    all_results.append(r)

            logger.info("  Page %d: %d new docs (total so far: %d)",
                        page, len(results), len(all_results))

        return all_results

    def crawl_toanvan(self, item_id: int, path_segment: str = "TW") -> dict:
        """
        Fetch the full text content of a document.

        Strategy (ordered by preference):
          1. Extract HTML body from ``div#toanvancontent`` (available for
             many Luật, Bộ luật, NĐ/TT cũ — rendered server-side).
          2. Fallback: locate the PDF URL via the AJAX VBGoc endpoint
             (needed for newer docs where VBPL only hosts the scanned PDF).

        Args:
            item_id: VBPL ItemID
            path_segment: e.g. "TW", "botaichinh"

        Returns:
            Dict with keys:
              - page_url:      URL of the toàn văn page
              - content_html:  inner HTML of div#toanvancontent (or None)
              - content_text:  plain-text extracted from toanvancontent (or None)
              - pdf_url:       direct link to the PDF file (or None)
              - pdf_filename:  filename of the PDF (or None)
              - source:        "html" | "pdf" | "empty"
        """
        page_url = (
            f"https://vbpl.vn/{path_segment}/Pages/"
            f"vbpq-toanvan.aspx?ItemID={item_id}"
        )
        page_html = self._fetch(page_url)
        soup = BeautifulSoup(page_html, "html.parser")

        result: dict = {
            "page_url": page_url,
            "content_html": None,
            "content_text": None,
            "pdf_url": None,
            "pdf_filename": None,
            "source": "empty",
        }

        # ----- Strategy 1: server-rendered HTML in div#toanvancontent -----
        toanvan_div = soup.find("div", id="toanvancontent")
        if toanvan_div and len(toanvan_div.get_text(strip=True)) > 200:
            result["content_html"] = str(toanvan_div)
            result["content_text"] = toanvan_div.get_text("\n", strip=True)
            result["source"] = "html"
            logger.info(
                "  HTML content found: %d chars",
                len(result["content_text"]),
            )
            # Still try to grab the PDF URL as a bonus
            self._extract_pdf_url(page_html, result)
            return result

        # ----- Strategy 2: PDF via AJAX VBGoc endpoint -----
        self._extract_pdf_url(page_html, result)
        if result["pdf_url"]:
            result["source"] = "pdf"

        return result

    def _extract_pdf_url(self, page_html: str, result: dict) -> None:
        """Populate *result* with ``pdf_url`` / ``pdf_filename`` if found."""
        vbgoc_match = re.search(r'pViewVBGoc\.aspx\?([^"]+)', page_html)
        if not vbgoc_match:
            return
        ajax_url = (
            "https://vbpl.vn/VBQPPL_UserControls/Publishing_22/"
            f"pViewVBGoc.aspx?{vbgoc_match.group(1)}"
        )
        try:
            ajax_html = self._fetch(ajax_url)
            soup = BeautifulSoup(ajax_html, "html.parser")
            obj_tag = soup.find("object", attrs={"type": "application/pdf"})
            if obj_tag and obj_tag.get("data"):
                pdf_path = obj_tag["data"]
                result["pdf_url"] = f"https://vbpl.vn{pdf_path}"
                result["pdf_filename"] = pdf_path.rsplit("/", 1)[-1]
                logger.info("  PDF found: %s", result["pdf_filename"])
        except Exception as e:
            logger.warning("  Failed to fetch VBGoc AJAX: %s", e)

    def download_pdf(
        self, pdf_url: str, save_path: str
    ) -> str:
        """
        Download a PDF file from vbpl.vn.

        Args:
            pdf_url:   Full URL to the PDF
            save_path: Local path to save the file

        Returns:
            The save_path on success.
        """
        self._rate_limit()
        resp = self.session.get(pdf_url, timeout=60, stream=True)
        resp.raise_for_status()

        import os
        os.makedirs(os.path.dirname(save_path), exist_ok=True)

        with open(save_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        logger.info("  Downloaded PDF: %s", save_path)
        return save_path
