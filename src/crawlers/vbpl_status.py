"""
Job B — VBPLStatusScraper: fetch Thuộc tính + Lịch sử for a known ItemID.

Thuộc tính page: vbpq-thuoctinh.aspx?ItemID={id}
  → Header:  div.vbInfo   → "Hiệu lực: …" + "Ngày có hiệu lực: …"
  → Table:   single <table> with rows for Số ký hiệu, Loại VB, Ngành, Lĩnh vực, etc.
  → Footer:  last row "Tình trạng hiệu lực: …"

Lịch sử page: vbpq-lichsu.aspx?ItemID={id}
  → Header:  same div.vbInfo
  → Table:   rows [Ngày | Trạng thái | Văn bản nguồn | Phần hết hiệu lực]
  → Balloons: div.balloonstyle id="balloon_{sourceItemID}_{index}" → scope text
"""

import re
import time
import logging
from typing import Optional

import requests
from bs4 import BeautifulSoup

from .models import (
    VBPLMatch,
    HistoryEvent,
    DocumentValidity,
    Evidence,
    EnrichedDocument,
    STATUS_MAP,
    ACTION_MAP,
)

logger = logging.getLogger(__name__)


class VBPLStatusScraper:
    """
    Step 2: Fetch thuộc tính + lịch sử and produce EnrichedDocument.

    Usage:
        scraper = VBPLStatusScraper()
        enriched = scraper.enrich(match)
        print(enriched.validity.status_current)
        print(enriched.validity.events)
    """

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }

    RE_ITEM_ID = re.compile(r"ItemID=(\d+)")

    def __init__(self, delay: float = 1.0):
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
        """Fetch a page with rate limiting."""
        self._rate_limit()
        logger.debug("Fetching: %s", url)
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.text

    # ──────────────────────────────────────────
    # Thuộc tính parser
    # ──────────────────────────────────────────

    @staticmethod
    def _parse_thuoctinh_header(soup: BeautifulSoup) -> dict[str, str]:
        """
        Parse the div.vbInfo header block.

        Structure:
            <div class="vbInfo">
                <span>Hiệu lực:</span>
                <span class="red/green">Hết hiệu lực một phần</span>
                <span>Ngày có hiệu lực:</span>
                <span>01/01/2022</span>
                [optional: <span>Ngày hết hiệu lực:</span> <span>01/07/2025</span>]
            </div>
        """
        result = {"status_raw": "", "effective_date": "", "expiry_date": ""}

        vbinfo = soup.find("div", class_="vbInfo")
        if not vbinfo:
            return result

        text = vbinfo.get_text(separator="|", strip=True)

        # Extract status
        # Pattern: "Hiệu lực:|Hết hiệu lực một phần|Ngày có hiệu lực:|01/01/2022"
        parts = [p.strip() for p in text.split("|") if p.strip()]

        for i, part in enumerate(parts):
            if part in ("Hiệu lực:", "Hiệu lực"):
                if i + 1 < len(parts):
                    result["status_raw"] = parts[i + 1]
            elif "Ngày có hiệu lực" in part:
                if i + 1 < len(parts):
                    result["effective_date"] = parts[i + 1]
            elif "Ngày hết hiệu lực" in part:
                if i + 1 < len(parts):
                    result["expiry_date"] = parts[i + 1]

        return result

    @staticmethod
    def _parse_thuoctinh_table(soup: BeautifulSoup) -> dict[str, str]:
        """
        Parse the attributes table.

        Rows (observed):
          0: Title (colspan)
          1: Số ký hiệu | VALUE | Ngày ban hành | VALUE
          2: Loại văn bản | VALUE | Ngày có hiệu lực | VALUE
          3: Nguồn thu thập | VALUE | Ngày đăng công báo | VALUE
          4: Ngành | VALUE | Lĩnh vực | VALUE
          5: Cơ quan ban hành/ Chức danh / Người ký | VALUE | Chức danh | VALUE
          6: Phạm vi | VALUE
          7: Thông tin áp dụng (colspan)
          8: Tình trạng hiệu lực: VALUE
        """
        result = {}
        table = soup.find("table")
        if not table:
            return result

        # Field mapping: label text → dict key
        FIELD_MAP = {
            "Số ký hiệu":        "so_ky_hieu",
            "Ngày ban hành":      "ngay_ban_hanh",
            "Loại văn bản":       "loai_van_ban",
            "Ngày có hiệu lực":  "ngay_co_hieu_luc",
            "Ngành":              "nganh",
            "Lĩnh vực":          "linh_vuc",
            "Phạm vi":           "pham_vi",
            "Tình trạng hiệu lực":  "tinh_trang",
        }

        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            # Process pairs of cells (label, value, label, value)
            i = 0
            while i < len(cells):
                cell_text = cells[i].get_text(strip=True).rstrip(":")
                # Check if this is a known label
                for label, key in FIELD_MAP.items():
                    if label in cell_text:
                        if i + 1 < len(cells):
                            result[key] = cells[i + 1].get_text(strip=True)
                        break

                # Special: "Cơ quan ban hành/ Chức danh / Người ký"
                if "Cơ quan ban hành" in cell_text:
                    if i + 1 < len(cells):
                        result["co_quan_ban_hanh"] = cells[i + 1].get_text(strip=True)
                    # Next pair is Chức danh | Người ký
                    if i + 2 < len(cells):
                        result["chuc_danh"] = cells[i + 2].get_text(strip=True)
                    if i + 3 < len(cells):
                        result["nguoi_ky"] = cells[i + 3].get_text(strip=True)

                i += 1

        return result

    def parse_thuoctinh(self, html: str) -> dict:
        """
        Parse full Thuộc tính page.
        Returns dict with header info + table fields merged.
        """
        soup = BeautifulSoup(html, "html.parser")
        header = self._parse_thuoctinh_header(soup)
        table = self._parse_thuoctinh_table(soup)
        return {**header, **table}

    # ──────────────────────────────────────────
    # Lịch sử parser
    # ──────────────────────────────────────────

    def parse_lichsu(self, html: str) -> list[HistoryEvent]:
        """
        Parse the Lịch sử page.

        Table structure:
          Row 0: Caption "Lịch sử hiệu lực: {title}"
          Row 1: Header "Ngày | Trạng thái | Văn bản nguồn | Phần hết hiệu lực"
          Row 2+: Data rows

        Balloon divs:
          <div class="balloonstyle" id="balloon_{itemID}_{index}">
            Điểm c khoản 2 Điều 8; …
          </div>

        "Xem tại đây" links have:
          <a href="javascript:;" rel="balloon_{itemID}_{index}">Xem tại đây</a>
        """
        soup = BeautifulSoup(html, "html.parser")
        events: list[HistoryEvent] = []

        # Collect balloon contents
        balloons: dict[str, str] = {}
        for div in soup.find_all("div", class_="balloonstyle"):
            div_id = div.get("id", "")
            text = div.get_text(strip=True)
            if div_id and text:
                balloons[div_id] = text

        # Parse table
        table = soup.find("table")
        if not table:
            return events

        rows = table.find_all("tr")

        for row in rows[2:]:  # Skip caption (row 0) and header (row 1)
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            # Column 0: Ngày
            event_date = cells[0].get_text(strip=True)

            # Column 1: Trạng thái
            action_raw = cells[1].get_text(strip=True)
            action_type = ACTION_MAP.get(action_raw, action_raw)

            # Column 2: Văn bản nguồn (has link)
            source_cell = cells[2]
            source_doc = source_cell.get_text(strip=True)
            source_url = ""
            source_item_id = None
            source_a = source_cell.find("a", href=True)
            if source_a:
                href = source_a.get("href", "")
                if href and "javascript" not in href:
                    source_url = f"https://vbpl.vn{href}" if href.startswith("/") else href
                    id_match = self.RE_ITEM_ID.search(href)
                    if id_match:
                        source_item_id = int(id_match.group(1))

            # Column 3: Phần hết hiệu lực (optional, may have "Xem tại đây" link)
            scope_text = ""
            balloon_id = ""
            if len(cells) >= 4:
                detail_cell = cells[3]
                detail_a = detail_cell.find("a", rel=True)
                if detail_a:
                    balloon_id = detail_a.get("rel", [""])[0] if isinstance(detail_a.get("rel"), list) else detail_a.get("rel", "")
                    scope_text = balloons.get(balloon_id, "")
                else:
                    # Sometimes the text is directly in the cell
                    scope_text = detail_cell.get_text(strip=True)
                    if scope_text == "Xem tại đây":
                        scope_text = ""  # Link without balloon content

            events.append(HistoryEvent(
                event_date=event_date,
                action_raw=action_raw,
                action_type=action_type,
                source_doc=source_doc,
                source_item_id=source_item_id,
                source_url=source_url,
                scope_text=scope_text,
                detail_balloon_id=balloon_id,
            ))

        return events

    # ──────────────────────────────────────────
    # Main enrich method
    # ──────────────────────────────────────────

    def enrich(self, match: VBPLMatch) -> EnrichedDocument:
        """
        Fetch Thuộc tính + Lịch sử for a matched document.
        Returns EnrichedDocument with validity + events + evidence.
        """
        if not match.vbpl_item_id or not match.path_segment:
            logger.warning("Cannot enrich: no ItemID for %s", match.so_hieu)
            return EnrichedDocument(match=match)

        evidence = Evidence()
        validity = DocumentValidity()

        # 1. Fetch Thuộc tính
        tt_url = match.detail_url("vbpq-thuoctinh")
        try:
            tt_html = self._fetch(tt_url)
            evidence.record(tt_url, tt_html)

            tt_data = self.parse_thuoctinh(tt_html)

            validity.status_raw = tt_data.get("status_raw", "")
            validity.status_current = STATUS_MAP.get(validity.status_raw, "unknown")
            validity.effective_date = tt_data.get("effective_date", "")
            validity.expiry_date = tt_data.get("expiry_date", "")
            validity.so_ky_hieu = tt_data.get("so_ky_hieu", "")
            validity.loai_van_ban = tt_data.get("loai_van_ban", "")
            validity.ngay_ban_hanh = tt_data.get("ngay_ban_hanh", "")
            validity.nganh = tt_data.get("nganh", "")
            validity.linh_vuc = tt_data.get("linh_vuc", "")
            validity.co_quan_ban_hanh = tt_data.get("co_quan_ban_hanh", "")
            validity.nguoi_ky = tt_data.get("nguoi_ky", "")
            validity.chuc_danh = tt_data.get("chuc_danh", "")
            validity.pham_vi = tt_data.get("pham_vi", "")

            # Also update match title if we got so_ky_hieu
            if not match.matched_title and validity.so_ky_hieu:
                match.matched_title = validity.so_ky_hieu

            logger.info("  Thuộc tính: status=%s, effective=%s",
                        validity.status_current, validity.effective_date)

        except requests.RequestException as e:
            logger.error("  Failed to fetch thuộc tính for %s: %s", match.so_hieu, e)

        # 2. Fetch Lịch sử
        ls_url = match.detail_url("vbpq-lichsu")
        try:
            ls_html = self._fetch(ls_url)
            evidence.record(ls_url, ls_html)

            events = self.parse_lichsu(ls_html)
            validity.events = events

            # Fallback: if thuoctinh didn't give us dates, extract from events
            if not validity.effective_date:
                for ev in events:
                    if ev.action_type == "effective":
                        validity.effective_date = ev.event_date
                        break

            if not validity.expiry_date:
                for ev in reversed(events):
                    if ev.action_type in ("expired", "expired_by"):
                        validity.expiry_date = ev.event_date
                        break

            # Fallback: if status is still unknown, infer from last event
            if validity.status_current == "unknown" and events:
                last = events[-1]
                if last.action_type in ("expired", "expired_by"):
                    validity.status_current = "expired"
                elif last.action_type == "effective":
                    validity.status_current = "valid"
                elif last.action_type in ("partial_abolish", "partial_amend"):
                    validity.status_current = "partial"

            logger.info("  Lịch sử: %d events", len(events))

        except requests.RequestException as e:
            logger.error("  Failed to fetch lịch sử for %s: %s", match.so_hieu, e)

        return EnrichedDocument(
            match=match,
            validity=validity,
            evidence=evidence,
        )
