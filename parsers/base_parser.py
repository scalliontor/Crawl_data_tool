"""
Base parser class and common data structures for legal document parsing.
"""

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from bs4 import BeautifulSoup


@dataclass
class LegalNode:
    """Represents a node in the legal document hierarchy."""
    level: int           # 0=Doc, 1=Part, 2=Chapter, 3=Section, 4=Article, 5=Clause, 6=Point
    type: str            # 'document', 'part', 'chapter', 'section', 'article', 'clause', 'point'
    title: str           # e.g., "Điều 1. Phạm vi điều chỉnh"
    content: List[str] = field(default_factory=list)  # Body text lines
    children: List['LegalNode'] = field(default_factory=list)
    html_id: Optional[str] = None  # Original HTML anchor id
    
    def add_text(self, text: str):
        """Add text content to this node."""
        if text.strip():
            self.content.append(text.strip())
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        data = {
            "type": self.type,
            "title": self.title,
        }
        if self.html_id:
            data["html_id"] = self.html_id
        
        full_content = "\n".join(self.content).strip()
        if full_content:
            data["content"] = full_content
        
        if self.children:
            data["children"] = [child.to_dict() for child in self.children]
        
        return data


class BaseParser(ABC):
    """Abstract base class for legal document parsers."""
    
    # Common regex patterns
    PATTERNS = {
        'part': re.compile(r'(?:^|.*[\.\:\n]\s*)((?:Phần\s+(?:thứ\s+)?[IVX]+|PHẦN\s+(?:THỨ\s+)?[IVX]+|Phần\s+[A-Z]+|PHẦN\s+[A-Z]+).*)\s*$', re.IGNORECASE | re.DOTALL),
        'chapter': re.compile(r'(?:^|.*[\.\:\n]\s*)(((?:Chương\s+[IVX0-9]+|CHƯƠNG\s+[IVX0-9]+)|(?:[IVX]+)\.\s+).*)\s*$', re.IGNORECASE | re.DOTALL),
        'section': re.compile(r'(?:^|.*[\.\:\n]\s*)((?:Mục\s+[0-9]+|MỤC\s+[0-9]+).*)\s*$', re.IGNORECASE | re.DOTALL),
        'article': re.compile(r'^\s*(Điều\s+\d+|ĐIỀU\s+\d+)[\.:]?\s*(.*)', re.IGNORECASE),
        'point': re.compile(r'^\s*([a-zđ])[\)\\.]\s+(.*)', re.IGNORECASE),
        'appendix': re.compile(r'^\s*(Phụ lục|PHỤ LỤC|Mẫu số|MẪU SỐ)\s+[0-9IVX]*.*\s*$', re.IGNORECASE),
        'recipients': re.compile(r'^\s*(Nơi nhận|Nơi gửi)[:;]\s*(.*)', re.IGNORECASE),
        'signature': re.compile(r'^\s*(TM\.|KT\.|TL\.|PP\.|CHỦ TỊCH|THỦ TƯỚNG|BỘ TRƯỞNG|THỐNG ĐỐC|GIÁM ĐỐC|TỔNG GIÁM ĐỐC|QUYỀN|KÝ THAY|Thay mặt).*\s*$', re.IGNORECASE),
        'loose_numbering': re.compile(r'^\s*(\d+(\.\d+)*)\.?\s+(.*)'),
    }
    
    def __init__(self):
        self.doc_type = "Unknown"
    
    def clean_text(self, text: str) -> str:
        """Clean and normalize text."""
        if not text:
            return ""
        return re.sub(r'\s+', ' ', text.replace('\xa0', ' ').replace('\r', '')).strip()
    
    def get_soup(self, html_content: str) -> BeautifulSoup:
        """Parse HTML and return BeautifulSoup object with content div."""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Try to find main content body
        content_div = (
            soup.find('div', class_='content1') or 
            soup.find('div', id='contentBody') or 
            soup.body or 
            soup
        )
        
        # Remove scripts/styles
        for tag in content_div.find_all(['script', 'style', 'iframe']):
            tag.extract()
        
        return content_div
    
    def detect_anchor_type(self, html_id: Optional[str]) -> Optional[str]:
        """Detect structure type from HTML anchor name attribute."""
        if not html_id:
            return None
        
        # Skip _name suffixes (these are titles, not structure markers)
        if html_id.endswith('_name'):
            return None
        
        if html_id.startswith('dieu_'):
            return 'article'
        elif html_id.startswith('chuong_'):
            return 'chapter'
        elif html_id.startswith('phan_'):
            return 'part'
        elif html_id.startswith('muc_'):
            return 'section'
        elif html_id.startswith('khoan_'):
            return 'clause'
        
        return None
    
    def is_bold(self, element) -> bool:
        """Check if an element contains bold text."""
        if element.find('b') or element.find('strong'):
            return True
        if element.name in ['h3', 'h4', 'h5']:
            return True
        return False
    
    @abstractmethod
    def parse(self, html_content: str, title: str = "Document") -> Dict[str, Any]:
        """
        Parse HTML content and return structured dictionary.
        
        Args:
            html_content: Raw HTML string
            title: Document title
            
        Returns:
            Dictionary with:
            - document_info: metadata
            - structure: parsed hierarchical content
            - metadata: recipients, signers
            - attachments: appendices
        """
        pass
