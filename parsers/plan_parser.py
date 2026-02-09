"""
Plan Parser for plan-style documents.
Handles: Kế hoạch, Hướng dẫn, Báo cáo

Structure: Roman numerals (I., II., III.) with Arabic sub-sections.
"""

import re
from typing import Dict, Any, List
from .base_parser import BaseParser, LegalNode


class PlanParser(BaseParser):
    """
    Parser for plan-style documents.
    These use Roman numeral sections with Arabic numbered sub-items.
    """
    
    # Additional patterns for plan documents
    ROMAN_SECTION = re.compile(r'^\s*([IVX]+)\.\s*(.*)', re.IGNORECASE)
    
    def __init__(self):
        super().__init__()
        self.doc_type = "Plan"
    
    def parse(self, html_content: str, title: str = "Document") -> Dict[str, Any]:
        """
        Parse plan document into section hierarchy.
        """
        if not html_content:
            return {"structure": None, "metadata": {}, "attachments": []}
        
        content_div = self.get_soup(html_content)
        
        root = LegalNode(level=0, type="document", title=title)
        stack: List[LegalNode] = [root]
        
        metadata = {"recipients": [], "signers": []}
        is_parsing_metadata = False
        
        elements = content_div.find_all(['p', 'div', 'h3', 'h4', 'h5'])
        
        for el in elements:
            text = self.clean_text(el.get_text(separator=" ", strip=True))
            if not text:
                continue
            
            is_bold = self.is_bold(el)
            
            # --- METADATA ---
            if self.PATTERNS['recipients'].match(text):
                is_parsing_metadata = True
                metadata['recipients'].append(text)
                continue
            
            if self.PATTERNS['signature'].match(text) and (len(text) < 100 or is_bold):
                is_parsing_metadata = True
                metadata['signers'].append(text)
                continue
            
            if is_parsing_metadata:
                if len(text) < 60:
                    metadata['signers'].append(text)
                continue
            
            # --- STRUCTURE MATCHING ---
            matched_node = None
            
            # Roman numeral section (I., II., III...)
            if (match := self.ROMAN_SECTION.match(text)) and is_bold:
                roman = match.group(1)
                section_title = match.group(2)
                matched_node = LegalNode(2, "section", f"{roman}. {section_title}")
            
            # Arabic numbered item (1., 2., 3...)
            elif (match := self.PATTERNS['loose_numbering'].match(text)):
                number = match.group(1)
                content = match.group(3)
                
                if stack[-1].type in ('section', 'item', 'point'):
                    matched_node = LegalNode(4, "item", number)
                    matched_node.add_text(content)
                else:
                    stack[-1].add_text(text)
            
            # Point detection (a), b)...)
            elif self.PATTERNS['point'].match(text):
                matched_node = LegalNode(6, "point", text)
            
            # --- STACK UPDATE ---
            if matched_node:
                while len(stack) > 1 and stack[-1].level >= matched_node.level:
                    stack.pop()
                stack[-1].children.append(matched_node)
                stack.append(matched_node)
            else:
                stack[-1].add_text(text)
        
        return {
            "structure": root.to_dict(),
            "metadata": metadata,
            "attachments": [],
        }
