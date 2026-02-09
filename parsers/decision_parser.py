"""
Decision Parser for short administrative documents.
Handles: Quyết định, Lệnh, Sắc lệnh, Nghị quyết

Structure: Simple list of Điều with minimal nesting.
"""

from typing import Dict, Any, List
from .base_parser import BaseParser, LegalNode


class DecisionParser(BaseParser):
    """
    Parser for decision-style documents.
    These are typically short with 2-5 Điều, minimal hierarchy.
    """
    
    def __init__(self):
        super().__init__()
        self.doc_type = "Decision"
    
    def parse(self, html_content: str, title: str = "Document") -> Dict[str, Any]:
        """
        Parse decision document into flat article structure.
        """
        if not html_content:
            return {"structure": None, "metadata": {}, "attachments": []}
        
        content_div = self.get_soup(html_content)
        
        root = LegalNode(level=0, type="document", title=title)
        stack: List[LegalNode] = [root]
        
        metadata = {"recipients": [], "signers": []}
        attachments = []
        is_parsing_metadata = False
        
        elements = content_div.find_all(['p', 'div', 'h3', 'h4', 'h5'])
        
        for el in elements:
            text = self.clean_text(el.get_text(separator=" ", strip=True))
            if not text:
                continue
            
            is_bold = self.is_bold(el)
            anchor = el.find('a', attrs={'name': True})
            html_id = anchor['name'] if anchor else None
            anchor_type = self.detect_anchor_type(html_id)
            
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
                if anchor_type or self.PATTERNS['article'].match(text):
                    is_parsing_metadata = False
                else:
                    if len(text) < 60:
                        metadata['signers'].append(text)
                    continue
            
            # --- STRUCTURE MATCHING ---
            matched_node = None
            
            # Điều detection (anchor or regex)
            if anchor_type == 'article':
                matched_node = LegalNode(4, "article", text, html_id=html_id)
            elif self.PATTERNS['article'].match(text):
                # For decisions, accept non-bold articles
                matched_node = LegalNode(4, "article", text, html_id=html_id)
            
            # Clause detection (1., 2., 3...)
            elif (match := self.PATTERNS['loose_numbering'].match(text)):
                number = match.group(1)
                content = match.group(3)
                
                if stack[-1].type in ('article', 'clause', 'point'):
                    matched_node = LegalNode(5, "clause", number)
                    matched_node.add_text(content)
                else:
                    stack[-1].add_text(text)
            
            # Point detection (a), b)...)
            elif self.PATTERNS['point'].match(text):
                matched_node = LegalNode(6, "point", text, html_id=html_id)
            
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
            "attachments": attachments,
        }
