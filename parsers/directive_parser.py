"""
Directive Parser for directive-style documents.
Handles: Chỉ thị, Thông báo, Công điện, Thông tri

Structure: Numbered paragraphs (1., 2., 3...) without formal Điều.
"""

from typing import Dict, Any, List
from .base_parser import BaseParser, LegalNode


class DirectiveParser(BaseParser):
    """
    Parser for directive-style documents.
    These use simple numbered paragraphs instead of Điều structure.
    """
    
    def __init__(self):
        super().__init__()
        self.doc_type = "Directive"
    
    def parse(self, html_content: str, title: str = "Document") -> Dict[str, Any]:
        """
        Parse directive document into numbered items.
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
            
            # Top-level numbered item (1., 2., 3...)
            if (match := self.PATTERNS['loose_numbering'].match(text)):
                number = match.group(1)
                content = match.group(3)
                
                # Check if this is a top-level item or sub-item
                if stack[-1].type == 'document':
                    # Top-level directive item
                    matched_node = LegalNode(4, "item", number)
                    matched_node.add_text(content)
                elif stack[-1].type in ('item', 'subitem'):
                    # Could be sibling or sub-item
                    if '.' in number:  # 1.1, 2.1 = sub-item
                        matched_node = LegalNode(5, "subitem", number)
                        matched_node.add_text(content)
                    else:
                        # Sibling item
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
