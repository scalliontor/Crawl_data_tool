"""
Hierarchical Parser for complex legal documents.
Handles: Thông tư, Luật, Nghị định, Pháp lệnh, Văn bản hợp nhất

Structure: Phần → Chương → Mục → Điều → Khoản → Điểm
"""

from typing import Dict, Any, List, Optional
from .base_parser import BaseParser, LegalNode


class HierarchicalParser(BaseParser):
    """
    Parser for complex hierarchical documents like Thông tư, Luật.
    Uses a stack-based algorithm to build nested structure with duplication check.
    """
    
    def __init__(self):
        super().__init__()
        self.doc_type = "Hierarchical"
    
    def _is_duplicate(self, node1: LegalNode, node2: LegalNode) -> bool:
        """
        Check if two nodes are duplicates and should be merged.
        Logic: Same type AND (Same title OR One title is prefix of other).
        """
        if node1.type != node2.type:
            return False
            
        t1 = self.clean_text(node1.title).lower().rstrip('.')
        t2 = self.clean_text(node2.title).lower().rstrip('.')
        
        # Exact match
        if t1 == t2:
            return True
            
        # Prefix match (e.g. "1" and "1. Phạm vi")
        # Ensure regex false positive protection (e.g. "Điều 1" vs "Điều 10" -> No)
        if t1.startswith(t2 + ".") or t1.startswith(t2 + " ") or t1.startswith(t2 + ":"):
            return True
        if t2.startswith(t1 + ".") or t2.startswith(t1 + " ") or t2.startswith(t1 + ":"):
            return True
            
        return False

    def parse(self, html_content: str, title: str = "Document") -> Dict[str, Any]:
        """
        Parse HTML into hierarchical structure.
        """
        if not html_content:
            return {"structure": None, "metadata": {}, "attachments": []}
        
        content_div = self.get_soup(html_content)
        
        # Initialize root node
        root = LegalNode(level=0, type="document", title=title)
        stack: List[LegalNode] = [root]
        
        # Metadata holders
        metadata = {"recipients": [], "signers": []}
        attachments = []
        
        # State flags
        is_parsing_appendices = False
        is_parsing_metadata = False
        
        # Get all relevant elements
        elements = content_div.find_all(['p', 'div', 'h3', 'h4', 'h5', 'table', 'span'])
        
        for el in elements:
            text = self.clean_text(el.get_text(separator=" ", strip=True))
            if not text:
                continue
            
            is_bold = self.is_bold(el)
            
            # Get anchor if present
            anchor = el.find('a', attrs={'name': True})
            html_id = anchor['name'] if anchor else None
            anchor_type = self.detect_anchor_type(html_id)
            
            # --- METADATA DETECTION ---
            if self.PATTERNS['recipients'].match(text):
                is_parsing_metadata = True
                metadata['recipients'].append(text)
                continue
            
            if self.PATTERNS['signature'].match(text) and (len(text) < 100 or is_bold):
                is_parsing_metadata = True
                metadata['signers'].append(text)
                continue
            
            if is_parsing_metadata:
                if anchor_type or any(p.match(text) for k, p in self.PATTERNS.items() 
                                      if k not in ['recipients', 'signature', 'loose_numbering', 'point']):
                    is_parsing_metadata = False
                else:
                    if len(text) < 50 and (text[0].isupper() or text.startswith("-")):
                        metadata['signers'].append(text)
                    elif text.startswith("-"):
                        metadata['recipients'].append(text)
                    continue
            
            # --- APPENDIX DETECTION ---
            is_appendix_header = self.PATTERNS['appendix'].match(text) and (is_bold or len(text) < 100)
            
            if is_appendix_header:
                is_parsing_appendices = True
                attachments.append({"title": text, "content": []})
                continue
            
            if is_parsing_appendices:
                if self.PATTERNS['recipients'].match(text) or self.PATTERNS['signature'].match(text):
                    is_parsing_appendices = False
                    is_parsing_metadata = True
                    if self.PATTERNS['recipients'].match(text):
                        metadata['recipients'].append(text)
                    else:
                        metadata['signers'].append(text)
                    continue
                
                if attachments:
                    attachments[-1]["content"].append(text)
                continue
            
            # --- STRUCTURAL MATCHING ---
            matched_node = None
            
            # Priority 1: Anchor-based
            if anchor_type == 'part':
                matched_node = LegalNode(1, "part", text, html_id=html_id)
            elif anchor_type == 'chapter':
                matched_node = LegalNode(2, "chapter", text, html_id=html_id)
            elif anchor_type == 'section':
                matched_node = LegalNode(3, "section", text, html_id=html_id)
            elif anchor_type == 'article':
                matched_node = LegalNode(4, "article", text, html_id=html_id)
            elif anchor_type == 'clause':
                matched_node = LegalNode(5, "clause", text, html_id=html_id)
            
            # Priority 2: Regex pattern
            if not matched_node:
                if self.PATTERNS['part'].match(text) and is_bold:
                    matched_node = LegalNode(1, "part", text, html_id=html_id)
                elif self.PATTERNS['chapter'].match(text) and is_bold:
                    matched_node = LegalNode(2, "chapter", text, html_id=html_id)
                elif self.PATTERNS['section'].match(text) and is_bold:
                    matched_node = LegalNode(3, "section", text, html_id=html_id)
                elif self.PATTERNS['article'].match(text):
                    matched_node = LegalNode(4, "article", text, html_id=html_id)
                
                # Clause detection (1., 2....)
                elif (match := self.PATTERNS['loose_numbering'].match(text)):
                    number = match.group(1)
                    content = match.group(3)
                    
                    if stack[-1].type in ('clause', 'point'):
                        matched_node = LegalNode(5, "clause", number)
                        matched_node.add_text(content)
                    elif stack[-1].type == 'article':
                        matched_node = LegalNode(5, "clause", number)
                        matched_node.add_text(content)
                    else:
                        stack[-1].add_text(text)
                
                # Point detection (a), b)...)
                elif self.PATTERNS['point'].match(text):
                    matched_node = LegalNode(6, "point", text, html_id=html_id)
            
            # --- STACK UPDATE WITH DUPLICATION CHECK ---
            if matched_node:
                # Pop (find parent)
                while len(stack) > 1 and stack[-1].level >= matched_node.level:
                    stack.pop()
                
                parent = stack[-1]
                
                # Check duplication with LAST SIBLING
                if parent.children:
                    last_sibling = parent.children[-1]
                    if self._is_duplicate(last_sibling, matched_node):
                        # MERGE DETECTED
                        # Update title if new one is longer
                        if len(matched_node.title) > len(last_sibling.title):
                            last_sibling.title = matched_node.title
                        
                        # Merge content
                        last_sibling.content.extend(matched_node.content)
                        
                        # Merge ID if missing
                        if not last_sibling.html_id and matched_node.html_id:
                            last_sibling.html_id = matched_node.html_id
                            
                        # Ensure stack points to the merged node for children
                        # (Need to check if last_sibling is already in stack? usually yes if it's high level)
                        # We re-append to be safe (duplicate in stack is harmless if level logic pops it)
                        stack.append(last_sibling)
                        continue
                
                # Normal append
                parent.children.append(matched_node)
                stack.append(matched_node)
            else:
                stack[-1].add_text(text)
        
        # Finalize attachments
        final_attachments = [
            {"title": att["title"], "content": "\n".join(att["content"])}
            for att in attachments
        ]
        
        return {
            "structure": root.to_dict(),
            "metadata": metadata,
            "attachments": final_attachments,
        }
