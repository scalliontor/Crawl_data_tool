"""
Specialized parsers for Vietnamese legal documents.
Each document type has its own parser class optimized for its structure.
"""

from .base_parser import BaseParser, LegalNode
from .hierarchical_parser import HierarchicalParser
from .decision_parser import DecisionParser
from .directive_parser import DirectiveParser
from .plan_parser import PlanParser

# Mapping from document types to parser classes
PARSER_MAP = {
    # Hierarchical documents (complex structure)
    "Luật": HierarchicalParser,
    "Thông tư": HierarchicalParser,
    "Thông tư liên tịch": HierarchicalParser,
    "Nghị định": HierarchicalParser,
    "Pháp lệnh": HierarchicalParser,
    "Văn bản hợp nhất": HierarchicalParser,
    "Quy chế": HierarchicalParser,
    "Quy định": HierarchicalParser,
    
    # Decision-style documents (short, with Điều)
    "Quyết định": DecisionParser,
    "Lệnh": DecisionParser,
    "Sắc lệnh": DecisionParser,
    
    # Directive-style documents (simple numbered paragraphs, no Roman sections)
    "Thông báo": DirectiveParser,
    "Công điện": DirectiveParser,
    "Thông tri": DirectiveParser,
    
    # Plan-style documents (Roman numerals + sections + a/b/c points)
    "Chỉ thị": PlanParser,  # Has I., II., III. sections + a), b), c) points
    
    # Plan-style documents (Roman numerals + sections)
    "Kế hoạch": PlanParser,
    "Hướng dẫn": PlanParser,
    "Báo cáo": PlanParser,
    
    # Resolutions (can be formal or informal)
    "Nghị quyết": DecisionParser,  # Often short with Điều
}

def get_parser(doc_type: str) -> BaseParser:
    """
    Get the appropriate parser for a document type.
    Falls back to HierarchicalParser for unknown types.
    """
    parser_class = PARSER_MAP.get(doc_type, HierarchicalParser)
    return parser_class()

__all__ = [
    'BaseParser',
    'LegalNode',
    'HierarchicalParser',
    'DecisionParser',
    'DirectiveParser',
    'PlanParser',
    'get_parser',
    'PARSER_MAP',
]
