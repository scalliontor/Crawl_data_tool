# Parsers Module

This module contains specialized parsers for different types of Vietnamese legal documents.

## Parser Classes

- `BaseParser` - Abstract base class with common functionality
- `HierarchicalParser` - For complex documents: Thông tư, Luật, Nghị định
- `DecisionParser` - For administrative decisions: Quyết định, Lệnh
- `DirectiveParser` - For directives: Chỉ thị, Thông báo
- `PlanParser` - For plans: Kế hoạch, Hướng dẫn
- `ResolutionParser` - For resolutions: Nghị quyết

## Usage

```python
from parsers import get_parser

parser = get_parser(doc_type="Thông tư")
result = parser.parse(html_content, title="Thông tư 80/2021/TT-BTC")
```
