# Legal Document Crawler

This project is a Python-based crawler designed to extract and structure legal documents from websites. It parses HTML content into a structured JSON format and a plain text file, preserving hierarchical information like Chapters, Articles, and Clauses.

## Project Structure

```
.
├── src/
│   └── crawl.py         # Main crawling script
├── inputs/
│   └── urls.txt         # List of URLs to crawl
├── outputs/
│   └── structured_data/ # Output JSON and TXT files
├── documents/           # Reference legal documents (PDF/DOC)
├── requirements.txt     # Python dependencies
└── README.md            # This file
```

## Installation

1.  **Clone the repository** (or download the source code).
2.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

1.  Add the URLs you want to crawl to `inputs/urls.txt`, one per line.
2.  Run the crawler:
    ```bash
    python src/crawl.py
    ```
3.  The results will be saved in `outputs/structured_data/`:
    - `[Title].json`: The structured legal data tree.
    - `[Title].txt`: The raw text content.

## Output Format

The JSON output contains:
- `document_info`: Title, URL, and crawl date.
- `body`: A nested tree of legal nodes (Chapter -> Section -> Article -> Clause -> Point).
- `footer`: Recipient information.
- `appendices`: Any detected appendices.
