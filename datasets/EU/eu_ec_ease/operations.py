"""
EASE portal JSON API crawler operations.

Downloads document PDFs from the European Commission's EASE portal.
Download URL patterns:
- HRS docs: /externalizedDocument/hrs/{hrsDocumentId}/{hrsItemId}/false/download
- ERS docs: /externalizedDocument/ers/{ersDocumentId}/{ersItemId}/false/download
"""

from datetime import datetime, timedelta
from urllib.parse import parse_qs, urlencode, urlparse

from anystore.types import SDict
from banal import ensure_dict

from memorious.logic.context import Context


BASE_URL = (
    "https://ec.europa.eu/transparency/documents-request/api/portal/search/criteria"
)
DOWNLOAD_BASE = "https://ec.europa.eu/transparency/documents-request/api/portal/externalizedDocument"
PAGE_SIZE = 100


def seed(context: Context, data: SDict):
    """Generate initial API URL with pagination parameters."""
    params = {
        "page": 0,
        "size": PAGE_SIZE,
        "sort": "publishedOn,DESC",
    }
    if not context.env.full_run:
        start_date = (
            context.env.start_date
            or (
                datetime.now()
                - timedelta(**ensure_dict(context.params.get("timedelta")))
            )
            .date()
            .isoformat()
        )
        params["from"] = start_date
    url = f"{BASE_URL}?{urlencode(params)}"
    context.emit(data={"url": url, "page": 0})


def parse(context: Context, data: dict):
    """Parse JSON response and emit download URLs for each document."""
    res = context.http.rehash(data)

    try:
        result = res.json
    except Exception as e:
        context.log.error(f"Failed to parse JSON: {e}")
        return

    current_page = data.get("page", 0)
    total_pages = result.get("totalPages", 0)
    total_elements = result.get("totalElements", 0)

    context.log.info(
        f"Page {current_page + 1}/{total_pages} " f"({total_elements} total documents)"
    )

    # Process each document
    for doc in result.get("content", []):
        doc_id = doc.get("publishedDocumentId")
        hrs_doc_id = doc.get("hrsDocumentId")
        hrs_item_id = doc.get("hrsItemId")
        ers_doc_id = doc.get("ersDocumentId")

        # Build download URL based on available IDs
        download_url = None
        if hrs_doc_id and hrs_item_id:
            download_url = (
                f"{DOWNLOAD_BASE}/hrs/{hrs_doc_id}/{hrs_item_id}/false/download"
            )
        elif ers_doc_id:
            # For ERS, check attachedDocuments for ersItemId
            for att in doc.get("attachedDocuments", []):
                ers_item_id = att.get("ersItemId")
                if ers_item_id:
                    download_url = (
                        f"{DOWNLOAD_BASE}/ers/{ers_doc_id}/{ers_item_id}/false/download"
                    )
                    break

        if not download_url:
            context.log.warning(f"No download URL for document {doc_id}")
            continue

        # Build filename from document title or ID
        title = doc.get("documentTitle", f"document_{doc_id}")
        # Clean filename
        file_name = "".join(c if c.isalnum() or c in "._- " else "_" for c in title)
        if not file_name.lower().endswith(".pdf"):
            file_name = f"{file_name}.pdf"

        doc_data = {
            "url": download_url,
            "file_name": file_name,
            "foreign_id": f"ease-{doc_id}",
            "publisher": context.crawler.config.publisher.name,
            **doc,
        }
        context.emit("download", data=doc_data)

    # Handle pagination - re-use the same query params, just increment page
    is_last = result.get("last", True)
    if not is_last and current_page < total_pages - 1:
        parsed = urlparse(data["url"])
        existing_params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        existing_params["page"] = current_page + 1
        next_url = f"{BASE_URL}?{urlencode(existing_params)}"
        context.emit("fetch", data={"url": next_url, "page": current_page + 1})
