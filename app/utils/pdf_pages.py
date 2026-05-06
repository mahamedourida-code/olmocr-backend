import logging
from typing import List

from app.utils.exceptions import ProcessingError

logger = logging.getLogger(__name__)


def is_pdf_bytes(file_data: bytes) -> bool:
    return file_data[:5] == b"%PDF-"


def render_pdf_pages_to_png(pdf_data: bytes, dpi: int = 180) -> List[bytes]:
    try:
        import pymupdf
    except ImportError as exc:
        raise ProcessingError("PDF support is not installed on this worker") from exc

    try:
        page_images: List[bytes] = []
        with pymupdf.open(stream=pdf_data, filetype="pdf") as document:
            if document.page_count == 0:
                raise ProcessingError("PDF has no pages to process")

            for page_number in range(document.page_count):
                page = document.load_page(page_number)
                pixmap = page.get_pixmap(dpi=dpi, alpha=False)
                page_images.append(pixmap.tobytes(output="png"))

        logger.info("Rendered PDF into %s page image(s)", len(page_images))
        return page_images
    except ProcessingError:
        raise
    except Exception as exc:
        raise ProcessingError(f"Failed to render PDF pages: {exc}") from exc
