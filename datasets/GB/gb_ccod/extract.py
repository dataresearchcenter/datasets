"""Extract stage for GB CCOD dataset.

Downloads the ZIP file via ctx.open() (with archiving/retry) and extracts CSV records.
"""

import csv
import zipfile
from io import BytesIO, TextIOWrapper

from investigraph.model import SourceContext
from investigraph.types import RecordGenerator


def handle(ctx: SourceContext) -> RecordGenerator:
    """Extract records from the archived ZIP file."""
    ctx.log.info("Downloading ZIP file", source=ctx.source.uri)

    with ctx.open() as fh:
        # Read ZIP content into memory
        zip_content = BytesIO(fh.read())

    with zipfile.ZipFile(zip_content) as zf:
        # Find the CSV file in the archive
        csv_files = [f for f in zf.namelist() if f.endswith(".csv")]
        if not csv_files:
            ctx.log.error("No CSV file found in ZIP")
            return

        csv_filename = csv_files[0]
        ctx.log.info("Processing CSV", filename=csv_filename)

        with zf.open(csv_filename) as csv_file:
            reader = csv.DictReader(TextIOWrapper(csv_file, encoding="utf-8"))
            for row in reader:
                yield {key: (None if value == "" else value) for key, value in row.items()}
