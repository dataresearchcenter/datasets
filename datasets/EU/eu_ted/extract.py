import io
import logging
import tarfile
from typing import BinaryIO, Generator

from anystore.exceptions import DoesNotExist
from investigraph.model import SourceContext as C
from investigraph.types import RecordGenerator

from common.ocds.eu_ted.parse import parse_ted_notice

log = logging.getLogger(__name__)


def _xml_members_from_tar(
    tar: tarfile.TarFile,
) -> Generator[tuple[str, BinaryIO], None, None]:
    """Yield (name, file_object) for all XML files in a tar archive.

    Handles nested .tar.gz archives (new TED format) by reading
    their contents in-memory without extracting to disk.
    Each XML member is buffered into a BytesIO to avoid truncation
    issues when reading from a remote gzip stream.
    """
    try:
        for member in tar:
            if not member.isfile():
                continue
            try:
                if member.name.endswith(".xml"):
                    fh = tar.extractfile(member)
                    if fh is not None:
                        yield member.name, io.BytesIO(fh.read())
                elif member.name.endswith(".tar.gz"):
                    inner_fh = tar.extractfile(member)
                    if inner_fh is not None:
                        with tarfile.open(fileobj=inner_fh, mode="r:gz") as inner_tar:
                            yield from _xml_members_from_tar(inner_tar)
            except (EOFError, tarfile.ReadError) as e:
                log.warning("Failed to read tar member %s: %s", member.name, e)
                continue
    except (EOFError, tarfile.ReadError) as e:
        log.warning("Truncated archive, yielding what was read: %s", e)


def handle(ctx: C, *args, **kwargs) -> RecordGenerator:
    """
    Extract OCDS releases from TED daily tar.gz archives.

    Streams XML files directly from the archive without extracting to disk.
    Handles both old and new archive formats:
    - Old format: XML files directly in archive
    - New format: Archive contains nested .tar.gz files
    """
    ctx.log.info("Processing TED archive", uri=ctx.source.uri)

    try:
        ctx.source.info()
    except (DoesNotExist, FileNotFoundError):
        ctx.log.warning("Source does not exist", uri=ctx.source.uri)
        return

    processed = 0
    total_releases = 0

    try:
        with ctx.open() as fh:
            with tarfile.open(fileobj=fh, mode="r:gz") as tar:
                for name, xml_fh in _xml_members_from_tar(tar):
                    try:
                        for release in parse_ted_notice(xml_fh):
                            release_dict = release.model_dump(
                                mode="json", exclude_none=True
                            )
                            release_dict["__source__"] = "eu_ted"
                            yield release_dict
                            total_releases += 1

                        processed += 1
                        if processed % 10_000 == 0:
                            ctx.log.info(
                                f"Processed {processed} XML files",
                                releases=total_releases,
                            )
                    except Exception as e:
                        ctx.log.warning(
                            "Failed to parse XML file", path=name, error=str(e)
                        )
                        continue
    except Exception as e:
        ctx.log.warning(
            "Archive stream interrupted, yielding what was read",
            uri=ctx.source.uri,
            error=str(e),
            processed=processed,
        )

    ctx.log.info(
        f"Completed processing {processed} XML files from archive",
        total_releases=total_releases,
    )
