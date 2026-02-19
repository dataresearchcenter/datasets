"""
Seed stage for EU TED dataset.

Generates Source objects for daily TED XML archive downloads from
https://ted.europa.eu/packages/daily/

Each daily package corresponds to one OJ S (Official Journal Supplement)
issue, published on business days. The URL format is:
    https://ted.europa.eu/packages/daily/{year}{issue:05d}
e.g. https://ted.europa.eu/packages/daily/202600022 for OJ S 022/2026

Environment variables:
    START_YEAR: Override first year to process (default: from config, 2004)
    END_YEAR:   Override last year to process (default: from config, current year)
"""

import os
from datetime import datetime
from typing import Generator

from investigraph.model import DatasetContext, Source

# ~22 business days per month (safe upper bound incl. padding for edge cases)
ISSUES_PER_MONTH = 23


def _max_issues(year: int) -> int:
    """Estimate max OJ S issue number for a given year.

    For past years, use the full-year upper bound (12 * ISSUES_PER_MONTH).
    For the current year, scale by the current month to avoid generating
    hundreds of non-existent source URLs.
    """
    now = datetime.now()
    if year < now.year:
        return 12 * ISSUES_PER_MONTH
    # Current year: scale to current month
    return now.month * ISSUES_PER_MONTH


def handle(ctx: DatasetContext) -> Generator[Source, None, None]:
    """
    Generate Source objects for TED daily XML archives.

    Yields one Source per OJ S issue (business day) for each year in the
    configured range, pointing to the daily package URL at ted.europa.eu.
    Sources are yielded in reverse chronological order (newest first).

    Environment variables START_YEAR and END_YEAR override config values.

    Configuration in config.yml:
        seed:
          handler: ./seed.py:handle
          first_year: 2004   # Optional, defaults to 2004
          last_year: null     # Optional, defaults to current year
    """
    first_year = int(
        os.environ.get("START_YEAR") or getattr(ctx.config.seed, "first_year", 2004)
    )
    last_year = int(
        os.environ.get("END_YEAR")
        or getattr(ctx.config.seed, "last_year", None)
        or datetime.now().year
    )

    ctx.log.info(
        f"Generating sources for TED daily packages from {first_year} to {last_year}"
    )

    sources = []
    for year in range(first_year, last_year + 1):
        max_issue = _max_issues(year)
        for issue in range(1, max_issue + 1):
            oj_number = f"{year}{issue:05d}"
            url = f"https://ted.europa.eu/packages/daily/{oj_number}"
            sources.append(
                Source(
                    uri=url,
                    metadata={
                        "oj_number": oj_number,
                        "year": year,
                        "issue": issue,
                    },
                )
            )

    # Yield in reverse order (newest first)
    for source in reversed(sources):
        yield source
