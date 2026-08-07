"""
Seed stage for EU TED dataset.

Generates Source objects for TED XML archive downloads from ted.europa.eu.

For past years (before the current year), uses monthly bulk packages:
    https://ted.europa.eu/packages/monthly/{YYYY-MM}
    e.g. https://ted.europa.eu/packages/monthly/2024-06

For the current year, uses daily packages:
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
import traceback

# ~22 business days per month (safe upper bound incl. padding for edge cases)
ISSUES_PER_MONTH = 23


def _monthly_sources(year: int) -> list[Source]:
    """Generate monthly Source objects for a past year."""
    sources = []
    for month in range(1, 13):
        year_month = f"{year}-{month:02d}"
        url = f"https://ted.europa.eu/packages/monthly/{year_month}"
        sources.append(
            Source(
                uri=url,
                metadata={
                    "year_month": year_month,
                    "year": year,
                    "month": month,
                },
            )
        )
    return sources


def _daily_sources(year: int) -> list[Source]:
    """Generate daily Source objects for the current year."""
    now = datetime.now()
    max_issue = now.month * ISSUES_PER_MONTH
    sources = []
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
    return sources


def handle(ctx: DatasetContext) -> Generator[Source, None, None]:
    """
    Generate Source objects for TED XML archives.

    Uses monthly packages for past years and daily packages for the current
    year. Sources are yielded in reverse chronological order (newest first).

    Environment variables START_YEAR and END_YEAR override config values.
    """
    now = datetime.now()
    first_year = int(
        os.environ.get("START_YEAR") or getattr(ctx.config.seed, "first_year", 2004)
    )
    last_year = int(
        os.environ.get("END_YEAR")
        or getattr(ctx.config.seed, "last_year", None)
        or now.year
    )

    ctx.log.info(
        f"Generating sources for TED packages from {first_year} to {last_year}"
        f" (monthly for past years, daily for {now.year})"
    )

    sources = []
    for year in range(first_year, last_year + 1):
        if year < now.year:
            sources.extend(_monthly_sources(year))
        else:
            sources.extend(_daily_sources(year))

    # Yield in reverse order (newest first)
    for source in reversed(sources):
        yield source
    traceback.print_exc()
