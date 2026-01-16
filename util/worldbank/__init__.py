"""Shared utilities for World Bank Finances One datasets."""

from datetime import datetime


def parse_date(date_str: str | None) -> str | None:
    """Parse date from DD-Mon-YYYY format (World Bank) to ISO format."""
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, "%d-%b-%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None
