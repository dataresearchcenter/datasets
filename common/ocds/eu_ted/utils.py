# SPDX-FileCopyrightText: 2023 Free Software Foundation Europe <contact@fsfe.org>
#
# SPDX-License-Identifier: GPL-3.0-or-later

import re
from functools import cache
from typing import Optional

import ftfy


@cache
def clean(string: str) -> Optional[str]:
    """Clean and normalize text strings"""
    try:
        string = re.sub(r"\s+", " ", string)
        string = string.strip()
        string = ftfy.fix_text(string)
        return string
    except (TypeError, AttributeError):
        return None
