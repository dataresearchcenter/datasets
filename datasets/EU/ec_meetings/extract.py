"""
Crawl the EC transparency portal for meetings and missions.

The portal groups its hosts into sections - commissioners (with their cabinets
and missions), directorates-general, executive agencies - and lists them in a
table per section. Every host has a detail page carrying an xlsx export of all
its rows, plus the same rows as a paginated HTML table.

Records come from the export: it holds every row in a single request, where the
rendered table serves 20 rows per page. Its "interest representative(s)" column
is the one thing the export gets worse: it joins the organisations with ", ",
which is ambiguous whenever a name contains a comma itself ("Človek v ohrození,
n.o."). Only for hosts that actually have such a cell the rendered table is
fetched as well - it lists one organisation per line - and used to split that
one column. Everything else is read from the export.
"""

import os
import re
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import pandas as pd
from investigraph.model import SourceContext
from investigraph.types import Record, RecordGenerator
from lxml.html import HtmlElement
from memorious.logic.fetch import fetch
from memorious.logic.http import ContextHttpResponse

BASE = "https://ec.europa.eu/transparency-initiative/meetings/"

# host kinds, taken from the last path segment of a detail page link
MISSION = "mission"

# "Information on meetings held by X" / "Information on missions of X" -> "X"
HOST_NAME = re.compile(r"^Information on (?:meetings held by|missions of)\s+", re.I)
PAGE_COUNT = re.compile(r"Page\s+\d+\s+of\s+(\d+)")
# a trailing acronym the rendered table appends but the export leaves out - it
# can nest one level itself ("... (NSC-FoE Hu (MTVSZ))")
ACRONYM = re.compile(r"\s*\((?:[^()]|\([^()]*\))*\)$")

INTEREST = "Interest representative(s)"
MINUTES = "Minutes"
DATE = "Date"

MEETING_COLUMNS = {
    "date": ("Date of meeting",),
    "location": ("Location",),
    "subject": ("Subject(s)", "Subject matter"),
    "representatives": ("Commission representative(s)", "Name"),
    "organisations": ("Interest representative(s) met", "Interest representative(s)"),
}
MISSION_COLUMNS = {
    "start_date": ("Date from",),
    "end_date": ("Date to",),
    "location": ("Location",),
    "purpose": ("Purpose",),
    "context": ("Context",),
    "travel_costs": ("Travel costs",),
    "accommodation_costs": ("Accommodation costs",),
    "daily_allowances": ("Daily allowances",),
    "miscellaneous_costs": ("Miscellaneous costs",),
    "comments": ("Comments",),
}


def get(ctx: SourceContext, url: str) -> ContextHttpResponse:
    res = fetch(url, dataset=ctx.dataset)
    res.raise_for_status()
    return res


def get_lines(el: HtmlElement) -> list[str]:
    """A table cell as one entry per rendered line - they use `white-space:
    pre-line` to stack multiple values into a single cell."""
    return [
        line.strip().rstrip(",")
        for line in el.text_content().split("\n")
        if line.strip()
    ]


def clean(value: str) -> str:
    """Normalize the line endings and padding the export cells are full of."""
    value = value.replace("\r\n", "\n").replace("\r", "\n")
    return "\n".join(line.strip() for line in value.split("\n")).strip()


def pick(row: Record, names: tuple[str, ...]) -> str:
    """The first of the alternative column names present in a row - the four
    sheet layouts label the same content differently."""
    for name in names:
        value = row.get(name)
        if value:
            return clean(value)
    return ""


def parse_date(value: str) -> str | None:
    """`DD/MM/YYYY` as published by the portal -> ISO."""
    parts = value.strip().split("/")
    if len(parts) == 3:
        day, month, year = parts
        if day.isdigit() and month.isdigit() and year.isdigit():
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    return None


def with_page(url: str, page: int) -> str:
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query))
    query["page"] = str(page)
    return urlunsplit(parts._replace(query=urlencode(query)))


def get_kind(url: str) -> str:
    return urlsplit(url).path.rstrip("/").split("/")[-1]


def get_host_id(url: str) -> str:
    return dict(parse_qsl(urlsplit(url).query)).get("id", "")


def get_list_pages(ctx: SourceContext, url: str) -> RecordGenerator:
    """Yield `(college, doc)` per section page. The commissioners section is
    split into one tab per college, the other sections are a single page."""
    doc = get(ctx, url).html
    tabs = doc.xpath('//a[contains(@href, "collegeid=")]')
    if not tabs:
        yield None, doc
        return
    for tab in tabs:
        # includes the tab that is already open - memorious serves it from cache
        college = tab.text_content().strip()
        yield college, get(ctx, urljoin(BASE, tab.get("href"))).html


def parse_list(doc: HtmlElement) -> RecordGenerator:
    """Yield `(label, [(name, url)])` per row of a section table - the label is
    the acronym for a DG or agency, and the plain name for a commissioner."""
    for tr in doc.xpath("//table//tbody/tr[td]"):
        label = tr.xpath("./td[1]")[0].text_content().strip()
        links = [
            (HOST_NAME.sub("", a.text_content().strip()), urljoin(BASE, a.get("href")))
            for a in tr.xpath(".//a[@href]")
        ]
        if links:
            yield label, links


def parse_table(doc: HtmlElement) -> list[dict[str, list[str]]]:
    rows = []
    for tr in doc.xpath('//table[@id="tableSection"]//tr[td]'):
        row = {
            td.get("data-ecl-table-header"): get_lines(td)
            for td in tr.xpath("./td[@data-ecl-table-header]")
        }
        # the published minutes, linked from the table only
        row[MINUTES] = [
            urljoin(BASE, href)
            for href in tr.xpath(f'./td[@data-ecl-table-header="{MINUTES}"]//a/@href')
        ]
        rows.append(row)
    return rows


def crawl_table(ctx: SourceContext, doc: HtmlElement, url: str) -> list[dict]:
    """The rendered table of a host, all pages. It repeats the export rows in
    the same order."""
    rows = parse_table(doc)
    match = PAGE_COUNT.search(doc.text_content())
    pages = int(match.group(1)) if match else 1
    for page in range(2, pages + 1):
        rows += parse_table(get(ctx, with_page(url, page)).html)
    return rows


def parse_export_url(doc: HtmlElement) -> str | None:
    for href in doc.xpath("//a/@href"):
        if "/export?" in href:
            return urljoin(BASE, href)
    return None


def load_export(ctx: SourceContext, url: str) -> pd.DataFrame:
    """The export sheet: a title row, a header row, then the data."""
    with get(ctx, url).local_path() as path:
        df = pd.read_excel(path, header=None, dtype=str)
    df.columns = [str(c).strip() for c in df.iloc[1]]
    return df[2:].fillna("")


def split_organisations(exported: str, rendered: list[str]) -> list[str]:
    """Split the export's comma-joined cell along the rendered table.

    The rendered names sometimes carry a trailing acronym the export leaves out
    ("European DIGITAL SME Alliance (DIGITAL SME)"), but sometimes that
    parenthesis is part of the name itself ("Atos SE (France)"). So try each
    rendered name against the export text both ways and keep the export's own
    wording, which is what every other row provides.

    The two are not always in the same order, so walk the export and consume
    whichever name fits at the current offset, longest first. Returns an empty
    list if they don't line up, leaving the fallback to the caller.
    """
    candidates = [
        [c for c in (name, ACRONYM.sub("", name).strip()) if c] for name in rendered
    ]
    names: list[str] = []
    taken: set[int] = set()
    pos = 0
    while pos < len(exported):
        match = None
        for ix, options in enumerate(candidates):
            if ix in taken:
                continue
            for candidate in options:
                if exported.startswith(candidate, pos):
                    if match is None or len(candidate) > len(match[1]):
                        match = (ix, candidate)
        if match is None:
            return []
        taken.add(match[0])
        names.append(match[1])
        pos += len(match[1]) + 2  # ", "
    return names


def make_organisations(
    ctx: SourceContext, exported: str, rendered: dict | None, url: str, ix: int
) -> list[str]:
    if "," not in exported:
        return [exported] if exported else []
    if rendered is not None:
        names = split_organisations(exported, rendered.get(INTEREST) or [])
        if names:
            return names
        ctx.log.warning("Cannot split organisations", url=url, row=ix, value=exported)
    return [name.strip() for name in exported.split(",") if name.strip()]


def make_meeting(
    ctx: SourceContext, row: Record, rendered: dict | None, url: str, ix: int
) -> Record:
    date = pick(row, MEETING_COLUMNS["date"])
    if rendered is not None and (rendered.get(DATE) or [None])[0] != date:
        # the export was regenerated while paging through the table
        ctx.log.warning("Export and table out of sync", url=url, row=ix)
        rendered = None
    return {
        "date": parse_date(date),
        "location": pick(row, MEETING_COLUMNS["location"]),
        "subject": pick(row, MEETING_COLUMNS["subject"]),
        "representatives": [
            line.strip()
            for line in pick(row, MEETING_COLUMNS["representatives"]).split("\n")
            if line.strip()
        ],
        "organisations": make_organisations(
            ctx, pick(row, MEETING_COLUMNS["organisations"]), rendered, url, ix
        ),
        "minutes": (rendered or {}).get(MINUTES) or [],
    }


def make_mission(row: Record) -> Record:
    record = {key: pick(row, names) for key, names in MISSION_COLUMNS.items()}
    record["start_date"] = parse_date(record["start_date"])
    record["end_date"] = parse_date(record["end_date"])
    return record


def crawl_host(
    ctx: SourceContext, url: str, label: str, name: str, college: str | None
) -> RecordGenerator:
    kind = get_kind(url)
    doc = get(ctx, url).html
    export_url = parse_export_url(doc)
    if export_url is None:
        ctx.log.warning("No export link", url=url)
        return
    df = load_export(ctx, export_url)
    base = {
        "type": kind,
        "host_id": get_host_id(url),
        "host_label": label,
        "host_name": name,
        "college": college,
        "url": url,
    }
    if kind == MISSION:
        for _, row in df.iterrows():
            yield {**base, **make_mission(dict(row))}
        return

    # the rendered table costs a request per 20 rows, so only fetch it where it
    # beats the export: to split an ambiguous cell, or for the minutes links it
    # is the only place to find, once there is a lakehouse to archive them in
    column = pick({c: c for c in df.columns}, MEETING_COLUMNS["organisations"])
    rendered = None
    if os.environ.get("LAKEHOUSE_URI") or (
        column and df[column].str.contains(",").any()
    ):
        rendered = crawl_table(ctx, doc, url)
        if len(rendered) != len(df):
            ctx.log.warning(
                "Table and export differ in length",
                url=url,
                table=len(rendered),
                export=len(df),
            )
    for ix, (_, row) in enumerate(df.iterrows()):
        page = rendered[ix] if rendered is not None and ix < len(rendered) else None
        yield {**base, **make_meeting(ctx, dict(row), page, url, ix)}


def handle(ctx: SourceContext, *args, **kwargs) -> RecordGenerator:
    total = 0
    for college, doc in get_list_pages(ctx, ctx.source.uri):
        for label, links in parse_list(doc):
            for name, url in links:
                for record in crawl_host(ctx, url, label, name, college):
                    total += 1
                    yield record
    ctx.log.info(f"Extracted `{total}` records", source=ctx.source.name)
