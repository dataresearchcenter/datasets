import re
from datetime import datetime, timedelta

import httpx
from anystore.types import SDict
from banal import ensure_dict
from furl import furl
from lxml.html import HtmlElement
from memorious.logic.context import Context

X_NEXT = ".//a[@class='page-link text-dark']/@data-seite"
X_ROWS = ".//div[@class='ps-vorgang']"
X_ROW_HEADER = "div/span/text()"
X_ROW_ID = "p[@class='ps-titel']/a/@href"
X_PDF_URL = "p[@class='ps-dokument']/a[@target='PDFs']/@href"
X_REFERENCE = "p[@class='ps-dokument']/a[@target='PDFs']/span/text()"
X_TITLE = "p[@class='ps-dokument']/span[1]/text()"

_X_CLS = ".//p[contains(concat(' ', normalize-space(@class), ' '), ' %s ')]"
X_METADATA = {
    "originator": _X_CLS % "ps-urheber" + "/span[2]/text()",
    "subject": _X_CLS % "ps-sachgebiet" + "/span[2]/text()",
    "keywords": _X_CLS % "ps-schlagwort" + "/span[2]/text()",
    "summary": _X_CLS % "ps-abstrakt" + "/span[2]/text()",
}

RE_REF = re.compile(r".*\s(\d{1,2}\/\d+).*")
RE_SACHSEN_PDF = re.compile(r"https://ws\.landtag\.sachsen\.de/images/[^'\"&\s]+\.pdf")


def extract_meta(el: HtmlElement) -> SDict:
    data = {}
    for key, xpath in X_METADATA.items():
        value = el.xpath(xpath)
        if value:
            if key == "keywords":
                data[key] = [v.strip() for v in value[0].split(",")]
            else:
                data[key] = value[0]
    return data


def extract_ref(value: str) -> str | None:
    m = RE_REF.match(value)
    if m:
        return m.groups()[0]


def extract_term(value: str) -> str:
    return value.split("/")[0]


RE_SACHSEN_POS_DOK = re.compile(r"anzeigeButton_\d+_(\d+)_\w+_\d+_\w+_btn")


def extract_sachsen_pdf_url(viewer_url: str) -> str | None:
    """
    Extract actual PDF URL from Sachsen EDAS viewer.
    The viewer.aspx is a frameset; viewer_navigation.aspx contains document
    buttons. Loading it with a pos_dok parameter reveals the actual PDF URL
    in the body onLoad handler.
    """
    f = furl(viewer_url)
    nav_url = f.copy()
    nav_url.path = "/viewer/viewer_navigation.aspx"
    try:
        # Step 1: Load navigation to get pos_dok from button IDs
        res = httpx.get(str(nav_url), timeout=30)
        buttons = RE_SACHSEN_POS_DOK.findall(res.text)
        if not buttons:
            return None
        # Step 2: Reload with pos_dok to get PDF URL from onLoad
        nav_url.args["pos_dok"] = buttons[0]
        nav_url.args["dok_id"] = ""
        res2 = httpx.get(str(nav_url), timeout=30)
        match = RE_SACHSEN_PDF.search(res2.text)
        if match:
            return match.group(0)
    except httpx.RequestError:
        pass
    return None


DE_DATE_FMT = "%d.%m.%Y"


def seed(context: Context, data: SDict) -> None:
    f = furl(context.params["url"])
    f.args["qyZeitBis"] = "heute"
    if not context.env.full_run:
        if context.env.start_date:
            start_date = context.env.start_date.strftime(DE_DATE_FMT)
        else:
            start_date = (
                datetime.now()
                - timedelta(**ensure_dict(context.params.get("timedelta")))
            ).strftime(DE_DATE_FMT)
        f.args["qyZeitAb"] = start_date
        if context.env.end_date:
            f.args["qyZeitBis"] = context.env.end_date.strftime(DE_DATE_FMT)

    data["url"] = f.url
    data["page"] = 0
    context.emit(data=data)


def parse(context: Context, data: SDict):
    res = context.http.rehash(data)
    for row in res.html.xpath(X_ROWS):
        header = row.xpath(X_ROW_HEADER)
        state, category, doc_type, date = header
        doc_id = row.xpath(X_ROW_ID)
        pdf_url = row.xpath(X_PDF_URL)
        reference = row.xpath(X_REFERENCE)
        title = row.xpath(X_TITLE)

        if all((doc_id, pdf_url, reference, title)):
            reference_id = extract_ref(reference[0])
            legislative_term = None
            if reference_id:
                legislative_term = extract_term(reference_id)
            detail_data = {
                **data,
                **extract_meta(row),
                "state": state,
                "category": category,
                "doc_type": doc_type,
                "date": datetime.strptime(date, "%d.%m.%Y").date().isoformat(),
                "foreign_id": doc_id[0].replace(".ps-detail-", ""),
                "url": pdf_url[0],
                "reference": reference[0],
                "reference_id": reference_id,
                "legislative_term": legislative_term,
                "title": title[0],
            }
            detail_data["keywords"] = [
                k.strip() for k in detail_data["subject"].split(";")
            ]

            if state == "Rheinland-Pfalz":
                # RP has redirects to it's parliament index page for some pdf
                # urls. We need to check first and only emit actual pdf urls.
                try:
                    head_res = httpx.head(detail_data["url"], follow_redirects=True)
                    content_type = head_res.headers.get("content-type", "")
                    if "application/pdf" not in content_type:
                        fid = detail_data.get("foreign_id")
                        context.log.warning(
                            f"Skipping non-PDF URL for `{fid}`: {content_type}",
                            url=detail_data["url"],
                        )
                        continue
                except httpx.RequestError as e:
                    context.log.error(f"Failed to check URL {detail_data['url']}: {e}")
                    continue
            elif state == "Sachsen":
                # Sachsen uses an ASPX viewer, extract actual PDF URL
                pdf_url = extract_sachsen_pdf_url(detail_data["url"])
                if pdf_url:
                    detail_data["url"] = pdf_url
                else:
                    context.log.warning(
                        f"Could not extract PDF URL for `{detail_data.get('foreign_id')}`",
                        url=detail_data["url"],
                    )
                    continue

            context.emit("download", data=detail_data)

    next_pages = set()
    for page in res.html.xpath(X_NEXT):
        next_pages.add(int(page))
    for page in sorted(next_pages):
        page = page - 1  # 0-indexed
        if page > data.get("page", 0):
            f = furl(data["url"])
            f.args["page"] = page
            context.emit("fetch", data={**data, "url": f.url, "page": page})
            break  # only emit next page
