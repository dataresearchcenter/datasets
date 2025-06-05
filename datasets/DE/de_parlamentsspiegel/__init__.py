import re
from datetime import datetime, timedelta

from banal import ensure_dict
from furl import furl
from lxml.html import HtmlElement
from memorious.logic.context import Context
from servicelayer import env

from utils import Data
from utils.operations import cached_emit

X_NEXT = ".//a[@class='page-link text-dark']/@data-seite"
X_ROWS = ".//div[@class='ps-vorgang']"
X_ROW_HEADER = "div/span/text()"
X_ROW_ID = "p[@class='ps-titel']/a/@href"
X_PDF_URL = "p[@class='ps-dokument']/a[@target='PDFs']/@href"
X_REFERENCE = "p[@class='ps-dokument']/a[@target='PDFs']/span/text()"
X_TITLE = "p[@class='ps-dokument']/span[1]/text()"

X_METADATA = {
    "originator": ".//p[contains(concat(' ', normalize-space(@class), ' '), ' ps-urheber ')]/span[2]/text()",
    "subject": ".//p[contains(concat(' ', normalize-space(@class), ' '), ' ps-sachgebiet ')]/span[2]/text()",
    "keywords": ".//p[contains(concat(' ', normalize-space(@class), ' '), ' ps-schlagwort ')]/span[2]/text()",
    "summary": ".//p[contains(concat(' ', normalize-space(@class), ' '), ' ps-abstrakt ')]/span[2]/text()",
}

RE_REF = re.compile(r".*\s(\d{1,2}\/\d+).*")


def extract_meta(el: HtmlElement) -> Data:
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


def seed(context: Context, data: Data) -> None:
    f = furl(context.params["url"])
    f.args["qyZeitBis"] = "heute"
    if not env.to_bool("FULL_RUN"):
        start_date = (
            env.get("START_DATE")
            or (
                datetime.now()
                - timedelta(**ensure_dict(context.params.get("timedelta")))
            )
            .date()
            .isoformat()
        )
        f.args["qyZeitAb"] = start_date
        if env.get("END_DATE"):
            f.args["qyZeitBis"] = env.get("END_DATE")

    data["url"] = f.url
    data["page"] = 0
    context.emit(data=data)


def parse(context: Context, data: Data):
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
                "doc_id": doc_id[0].replace(".ps-detail-", ""),
                "url": pdf_url[0],
                "reference": reference[0],
                "reference_id": reference_id,
                "legislative_term": legislative_term,
                "title": title[0],
            }

            # FIXME
            if state not in ("Sachsen", "Rheinland-Pfalz"):
                cached_emit(context, detail_data, "download")

    next_pages = set()
    for page in res.html.xpath(X_NEXT):
        next_pages.add(int(page))
    for page in sorted(next_pages):
        page = page - 1  # 0-indexed
        if page > data["page"]:
            f = furl(data["url"])
            f.args["page"] = page
            context.emit("fetch", data={**data, "url": f.url, "page": page})
            break  # only emit next page
