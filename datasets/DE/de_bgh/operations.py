from datetime import datetime, timedelta
from urllib.parse import urljoin

from anystore.types import SDict
from memorious.logic.context import Context
from normality import latinize_text, slugify

BASE_URL = "https://www.bundesgerichtshof.de"

X_ROWS = ".//tbody/tr"
X_NEXT = ".//a[contains(@class, 'forward')]/@href"
X_DEP = "./td[1]/text()"
X_DATE = "./td[2]/text()"
X_REF = "./td[3]/text()"
X_URL = "./td[4]//a/@href"

DATE = "%d.%m.%Y"


def stringify(e) -> str | None:
    for item in e:
        return latinize_text(item)


def dateformat(e) -> str | None:
    value = stringify(e)
    if value:
        return datetime.strptime(value, DATE).date().isoformat()


def seed(context: Context, data: SDict):
    base_url = context.params["url"]
    days = context.params.get("days", 30)

    if context.env.full_run:
        # Full run: start from 2000
        start_date = datetime(2000, 1, 1).date()
    else:
        start_date = (
            context.env.start_date or (datetime.now() - timedelta(days=days)).date()
        )

    end_date = datetime.now().date()

    url = (
        f"{base_url}?startDate={start_date.isoformat()}"
        f"&endDate={end_date.isoformat()}"
        f"&submit=Datum+einschr%C3%A4nken"
    )
    data["url"] = url
    context.emit(data=data)


def parse(context: Context, data: SDict):
    with context.http.rehash(data) as result:
        for row in result.html.xpath(X_ROWS):
            url = stringify(row.xpath(X_URL))
            if url is not None:
                _data = {**data}
                _data["department"] = stringify(row.xpath(X_DEP)).strip()
                _data["date"] = dateformat(row.xpath(X_DATE))
                _data["reference"] = stringify(row.xpath(X_REF))
                _data["url"] = urljoin(BASE_URL, url)
                _data["foreign_id"] = slugify(_data["reference"])
                context.emit("download", data=_data)
        next_url = stringify(result.html.xpath(X_NEXT))
        if next_url:
            data["url"] = urljoin(BASE_URL, next_url)
            context.emit("fetch", data=data)
