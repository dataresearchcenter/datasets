from urllib.parse import urljoin
from normality import latinize_text
from datetime import datetime


X_ROWS = ".//td[@class='ESpruchk']/.."
X_URL = ".//td[@class='EAz']//a[@type='application/pdf']/@href"
X_DEP = ".//td[@class='ESpruchk']/text()"
X_DATE = ".//td[@class='EDatum']/text()"
X_REF = ".//td[@class='EAz']//a[@class='doklink']/text()"

DATE = "%d.%m.%Y"


def stringify(e) -> str | None:
    for item in e:
        return latinize_text(item)


def dateformat(e) -> str | None:
    value = stringify(e)
    if value:
        return datetime.strptime(value, DATE).date().isoformat()


def parse(context, data):
    with context.http.rehash(data) as result:
        for row in result.html.xpath(X_ROWS):
            url = stringify(row.xpath(X_URL))
            if url is not None:
                data["department"] = stringify(row.xpath(X_DEP))
                data["date"] = dateformat(row.xpath(X_DATE))
                data["reference"] = stringify(row.xpath(X_REF))
                data["url"] = urljoin(data["url"], url)
                context.emit(data=data)
