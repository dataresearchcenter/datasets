from datetime import datetime
from urllib.parse import urljoin

from dateutil.relativedelta import relativedelta
from memorious.logic.context import Context
from normality import latinize_text

from utils import Data
from utils.operations import cached_emit

X_ROWS = ".//td[@class='ESpruchk']/.."
X_NEXT = ".//img[@src='/rechtsprechung/bgh/pics/weiter.gif']/../@href"
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


def parse(context: Context, data: Data):
    with context.http.rehash(data) as result:
        for row in result.html.xpath(X_ROWS):
            url = stringify(row.xpath(X_URL))
            if url is not None:
                _data = {**data}
                _data["department"] = stringify(row.xpath(X_DEP))
                _data["date"] = dateformat(row.xpath(X_DATE))
                _data["reference"] = stringify(row.xpath(X_REF))
                _data["url"] = urljoin(data["url"], url)
                cached_emit(context, _data, "download")
        next_url = stringify(result.html.xpath(X_NEXT))
        if next_url:
            data["url"] = urljoin(data["url"], next_url)
            context.emit(rule="fetch", data=data)


def dates(context: Context, data: Data):
    """memorious built-in dates operation is somehow broken"""
    dateformat = context.params.get("format", "%Y-%m-%d")
    end = datetime.strptime(context.params["end"], dateformat)
    current = datetime.now()
    while current > end:
        context.emit(data={**data, "date": current.strftime(dateformat)})
        current = current - relativedelta(months=1)
