# https://github.com/okfde/sehrgutachten/blob/master/app/scrapers/wd_ausarbeitungen_scraper.rb

import re
from datetime import datetime, timedelta
from urllib.parse import urljoin

from anystore.types import SDict
from banal import ensure_dict
from furl import furl
from memorious.helpers.xpath import extract_xpath as x
from memorious.logic.context import Context

MONTHS = (
    "januar",
    "februar",
    "märz",
    "april",
    "mai",
    "juni",
    "juli",
    "august",
    "september",
    "oktober",
    "november",
    "dezember",
)
WD_NAMES = {
    "wd1": "Geschichte, Zeitgeschichte und Politik",
    "wd2": "Auswärtiges, Völkerrecht, Wirtschaftliche Zusammenarbeit und Entwicklung, Verteidigung, Menschenrechte und humanitäre Hilfe",
    "wd3": "Verfassung und Verwaltung",
    "wd4": "Haushalt und Finanzen",
    "wd5": "Wirtschaft und Technologie, Ernährung, Landwirtschaft und Verbraucherschutz, Tourismus",
    "wd6": "Arbeit und Soziales",
    "wd7": "Zivil-, Straf- und Verfahrensrecht, Umweltschutzrecht, Verkehr, Bau und Stadtentwicklung",
    "wd8": "Umwelt, Naturschutz, Reaktorsicherheit, Bildung und Forschung",
    "wd9": "Gesundheit, Familie, Senioren, Frauen und Jugend",
    "wd10": "Kultur, Medien und Sport",
    "wd11": "Europa",
    "pe6": "Europa",
    "eu6": "Fachbereich Europa",
}


# a row can hold additional links besides the document itself (e.g. an audio
# version), so restrict to the main link
DOC_LINK = (
    './/a[contains(@class, "e-linkListItem__anchor")]'
    '[not(ancestor::ul[contains(@class, "e-linkListItem__additionalLinks")])]'
)


REFERENCE = re.compile(
    r"\b(?P<wd>wd|pe|eu)-(?P<wd_id>\d{1,2})-"  # unit, e.g. "WD 3", "EU-6"
    r"(?:3000-)?"  # the "3000" series marker is not part of the id
    r"(?P<doc_id>(?:30000-)?\d+(?:-\d+)*[-/]\d+)",
    re.IGNORECASE,
)


def _find_reference(*values: str | None) -> re.Match | None:
    """Find a document reference such as `WD 6 - 044/26` in the first value
    that has one.

    Spellings vary (`WD 5-034-26`, `WD 5 – 048/26`, `WD 8 - 3000 - 034/26`), so
    separators are unified before matching. The file name is looked at before
    the title, because titles also cite *other* documents ("Aktualisierung des
    Sachstands WD 6 – 097/18 vom 27. Juni 2024, WD 6 - 074/25").
    """
    for value in values:
        if not value:
            continue
        match = REFERENCE.search(re.sub(r"\s*[-–—]\s*|\s+", "-", value))
        if match:
            return match


def _clean_date(value: str | None) -> str | None:
    if not value:
        return
    value = value.lower().replace(" ", "")
    for i, month in enumerate(MONTHS):
        if month in value:
            value = value.replace(month, "%s." % str(i + 1).zfill(2))
            return datetime.strptime(value, "%d.%m.%Y").date().isoformat()


def seed(context: Context, data: SDict):
    f = furl(context.params["url"])
    if not context.env.full_run:
        start_date = (
            context.env.start_date
            or (
                datetime.now()
                - timedelta(**ensure_dict(context.params.get("timedelta")))
            ).date()
        )
        start_date = start_date.strftime("%s000")
        end_date = datetime.now().date().strftime("%s000")
        f.args["startdate"] = start_date
        f.args["enddate"] = end_date
        f.args["startfield"] = "date"
        f.args["endfield"] = "date"
    f.args["limit"] = 10
    data["url"] = f.url
    context.emit(data=data)


def parse(context: Context, data: SDict):
    res = context.http.rehash(data)

    # the response contains the same results twice: once as table rows, once as
    # list items – only look at the table rows
    rows = res.html.xpath('//tr[contains(@class, "m-documents__tableRow")]')

    found = 0
    for row in rows:
        path = x(row, f"{DOC_LINK}/@href")
        if not path:  # e.g. the "no results" row
            continue

        url = urljoin(data["url"], path)
        found += 1

        try:
            title = x(row, f"normalize-space({DOC_LINK})")
            detail_data = {
                "url": url,
                "title": title,
                "file_name": url.split("/")[-1],
                "published_at": _clean_date(x(row, "normalize-space(td[1])")),
                "publisher": context.crawler.config.publisher.model_dump(mode="json"),
                "reference": "",
            }

            wd_match = _find_reference(detail_data["file_name"], title)
            if wd_match:
                wd_id = wd_match.group("wd").lower() + wd_match.group("wd_id")
                wd_id_nice = f"{wd_match.group('wd')} {wd_match.group('wd_id')}"
                wd_name = WD_NAMES.get(wd_id, wd_id_nice)
                # the year is separated by a slash: `WD-5-034-26` -> `034/26`
                doc_id = re.sub(r"-(\d{2})$", r"/\1", wd_match.group("doc_id"))
                detail_data["publisher"].update(
                    {
                        "id": wd_id,
                        "name": f"{wd_id_nice} - {wd_name}",
                        "url": f"https://www.bundestag.de/dokumente/analysen/{wd_id}",
                    }
                )
                detail_data["reference"] = doc_id
                detail_data["foreign_id"] = "-".join((wd_id, doc_id))

            context.emit("download", data={**data, **detail_data})

        except Exception as e:
            context.log.error(f"Error at `{url}`: {e}")

    # pagination
    if found:
        f = furl(data["url"])
        f.args["offset"] = int(f.args["limit"]) + int(f.args.get("offset", 0))
        context.emit("fetch", data={"url": f.url})
