# https://github.com/opensanctions/opensanctions/blob/main/datasets/de/abgeordnetenwatch/crawler.py

from typing import Any
import math

from banal import ensure_dict
from investigraph import Context, Resolver
from investigraph.logic import fetch
from investigraph.types import RecordGenerator


def fetch_json(
    url: str, params: dict[str, Any] | None = None, **kwargs
) -> dict[str, Any]:
    res = fetch.get(url, params=ensure_dict(params), url_key_only=True, **kwargs)
    data = res.json()
    return data.pop("data")


def fetch_json_api(
    url: str, params: dict[str, Any] | None = None, **kwargs
) -> dict[str, Any]:
    res = fetch.get(url, params=ensure_dict(params), url_key_only=True, **kwargs)
    return res.json()


def make_records(context, mandates: list[dict[str, Any]]) -> RecordGenerator:
    for mandate in mandates:
        parliament_period = mandate.pop("parliament_period")
        parliament_period_detail = fetch_json(parliament_period["api_url"])

        # Don't get Members of EU-Parlament
        if "EU" in parliament_period_detail["parliament"]["label"]:
            continue

        parliament_detail = fetch_json(
            parliament_period_detail["parliament"]["api_url"]
        )

        politician = mandate.pop("politician")
        politician_fullname = politician.pop("label")
        context.log.info("Get Politician {} detail".format(politician_fullname))
        politician_detail = fetch_json(politician.pop("api_url"))

        yield {
            "mandate": mandate,
            "parliament": parliament_detail,
            "period": parliament_period_detail,
            "politician": politician_detail,
        }


def handle(context: Context, res: Resolver) -> RecordGenerator:
    api_response = res.get_json()
    if api_response:
        yield from make_records(context, api_response.pop("data"))

        total_results = api_response.pop("meta").pop("result").pop("total")
        num_batches = math.ceil(total_results / 100)

        bi = 0
        while bi < num_batches:
            context.log.info(
                "Get Politicians from range {} to {}".format(bi * 100, (bi + 1) * 100)
            )

            if bi > 0:
                api_response = fetch_json_api(
                    context.source.uri, params={"range_start": bi * 100}
                )

                if api_response:
                    yield from make_records(context, api_response.pop("data"))

                    # We need to always recheck total number of results because the API
                    # returns a incorrect value in the first request without range_start
                    batch_total_results = (
                        api_response.pop("meta").pop("result").pop("total")
                    )
                    if batch_total_results != total_results:
                        num_batches = math.ceil(batch_total_results / 100)

            # Increment batch number
            bi += 1
