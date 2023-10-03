import copy
import logging
import time

from investigraph import Context
from investigraph.logic import requests

URL = "https://www.abgeordnetenwatch.de/api/v2"

log = logging.getLogger(__name__)


def make_request(*args, **kwargs):
    backoff = 5
    while True:
        try:
            res = requests.get(*args, **kwargs)
            return res.json()
        except Exception:
            log.warn(
                "API %s [%d], backoff %d sec." % (args[0], res.status_code, backoff)
            )
            time.sleep(backoff)
            backoff = backoff * 2  # gosh


def fetchByIds(path, ids={}, queryParams={}):
    # Fetch by IDs using chunks/batch to prevent too long URLs or bad gateway response.
    chunkSize = 100  # maximum number of IDs to request without failure
    data = []
    for idChunk in [
        list(ids)[i : i + chunkSize] for i in range(0, len(ids), chunkSize)
    ]:
        queryParams["id[in]"] = "[" + ",".join(str(id) for id in idChunk) + "]"
        data += fetchAll(path, queryParams)
    return data


def fetchAll(path, queryParams={}):
    ix = 0
    range_start = 0
    range_end = 100  # aka limit, default is 100, max. is 1.000
    dataAll = []

    # Fetch all entities with multiple requests (AW API has limit per request)
    while range_start >= 0:
        ix + 1
        queryParams["range_start"] = range_start
        queryParams["range_end"] = range_end
        data = make_request(URL + path, params=queryParams)
        meta = data["meta"]

        total = int(meta["result"]["total"])
        range_end = int(meta["result"]["range_end"])
        if total > (range_start + range_end):
            range_start = range_start + range_end
        else:
            range_start = -1

        dataAll += data["data"]

    return dataAll


def handle(ctx: Context, *args, **kwargs):
    ctx.log.info("Fetching sidejobs ...")
    sideJobsData = fetchAll("/sidejobs", {"range_end": 1000})
    ctx.log.info("%d sidejobs" % len(sideJobsData))
    # The API doesn't return related data.
    # To get the relevant entities and minimize the number of requests,
    # we collect their IDs, load them and merge the data before parsing.
    relatedMandateIds = set()
    relatedOrganizationIds = set()
    for sideJob in sideJobsData:
        for mandate in sideJob["mandates"]:
            relatedMandateIds.add(mandate["id"])
        if sideJob["sidejob_organization"]:
            relatedOrganizationIds.add(sideJob["sidejob_organization"]["id"])

    if len(relatedMandateIds) > 0:
        ctx.log.info("Fetching mandates ...")
        mandatesData = fetchByIds(
            "/candidacies-mandates", relatedMandateIds, {"current_on": "all"}
        )
        ctx.log.info("%d mandates" % len(mandatesData))
        relatedPoliticianIds = set()
        for mandate in mandatesData:
            relatedPoliticianIds.add(mandate["politician"]["id"])

        if len(relatedPoliticianIds) > 0:
            ctx.log.info("Fetching related politicians")
            politiciansData = fetchByIds("/politicians", relatedPoliticianIds)
            ctx.log.info("%d related politicians" % len(politiciansData))
            for mandatesRecord in mandatesData:
                for politiciansRecord in politiciansData:
                    if mandatesRecord["politician"]["id"] == politiciansRecord["id"]:
                        mandatesRecord["politician"] = copy.deepcopy(politiciansRecord)
                        break

    if len(relatedOrganizationIds) > 0:
        ctx.log.info("Fetching organizations ...")
        organizationsData = fetchByIds("/sidejob-organizations", relatedOrganizationIds)
        ctx.log.info("%d organizations" % len(organizationsData))

    for sideJobRecord in sideJobsData:
        if sideJobRecord["sidejob_organization"] and organizationsData:
            # Attach organization to sidejob record.
            for organizationRecord in organizationsData:
                if (
                    sideJobRecord["sidejob_organization"]["id"]
                    == organizationRecord["id"]  # noqa
                ):
                    sideJobRecord["sidejob_organization"] = copy.deepcopy(
                        organizationRecord
                    )
                    break
        if mandatesData:
            # Attach mandates including its politician to sidejob record.
            for im, mandate in enumerate(sideJobRecord["mandates"]):
                for mandateRecord in mandatesData:
                    if mandate["id"] == mandateRecord["id"]:
                        sideJobRecord["mandates"][im] = copy.deepcopy(mandateRecord)
                        break

        yield sideJobRecord
