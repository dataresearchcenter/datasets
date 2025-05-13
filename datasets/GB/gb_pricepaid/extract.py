import csv
from io import StringIO
from typing import Any, Generator
import requests

from investigraph.model import Context, Resolver

HEADERS = ('id', 'amount', 'createDate', 'postalCode', 'propertyType', 'newlyBuilt', 'tenure', 'houseNumber', 'unit', 'street', 'locality', 'city', 'district', 'county', 'recordType', 'recordStatus')

def handle(ctx, res) -> Generator[dict[str, Any], None, None]:
	uri = ctx.source.uri
	with requests.get(uri, stream=True) as response:
		lines = response.iter_lines(decode_unicode=True)
		reader = csv.reader(lines, delimiter=",")
		for line in reader:
			yield dict(zip(HEADERS, line))
