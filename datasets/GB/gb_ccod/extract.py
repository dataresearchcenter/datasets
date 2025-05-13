from investigraph.model import Context
from typing import Any, Generator
import requests
import zipfile
import os
import csv

def download(lnk):
	response = requests.get(lnk)
	with open("/tmp/latest.zip", "wb") as f:
		f.write(response.content)

def unpack():
	with zipfile.ZipFile("/tmp/latest.zip", 'r') as arch:
		arch.extractall("/tmp/")

def handle(ctx: Context) -> Generator[dict[str, Any], None, None]:
	uri = ctx.source.uri
	headers = {"Authorization": f"{os.getenv('GB_OCOD_KEY')}", "Accept": "application/json"}
	res = requests.get(uri, headers=headers)
	data = res.json()
	resource_name = [f for f in data['result'].get('resources') if f['name']=="Full File"][0]['file_name']
	resource_link = requests.get(f"{uri}/{resource_name}", headers=headers).json().get('result', {}).get('download_url')
	download(resource_link)
	unpack()
	filename = f"/tmp/{os.path.splitext(resource_name)[0]}.csv"
	with open(filename, newline='') as csvfile:
		reader = csv.DictReader(csvfile)
		for row in reader:
			yield {key: (None if value == "" else value) for key, value in row.items()}

