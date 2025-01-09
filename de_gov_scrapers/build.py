#!/usr/bin/env python
"""
Build memorious scraper configs from departments.csv
"""


import csv
import io
import os

import requests
from jinja2 import Environment, FileSystemLoader

CSV_URL = "https://docs.google.com/spreadsheets/d/1AK1_GQ9fyZn-_Ni-SxiA4IvJ-DdiN8HjeeXmtX6uBuc/pub?output=csv"


def clean_domain(domain: str) -> str:
    return domain.replace("https://", "")


if __name__ == "__main__":
    res = requests.get(CSV_URL)
    res.raise_for_status()
    # FIXME wtf google?
    text = res.text.encode("latin1").decode("utf8")
    reader = csv.DictReader(io.StringIO(text))
    env = Environment(loader=FileSystemLoader("."))
    fid_seen = set()
    domain_seen = set()

    # build crawler memorious configs
    for crawler in reader:
        if int(crawler["active"]):
            if crawler["foreign_id"] in fid_seen:
                raise ValueError(f"Duplicate foreign_id: `{crawler['foreign_id']}`")
            fid_seen.add(crawler["foreign_id"])
            crawler["domain"] = clean_domain(crawler["domain"])
            if crawler["domain"] in domain_seen:
                raise ValueError(f"Duplicate domain: `{crawler['domain']}`")
            domain_seen.add(crawler["domain"])
            template = env.get_template(f'{crawler["template"]}.yml.j2')
            path = f'../datasets/de/{crawler["foreign_id"]}'
            if not os.path.exists(path):
                os.makedirs(path)
            with open(f'../datasets/de/{crawler["foreign_id"]}/config.yml', "w") as f:
                f.write(template.render(**crawler))
