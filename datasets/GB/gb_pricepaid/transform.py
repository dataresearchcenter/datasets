from investigraph.model import Context
from investigraph.types import Record

def getType(code):
	# D = Detached, S = Semi-Detached, T = Terraced, F = Flats/Maisonettes, O = Other
	if code == 'D':
		return "Detached"
	if code == 'S':
		return "Semi-Detached"
	if code == 'T':
		return "Terraced"
	if code == 'F':
		return "Flats/Maisonettes"
	else:
		return "Other"

def getNew(code):
	if code == 'Y':
		return "newly built"
	else:
		return "not newly built"

def handle(ctx: Context, record: Record, ix: int):
	proxy = ctx.make("RealEstate")
	address = f"{record['houseNumber']} {record['street']}, {record['locality']}, {record['city']} {record['postalCode']}, {record['district']}, {record['county']}, Great Britain"
	proxy.id = ctx.make_slug(address)
	proxy.add('address', address)
	if record['unit'] and record['unit'] != "":
		proxy.add('description', record['unit'])
	proxy.add('propertyType', f"{getType(record['propertyType'])}, {getNew(record['newlyBuilt'])}")
	proxy.add("amount", record['amount'])
	proxy.add("createDate", record['createDate'])
	proxy.add("country", "gb")
	proxy.add("currency", "GBP")
	proxy.add("tenure", "Leasehold" if record['tenure'] == 'L' else "Freehold")
	proxy.add("sourceUrl", f"https://landregistry.data.gov.uk/data/ppi/transaction/{record['id'][1:-1]}/current")
	yield proxy
