from lxml import etree

from common.ocds.eu_ted import model, extractors
from typing import Any, BinaryIO, Generator, List, Optional, Union
from common.ocds.eu_ted.utils import clean
from investigraph.util import make_fingerprint
import uuid
from datetime import datetime

def extract_eform_address(addr_elem: etree._Element, EFORMS_NSMAP)-> Optional[model.Address]:
	if addr_elem is None:
		return None
	street =  addr_elem.findtext(
		".//cbc:StreetName", namespaces=EFORMS_NSMAP
	)
	city = addr_elem.findtext(
		".//cbc:CityName", namespaces=EFORMS_NSMAP
	)
	postal = addr_elem.findtext(
		".//cbc:PostalZone", namespaces=EFORMS_NSMAP
	)
	# check whether matches with ftm codes
	country = addr_elem.findtext(
		".//cac:Country/cbc:IdentificationCode", namespaces=EFORMS_NSMAP,
	)
	# ??
	nuts = addr_elem.findtext(
		".//cbc:CountrySubentityCode", namespaces=EFORMS_NSMAP
	)
	countryCode3 = None
	if country is not None:
		countryCode3 = extractors.get_alpha3_country_code(country)

	return model.Address(
		streetAddress=street,
		locality=city,
		region=nuts,
		postalCode=postal,
		countryCode=country,
		countryCode3=countryCode3,

		)

def extract_org_contact_point(
	contact_elem: etree._Element,
	EFORMS_NSMAP
	) -> Optional[model.ContactPoint]:
	phone = contact_elem.findtext(".//cac:Contact/cbc:Telephone", namespaces=EFORMS_NSMAP)
	fax = contact_elem.findtext(".//cac:Contact/cbc:Telefax", namespaces=EFORMS_NSMAP)
	email = contact_elem.findtext(".//cac:Contact/cbc:ElectronicMail", namespaces=EFORMS_NSMAP)
	name = contact_elem.findtext(".//cac:Contact/cbc:Name", namespaces=EFORMS_NSMAP)
	# name = contact_elem.findtext(".//cac:PartyName/cbc:Name", namespaces=EFORMS_NSMAP)
	# url = contact_elem.findtext(".//cbc:WebsiteURI", namespaces=EFORMS_NSMAP)

	return model.ContactPoint(
		name=name,
		telephone=phone,
		email=email,
		faxNumber=fax,
		url=None
		# url=url
	)

def extract_eform_description(
	root: etree._Element,
	EFORMS_NSMAP
) -> str:
	"""Extract description from element"""
	short_descr = root.findtext(".//cac:ProcurementProject/cbc:Description", namespaces=EFORMS_NSMAP)
	note = root.findtext(".//cac:ProcurementProject/cbc:Note", namespaces=EFORMS_NSMAP)
	if note is not None:
		ret = short_descr + " | note: " + note
	else:
		ret = short_descr
	return clean(ret)


def get_lot_result_details(
	lot_result_elem: etree._Element,
	root: etree._Element,
	notice_result: etree._Element,
	EFORMS_NSMAP,
	orgs: dict[str:model.Organization]
) -> dict: # settled_contract CON-0001
	d = {}
	#lot_res_id = lot_result_elem.findtext(".//cbc:ID", namespaces=EFORMS_NSMAP) # RES-0001 - do we really need this? kind of top level
	result_code = lot_result_elem.findtext(".//cbc:TenderResultCode", namespaces=EFORMS_NSMAP) # selec-w, clos-nw, open-nw
	
	lot_tender_id = lot_result_elem.findtext(".//efac:LotTender/cbc:ID", namespaces=EFORMS_NSMAP) # TEN-0001 repeatable
	settled_contract_id = lot_result_elem.findtext(".//efac:SettledContract/cbc:ID", namespaces=EFORMS_NSMAP) # CON-0001 repeatable
	tender_lot_id = lot_result_elem.findtext(".//efac:TenderLot/cbc:ID", namespaces=EFORMS_NSMAP) # LOT-0001
	d["result-status"] = result_code
	d["TEN"] = lot_tender_id
	d["CON"] = settled_contract_id
	d["LOT"] = tender_lot_id
	return d
	# lot_tender_elem = result_elem.find(".//efac:TenderLot/cbc:ID", namespaces=EFORMS_NSMAP)
	# tendering_party_elem = notice_result.find("", namespaces=EFORMS_NSMAP)
	# efac:LotResult (RES-0001) TotalAmount ResultCode -> LotTender TEN-0001 | settledContract CON-0001 | tenderLot LOT-0001
	# efac:LotTender (TEN-0001) -> legalMonetaryTotal | tenderingParty TPA-0001 | tenderLot LOT-0001 | tenderingReference "CON-Submission ID : 300132953"
	# efac:SettledContract (CON-0001) -> ContractReference 25.RTI.CP.060 | LotTender TEN-0001
	# efac:TenderingParty (TPA-0001) cbc:Name:SRA4AIR -> Tenderer ORG-0003 ORG-0004 ORG-0005

def get_settled_contract_details(
	notice_result: etree._Element,
	EFORMS_NSMAP,
	contract_id: str
) -> dict:
	d = {}
	contracts = notice_result.findall(".//efac:SettledContract", namespaces=EFORMS_NSMAP)
	for contract in contracts:
		curr_id = contract.findtext(".//cbc:ID",  namespaces=EFORMS_NSMAP)
		if curr_id is not None and curr_id == contract_id:
			contract_ref = contract.findtext(".//efac:ContractReference/cbc:ID",  namespaces=EFORMS_NSMAP)
			lot_tender = contract.findtext(".//efac:LotTender/cbc:ID",  namespaces=EFORMS_NSMAP) # TEN
			d[curr_id] = {
			"REF": contract_ref,
			"TEN": lot_tender
			}
	return d
'''
LotTenders

yes LotResult RES-0001 ResultCode > TEN-0001 , CON-0001, LOT-0001
yes LotTender TEN-0001 Total Currency TenderReference > TPA-0001 , LOT-0001
yes SettledContract CON-0001 ContractReference > TEN-0001
Tenderingparty TPA-0001 > ORG-0001, ORG-0002 GroupLeadIndicator

non-LotTenders

ProcurementProjectLot LOT-0001
'''
def get_tendering_party_details(
	notice_result:etree._Element,
	root:etree._Element,
	EFORMS_NSMAP,
	tender_party_id: str
) -> [str]:
	arr = []
	tpa_elems = root.findall(".//efac:NoticeResult/efac:TenderingParty", namespaces=EFORMS_NSMAP)

	for elem in tpa_elems:
		curr_id = elem.findtext(".//cbc:ID", namespaces=EFORMS_NSMAP)
		# print(f"curr supp id: {curr_id} : tender_party_id: {tender_party_id}")
		if curr_id is not None and curr_id == tender_party_id:
			tenderer_elems = elem.findall(".//efac:Tenderer/cbc:ID", namespaces=EFORMS_NSMAP)
			# print(f"item: {item.text}")
			for item in tenderer_elems:
				# print(f"item: {item.text}")
				arr.append(item.text)
	# print(f"suppliers: {arr}")
	return arr

def _make_value(value: str, currency: str) -> Optional[model.Value]:
    """Extract value with amount and currency"""
    # amount_str = extract_text(elem, path)
    # currency = extract_attribute(elem, path, "CURRENCY")

    if value is None or currency is None:
        return None

    try:
        amount = float(value.strip())
    except (ValueError, AttributeError):
        return None

    return model.Value(amount=amount, currency=currency)  # ISO 4217 string

def get_lot_tender_details(
	notice_result: etree._Element,
	root: etree._Element,
	EFORMS_NSMAP,
	lot_tender_id: str,
	opt_amount: str,
	opt_curr:str
) -> dict:
	d = {}
	lot_tenders = root.findall(".//efac:NoticeResult/efac:LotTender", namespaces=EFORMS_NSMAP)
	for tender in lot_tenders:
		curr_id = tender.findtext(".//cbc:ID",  namespaces=EFORMS_NSMAP)
		# print(f"curr LotTender id: {curr_id}, lotTenderID: {lot_tender_id}")
		if curr_id is not None:
			if curr_id == lot_tender_id:
				# for elem in tender.iter():
				# 	print(elem.tag, ":", elem.text)
				tender_party = tender.findtext(".//efac:TenderingParty/cbc:ID",  namespaces=EFORMS_NSMAP) # TPA-0001
				# print(f"002tender_party: {tender_party}")
				amount_elem = tender.find(".//cac:LegalMonetaryTotal/cbc:PayableAmount",  namespaces=EFORMS_NSMAP)
				tender_value = None
				tender_curr = None
				if amount_elem is not None:
					tender_value = amount_elem.text
					tender_curr = amount_elem.get("currencyID")
				else:
					tender_value = opt_amount
					tender_curr = opt_curr
				value = _make_value(tender_value, tender_curr)
				# cac:LegalMonetaryTotal> <cbc:PayableAmount> Currency
				# efac:TenderReference
				# efac:TenderLot> LOT-0001
				d["TPA"] = tender_party
				d["value"] = value
				break
	return d

# extract all the companies/organizations, their contacts and addresses without assigning them a role
#efac:Company/	
def extract_orgs(
	root: etree._Element,
	EFORMS_NSMAP
) -> dict[str:model.Organization]:
	org_elems = root.findall(".//efac:Organizations/efac:Organization/efac:Company", namespaces=EFORMS_NSMAP)
	if org_elems is None:
		org_elems = root.findall(".//efac:Organizations/efac:Organization/efac:Party", namespaces=EFORMS_NSMAP)
	ret = {}
	for org_elem in org_elems:
		org_id = org_elem.findtext( ".//cac:PartyIdentification/cbc:ID", namespaces=EFORMS_NSMAP)
		org_name = org_elem.findtext( ".//cac:PartyName/cbc:Name", namespaces=EFORMS_NSMAP)
		org_address = extract_eform_address(org_elem.find(".//cac:PostalAddress", namespaces=EFORMS_NSMAP), EFORMS_NSMAP)
		contact_point = None
		contact_elem =org_elem.find("./cac:Contact", namespaces=EFORMS_NSMAP)
		contact_name = None
		if contact_elem is not None:
			contact_name = contact_elem.findtext("./cbc:Name", namespaces=EFORMS_NSMAP)
		if contact_name is not None:
			contact_point = extract_org_contact_point(org_elem, EFORMS_NSMAP)
		else:
			touch_point_elem = org_elem.find( ".//efac:TouchPoint", namespaces=EFORMS_NSMAP)
			if touch_point_elem is not None:
				contact_point = extract_org_contact_point(touch_point_elem, EFORMS_NSMAP)
		org_email = org_elem.findtext( ".//cac:Contact/cbc:ElectronicMail", namespaces=EFORMS_NSMAP)
		org_url = org_elem.findtext(".//cbc:WebsiteURI", namespaces=EFORMS_NSMAP)
		org_telephone= org_elem.findtext(".//cac:Contact/cbc:Telephone", namespaces=EFORMS_NSMAP)
		org_telefax= org_elem.findtext(".//cac:Contact/cbc:Telefax", namespaces=EFORMS_NSMAP)
		org_identifier = model.Identifier(
			legalName=org_name,
			id = org_elem.findtext(".//cac:PartyLegalEntity/cbc:CompanyID", namespaces=EFORMS_NSMAP),
			scheme="National-ID",
			)
		# Use fingerprint from name, or generate from identifier if name can't be fingerprinted
		org_fingerprint = make_fingerprint(org_name)
		if not org_fingerprint:
			# Fallback: generate fingerprint from identifier
			identifier_str = (
				f"{org_identifier.scheme}:{org_identifier.id}"
				if org_identifier.id
				else str(org_identifier.legalName)
				)
			org_fingerprint = str(uuid.uuid5(uuid.NAMESPACE_URL, identifier_str))

		org_uuid = uuid.uuid5(uuid.NAMESPACE_URL, org_fingerprint)
		org = model.Organization(
			id=str(org_uuid),
			name=org_name,
			identifier=org_identifier,
			address= org_address,
			contactPoint=contact_point,
			details={"url":org_url, "email":org_email, "telephone":org_telephone, "telefax":org_telefax},
			# roles=["buyer"],
		)
		ret[org_id] = org
	return ret


def _extract_award_date_str(
	award_date_str: str)->Optional[datetime|None]:
	award_date = None
	if award_date_str:
		try:
			temp_value = award_date_str.replace("Z", "+00:00")
			award_date = datetime.fromisoformat(temp_value)
			# award_date = datetime.strptime(award_date_str, "%Y-%m-%d")
		except ValueError:
			try:
				award_date = datetime.strptime(
					award_date_str, "%Y%m%d"
				)
			except ValueError:
				pass  # Leave as None if parsing fails
	return award_date


'''
Signed Date = Issue Date
Award Date = Award Date
'''
def get_contract_details(
	root: etree._Element, 
	EFORMS_NSMAP, 
	contract_id: str,
	# opt_award_date: str
) -> dict:
	d = {}
	settled_contract_elems = root.findall(".//efac:NoticeResult/efac:SettledContract", namespaces=EFORMS_NSMAP)
	award_date = None
	issue_date = None
	if settled_contract_elems is not None:
		for elem in settled_contract_elems:
			curr_id = elem.findtext(".//cbc:ID", namespaces=EFORMS_NSMAP)
			if curr_id is not None and curr_id == contract_id:
				award_date = _extract_award_date_str(elem.findtext(".//cbc:AwardDate", namespaces=EFORMS_NSMAP))
				issue_date = _extract_award_date_str(elem.findtext(".//cbc:IssueDate", namespaces=EFORMS_NSMAP))
				d["REF"] = elem.findtext(".//efac:ContractReference/cbc::ID", namespaces=EFORMS_NSMAP)
				break

	# if award_date is None and opt_award_date is not None:
	# 	award_date = opt_award_date
	# if issue_date is None and opt_award_date is not None:
	# 	issue_date = opt_award_date

	d["SIGN"] = issue_date
	d["AWARD"] = award_date

	return d


def extract_decision_reason(
	notice_result: etree._Element,
	EFORMS_NSMAP,
	lot_res_id: str
)-> str|None:
	decision_reason = None
	lot_elems = notice_result.findall("./efac:LotResult", namespaces=EFORMS_NSMAP)
	curr_elem = None
	for elem in lot_elems:
		curr_id = elem.findtext("./cbc:ID", namespaces=EFORMS_NSMAP)
		if curr_id is not None and curr_id == lot_res_id:
			decision_elem = elem.find("./efac:DecisionReason/efbc:DecisionReasonCode", namespaces=EFORMS_NSMAP)
			if decision_elem is not None:
				if decision_elem.get("listName") is not None:
					decision_reason = " | ".join([decision_elem.text, decision_elem.get("listName")])
				else:
					decision_reason = decision_elem.text
			break
	return decision_reason

#HERE
def extract_contracting_parties(root: etree._Element,EFORMS_NSMAP, organizations:dict[str:model.Organization])-> dict:
	d = {}
	# find all buyer/contracting party elements
	buyer_elems: List = root.findall(".//cac:ContractingParty",
		namespaces=EFORMS_NSMAP)
	if buyer_elems is not None:
		for elem in buyer_elems:
			# find buyer id: eg ORG 0001
			curr_id = elem.findtext("./cac:Party/cac:PartyIdentification/cbc:ID", namespaces=EFORMS_NSMAP)
			# Type of contracting authority - buyer-legal-type and buyer-contracting-type
			buyer_legal_elems = elem.findall("./cac:ContractingPartyType/cbc:PartyTypeCode", namespaces=EFORMS_NSMAP)
			# Main activity - authority-activity
			buyer_activity = elem.findtext("./cac:ContractingActivity/cbc:ActivityTypeCode", namespaces=EFORMS_NSMAP)
			if curr_id is not None:
				# find matching organization object 
				org = organizations.get(curr_id)
				if org is not None:
					# add role as buyer
					org.roles = ["buyer"]
					details = []
					buyer_legal_type = None
					if buyer_legal_elems is not None:
						for element in buyer_legal_elems:
							attr = element.get("listName")
							if attr is not None and attr == "buyer-legal-type":
								buyer_legal_type = element.text
								details.append(model.Classification(scheme="TED_CA_TYPE", id=str(buyer_legal_type)))
					if buyer_activity is not None:
						details.append(
							model.Classification(scheme="COFOG", id=str(buyer_activity)))
					if details:
						org.details = {
						"classifications": [c.model_dump() for c in details]
						}
					d[curr_id] = org
	# # find all buyer ids : eg: ORG 0001, ORG 0002
	# ids: List = [e.text for e in root.findall(
	# 	".//cac:ContractingParty/cac:Party/cac:PartyIdentification/cbc:ID",
	# 	namespaces=EFORMS_NSMAP
	# 	)]
	# if ids is not None: #else -> what is the fall back mechanism?
	# 	for item in ids:
	# 		org = None
	# 		org = organizations.get(item)
	# 		if org is not None:
	# 			org.roles=["buyer"]
	# 			d[item] = org
	return d

# def extract_eufunded(lot_elems: etree._Element, EFORMS_NSMAP)->list[str] | None:
# 	if lot_elems is None:
# 		return None
# 	arr: List = [e.findtext(
# 		"./cac:TenderingTerms/cbc:FundingProgramCode",
# 		namespaces=EFORMS_NSMAP
# 		) for e in lot_elems]
# 	return arr

def extract_eufunded(lot_elem: etree._Element, EFORMS_NSMAP)->list[str] | None:
	if lot_elem is None:
		return None
	ret: str = lot_elem.findtext(
		"./cac:TenderingTerms/cbc:FundingProgramCode",
		namespaces=EFORMS_NSMAP
		)
	return ret