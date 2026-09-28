# SPDX-FileCopyrightText: 2022 Free Software Foundation Europe <contact@fsfe.org>
#
# SPDX-License-Identifier: GPL-3.0-or-later

import uuid
from datetime import datetime
from typing import Any, BinaryIO, Generator, List, Optional, Union

from investigraph.util import make_fingerprint
from lxml import etree
from pydantic import BaseModel

from common.ocds.eu_ted import extractors, model
from common.ocds.eu_ted import eform_extractors as eform
import traceback

# Implemented form types
IMPLEMENTED_OLD_FORMS = ["F01", "F02", "F03"]
IMPLEMENTED_EFORMS = [
    "planning",
    "competition",
    "change",
    "result",
    "dir-awa-pre",
    "cont-modif",
    # "can-standard",
    # "cn-standard",
]

# eForms namespaces
EFORMS_NSMAP = {
    "cac": "urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2",
    "cbc": "urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2",
    "efac": "http://data.europa.eu/p27/eforms-ubl-extension-aggregate-components/1",
    "efbc": "http://data.europa.eu/p27/eforms-ubl-extension-basic-components/1",
    "efext": "http://data.europa.eu/p27/eforms-ubl-extensions/1",
    "ext": "urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2",
    "can": "urn:oasis:names:specification:ubl:schema:xsd:ContractAwardNotice-2",
}

def extract_all_cpvs(
    tender: model.Tender,
    doc_sec_coded: etree._Element,
    doc_sec_forms: etree._Element
) -> model.Tender:

    cpv: list = doc_sec_coded.findall(
        ".//ORIGINAL_CPV", namespaces=doc_sec_coded.nsmap
        )
    if cpv is not None:
        cpvCode = []
        cpvName = []
        for item in cpv:
            if item is not None:
                cpvCode.append(item.get("CODE"))
                cpvName.append(item.text)
        cpvCode = list(dict.fromkeys(cpvCode))
        cpvName = list(dict.fromkeys(cpvName))
        tender.cpvCode = cpvCode
        tender.cpvName = cpvName

    # Check CPV codes in both sections match
    other_cpv: list = doc_sec_forms.findall(
        ".//CPV_CODE", namespaces=doc_sec_forms.nsmap
        )
    other_cpv = list(dict.fromkeys(other_cpv))
    if other_cpv is not None:
        tempCPV = []
        for item in other_cpv:
            if item is not None:
                tempCPV.append(item.get("CODE"))
        if set(tender.cpvCode) != set(tempCPV):
            print("\033[38;5;208mError: CPV code lists are not same.\033[0m")

    return tender


def ted_notice_to_ocds_releases(
    doc_id: str,
    form_type: str,
    ocid_prefix: str,
    object_contract: List[etree._Element],
    doc_sec_coded: etree._Element,
    doc_sec_forms: etree._Element,
    doc_sec_trans: etree._Element,
    translate_from: Optional[str] = None,
) -> Generator[BaseModel, None, None]:
    """
    Yields OCDS releases from the XML source of TED notices.

    :param doc_id: Document ID
    :param form_type: Form type (F01, F02, F03)
    :param ocid_prefix: OCID prefix
    :param object_contract: List of object contract elements
    :param doc_sec_coded: Coded section element
    :param doc_sec_forms: Forms section element
    :param doc_sec_trans: Translation section element
    :param translate_from: Language to translate from (if applicable)
    :yield: OCDS objects (releases, tenders, organizations, etc.)
    """
    # Only process implemented form types
    if form_type not in ["F01", "F02", "F03"]:
        return

    # Extract publication date once
    pub_date = datetime.strptime(
        doc_sec_coded.find(".//DATE_PUB", namespaces=doc_sec_coded.nsmap).text,
        "%Y%m%d",
    )

    # Generate an OCDS release for each '/OBJECT_CONTRACT' element
    for elem_count, elem in enumerate(object_contract, start=1):
        # Create release and tender
        release = model.Release(
            ocid="",  # Will be set later
            id="",  # Will be set later
            date=pub_date,
            tag=[],  # Will be set based on form type
            initiationType=model.InitiationType.tender,
        )

        tender = model.Tender(id="")  # Will be set to OCID later
        release.tender = tender

        # Initialize parties list for all organizations
        parties = []

        # Set release tags and tender status based on form type
        if form_type == "F01":
            notice_type = doc_sec_forms.find(
                ".//NOTICE", namespaces=doc_sec_forms.nsmap
            ).get("TYPE")
            if notice_type in ("PRI_ONLY", "PRI_REDUCING_TIME_LIMITS"):
                release.tag = [model.Tag.planning]
                tender.status = model.Status.planned
            elif notice_type == "PRI_CALL_COMPETITION":
                release.tag = [model.Tag.planning, model.Tag.tender]
                tender.status = model.Status.active
        elif form_type == "F02":
            release.tag = [model.Tag.tender]
            tender.status = model.Status.active
        elif form_type == "F03":
            release.tag = [model.Tag.award, model.Tag.contract]
            tender.status = model.Status.complete

        # Extract and create primary buyer organization
        buyer = _extract_buyer(doc_sec_forms)
        parties.append(buyer)
        release.buyer = buyer

        # Extract additional buyer if present
        if (
            doc_sec_forms.find(
                ".//CONTRACTING_BODY/ADDRESS_CONTRACTING_BODY_ADDITIONAL",
                namespaces=doc_sec_forms.nsmap,
            )
            is not None
        ):
            add_buyer = _extract_additional_buyer(doc_sec_forms, buyer.id)
            if add_buyer:
                parties.append(add_buyer)
                # OCDS supports only one buyer; additional go to parties only

        # Extract joint procurement information
        cb_pl = doc_sec_forms.find(
            ".//CONTRACTING_BODY/PROCUREMENT_LAW", namespaces=doc_sec_forms.nsmap
        )
        if cb_pl is not None and len(cb_pl):
            tender.procurementMethodDetails = cb_pl[0].text

        # Extract communication details
        cb_url_p = doc_sec_forms.find(
            ".//CONTRACTING_BODY/URL_PARTICIPATION", namespaces=doc_sec_forms.nsmap
        )
        if cb_url_p is not None:
            tender.submissionMethodDetails = cb_url_p.text

        # Add buyer classifications as details
        classifications = _extract_buyer_classifications(doc_sec_forms)
        if classifications:
            buyer.details = {
                "classifications": [c.model_dump() for c in classifications]
            }

        # Extract tender details
        tender.title = extractors.extract_title(elem, doc_sec_trans)
        tender.description = extractors.extract_description(elem)
        tender.value = extractors.extract_value(doc_sec_forms, "tender")

        # Extract all CPV
        tender = extract_all_cpvs(tender, doc_sec_coded, doc_sec_forms)

        # Process awards for F03 forms
        if form_type == "F03":
            release.awards = []
            release.contracts = []

            award_elems = doc_sec_forms.findall(
                ".//AWARD_CONTRACT", namespaces=doc_sec_forms.nsmap
            )
            for award_elem in award_elems:
                award_id = str(uuid.uuid4())
                award_title = extractors.extract_title(award_elem, doc_sec_trans)
                award = model.Award(id=award_id, title=award_title)

                if extractors.extract_text(award_elem, ".//AWARDED_CONTRACT"):
                    award.status = model.AwardStatus.active

                    # Parse date signed
                    date_signed_str = extractors.extract_text(
                        award_elem, ".//DATE_CONCLUSION_CONTRACT"
                    )
                    date_signed = None
                    if date_signed_str:
                        try:
                            date_signed = datetime.strptime(date_signed_str, "%Y-%m-%d")
                        except ValueError:
                            try:
                                date_signed = datetime.strptime(
                                    date_signed_str, "%Y%m%d"
                                )
                            except ValueError:
                                pass  # Leave as None if parsing fails

                    # Create contract
                    contract = model.Contract(
                        id=str(uuid.uuid4()),
                        awardID=award_id,
                        title=award_title,
                        description=None,
                        status=model.ContractStatus.active,
                        period=None,
                        value=extractors.extract_value(award_elem, "award"),
                        items=None,
                        dateSigned=date_signed,
                        documents=None,
                    )
                    release.contracts.append(contract)
                else:
                    award.status = model.AwardStatus.unsuccessful

                # Extract suppliers
                award.suppliers = []
                supplier_elems = award_elem.findall(
                    ".//CONTRACTOR", namespaces=award_elem.nsmap
                )
                for supplier_elem in supplier_elems:
                    supplier = _extract_supplier(supplier_elem)
                    parties.append(supplier)
                    award.suppliers.append(supplier)

                release.awards.append(award)

        # Generate OCID
        release.ocid = _generate_ocid(
            form_type=form_type,
            ocid_prefix=ocid_prefix,
            doc_id=doc_id,
            doc_sec_forms=doc_sec_forms,
            object_contract_count=len(object_contract),
            elem_count=elem_count,
        )

        # Set tender.id and release.id to OCID
        tender.id = release.ocid
        release.id = release.ocid

        # Set parties list
        release.parties = parties

        # Yield the complete release
        yield release


def _extract_buyer(doc_sec_forms: etree._Element) -> model.Organization:
    """Extract primary buyer organization from forms section."""
    buyer_name = extractors.extract_text(
        doc_sec_forms,
        ".//CONTRACTING_BODY/ADDRESS_CONTRACTING_BODY/OFFICIALNAME",
    )

    buyer_identifier = model.Identifier(
        legalName=buyer_name,
        id=extractors.extract_text(
            doc_sec_forms,
            ".//CONTRACTING_BODY/ADDRESS_CONTRACTING_BODY/NATIONALID",
        ),
        scheme="National-ID",
    )

    # Use fingerprint from name, or generate from identifier if name can't be fingerprinted
    buyer_fingerprint = make_fingerprint(buyer_name)
    if not buyer_fingerprint:
        # Fallback: generate fingerprint from identifier
        identifier_str = (
            f"{buyer_identifier.scheme}:{buyer_identifier.id}"
            if buyer_identifier.id
            else str(buyer_identifier.legalName)
        )
        buyer_fingerprint = str(uuid.uuid5(uuid.NAMESPACE_URL, identifier_str))

    buyer_uuid = uuid.uuid5(uuid.NAMESPACE_URL, buyer_fingerprint)

    buyer_details = extractors.extract_buyer_details(doc_sec_forms)

    return model.Organization(
        id=str(buyer_uuid),
        name=buyer_name,
        identifier=buyer_identifier,
        address=extractors.extract_buyer_address(doc_sec_forms),
        contactPoint=extractors.extract_buyer_contact_point(doc_sec_forms),
        details=dict(buyer_details) if buyer_details else None,
        roles=["buyer"],
    )


def _extract_additional_buyer(
    doc_sec_forms: etree._Element, primary_buyer_id: str
) -> Optional[model.Organization]:
    """Extract additional buyer organization if different from primary buyer."""
    add_buyer_name = extractors.extract_text(
        doc_sec_forms,
        ".//CONTRACTING_BODY/ADDRESS_CONTRACTING_BODY_ADDITIONAL/OFFICIALNAME",
    )

    add_buyer_identifier = model.Identifier(
        legalName=add_buyer_name,
        id=extractors.extract_text(
            doc_sec_forms,
            ".//CONTRACTING_BODY/ADDRESS_CONTRACTING_BODY_ADDITIONAL/NATIONALID",
        ),
        scheme="National-ID",
    )

    # Use fingerprint from name, or generate from identifier if name can't be fingerprinted
    add_buyer_fingerprint = make_fingerprint(add_buyer_name)
    if not add_buyer_fingerprint:
        # Fallback: generate fingerprint from identifier
        identifier_str = (
            f"{add_buyer_identifier.scheme}:{add_buyer_identifier.id}"
            if add_buyer_identifier.id
            else str(add_buyer_identifier.legalName)
        )
        add_buyer_fingerprint = str(uuid.uuid5(uuid.NAMESPACE_URL, identifier_str))

    add_buyer_uuid = uuid.uuid5(uuid.NAMESPACE_URL, add_buyer_fingerprint)

    # Skip if same as primary buyer
    if str(add_buyer_uuid) == primary_buyer_id:
        return None

    add_buyer_details = extractors.extract_buyer_details(
        doc_sec_forms, "ADDRESS_CONTRACTING_BODY_ADDITIONAL"
    )

    return model.Organization(
        id=str(add_buyer_uuid),
        name=add_buyer_name,
        identifier=add_buyer_identifier,
        address=extractors.extract_buyer_address(
            doc_sec_forms, "ADDRESS_CONTRACTING_BODY_ADDITIONAL"
        ),
        contactPoint=extractors.extract_buyer_contact_point(
            doc_sec_forms, "ADDRESS_CONTRACTING_BODY_ADDITIONAL"
        ),
        details=dict(add_buyer_details) if add_buyer_details else None,
        roles=["buyer"],
    )


def _extract_buyer_classifications(
    doc_sec_forms: etree._Element,
) -> List[model.Classification]:
    """Extract buyer classifications (type and main activity)."""
    classifications = []

    # Type of contracting authority
    ca_type = extractors.extract_attribute(
        doc_sec_forms, ".//CONTRACTING_BODY/CA_TYPE", "VALUE"
    )
    if ca_type:
        classifications.append(
            model.Classification(scheme="TED_CA_TYPE", id=str(ca_type))
        )

    # Main activity
    ca_activity = extractors.extract_attribute(
        doc_sec_forms, ".//CONTRACTING_BODY/CA_ACTIVITY", "VALUE"
    )
    if ca_activity:
        classifications.append(model.Classification(scheme="COFOG", id=ca_activity))

    return classifications


def _extract_supplier(supplier_elem: etree._Element) -> model.Organization:
    """Extract supplier organization from supplier element."""
    supplier_name = extractors.extract_text(supplier_elem, ".//OFFICIALNAME")

    supplier_identifier = model.Identifier(
        id=extractors.extract_text(supplier_elem, ".//NATIONALID"),
        scheme="National-ID",
        legalName=supplier_name,
    )

    # Use fingerprint from name, or generate from identifier if name can't be fingerprinted
    supplier_fingerprint = make_fingerprint(supplier_name)
    if not supplier_fingerprint:
        # Fallback: generate fingerprint from identifier
        identifier_str = (
            f"{supplier_identifier.scheme}:{supplier_identifier.id}"
            if supplier_identifier.id
            else str(supplier_identifier.legalName)
        )
        supplier_fingerprint = str(uuid.uuid5(uuid.NAMESPACE_URL, identifier_str))

    supplier_id = uuid.uuid5(uuid.NAMESPACE_URL, supplier_fingerprint)

    return model.Organization(
        id=str(supplier_id),
        name=supplier_name,
        identifier=supplier_identifier,
        roles=["supplier"],
        address=extractors.extract_supplier_address(supplier_elem),
    )


def _generate_ocid(
    form_type: str,
    ocid_prefix: str,
    doc_id: str,
    doc_sec_forms: etree._Element,
    object_contract_count: int,
    elem_count: int,
) -> str:
    """Generate OCID based on form type and document information."""
    # Determine the base ID to use
    if form_type in ("F02", "F03"):
        related_doc_id = extractors.extract_text(
            doc_sec_forms, ".//PROCEDURE/NOTICE_NUMBER_OJ"
        )
        base_id = related_doc_id if related_doc_id else doc_id
    else:
        base_id = doc_id

    # Add element count suffix for multi-contract documents
    if object_contract_count > 1:
        ocid_suffix = f"{base_id}-{elem_count}"
    else:
        ocid_suffix = base_id

    return ocid_prefix + str(uuid.uuid5(uuid.NAMESPACE_URL, ocid_suffix))

def _get_eform_metadata(
    root: etree._Element
    ):
    # get publication date
    publication = (
        root.findtext(".//efac:Publication/efbc:PublicationDate", namespaces=EFORMS_NSMAP)
        or root.findtext(".//cbc:IssueDate", namespaces=EFORMS_NSMAP)
        or root.findtext(".//efac:SettledContract/cbc:IssueDate", namespaces=EFORMS_NSMAP)
        or root.findtext(".//cac:ContractAwardNotice/cbc:IssueDate", namespaces=EFORMS_NSMAP)
    )

    # no publication date in the eForm
    # if not publication or not publication[0].text:
    #     return None
    # if publication is None:
    #     return None
    # print(f"pub: {publication.tag} {publication.text}")
    # date_str = publication[0].text
    # print(f"date: {publication} ")
    date_str = publication
    if date_str is None:
        return None
    # print(f"date: {date_str} ")
    # Remove timezone part if present
    if date_str.endswith("Z"):
        date_str = date_str[:-1]
    elif "+" in date_str:
        date_str = date_str.split("+")[0]
    elif "-" in date_str and date_str.count("-") > 2:
        date_str = date_str.rsplit("-", 1)[0]
    
    pub_date = datetime.strptime(date_str, "%Y-%m-%d")

    form_type_elem = root.find(
        ".//cbc:NoticeTypeCode",
        namespaces=EFORMS_NSMAP
        )

    form_type_name = form_type_elem.get("listName")
    form_type_text = form_type_elem.text

    return {
    "date": pub_date,
    "type": form_type_name,
    "subtype": form_type_text,
    }

def parse_eform_notice(root: etree._Element) -> Generator[BaseModel, None, None]:
    """
    Parse an eForms notice (new format) and return OCDS releases.

    :param root: Root element of the eForms XML
    :return: Generator of OCDS releases
    """

    # # Check for eForm type
    # eform_type_elem = root.find(".//cbc:NoticeTypeCode", EFORMS_NSMAP)
    # if eform_type_elem is None:
    #     return

    # eform_type = eform_type_elem.get("listName")

    # # Only process implemented eForms
    # if eform_type not in IMPLEMENTED_EFORMS:
    #     return

    try:
        # Extract basic notice information
        doc_id_elem = root.find(".//efbc:NoticePublicationID", EFORMS_NSMAP)
        if doc_id_elem is None:
            return
        doc_id = doc_id_elem.text

        meta  = _get_eform_metadata(root)
        if meta is None:
            return

        # if meta.get("type") not in ["competition", "planning", "result", "cont-modif", "dir-awa-pre"]:
        #     print(f"name:{meta.get("type")} text:{meta.get("subtype")}")

        pub_date = meta.get("date")
        if pub_date is None:
            return

        # Create release and tender
        release = model.Release(
            ocid="",  # Will be set later
            id="",  # Will be set later
            date=pub_date,
            tag=[],  # Will be set based on form type
            initiationType=model.InitiationType.tender,
        )

        tender = model.Tender(id="")  # Will be set to OCID later
        release.tender = tender

        form_type = meta.get("type")
        if form_type is None:
            return
        # print(f"form type: {form_type}")
        # Initialize parties list for all organizations
        parties = []

        # Set release tags and tender status based on form type
        if form_type == "planning": #F01
            notice_type = meta.get("subtype")
            if notice_type in ("pin-only", "pin-rtl"):
                release.tag = [model.Tag.planning]
                tender.status = model.Status.planned
        elif form_type == "competition": #F02
            notice_type = meta.get("subtype")
            if notice_type == "pin-cfc-standard" or notice_type == "pin-cfc-social": # F01
                release.tag = [model.Tag.planning, model.Tag.tender]
                tender.status = model.Status.active
            else: # F02
                release.tag = [model.Tag.tender]
                tender.status = model.Status.active
        elif form_type == "result": #F03
            release.tag = [model.Tag.award, model.Tag.contract]
            tender.status = model.Status.complete

        # do we really need uuid component here?
        ocid = "ocds-jyvdv7-" + str(uuid.uuid5(uuid.NAMESPACE_URL, doc_id))
        # ocid = "ocds-jyvdv7-" + str(doc_id)
        release.ocid = ocid

        # Extract title and description
        title_elem = root.find(".//cac:ProcurementProject/cbc:Name", EFORMS_NSMAP)
        if title_elem is not None:
            release.tender.title = title_elem.text

        release.tender.id = release.ocid
        # release.tender.id = str(uuid.uuid4())

        # # Extract tender details
        # tender.title = extractors.extract_title(elem, doc_sec_trans)
        release.tender.description = eform.extract_eform_description(root, EFORMS_NSMAP)
        # HERE
        # tender.value = extractors.extract_value(doc_sec_forms, "tender")

        cpv_elems: List = root.findall(".//cbc:ItemClassificationCode", EFORMS_NSMAP)

        if cpv_elems is not None and len(cpv_elems) > 0:
            arr = []
            for item in cpv_elems:
                arr.append(item.text)

            cpvCode = list(dict.fromkeys(arr))
            if len(cpvCode) > 0 and release.tender is not None:
                release.tender.cpvCode = cpvCode
                # print(cpvCode)

        organizations = eform.extract_orgs(root, EFORMS_NSMAP)

        # extract all eform buyers, first is the primary buyer for OCDS format
        buyers = None
        buyers_dict= eform.extract_contracting_parties(root,EFORMS_NSMAP, organizations)
        if len(buyers_dict) > 0:
            first = True
            for key,value in buyers_dict.items():
                parties.append(value)
                #OCDS supports only one buyer; additional go to parties only
                if first:
                    release.buyer = value
                    first = False

        release.parties = parties

        # Find ProcurementProjectLot
        proc_proj_lot_elems = root.findall("cac:ProcurementProjectLot", namespaces=EFORMS_NSMAP)

        # Process awards for F03 forms - 
        if form_type == "result":
            release.awards = []
            release.contracts = []

            notice_result = root.find(
                ".//efac:NoticeResult", namespaces=EFORMS_NSMAP
                )
            if notice_result is None:
                print("No .//efac:NoticeResult")
                return
            
            amount_elem = notice_result.find(".//cbc:TotalAmount", namespaces=EFORMS_NSMAP)
            amount = None
            currency = None
            if amount_elem is not None:
                amount = amount_elem.text
                currency = amount_elem.get("currencyID")
            
            lot_result_elems=notice_result.findall(".//efac:LotResult", namespaces=EFORMS_NSMAP)

            if len(lot_result_elems) > 0:
                # RES-0001
                docs = []
                lot_id = None
                award_date = None
                signed_date = None
                for lot_result_elem in lot_result_elems:
                    decision_reason = None
                    lot_res_id = lot_result_elem.findtext(".//cbc:ID", namespaces=EFORMS_NSMAP) # RES-0001 - do we really need this? kind of top level
                    lot_results = eform.get_lot_result_details(lot_result_elem, root, notice_result, EFORMS_NSMAP, organizations)
                    contract_details = None
                    if lot_results.get("CON") is not None:
                        contract_details = eform.get_contract_details(root, EFORMS_NSMAP, lot_results.get("CON"))#, opt_award_date)
                        award_date = contract_details.get("AWARD")
                        signed_date = contract_details.get("SIGN")
                    result_code = lot_results.get("result-status")
                    lot_id = lot_results.get("LOT")
                    if lot_id is not None:
                        docs.append(lot_id)
                    value = None
                    tender_parties = None
                    tender_party_id = None
                    if result_code == "selec-w" or result_code == "open-nw":
                        # settled_con = eform.get_settled_contract_details(notice_result, EFORMS_NSMAP, lot_results.get("CON"))
                        lot_tender_details= eform.get_lot_tender_details(notice_result, root, EFORMS_NSMAP, lot_results.get("TEN"), amount, currency)
                        if lot_tender_details is not None:
                            value = lot_tender_details.get("value")
                        else:
                            print(f".//efac:LotTender not available")

                        if lot_tender_details is not None:
                            tender_party_id  = lot_tender_details.get("TPA")
                            # print(f"0001 tendering_party_id: {tender_party_id}")
                            tender_parties = eform.get_tendering_party_details(notice_result, root, EFORMS_NSMAP, tender_party_id)
                        else:
                            print(f"Find tender party without ID?")
                    else:
                        decision_reason = eform.extract_decision_reason(notice_result, EFORMS_NSMAP, lot_res_id)
                    
                    award_title=None
                    award_descr=None
                    award_period = dict()
                    award_items = []

                    # Find the ProcurementProjectLot
                    if proc_proj_lot_elems is not None:
                        for lot in proc_proj_lot_elems:
                            curr_id = lot.findtext(".//cbc:ID", namespaces=EFORMS_NSMAP)
                            if curr_id == lot_id:
                                award_title = lot.findtext(".//cac:ProcurementProject/cbc:Name", namespaces=EFORMS_NSMAP)
                                award_descr = lot.findtext(".//cac:ProcurementProject/cbc:Description", namespaces=EFORMS_NSMAP)
                                award_period["startDate"] = eform._extract_award_date_str(lot.findtext(".//cac:ProcurementProject/cac:PlannedPeriod/cbc:StartDate", namespaces=EFORMS_NSMAP))
                                award_period["endDate"] = eform._extract_award_date_str(lot.findtext(".//cac:ProcurementProject/cac:PlannedPeriod/cbc:EndDate", namespaces=EFORMS_NSMAP))
                                lot_cpv = lot.findtext(".//cac:ProcurementProject/cac:MainCommodityClassification/cbc:ItemClassificationCode", namespaces=EFORMS_NSMAP)
                                award_items.append({"classification":{"scheme": "CPV"}, "id": lot_cpv})
                                eu_funded = eform.extract_eufunded(lot, EFORMS_NSMAP)
                                if eu_funded is not None:
                                    award_items.append({"classification": { "scheme": "FUNDING", "id": eu_funded}})
                                break

                    award_id = str(uuid.uuid4())
                    award = model.Award(
                        id=award_id,
                        title=award_title,
                        # description=award_descr,
                        status=model.AwardStatus.active,
                        date=award_date,
                        value=value,
                        items=award_items,
                        contractPeriod=award_period,
                        decisionReason=decision_reason,
                        documents=docs
                        )
                    # For now
                    if result_code == "selec-w" or result_code == "open-nw":
                        award.status = model.AwardStatus.active
                    else:
                        award.status = model.AwardStatus.unsuccessful

                    # Create contract
                    contract = model.Contract(
                        id=str(uuid.uuid4()),
                        awardID=award_id,
                        title=award_title,
                        description=award_descr,
                        status=model.ContractStatus.active,
                        period=award_period,
                        value=value,
                        items=award_items,
                        dateSigned=signed_date,
                        documents=docs,
                    )
                    release.contracts.append(contract)

                    # Extract suppliers
                    award.suppliers = []
                    if tender_parties is not None:
                        for item in tender_parties:
                            supplier = organizations.get(item)
                            supplier.roles = ["supplier"]
                            parties.append(supplier)
                            award.suppliers.append(supplier)

                    release.awards.append(award)
            else:
                # no LotResult section
                print(f"no lotResult section")

        # Set parties list
        release.parties = parties

        yield release

    # except Exception:
    #     # If parsing fails, skip this notice silently
    #     return
    except Exception as e:
        # If parsing fails, throw and print the error
        print(e)
        traceback.print_exc()
        return

def parse_ted_notice(
    xml_source: Union[str, BinaryIO],
) -> Generator[BaseModel, None, None]:
    """
    Parse a TED notice XML and return OCDS releases.

    Handles both old TED format (<TED_EXPORT>) and new eForms format (UBL schema).

    :param xml_source: Path to XML file or file-like object
    :return: Generator of OCDS releases
    """
    # Optimize performance for XMLParser
    parser = etree.XMLParser(ns_clean=True, huge_tree=True, remove_blank_text=True)

    # Parse XML (etree.parse accepts both file paths and file-like objects)
    tree: etree._ElementTree[Any] = etree.parse(xml_source, parser)
    root: etree._Element = tree.getroot()

    # Check if this is an eForm (new format)
    # eForms have a different structure - check for UBL namespace
    if root.tag.startswith("{urn:oasis:names:specification:ubl:schema:xsd:"):
        # This is an eForm - use eForm parser
        yield from parse_eform_notice(root)
        return

    # Old TED format processing
    # Define sections based on list indices
    # sec_techn = root[0]
    # sec_links = root[1]
    sec_coded: etree._Element = root[2]
    sec_trans = root[3]
    sec_forms = root[4]

    # Extract notice ID
    doc_id = sec_coded.find(".//NOTICE_DATA/NO_DOC_OJS", namespaces=sec_coded.nsmap)
    if doc_id is None:
        return

    doc_id = doc_id.text

    # Extract form type
    form_type = sec_forms[0].get("FORM")
    # form_version = sec_forms[0].get("VERSION")

    if form_type is None:
        try:
            form_type = sec_forms[1].get("FORM")
            # form_version = sec_forms[1].get("VERSION")
            if form_type is None:
                return
        except IndexError:
            # if form_version is None:
            #     return
            # else:
            #     return
            return

    # Normalize form type format
    # eg: 1_2014 -> F01_2024 ; 15_2014 -> F15_2014
    if form_type[0].isdigit():
        if len(form_type) == 1:
            form_type = f"F0{form_type}"
        else:
            form_type = f"F{form_type}"

    # Extract English version or another language version
    translate_from = None
    new_sec_forms: Optional[etree._Element] = None
    for elem in sec_forms:
        if elem.tag.endswith("NOTICE_UUID"):
            continue
        lang: str = elem.get("LG")
        if lang == "EN":
            new_sec_forms = elem
            break
        # No English form found, translation needed
        new_sec_forms = elem
        translate_from = lang.lower()

    if new_sec_forms is None:
        return

    sec_forms = new_sec_forms

    # Find OBJECT_CONTRACT elements
    object_contract: List = sec_forms.findall(
        ".//OBJECT_CONTRACT", namespaces=sec_forms.nsmap
    )
    assert isinstance(object_contract, List)

    # Generate OCDS releases
    yield from ted_notice_to_ocds_releases(
        doc_id=doc_id,
        form_type=form_type,
        ocid_prefix="ocds-jyvdv7-",
        object_contract=object_contract,
        doc_sec_forms=sec_forms,
        doc_sec_coded=sec_coded,
        doc_sec_trans=sec_trans,
        translate_from=translate_from,
    )
