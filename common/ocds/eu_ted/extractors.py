# SPDX-FileCopyrightText: 2023 Free Software Foundation Europe <contact@fsfe.org>
#
# SPDX-License-Identifier: GPL-3.0-or-later

from typing import Dict, Optional

import ftfy
import phonenumbers
import pycountry
from lxml import etree

from common.ocds.eu_ted import model
from common.ocds.eu_ted.utils import clean


def get_alpha3_country_code(alpha2_code):
    """Convert ISO 3166-1 alpha-2 to alpha-3 country code using pycountry."""
    if not alpha2_code:
        return None
    try:
        country = pycountry.countries.get(alpha_2=alpha2_code)
        return country.alpha_3 if country else None
    except (AttributeError, KeyError):
        return None


def extract_title(
    elem: etree._Element,
    doc_sec_trans: etree._Element,
) -> Optional[str]:
    """Extract title from TED notice element"""
    if elem.find(".//TITLE", namespaces=elem.nsmap) is None:
        return [
            doc.find(".//TI_TEXT", namespaces=elem.nsmap)[0].text
            for doc in doc_sec_trans.iterfind(".//ML_TI_DOC", namespaces=elem.nsmap)
            if doc.get("LG") == "EN"
        ][0]

    orig_title = elem.find(".//TITLE", namespaces=elem.nsmap).text
    if not orig_title:
        orig_title = elem.find(".//TITLE", namespaces=elem.nsmap)[0].text

    if doc_sec_trans is None:
        return clean(orig_title)
    else:
        try:
            title = (
                [
                    doc.find(".//TI_TEXT", namespaces=elem.nsmap)[0].text
                    for doc in doc_sec_trans.iterfind(
                        ".//ML_TI_DOC", namespaces=elem.nsmap
                    )
                    if doc.get("LG") == "EN"
                ][0]
                + " ["
                + orig_title
                + "]"
            )
            return clean(title)
        except TypeError:
            return orig_title


def extract_description(elem: etree._Element) -> str:
    """Extract description from element"""
    description = elem.find(".//SHORT_DESCR", namespaces=elem.nsmap)[0].text
    return clean(description)


def extract_text(elem: etree._Element, path: str) -> Optional[str]:
    """Extract text from XML element at given path"""
    try:
        to_return = elem.find(path, namespaces=elem.nsmap).text
        return ftfy.fix_text(to_return)
    except TypeError:
        to_return = elem.find(path, namespaces=elem.nsmap)[0].text
        try:
            return ftfy.fix_text(to_return)
        except TypeError:
            return None
    except AttributeError:
        return None


def extract_buyer_contact_point(
    elem: etree._Element, buyer_search_str: Optional[str] = "ADDRESS_CONTRACTING_BODY"
) -> model.ContactPoint:
    """Extract buyer contact point information"""
    name = extract_text(elem, f".//CONTRACTING_BODY/{buyer_search_str}/CONTACT_POINT")
    phone_unformatted = extract_text(
        elem, f".//CONTRACTING_BODY/{buyer_search_str}/PHONE"
    )
    if phone_unformatted:
        if "/" in phone_unformatted:
            phone_unformatted = phone_unformatted.split("/")[0]
        try:
            phonenumber = phonenumbers.parse(phone_unformatted)
            phone = phonenumbers.format_number(
                phonenumber, phonenumbers.PhoneNumberFormat.INTERNATIONAL
            )
        except phonenumbers.phonenumberutil.NumberParseException:
            phone = phone_unformatted + " [likely invalid]"
    else:
        phone = None
    email = extract_text(elem, f".//CONTRACTING_BODY/{buyer_search_str}/E_MAIL")
    fax_unformatted = extract_text(elem, f".//CONTRACTING_BODY/{buyer_search_str}/FAX")
    if fax_unformatted:
        try:
            faxnumber = phonenumbers.parse(fax_unformatted)
            fax = phonenumbers.format_number(
                faxnumber, phonenumbers.PhoneNumberFormat.INTERNATIONAL
            )
        except phonenumbers.phonenumberutil.NumberParseException:
            fax = fax_unformatted + " [likely invalid]"
    else:
        fax = None

    url = extract_text(elem, f".//CONTRACTING_BODY/{buyer_search_str}/URL_GENERAL")

    return model.ContactPoint(
        name=name, telephone=phone, email=email, faxNumber=fax, url=url
    )


def extract_buyer_details(
    elem: etree._Element, buyer_search_str: Optional[str] = "ADDRESS_CONTRACTING_BODY"
) -> Dict:
    """Extract buyer details"""
    url = extract_text(elem, f".//CONTRACTING_BODY/{buyer_search_str}/URL_GENERAL")
    buyerProfile = extract_text(
        elem, f".//CONTRACTING_BODY/{buyer_search_str}/URL_BUYER"
    )
    return {"url": url, "buyerProfile": buyerProfile}


def extract_buyer_address(
    elem: etree._Element,
    base_path: Optional[str] = "CONTRACTING_BODY",
    search_path: Optional[str] = "ADDRESS_CONTRACTING_BODY",
) -> model.Address:
    """Extract buyer address"""
    streetAddress = extract_text(elem, f".//{base_path}/{search_path}/ADDRESS")
    locality = extract_text(elem, f".//{base_path}/{search_path}/TOWN")
    postalCode = extract_text(elem, f".//{base_path}/{search_path}/POSTAL_CODE")

    try:
        region = elem.find(
            f".//{base_path}/{search_path}" + "/{*}NUTS", namespaces=elem.nsmap
        ).get("CODE")
    except AttributeError:
        region = None

    try:
        countryCode = elem.find(
            f".//{base_path}/{search_path}/COUNTRY",
            namespaces=elem.nsmap,
        ).get("VALUE")
        countryCode3 = get_alpha3_country_code(countryCode)
    except AttributeError:
        countryCode = None
        countryCode3 = None

    return model.Address(
        streetAddress=streetAddress,
        locality=locality,
        region=region,
        postalCode=postalCode,
        countryCode=countryCode,
        countryCode3=countryCode3,
    )


def extract_supplier_address(elem: etree._Element) -> model.Address:
    """Extract supplier address"""
    streetAddress = extract_text(elem, ".//ADDRESS")
    locality = extract_text(elem, ".//TOWN")
    postalCode = extract_text(elem, ".//POSTAL_CODE")

    try:
        region = elem.find(".//{*}NUTS", namespaces=elem.nsmap).get("CODE")
    except AttributeError:
        region = None

    try:
        countryCode = elem.find(
            ".//COUNTRY",
            namespaces=elem.nsmap,
        ).get("VALUE")
        countryCode3 = get_alpha3_country_code(countryCode)
    except AttributeError:
        countryCode = None
        countryCode3 = None

    return model.Address(
        streetAddress=streetAddress,
        locality=locality,
        region=region,
        postalCode=postalCode,
        countryCode=countryCode,
        countryCode3=countryCode3,
    )


def _extract_value(elem: etree._Element, path: str) -> Optional[model.Value]:
    """Extract value with amount and currency"""
    amount_str = extract_text(elem, path)
    currency = extract_attribute(elem, path, "CURRENCY")

    if amount_str is None or currency is None:
        return None

    try:
        amount = float(amount_str.strip())
    except (ValueError, AttributeError):
        return None

    return model.Value(amount=amount, currency=currency)  # ISO 4217 string


def extract_value(elem: etree._Element, ocds_object: str) -> Optional[model.Value]:
    """Extract value for tender or award"""
    if ocds_object == "tender":
        tender_value_paths = [
            ".//OBJECT_CONTRACT/VAL_ESTIMATED_TOTAL",
        ]
        for path in tender_value_paths:
            value = _extract_value(elem, path)
            if value is not None:
                return value

    elif ocds_object == "award":
        award_value_paths = [
            ".//VAL_TOTAL",
            ".//VALUES/VAL_TOTAL",
        ]
        for path in award_value_paths:
            value = _extract_value(elem, path)
            if value is not None:
                return value

    return None


def extract_attribute(elem: etree._Element, path: str, attribute: str) -> Optional[str]:
    """Extract attribute from XML element"""
    try:
        to_return = elem.find(path, namespaces=elem.nsmap).get(attribute)
        return ftfy.fix_text(to_return)
    except AttributeError:
        return None
