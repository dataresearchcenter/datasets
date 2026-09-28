# SPDX-FileCopyrightText: 2023 Free Software Foundation Europe <contact@fsfe.org>
#
# SPDX-License-Identifier: GPL-3.0-or-later

"""
OCDS 1.1.5 compliant Pydantic schema for TED notice parsing
"""

from datetime import datetime
from enum import Enum
from typing import Any, List, Optional

from pydantic import BaseModel, Field


class InitiationType(str, Enum):
    tender = "tender"


class Status(str, Enum):
    planning = "planning"
    planned = "planned"
    active = "active"
    cancelled = "cancelled"
    unsuccessful = "unsuccessful"
    complete = "complete"
    withdrawn = "withdrawn"


class Tag(str, Enum):
    planning = "planning"
    planningUpdate = "planningUpdate"
    tender = "tender"
    tenderAmendment = "tenderAmendment"
    tenderUpdate = "tenderUpdate"
    tenderCancellation = "tenderCancellation"
    award = "award"
    awardUpdate = "awardUpdate"
    awardCancellation = "awardCancellation"
    contract = "contract"
    contractUpdate = "contractUpdate"
    contractAmendment = "contractAmendment"
    implementation = "implementation"
    implementationUpdate = "implementationUpdate"
    contractTermination = "contractTermination"
    compiled = "compiled"


class AwardStatus(str, Enum):
    pending = "pending"
    active = "active"
    cancelled = "cancelled"
    unsuccessful = "unsuccessful"


class ContractStatus(str, Enum):
    pending = "pending"
    active = "active"
    cancelled = "cancelled"
    terminated = "terminated"


class Classification(BaseModel):
    scheme: Optional[str] = None
    id: Optional[str] = None
    description: Optional[str] = None
    uri: Optional[str] = None


class Identifier(BaseModel):
    scheme: Optional[str] = None
    id: Optional[str] = None
    legalName: Optional[str] = None
    uri: Optional[str] = None


class ContactPoint(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None
    telephone: Optional[str] = None
    faxNumber: Optional[str] = None
    url: Optional[str] = None


class Address(BaseModel):
    streetAddress: Optional[str] = None
    locality: Optional[str] = None
    region: Optional[str] = None
    postalCode: Optional[str] = None
    countryCode: Optional[str] = None
    countryCode3: Optional[str] = None


class Value(BaseModel):
    """Monetary value with amount and currency."""

    amount: Optional[float] = None
    currency: Optional[str] = None  # ISO 4217 currency code


class Organization(BaseModel):
    """Organization/party involved in the contracting process."""

    id: str
    name: Optional[str] = None
    identifier: Optional[Identifier] = None
    address: Optional[Address] = None
    contactPoint: Optional[ContactPoint] = None
    roles: List[str] = Field(default_factory=list)
    # Extensions for internal use (not part of OCDS spec)
    details: Optional[Any] = None


class Tender(BaseModel):
    """Tender/procurement process details."""

    id: str
    title: Optional[str] = None
    description: Optional[str] = None
    status: Optional[Status] = None
    value: Optional[Value] = None
    procurementMethod: Optional[str] = None
    procurementMethodDetails: Optional[str] = None
    tenderPeriod: Optional[Any] = None
    enquiryPeriod: Optional[Any] = None
    hasEnquiries: Optional[bool] = None
    eligibilityCriteria: Optional[str] = None
    awardCriteria: Optional[str] = None
    awardCriteriaDetails: Optional[str] = None
    submissionMethod: Optional[List[str]] = None
    submissionMethodDetails: Optional[str] = None
    tenderPeriod: Optional[Any] = None
    documents: Optional[List[Any]] = None
    milestones: Optional[List[Any]] = None
    amendments: Optional[List[Any]] = None
    # extra
    cpvCode: Optional[List[str]] = None
    cpvName: Optional[List[str]] = None



class Award(BaseModel):
    """Award decision details."""

    id: str
    title: Optional[str] = None
    description: Optional[str] = None
    status: Optional[AwardStatus] = None
    date: Optional[datetime] = None
    value: Optional[Value] = None
    suppliers: List[Organization] = Field(default_factory=list)
    items: Optional[List[Any]] = None
    contractPeriod: Optional[Any] = None
    documents: Optional[List[Any]] = None
    amendments: Optional[List[Any]] = None
    # extra
    decisionReason: Optional[str] = None


class Contract(BaseModel):
    """Contract implementation details."""

    id: str
    awardID: str
    title: Optional[str] = None
    description: Optional[str] = None
    status: Optional[ContractStatus] = None
    period: Optional[Any] = None
    value: Optional[Value] = None
    items: Optional[List[Any]] = None
    dateSigned: Optional[datetime] = None
    documents: Optional[List[Any]] = None
    implementation: Optional[Any] = None
    relatedProcesses: Optional[List[Any]] = None
    milestones: Optional[List[Any]] = None
    amendments: Optional[List[Any]] = None


class Release(BaseModel):
    """OCDS Release - a snapshot of contracting process information."""

    ocid: str
    id: str  # Required in OCDS 1.1.5
    date: datetime
    tag: List[Tag]
    initiationType: InitiationType
    parties: Optional[List[Organization]] = None
    buyer: Optional[Organization] = None
    planning: Optional[Any] = None
    tender: Optional[Tender] = None
    awards: Optional[List[Award]] = None
    contracts: Optional[List[Contract]] = None
    language: Optional[str] = "en"
    relatedProcesses: Optional[List[Any]] = None
