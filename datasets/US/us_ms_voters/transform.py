from ftmq.types import Generator
from investigraph.model import SourceContext
from investigraph.types import Record
from investigraph.util import join_text


def handle(ctx: SourceContext, record: Record, ix: int) -> Generator:
    """Transform Mississippi voter file records into FollowTheMoney entities"""
    
    # Create a copy to track processed fields
    data = dict(record)
    
    # Extract basic voter information
    voter_id = data.pop("Mapping Value")
    if not voter_id:
        ctx.log.warning("Missing voter ID", row=ix)
        return
    
    # Create Person entity
    person = ctx.make_entity("Person")
    person.id = ctx.make_slug("voter", voter_id)
    
    # Name fields
    first_name = (data.pop("FIRST_NAME") or "").strip()
    middle_name = (data.pop("MIDDLE_NAME") or "").strip()
    last_name = (data.pop("LAST_NAME") or "").strip()
    suffix = (data.pop("SUFFIX") or "").strip()
    
    # Construct full name
    name_parts = [first_name, middle_name, last_name]
    if suffix:
        name_parts.append(suffix)
    full_name = join_text(*name_parts)
    
    if not full_name:
        ctx.log.warning("Missing name", voter_id=voter_id, row=ix)
        return
    
    person.add("name", full_name)
    person.add("firstName", first_name or None)
    person.add("middleName", middle_name or None)
    person.add("lastName", last_name or None)
    
    # Registration information
    registration_date = (data.pop("EFFECTIVE_REGN_DATE") or "").strip()
    status = (data.pop("STATUS") or "").strip()
    last_voted = (data.pop("DATE_VOTED") or "").strip()
    
    # Build notes components
    notes_parts = []
    if registration_date:
        notes_parts.append(f"Registration date: {registration_date}")
    if status:
        notes_parts.append(f"Status: {status}")
    if last_voted:
        notes_parts.append(f"Last voted: {last_voted}")
    
    # Country (always US for Mississippi voters)
    person.add("country", "us")
    
    # Residential address
    house_num = (data.pop("HOUSE_NUM") or "").strip()
    street_name = (data.pop("STREET_NAME") or "").strip()
    street_type = (data.pop("STREET_TYPE") or "").strip()
    pre_direction = (data.pop("PRE-DIRECTION") or "").strip()
    post_direction = (data.pop("POST-DIRECTION") or "").strip()
    res_city = (data.pop("RES_CITY") or "").strip()
    res_state = (data.pop("RES_STATE") or "").strip()
    res_zip = (data.pop("RES_ZIP_CODE") or "").strip()
    
    # Use RESIDENTIAL_ADDRESS if available, otherwise build from components
    residential_address = (data.pop("RESIDENTIAL_ADDRESS") or "").strip()
    
    if residential_address:
        # Use the provided residential address
        address_full = [residential_address]
        if res_city:
            address_full.append(res_city)
        if res_state:
            address_full.append(res_state)
        if res_zip:
            address_full.append(res_zip)
        person.add("address", ", ".join(address_full))
    elif street_name:
        # Build address from components
        address_parts = []
        
        # House number
        if house_num:
            address_parts.append(house_num)
        
        # Pre-direction
        if pre_direction:
            address_parts.append(pre_direction)
        
        # Street name and type
        street_part = street_name
        if street_type:
            street_part = f"{street_part} {street_type}"
        address_parts.append(street_part)
        
        # Post-direction
        if post_direction:
            address_parts.append(post_direction)
        
        # Build complete address string
        street_address = " ".join(address_parts)
        
        address_full = [street_address]
        if res_city:
            address_full.append(res_city)
        if res_state:
            address_full.append(res_state)
        if res_zip:
            address_full.append(res_zip)
        
        person.add("address", ", ".join(address_full))
    
    # Electoral district information
    district_info = []
    
    # Extract county and precinct
    county = (data.pop("RES_COUNTY") or "").strip()
    precinct_code = (data.pop("PRECINCT_CODE") or "").strip()
    precinct_name = (data.pop("PRECINCT_NAME") or "").strip()
    
    if county:
        district_info.append(f"County: {county}")
    if precinct_code:
        if precinct_name:
            district_info.append(f"Precinct: {precinct_code} ({precinct_name})")
        else:
            district_info.append(f"Precinct: {precinct_code}")
    
    # Federal districts
    us_district = (data.pop("US") or "").strip()
    ms_district = (data.pop("MS") or "").strip()
    congressional = (data.pop("CONG") or "").strip()
    
    if us_district:
        district_info.append(f"US: {us_district}")
    if ms_district:
        district_info.append(f"MS: {ms_district}")
    if congressional:
        district_info.append(f"Congressional: {congressional}")
    
    # State districts
    state_senate = (data.pop("SEN") or "").strip()
    state_house = (data.pop("REP") or "").strip()
    
    if state_senate:
        district_info.append(f"State Senate: {state_senate}")
    if state_house:
        district_info.append(f"State House: {state_house}")
    
    # Add district information to notes
    if district_info:
        notes_parts.append("Districts: " + ", ".join(district_info))
    
    # Add all notes as a single combined note
    if notes_parts:
        person.add("notes", " | ".join(notes_parts))
    
    # Remove remaining known fields (mailing address and other districts)
    mailing_fields = [
        "MAILING_ADDRESS", "MAIL_CITY", "MAIL_STATE", "MAIL_ZIP_CODE"
    ]
    
    for field in mailing_fields:
        data.pop(field, None)
    
    # Remove remaining district/administrative fields
    district_fields = [
        "SC", "PSC", "TC", "DA", "CA", "CHC", "CIR", "CNT", "LEV", 
        "COCT", "JUD", "SCHC", "SCIR", "SUP", "SUPR", "EC", "JC", 
        "JCJ", "CON", "MUN", "WARD", "WARDP", "MP", "SCHD", "SCHB", 
        "SBAL", "FIRE", "FCD", "WSD", "IPD"
    ]
    
    for field in district_fields:
        data.pop(field, None)
    
    # Remove unnamed columns from trailing commas in CSV
    for key in list(data.keys()):
        if key.startswith("Unnamed:"):
            data.pop(key, None)
    
    # Log any remaining unhandled fields
    data.pop("__source__", None)
    if data:
        ctx.log.info("Unhandled fields", fields=list(data.keys()), record=ix)
    
    yield person