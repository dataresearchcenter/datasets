"""
Code to label lookups for the Land Matrix API.

Transcribed from the upstream django model choices, so that the opaque codes
used by the API (`RIC`, `STATE_OWNED_COMPANY`, ...) end up as readable values:
https://github.com/sinnwerkstatt/landmatrix/blob/main/apps/landmatrix/models/choices.py
"""

from typing import Iterable

CROPS = {
    "ACC": "Accacia",
    "ALF": "Alfalfa",
    "ALG": "Seaweed / Macroalgae(unspecified)",
    "ALM": "Almond",
    "ALV": "Aloe Vera",
    "APL": "Apple",
    "AQU": "Aquaculture (unspecified crops)",
    "BAM": "Bamboo",
    "BAN": "Banana",
    "BEA": "Bean",
    "BOT": "Bottle Gourd",
    "BRL": "Barley",
    "BWT": "Buckwheat",
    "CAC": "Cacao",
    "CAS": "Cassava (Maniok)",
    "CAW": "Cashew",
    "CHA": "Chat",
    "CHE": "Cherries",
    "CNL": "Canola",
    "COC": "Coconut",
    "COF": "Coffee Plant",
    "COT": "Cotton",
    "CRL": "Cereals (unspecified)",
    "CRN": "Corn (Maize)",
    "CRO": "Croton",
    "CST": "Castor Oil Plant",
    "CTR": "Citrus Fruits (unspecified)",
    "DIL": "Dill",
    "EUC": "Eucalyptus",
    "FLW": "Flowers (unspecified)",
    "FNT": "Fig-Nut",
    "FOD": "Fodder Plants (unspecified)",
    "FOO": "Food crops (unspecified)",
    "FRT": "Fruit (unspecified)",
    "GRA": "Grapes",
    "GRN": "Grains (unspecified)",
    "HRB": "Herbs (unspecified)",
    "JTR": "Jatropha",
    "LNT": "Lentils",
    "MAN": "Mango",
    "MUS": "Mustard",
    "OAT": "Oats",
    "OIL": "Oil Seeds (unspecified)",
    "OLE": "Oleagionous plant",
    "OLV": "Olives",
    "ONI": "Onion",
    "OPL": "Oil Palm",
    "OTH": "Other crops",
    "PAL": "Palms",
    "PAP": "Papaya",
    "PAS": "Passion fruit",
    "PEA": "Peanut (groundnut)",
    "PEP": "Pepper",
    "PES": "Peas",
    "PIE": "Pine",
    "PIN": "Pineapple",
    "PLS": "Pulses (unspecified)",
    "POM": "Pomegranate",
    "PON": "Pongamia Pinnata",
    "PTT": "Potatoes",
    "RAP": "Rapeseed",
    "RCH": "Rice (hybrid)",
    "RIC": "Rice",
    "ROS": "Roses",
    "RUB": "Rubber tree",
    "RYE": "Rye",
    "SEE": "Seeds Production (unspecified)",
    "SES": "Sesame",
    "SOR": "Sorghum",
    "SOY": "Soya Beans",
    "SPI": "Spices (unspecified)",
    "SSL": "Sisal",
    "SUB": "Sugar beet",
    "SUC": "Sugar Cane",
    "SUG": "Sugar (unspecified)",
    "SUN": "Sun Flower",
    "SWP": "Sweet Potatoes",
    "TBC": "Tobacco",
    "TEA": "Tea",
    "TEF": "Teff",
    "TEK": "Teak",
    "TOM": "Tomatoes",
    "TRE": "Trees (unspecified)",
    "VGT": "Vegetables (unspecified)",
    "VIN": "Vineyard",
    "WHT": "Wheat",
    "YAM": "Yam",
}

ANIMALS = {
    "AQU": "Aquaculture (animals)",
    "BEE": "Beef Cattle",
    "CTL": "Cattle",
    "DCT": "Dairy Cattle",
    "FSH": "Fish",
    "GOT": "Goats",
    "OTH": "Other livestock",
    "PIG": "Pork",
    "POU": "Poultry",
    "SHP": "Sheep",
    "SHR": "Shrimp",
}

MINERALS = {
    "ALU": "Aluminum",
    "ASP": "Asphaltite",
    "ATC": "Anthracite",
    "BAR": "Barite",
    "BAS": "Basalt",
    "BAX": "Bauxite",
    "BEN": "Bentonite",
    "BUM": "Building materials",
    "CAR": "Carbon",
    "CHR": "Chromite",
    "CLA": "Clay",
    "COA": "Coal",
    "COB": "Cobalt",
    "COP": "Copper",
    "DIA": "Diamonds",
    "EME": "Emerald",
    "FLD": "Feldspar",
    "FLO": "Fluoride",
    "GAS": "Gas",
    "GLD": "Gold",
    "GRT": "Granite",
    "GRV": "Gravel",
    "HEA": "Heavy Mineral Sands",
    "ILM": "Ilmenite",
    "IRO": "Iron",
    "JAD": "Jade",
    "LED": "Lead",
    "LIM": "Limestone",
    "LIT": "Lithium",
    "MAG": "Magnetite",
    "MBD": "Molybdenum",
    "MGN": "Manganese",
    "MRB": "Marble",
    "NIK": "Nickel",
    "OTH": "Other minerals",
    "PET": "Petroleum",
    "PHP": "Phosphorous",
    "PLT": "Platinum",
    "PUM": "Hydrocarbons (e.g. crude oil)",
    "PYR": "Pyrolisis Plant",
    "RUT": "Rutile",
    "SAN": "Sand",
    "SIC": "Silica",
    "SIL": "Silver",
    "SLT": "Salt",
    "STO": "Stone",
    "TIN": "Tin",
    "TTM": "Titanium",
    "URM": "Uranium",
    "ZNC": "Zinc",
}

ELECTRICITY_GENERATION = {
    "WIND": "On-shore wind turbines",
    "PHOTOVOLTAIC": "Solar (Photovoltaic)",
    "SOLAR_HEAT": "Solar (Thermal system)",
}

CARBON_SEQUESTRATION = {
    "REFORESTATION": "Reforestation & afforestation",
    "AVOIDED_FOREST_CONVERSION": "Avoided forest conversion",
    "AVOIDED_GRASSLAND_CONVERSION": "Avoided grassland conversion",
    "PEATLAND_RESTORATION": "Peatland restoration",
    "IMPROVED_FOREST_MANAGEMENT": "Improved forest management",
    "SUSTAINABLE_AGRICULTURE": "Sustainable agriculture",
    "SUSTAINABLE_GRASSLAND_MANAGEMENT": "Sustainable grassland management",
    "RICE_EMISSION_REDUCTIONS": "Rice emission reductions",
    "SOLAR_PARK": "Solar park",
    "WIND_FARM": "Wind farm",
    "OTHER": "Other",
}

INTENTION_OF_INVESTMENT = {
    "BIOFUELS": "Biomass for biofuels",
    "BIOMASS_ENERGY_GENERATION": "Biomass for energy generation (agriculture)",
    "FODDER": "Fodder",
    "FOOD_CROPS": "Food crops",
    "LIVESTOCK": "Livestock",
    "NON_FOOD_AGRICULTURE": "Non-food agricultural commodities",
    "AGRICULTURE_UNSPECIFIED": "Agriculture unspecified",
    "BIOMASS_ENERGY_PRODUCTION": "Biomass for energy generation (forestry)",
    "CARBON": "For carbon sequestration/REDD",
    "FOREST_LOGGING": "Forest logging / management for wood and fiber",
    "TIMBER_PLANTATION": "Timber plantation for wood and fiber",
    "FORESTRY_UNSPECIFIED": "Forestry unspecified",
    "SOLAR_PARK": "Solar park",
    "WIND_FARM": "Wind farm",
    "RENEWABLE_ENERGY": "Renewable energy unspecified",
    "CONVERSATION": "Conservation",
    "INDUSTRY": "Industry",
    "LAND_SPECULATION": "Land speculation",
    "MINING": "Mining",
    "OIL_GAS_EXTRACTION": "Oil / Gas extraction",
    "TOURISM": "Tourism",
    "OTHER": "Other",
}

NEGOTIATION_STATUS = {
    "EXPRESSION_OF_INTEREST": "Intended (Expression of interest)",
    "UNDER_NEGOTIATION": "Intended (Under negotiation)",
    "MEMORANDUM_OF_UNDERSTANDING": "Intended (Memorandum of understanding)",
    "ORAL_AGREEMENT": "Concluded (Oral Agreement)",
    "CONTRACT_SIGNED": "Concluded (Contract signed)",
    "CHANGE_OF_OWNERSHIP": "Concluded (Change of ownership)",
    "NEGOTIATIONS_FAILED": "Failed (Negotiations failed)",
    "CONTRACT_CANCELED": "Failed (Contract cancelled)",
    "CONTRACT_EXPIRED": "Contract expired",
}

IMPLEMENTATION_STATUS = {
    "PROJECT_NOT_STARTED": "Project not started",
    "STARTUP_PHASE": "Startup phase (no production)",
    "IN_OPERATION": "In operation (production)",
    "PROJECT_ABANDONED": "Project abandoned",
}

INVESTOR_CLASSIFICATION = {
    "GOVERNMENT": "Government",
    "GOVERNMENT_INSTITUTION": "Government institution",
    "STATE_OWNED_COMPANY": "State-/government (owned) company",
    "SEMI_STATE_OWNED_COMPANY": "Semi state-owned company",
    "ASSET_MANAGEMENT_FIRM": "Asset management firm",
    "BILATERAL_DEVELOPMENT_BANK": (
        "Bilateral Development Bank / Development Finance Institution"
    ),
    "STOCK_EXCHANGE_LISTED_COMPANY": "Stock-exchange listed company",
    "COMMERCIAL_BANK": "Commercial Bank",
    "INSURANCE_FIRM": "Insurance firm",
    "INVESTMENT_BANK": "Investment Bank",
    "INVESTMENT_FUND": "Investment fund",
    "MULTILATERAL_DEVELOPMENT_BANK": "Multilateral Development Bank (MDB)",
    "PRIVATE_COMPANY": "Private company",
    "PRIVATE_EQUITY_FIRM": "Private equity firm",
    "INDIVIDUAL_ENTREPRENEUR": "Individual entrepreneur",
    "NON_PROFIT": "Non - Profit organization (e.g. Church, University etc.)",
    "OTHER": "Other",
}

LOCATION_ACCURACY = {
    "COUNTRY": "Country",
    "ADMINISTRATIVE_REGION": "Administrative region",
    "APPROXIMATE_LOCATION": "Approximate location",
    "EXACT_LOCATION": "Exact location",
    "COORDINATES": "Coordinates",
}

INVOLVEMENT_ROLE = {
    "PARENT": "Parent company",
    "LENDER": "Tertiary investor/lender",
}

PARENT_RELATION = {
    "SUBSIDIARY": "Subsidiary of parent company",
    "LOCAL_BRANCH": "Local branch of parent company",
    "JOINT_VENTURE": "Joint venture of parent companies",
}

INVESTMENT_TYPE = {
    "EQUITY": "Shares/Equity",
    "DEBT_FINANCING": "Debt financing",
}


# FollowTheMoney schema to use for an investor, by its Land Matrix classification
INVESTOR_SCHEMATA = {
    "GOVERNMENT": "PublicBody",
    "GOVERNMENT_INSTITUTION": "PublicBody",
    "INDIVIDUAL_ENTREPRENEUR": "Person",
    "NON_PROFIT": "Organization",
}
DEFAULT_INVESTOR_SCHEMA = "Company"


def get_label(vocabulary: dict[str, str], code: str | None) -> str | None:
    """Resolve a Land Matrix code, falling back to a humanized version of codes
    that are newer than this module."""
    if not code:
        return None
    label = vocabulary.get(code)
    if label is not None:
        return label
    if "_" in code or len(code) > 3:
        return code.replace("_", " ").capitalize()
    return code


def get_labels(vocabulary: dict[str, str], codes: Iterable[str] | None) -> list[str]:
    """Resolve a list of codes, dropping duplicates but keeping the order."""
    labels = (get_label(vocabulary, code) for code in codes or [])
    return list(dict.fromkeys(label for label in labels if label))
