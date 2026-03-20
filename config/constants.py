"""
Project-wide constants and enumerations.
"""

VALID_CITY_CODES = {
    "MUM", "DEL", "BLR", "CHN", "HYD",
    "PUN", "KOL", "AMD", "TVM", "KCH",
}

VALID_CATEGORIES = {
    "roads",
    "garbage",
    "water",
    "railways",
    "streetlights",
    "cross_category",
}

VALID_TIERS = {1, 2, 3, 4}

SOURCE_PRIORITY = {
    "pmgsy": 100,
    "nhai": 90,
    "smartcity": 80,
    "eprocure": 70,
    "gem": 60,
    "municipal": 50,
}

REFRESH_FREQUENCIES = {"weekly", "monthly", "quarterly", "annual"}

STATE_CODES = {
    "MH": "Maharashtra",
    "DL": "Delhi",
    "KA": "Karnataka",
    "TN": "Tamil Nadu",
    "TS": "Telangana",
    "WB": "West Bengal",
    "GJ": "Gujarat",
    "KL": "Kerala",
}

CITY_TO_STATE = {
    "MUM": "MH",
    "DEL": "DL",
    "BLR": "KA",
    "CHN": "TN",
    "HYD": "TS",
    "PUN": "MH",
    "KOL": "WB",
    "AMD": "GJ",
    "TVM": "KL",
    "KCH": "KL",
}
