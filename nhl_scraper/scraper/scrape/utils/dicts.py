"""
Dictionaries for the NHL Scraper module.

This module contains dictionaries that are used throughout the NHL Scraper module.
"""

# Draft Rankings Categories
DRAFT_RANKINGS_CATEGORIES = {
    "north-american-skater": 1,
    "international-skater": 2,
    "north-american-goalie": 3,
    "international-goalie": 4,
}

# Sessions Types
SESSION_TYPES = {"PRESEASON": 1, "REGULAR": 2, "PLAYOFFS": 3}

# Session Types reverse
SESSION_TYPES_REVERSE = {1: "PRESEASON", 2: "REGULAR", 3: "PLAYOFFS"}

# NHL Team Colors
NHL_TEAM_COLORS = {
    "description": "The primary, secondary, tertiary, and quaternary colors for each NHL team.",
    "source": "Mainly https://teamcolorcodes.com/nhl-team-color-codes/",
    "data": {
        "ANA": {
            "Primary": "#F47A38",
            "Secondary": "#B9975B",
            "Tertiary": "#000000",
            "Quaternary": "#FFFFFF",
        },
        "ARI": {
            "Primary": "#8C2633",
            "Secondary": "#E2D6B5",
            "Tertiary": "#000000",
            "Quaternary": "#FFFFFF",
        },
        "BOS": {"Primary": "#FFB81C", "Secondary": "#000000", "Tertiary": "#FFFFFF"},
        "BUF": {
            "Primary": "#002654",
            "Secondary": "#FCB514",
            "Tertiary": "#ADAFAA",
            "Quaternary": "#FFFFFF",
        },
        "CGY": {
            "Primary": "#C8102E",
            "Secondary": "#F1BE48",
            "Tertiary": "#000000",
            "Quaternary": "#FFFFFF",
        },
        "CAR": {
            "Primary": "#CC0000",
            "Secondary": "#000000",
            "Tertiary": "#A2AAAD",
            "Quaternary": "#FFFFFF",
        },
        "CHI": {"Primary": "#CF0A2C", "Secondary": "#000000", "Tertiary": "#FFFFFF"},
        "COL": {
            "Primary": "#6F263D",
            "Secondary": "#236192",
            "Tertiary": "#A2AAAD",
            "Quaternary": "#000000",
        },
        "CBJ": {
            "Primary": "#041E42",
            "Secondary": "#C8102E",
            "Tertiary": "#A2AAAD",
            "Quaternary": "#FFFFFF",
        },
        "DAL": {
            "Primary": "#006847",
            "Secondary": "#8F8F8C",
            "Tertiary": "#000000",
            "Quaternary": "#FFFFFF",
        },
        "DET": {"Primary": "#CE1126", "Secondary": "#FFFFFF"},
        "EDM": {"Primary": "#041E42", "Secondary": "#FF4C00", "Tertiary": "#FFFFFF"},
        "FLA": {
            "Primary": "#041E42",
            "Secondary": "#C8102E",
            "Tertiary": "#B9975B",
            "Quaternary": "#FFFFFF",
        },
        "LAK": {"Primary": "#111111", "Secondary": "#A2AAAD", "Tertiary": "#FFFFFF"},
        "MIN": {
            "Primary": "#154734",
            "Secondary": "#A6192E",
            "Tertiary": "#EAAA00",
            "Quaternary": "#FFFFFF",
        },
        "MTL": {"Primary": "#AF1E2D", "Secondary": "#192168", "Tertiary": "#FFFFFF"},
        "NSH": {"Primary": "#FFB81C", "Secondary": "#041E42", "Tertiary": "#FFFFFF"},
        "NJD": {"Primary": "#CE1126", "Secondary": "#000000", "Tertiary": "#FFFFFF"},
        "NYI": {"Primary": "#00539B", "Secondary": "#F47D30", "Tertiary": "#FFFFFF"},
        "NYR": {"Primary": "#0038A8", "Secondary": "#CE1126", "Tertiary": "#FFFFFF"},
        "OTT": {
            "Primary": "#E31837",
            "Secondary": "#C69214",
            "Tertiary": "#000000",
            "Quaternary": "#FFFFFF",
        },
        "PHI": {"Primary": "#F74902", "Secondary": "#000000", "Tertiary": "#FFFFFF"},
        "PIT": {"Primary": "#000000", "Secondary": "#CFC493", "Tertiary": "#FFFFFF"},
        "SJS": {
            "Primary": "#006D75",
            "Secondary": "#EA7200",
            "Tertiary": "#000000",
            "Quaternary": "#FFFFFF",
        },
        "SEA": {
            "Primary": "#001628",
            "Secondary": "#355464",
            "Tertiary": "#99D9D9",
            "Quaternary": "#E9072B",
        },
        "STL": {"Primary": "#002F87", "Secondary": "#FCB514", "Tertiary": "#FFFFFF"},
        "TBL": {"Primary": "#002868", "Secondary": "#FFFFFF"},
        "TOR": {"Primary": "#00205B", "Secondary": "#FFFFFF"},
        "UTA": {"Primary": "#000000", "Secondary": "#FFFFFF", "Tertiary": "#0057B8"},
        "VAN": {
            "Primary": "#00205B",
            "Secondary": "#00843D",
            "Tertiary": "#041C2C",
            "Quaternary": "#FFFFFF",
        },
        "VGK": {
            "Primary": "#B4975A",
            "Secondary": "#333F42",
            "Tertiary": "#000000",
            "Quaternary": "#E31837",
        },
        "WSH": {"Primary": "#041E42", "Secondary": "#C8102E", "Tertiary": "#FFFFFF"},
        "WPG": {
            "Primary": "#041E42",
            "Secondary": "#004C97",
            "Tertiary": "#AC162C",
            "Quaternary": "#7B303E",
        },
    },
}
