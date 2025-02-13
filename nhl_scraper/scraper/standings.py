"""
NHL Standings Data Scraper Module.

This module provides functions to scrape NHL standings data including:
- League standings
- Division rankings
- Conference standings
- Wild card positions

Functions:
    scrapeStandings: Get standings for specific date
"""

import warnings
from datetime import date, datetime
from typing import Optional, Union

import pandas as pd
import requests

warnings.filterwarnings("ignore")


# Standings
def scrapeLeagueStandings(date_str: Optional[Union[str, date]] = None) -> pd.DataFrame:
    """
    Scrapes NHL standings data for a specific date.

    Args:
        date_str: Date for standings in 'YYYY-MM-DD' format or datetime.date object.
                 If None, uses current date.

    Returns:
        pd.DataFrame: NHL standings data including conference, division, and team statistics.
    """
    try:
        # Process date parameter
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")
        elif isinstance(date_str, date):
            date_str = date_str.strftime("%Y-%m-%d")
        elif not isinstance(date_str, str):
            raise ValueError(
                f"Invalid date type: {type(date_str)}. " "Expected string or datetime.date"
            )

        # Validate date format
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid date format: {date_str}. " "Expected 'YYYY-MM-DD'")

        # Make API request
        url = f"https://api-web.nhle.com/v1/standings/{date_str}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Create DataFrame
        standings_df = pd.json_normalize(data["standings"])

        # Validate essential columns that we know exist
        required_columns = [
            "conferenceAbbrev",
            "conferenceName",
            "divisionAbbrev",
            "divisionName",
            "gamesPlayed",
            "points",
            "wins",
            "losses",
            "otLosses",
            "goalFor",
            "goalAgainst",
            "pointPctg",
            "regulationWins",
            "regulationPlusOtWins",
            "teamName.default",
            "teamAbbrev.default",
        ]

        missing_columns = [col for col in required_columns if col not in standings_df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        # Clean up column names
        rename_map = {
            "teamName.default": "teamName",
            "teamAbbrev.default": "teamAbbrev",
            "placeName.default": "placeName",
            "teamCommonName.default": "teamCommonName",
            "goalFor": "goalsFor",
            "goalAgainst": "goalsAgainst",
        }
        standings_df = standings_df.rename(columns=rename_map)

        # Add metadata
        standings_df["standingsDate"] = date_str
        standings_df["meta_datetime"] = pd.to_datetime("now")
        standings_df["meta_source"] = "NHL API"

        # Sort by conference, division, and points
        standings_df = standings_df.sort_values(
            ["conferenceAbbrev", "divisionAbbrev", "points", "regulationWins"],
            ascending=[True, True, False, False],
        )

        return standings_df

    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to fetch standings data: {str(e)}")
    except (KeyError, ValueError) as e:
        raise ValueError(f"Error processing standings data: {str(e)}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {str(e)}")
