"""
NHL Draft Data Scraper Module.

This module provides functions to scrape NHL draft data including:
- Draft picks for specific years and rounds
- Draft rankings for different categories (NA/International Skaters/Goalies)

Functions:
    scrapeDraft: Retrieves draft picks data
    scrapeDraftLegacy: Retrieves legacy draft data
    scrapeRankings: Retrieves draft rankings data
"""

import warnings
from datetime import datetime
from typing import Union

import pandas as pd
import requests

warnings.filterwarnings("ignore")


# Scrape Draft
def scrapeDraft(year: int = 2023, round: Union[int, str] = "all") -> pd.DataFrame:
    """
    Scrapes draft data from the NHL website for a given year and round.

    Parameters:
      - year (int): The year of the Draft you want to scrape the data from. Default is 2023.
      - round (int/str): The round of the Draft you want to scrape the data from.
                         Can be an integer or "all" to get all rounds. Default is "all".

    Returns:
      - pd.DataFrame: A DataFrame containing the scraped draft data.

    Raises:
      - ValueError: If the year or round is invalid
      - requests.HTTPError: If the API request fails
      - KeyError: If the API response format is unexpected
    """
    # Validate year
    if not isinstance(year, int) or year < 1963:  # NHL draft started in 1963
        raise ValueError(f"Invalid year: {year}. Year must be an integer >= 1963.")

    # Validate round
    if isinstance(round, int):
        # Historical context: NHL draft had up to 25 rounds in the past
        if year < 2005 and (round < 1 or round > 25):
            raise ValueError(
                f"Invalid round: {round}. For years before 2005, round must be between 1-25."
            )
        elif year >= 2005 and (round < 1 or round > 7):
            raise ValueError(
                f"Invalid round: {round}. For years 2005 and later, round must be between 1-7."
            )
    elif round != "all":
        raise ValueError(f"Invalid round: {round}. Round must be an integer or 'all'.")

    try:
        # Make API request
        url = f"https://api-web.nhle.com/v1/draft/picks/{year}/{round}"
        response = requests.get(url)
        response.raise_for_status()  # Raise exception for bad status codes
        data = response.json()

        # Check if expected data is present
        if "picks" not in data:
            raise KeyError("Unexpected API response format: 'picks' key not found")

        # Process data
        draft_df = pd.json_normalize(data["picks"])

        # Add metadata
        draft_df["meta_datetime"] = datetime.now()
        draft_df["meta_year"] = year
        draft_df["meta_round"] = round
        draft_df["meta_source"] = "NHL API"

        return draft_df

    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to fetch draft data: {str(e)}")
    except (KeyError, ValueError) as e:
        raise ValueError(f"Error processing draft data: {str(e)}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {str(e)}")


def scrapeDraftLegacy(draft_year: int = 2023) -> pd.DataFrame:
    """
    Scrapes legacy draft data from the NHL website for a given year.

    Parameters:
        draft_year (int): The year of the Draft you want to scrape the data from. Default is 2023.

    Returns:
        pd.DataFrame: A DataFrame containing the scraped draft data.

    Raises:
        ValueError: If the year or round is invalid
        requests.HTTPError: If the API request fails
        KeyError: If the API response format is unexpected
    """
    try:
        # Base URL for NHL Records API
        base_url = "https://records.nhl.com/site/api/draft"

        # Include parameters for the API request
        includes = [
            "draftProspect.id",
            "player.birthStateProvince",
            "player.birthCountry",
            "player.position",
            "player.onRoster",
            "player.yearsPro",
            "player.firstName",
            "player.lastName",
            "player.id",
            "team.id",
            "team.placeName",
            "team.commonName",
            "team.fullName",
            "team.triCode",
            "team.logos",
            "franchiseTeam.franchise.mostRecentTeamId",
            "franchiseTeam.franchise.teamCommonName",
            "franchiseTeam.franchise.teamPlaceName",
        ]

        # Build the URL with parameters
        url = (
            f"{base_url}"
            f"?{'&'.join(f'include={include}' for include in includes)}"
            f"&cayenneExp=%20draftYear%20=%20{draft_year}"
            f"&start=0"
            f"&limit=500"
        )

        response = requests.get(url)
        response.raise_for_status()  # Raise exception for bad status codes
        data = response.json()

        draft_df = pd.json_normalize(data["data"])

        draft_df["meta_datetime"] = pd.to_datetime("now")
        draft_df["meta_year"] = draft_year
        draft_df["meta_source"] = "NHL Records API"

        return draft_df

    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to fetch draft data: {str(e)}")
    except (KeyError, ValueError) as e:
        raise ValueError(f"Error processing draft data: {str(e)}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {str(e)}")


# Scrape Draft Rankings
def scrapeRankings(year: int = 2025, category: int = 1) -> pd.DataFrame:
    """
    Scrapes draft rankings from the NHL website for a given year and category.

    Parameters:
        year (int): The year of the Draft you want to scrape the data from. Default is 2025.
        category (int): The category of the Draft rankings:
            1 = North American Skaters
            2 = International Skaters
            3 = North American Goalies
            4 = International Goalies

    Returns:
        pd.DataFrame: A DataFrame containing the scraped draft rankings data.

    Raises:
        ValueError: If year or category is invalid
        requests.HTTPError: If the API request fails
        KeyError: If the API response format is unexpected
    """
    # Category mapping for reference and validation
    CATEGORY_DICT = {
        1: "north-american-skater",
        2: "international-skater",
        3: "north-american-goalie",
        4: "international-goalie",
    }

    try:
        # Validate year
        if not isinstance(year, int) or year < 1963:
            raise ValueError(f"Invalid year: {year}. Year must be an integer >= 1963.")

        # Validate category
        if not isinstance(category, int) or category not in CATEGORY_DICT:
            raise ValueError(
                f"Invalid category: {category}. Must be one of {list(CATEGORY_DICT.keys())}"
            )

        # Make API request
        url = f"https://api-web.nhle.com/v1/draft/rankings/{year}/{category}"
        response = requests.get(url)
        response.raise_for_status()  # Raise exception for bad status codes
        data = response.json()

        # Check if expected data is present
        if "rankings" not in data:
            raise KeyError("Unexpected API response format: 'rankings' key not found")

        # Process data
        draft_rankings_df = pd.json_normalize(data["rankings"])

        # Add metadata
        draft_rankings_df["meta_datetime"] = pd.to_datetime("now")
        draft_rankings_df["meta_year"] = year
        draft_rankings_df["meta_category"] = category
        draft_rankings_df["meta_category_name"] = CATEGORY_DICT[category]

        return draft_rankings_df

    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to fetch rankings data: {str(e)}")
    except (KeyError, ValueError) as e:
        raise ValueError(f"Error processing rankings data: {str(e)}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {str(e)}")
