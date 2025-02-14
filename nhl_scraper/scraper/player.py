"""
NHL Player Data Scraper Module.

This module provides functions to scrape NHL player data including:
- Player statistics
- Career information
- Biographical data
- Game logs

Functions:
    scrapePlayer: Get detailed player information
    scrapePlayerStats: Get player statistics
    scrapePlayerGameLog: Get player game log
"""

import warnings
from datetime import datetime
from typing import Dict, Optional, Union

import numpy as np
import pandas as pd
import requests

warnings.filterwarnings("ignore")


# Scrape Player Profile
def scrapePlayerProfile(player_id: int) -> Dict:
    """
    Scrapes basic player profile information.

    Args:
        player_id (int): NHL player ID

    Returns:
        Dict containing basic player information:
            - firstName, lastName, fullName
            - birthDate, birthCity, birthCountry
            - height, weight
            - shootsCatches
            - position
            - currentTeamInfo
            - draftDetails
    """
    try:
        url = f"https://api-web.nhle.com/v1/player/{player_id}/landing"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Extract essential profile data
        profile = {
            "playerId": player_id,
            "firstName": data["firstName"]["default"],
            "lastName": data["lastName"]["default"],
            "fullName": f"{data['firstName']['default']} {data['lastName']['default']}",
            "birthDate": data.get("birthDate"),
            "birthCity": data.get("birthCity", {}).get("default"),
            "birthStateProvince": data.get("birthStateProvince", {}).get("default"),
            "birthCountry": data.get("birthCountry", {}),
            "heightInInches": data.get("heightInInches"),
            "heightInCentimeters": data.get("heightInCentimeters"),
            "weightInPounds": data.get("weightInPounds"),
            "weightInKilograms": data.get("weightInKilograms"),
            "shootsCatches": data.get("shootsCatches"),
            "position": data.get("position"),
            "currentTeamId": data.get("currentTeamId"),
            "currentTeamAbbrev": data.get("currentTeamAbbrev"),
            "draftDetails": data.get("draftDetails"),
            "meta_datetime": datetime.now(),
            "meta_source": "NHL API",
        }

        return profile

    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to fetch player profile: {str(e)}")
    except Exception as e:
        raise Exception(f"Error processing player data: {str(e)}")


# Scrape Player Stats
def scrapePlayerStats(
    player_id: int, season: Optional[str] = None, stats_type: str = "featuredStats"
) -> pd.DataFrame:
    """
    Scrapes player statistics.

    Args:
        player_id (int): NHL player ID
        season (str, optional): Season in 'YYYYYYYY' format. If None, returns all seasons.
        stats_type (str): Type of stats to return:
            - 'featuredStats': Featured stats for the current season
            - 'careerTotals': Career totals
            - 'last5Games': Last 5 games stats
            - 'seasonTotals': Season totals


    Returns:
        pd.DataFrame: Player statistics based on specified type

    Raises:
        ValueError: If stats_type is invalid
        requests.HTTPError: If API request fails
        Exception: If other error occurs
    """
    try:
        url = f"https://api-web.nhle.com/v1/player/{player_id}/landing"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Validate stats_type
        valid_types = ("featuredStats", "careerTotals", "last5Games", "seasonTotals")
        if stats_type not in valid_types:
            raise ValueError(f"Invalid stats_type. Must be one of: {', '.join(valid_types)}")

        stats = pd.json_normalize(data[stats_type])

        # Add player info
        stats["firstName"] = data.get("firstName", {}).get("default")
        stats["lastName"] = data.get("lastName", {}).get("default")
        stats["fullName"] = f"{data['firstName']['default']} {data['lastName']['default']}"
        stats["positionCode"] = data.get("position")
        stats["position"] = np.where(
            ~stats["positionCode"].isin(["G", "D"]), "F", stats["positionCode"]
        )
        stats["currentTeamId"] = data.get("currentTeamId")
        stats["currentTeamAbbrev"] = data.get("currentTeamAbbrev")

        # Add metadata
        stats["playerId"] = player_id
        stats["meta_datetime"] = datetime.now()
        stats["meta_source"] = "NHL API"

        return stats

    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to fetch player stats: {str(e)}")
    except Exception as e:
        raise Exception(f"Error processing stats data: {str(e)}")


# Scrape Player Game Log
def scrapePlayerGameLog(
    player_id: int, season: str, session_type: Union[str, int] = 2
) -> pd.DataFrame:
    """
    Scrapes player game log for a specific season.

    Args:
        player_id (int): NHL player ID
        season (str): Season in 'YYYYYYYY' format
        session_type (Union[str, int]): Game type:
            - 1 or 'preseason': Preseason games
            - 2 or 'regular': Regular season (default)
            - 3 or 'playoffs': Playoff games

    Returns:
        pd.DataFrame: Game-by-game statistics
    """
    try:
        # Session type mapping
        SESSION_DICT = {"preseason": 1, "regular": 2, "playoffs": 3, 1: 1, 2: 2, 3: 3}

        if session_type not in SESSION_DICT:
            raise ValueError("Invalid session_type. Must be 1/2/3 or preseason/regular/playoffs")

        session_value = SESSION_DICT[session_type]

        url = f"https://api-web.nhle.com/v1/player/{player_id}/game-log/{season}/{session_value}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if "gameLog" not in data:
            raise KeyError("No game log data found")

        gamelog = pd.json_normalize(data["gameLog"])

        # Add player info and metadata
        profile = scrapePlayerProfile(player_id)
        gamelog["playerId"] = player_id
        gamelog["playerName"] = profile["fullName"]
        gamelog["position"] = profile["position"]
        gamelog["meta_datetime"] = datetime.now()
        gamelog["meta_source"] = "NHL API"

        return gamelog

    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to fetch game log: {str(e)}")
    except Exception as e:
        raise Exception(f"Error processing game log: {str(e)}")
