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
"""

import warnings
from datetime import datetime
from typing import Dict, Optional, Union

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
            "birthCountry": data.get("birthCountry", {}).get("default"),
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
    player_id: int, season: Optional[str] = None, stats_type: str = "yearByYear"
) -> pd.DataFrame:
    """
    Scrapes player statistics.

    Args:
        player_id (int): NHL player ID
        season (str, optional): Season in 'YYYYYYYY' format. If None, returns all seasons.
        stats_type (str): Type of stats to return:
            - 'yearByYear': Career statistics by season
            - 'careerRegularSeason': Career regular season totals
            - 'careerPlayoffs': Career playoff totals
            - 'homeAndAway': Current season home/away splits
            - 'winLoss': Current season win/loss splits
            - 'byMonth': Current season monthly splits
            - 'byDayOfWeek': Current season day-of-week splits

    Returns:
        pd.DataFrame: Player statistics based on specified type
    """
    try:
        url = f"https://api-web.nhle.com/v1/player/{player_id}/stats/{stats_type}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Process different stat types
        if stats_type == "yearByYear":
            stats = pd.json_normalize(data["splits"])
        else:
            stats = pd.json_normalize(data)

        # Filter for specific season if provided
        if season and stats_type == "yearByYear":
            stats = stats[stats["season"] == season]

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


# Scrape Player Career Highlights
def scrapePlayerCareerHighlights(player_id: int) -> pd.DataFrame:
    """
    Scrapes player career highlights and milestones.

    Args:
        player_id (int): NHL player ID

    Returns:
        pd.DataFrame: Career highlights and milestones
    """
    try:
        url = f"https://api-web.nhle.com/v1/player/{player_id}/landing"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if "careerHighlights" not in data:
            return pd.DataFrame()  # Return empty DataFrame if no highlights

        highlights = pd.json_normalize(data["careerHighlights"])

        # Add metadata
        highlights["playerId"] = player_id
        highlights["meta_datetime"] = datetime.now()
        highlights["meta_source"] = "NHL API"

        return highlights

    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to fetch career highlights: {str(e)}")
    except Exception as e:
        raise Exception(f"Error processing highlights data: {str(e)}")
