"""
Fetch data from the NHL API.
"""

from typing import Union

import pandas as pd
import polars as pl
import requests

from nhl_scraper.scraper.scrape.utils import common, endpoints


def _fetch_schedule(team: str, season: str, format: str = "polars") -> pl.DataFrame | pd.DataFrame:
    """
    Fetch schedule data from the NHL API.
    """
    # Validate team
    if not isinstance(team, str):
        raise ValueError(f"Invalid team: {team}")

    url = endpoints.TEAM_SCHEDULE_URL.format(team=team, season=season)

    try:
        data = common._get_api_data(url, key="games")
        if format == "polars":
            df = pl.json_normalize(data)
        elif format == "pandas":
            df = pd.json_normalize(data)
    except Exception as e:
        raise ValueError(f"Error fetching schedule data: {e}")

    return df


def _fetch_draft(
    year: int, round: Union[int, str], format: str = "polars"
) -> pl.DataFrame | pd.DataFrame:
    """
    Fetch draft data from the NHL API.
    """
    # Validate round
    if round not in ["all", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:
        raise ValueError(f"Invalid round: {round}")

    url = endpoints.DRAFT_PICKS_URL.format(year=year, round=round)
    try:
        data = common._get_api_data(url, key="picks")
        if format == "polars":
            df = pl.json_normalize(data)
        elif format == "pandas":
            df = pd.json_normalize(data)
    except Exception as e:
        raise ValueError(f"Error fetching draft data: {e}")

    return df


def _fetch_draft_rankings(
    year: int, category: str, format: str = "polars"
) -> pl.DataFrame | pd.DataFrame:
    """
    Fetch draft rankings from the NHL API.
    """
    url = endpoints.DRAFT_RANKINGS_URL.format(year=year, category=category)
    try:
        data = common._get_api_data(url, key="rankings")
        if format == "polars":
            df = pl.json_normalize(data)
        elif format == "pandas":
            df = pd.json_normalize(data)

    except Exception as e:
        raise ValueError(f"Error fetching draft rankings: {e}")

    return df


def _fetch_active_teams(format: str = "polars") -> pl.DataFrame | pd.DataFrame:
    """
    Fetch active teams from the NHL API.
    """
    url = endpoints.SCHEDULE_URL
    try:
        data = common._get_api_data(url, key="teams")

        if format == "polars":
            df = pl.json_normalize(data)

        elif format == "pandas":
            df = pd.json_normalize(data)
    except Exception as e:
        raise ValueError(f"Error fetching active teams: {e}")

    return df


def _fetch_team_stats(
    team: str, season: str, sessionId: int, goalies: bool = False, format: str = "polars"
) -> pl.DataFrame | pd.DataFrame:
    """
    Fetch team stats from the NHL API.
    """
    url = endpoints.TEAM_SCORING_URL.format(team=team, season=season, sessionId=sessionId)

    if goalies:
        key = "goalies"
    else:
        key = "skaters"

    try:
        data = common._get_api_data(url, key=key)
        if format == "polars":
            df = pl.json_normalize(data)
        elif format == "pandas":
            df = pd.json_normalize(data)
    except Exception as e:
        raise ValueError(f"Error fetching team stats: {e}")

    return df


def _fetch_team_prospects(team: str, format: str = "polars") -> pl.DataFrame | pd.DataFrame:
    """
    Fetch team prospects from the NHL API.
    """
    url = endpoints.PROSPECTS_URL.format(team=team)
    response = requests.get(url).json()

    try:
        if format == "polars":
            df = pl.concat(
                [pl.json_normalize(response[key]) for key in response.keys()], how="diagonal"
            )
        elif format == "pandas":
            df = pd.concat([pd.json_normalize(response[key]) for key in response.keys()])

    except Exception as e:
        raise ValueError(f"Error fetching team prospects: {e}")

    return df


def _fetch_game_plays(
    gameId: Union[int, str], format: str = "polars"
) -> pl.DataFrame | pd.DataFrame:
    """
    Fetch game plays from the NHL API.
    """
    url = endpoints.GAME_DATA.format(gameId=gameId)
    try:
        response = requests.get(url).json()
        data = response.get("plays", [])
    except Exception as e:
        raise ValueError(f"Error fetching game plays (requests): {e}")

    try:
        if format == "polars":
            df = pl.json_normalize(data)
        elif format == "pandas":
            df = pd.json_normalize(data)

    except Exception as e:
        raise ValueError(f"Error fetching game plays (json_normalize): {e}")

    return df
