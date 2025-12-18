import requests
from bs4 import BeautifulSoup
import json
import os
import numpy as np
import pandas as pd
import polars as pl
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, Iterator, Literal, Mapping, MutableMapping, Optional, Protocol, Sequence, Tuple, TypeVar, Union, overload, List
import asyncio
from functools import lru_cache
from selectolax.lexbor import LexborHTMLParser
import re 
from itertools import combinations
from collections import defaultdict, Counter, namedtuple
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from collections import defaultdict

import xgboost as xgb
import joblib


from functools import lru_cache
from selectolax.lexbor import LexborHTMLParser
import re 
from itertools import combinations
from collections import defaultdict, Counter, namedtuple
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import logging

# Logging setup
LOG = logging.getLogger(__name__)
if not logging.getLogger().handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

# Constants and session setup
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119 Safari/537.36",
    "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
}
SESSION = requests.Session()
_retries = Retry(
    total=5,
    backoff_factor=0.3,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    raise_on_status=False,
)
_adapter = HTTPAdapter(max_retries=_retries, pool_connections=50, pool_maxsize=50)
SESSION.mount("https://", _adapter)
SESSION.mount("http://", _adapter)
DEFAULT_TIMEOUT = 10  # seconds

# Mapping of NHL event types to standardized codes
EVENT_MAPPING: Dict[str, str] = {
    "blocked-shot": "BLOCK",
    "delayed-penalty": "DELPEN",
    "faceoff": "FAC",
    "giveaway": "GIVE",
    "goal": "GOAL",
    "hit": "HIT",
    "missed-shot": "MISS",
    "penalty": "PENL",
    "shot-on-goal": "SHOT",
    "stoppage": "STOP",
    "takeaway": "TAKE",
    "game-end": "GEND",
    "period-end": "PEND",
    "period-start": "PSTR",
    "shootout-completed": "SOC",
}


# XGBoost model and feature paths
import os
_PKG_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.path.join(_PKG_DIR, "models", "xgboost_xG_model1.json")
FEAT_PATH  = os.path.join(_PKG_DIR, "models", "xgboost_xG_features1.pkl")

# Feature lists
BASE_NUM = [
   "distanceFromGoal","angle_signed","x_norm","y_norm",
    "Per","timeInPeriodSec","timeRemainingSec",
    "strengthDiff","scoreDiff", "timeDiff",
    "shooterSkaters","defendingSkaters",
    "previousEventSameTeam",
    "previousEventDistanceFromGoal","previousEventAngleSigned","previousEventXNorm","previousEventYNorm",
    "shotType","strength", "isRebound","isHome","shootEmptyNet",
    "previousEvent"
    
]
BASE_BOOL = ["isRebound","isHome","shootEmptyNet", "previousEventSameTeam"]
CAT_COLS  = ["shotType","strength", "previousEvent"]  

#  Events considered for xG calculation
EVENTS_FOR_XG = ["GOAL", "SHOT", "MISS"]  

def time_str_to_seconds(time_str: Optional[str]) -> Optional[int]:
    """Convert a time string in 'MM:SS' format to total seconds."""
    if not time_str or not isinstance(time_str, str):
        return None
    try:
        m, s = time_str.split(":")
        return int(m) * 60 + int(s)
    except Exception:
        return None
    
def _group_merge_index(df: pd.DataFrame, keys: Sequence[str], out_col: str = "merge_idx") -> pd.Series:
    """Helper to create a merge index for deduplication."""
    k = df[keys].astype(str).agg("|".join, axis=1)
    return k.groupby(k).cumcount().rename(out_col)

def _dedup_cols(cols: pd.Index) -> pd.Index:
    """Helper to deduplicate column names by appending suffixes."""
    seen: Dict[str, int] = {}
    out: list[str] = []
    for c in cols:
        if c not in seen:
            seen[c] = 0
            out.append(c)
        else:
            seen[c] += 1
            out.append(f"{c}_{seen[c]}")
    return pd.Index(out)

# Helper fetch functions (json and html -- synchronous -- need to add async versions later)
def fetch_json(url: str) -> dict:
    """Fetch JSON data from a URL synchronously with retry/session."""
    try:
        resp = SESSION.get(url, headers=DEFAULT_HEADERS, timeout=DEFAULT_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise Exception(f"Failed to fetch {url}: {e}")

def fetch_html(url, timeout=10000):
    """Fetch HTML content using requests (fast path for static NHL reports).
    Timeout is in milliseconds (kept for backward compat).
    """
    try:
        resp = SESSION.get(url, headers=DEFAULT_HEADERS, timeout=max(0.001, timeout/1000.0))
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

async def fetch_html_async(url, timeout=10000):
    """Async wrapper around fetch_html using a background thread."""
    return await asyncio.to_thread(fetch_html, url, timeout)


# Helper function for converting list of dicts to dataframe with pandas or polars
def json_normalize(data: List[Dict], output_format: str = "pandas") -> pd.DataFrame | pl.DataFrame:
    """
    Normalize nested JSON data to a flat table.

    Parameters:
    - data (List[Dict]): List of dictionaries to normalize.
    - output_format (str): One of ["pandas", "polars"]

    Returns:
    - pd.DataFrame or pl.DataFrame: Normalized data in the specified format.
    """
    if output_format == "pandas":
        return pd.json_normalize(data)
    elif output_format == "polars":
        return pl.json_normalize(data)
    else:
        raise ValueError("output_format must be one of ['pandas', 'polars']")

# Helper PBP functions (normalize coordinates and fetch goal replay data)
def _add_normalized_coordinates(events: List) -> List:
    """Add normalized coordinate system (attacking direction)."""

    for event in events:
        details = event.get('details', {}) if isinstance(event, dict) else {}
        x_coord = details.get('x_coord') or details.get('xCoord')
        y_coord = details.get('y_coord') or details.get('yCoord')

        # Safely coerce to floats (or 0.0 if missing/invalid)
        try:
            xf = float(x_coord)
        except (TypeError, ValueError):
            xf = 0.0
        try:
            yf = float(y_coord)
        except (TypeError, ValueError):
            yf = 0.0

        # Store normalized coordinates
        event["x_normalized"] = xf
        event["y_normalized"] = yf

        # Euclidean distance from (0,0) as a proxy for distance from goal
        event["distance_from_goal"] = (xf ** 2 + yf ** 2) ** 0.5

    return events

def getGoalReplayData(json_url):
    """
    Convert a JSON URL to the NHL goal replay.
    
    Args:
        json_url (str): The URL of the JSON file containing goal data.
        
    Returns:
        list[dict]: A list of dictionaries containing goal replay data.
    """
    goal_url = convert_json_to_goal_url(json_url)
    

    # Custom headers to simulate a browser request
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Referer": goal_url,  # goal URL as the referer
        "Origin": "https://www.nhl.com",
    }

    # Make the request
    response = SESSION.get(json_url, headers={**DEFAULT_HEADERS, **headers}, timeout=DEFAULT_TIMEOUT)
    data = response.json() if response.status_code == 200 else []
    
    
    return data

# Helper function to convert JSON URL to NHL goal replay URL
def convert_json_to_goal_url(json_url):
    parts = json_url.split('/')
    game_id = parts[-2]
    event_id = parts[-1].replace('ev', '').replace('.json', '')
    return f"https://www.nhl.com/ppt-replay/goal/{game_id}/{event_id}"


# Scrape NHL Teams
def getTeamsData(source: str = "default") -> List[Dict]:
    """
    Scrapes NHL team data from various public endpoints and enriches it with metadata to dict format.

    Parameters:
    - source (str): One of ["default", "calendar", "records"]

    Returns:
    - List[Dict]: Raw enriched team data with metadata.
    """
    source_dict = {
        "default": "https://api.nhle.com/stats/rest/en/franchise?sort=fullName&include=lastSeason.id&include=firstSeason.id",
        "calendar": "https://api-web.nhle.com/v1/schedule-calendar/now",
        "records": (
            "https://records.nhl.com/site/api/franchise?"
            "include=teams.id&include=teams.active&include=teams.triCode&"
            "include=teams.placeName&include=teams.commonName&include=teams.fullName&"
            "include=teams.logos&include=teams.conference.name&include=teams.division.name&"
            "include=teams.franchiseTeam.firstSeason.id&include=teams.franchiseTeam.lastSeason.id"
        ),
    }

    if source not in source_dict:
        print(f"[Warning] Invalid source '{source}', falling back to 'default'.")
        source = "default"

    try:
        url = source_dict[source]
        response = fetch_json(url)

        # Normalize nested keys
        if isinstance(response, dict) and "data" in response:
            data = response["data"]
        elif isinstance(response, dict) and "teams" in response:
            data = response["teams"]
        elif isinstance(response, list):
            data = response
        else:
            data = [response]

    except Exception as e:
        raise RuntimeError(f"Error fetching data from {source}: {e}")

    now = datetime.utcnow().isoformat()
    return [
        {**record, "scrapedOn": now, "source": source}
        for record in data
        if isinstance(record, dict)
    ]

def scrapeTeams(source: str = "default", output_format: str = "pandas") -> pd.DataFrame | pl.DataFrame:
    """
    Scrapes NHL team data from various public endpoints and enriches it with metadata.

    Parameters:
    - source (str): One of ["default", "calendar", "records"]
    - output_format (str): One of ["pandas", "polars"]

    Returns:
    - pd.DataFrame or pl.DataFrame: Enriched team data with metadata in the specified format.
    """
    raw_data = getTeamsData(source)
    return json_normalize(raw_data, output_format)

# Scrape NHL Schedule
def getScheduleData(team: str = "MTL", season: Union[str, int] = "20252026") -> List[Dict]:
    """
    Scrapes raw NHL schedule data for a given team and season.

    Parameters:
    - team (str): Team abbreviation (e.g., "MTL")
    - season (str or int): Season ID (e.g., "20242025")

    Returns:
    - List[Dict]: Raw schedule records with metadata
    """
    season = str(season)
    url = f"https://api-web.nhle.com/v1/club-schedule-season/{team}/{season}"

    try:
        response = fetch_json(url)

        if isinstance(response, dict) and "games" in response:
            data = response["games"]
        elif isinstance(response, list):
            data = response
        else:
            raise ValueError(f"Unexpected response format: {response}")

    except Exception as e:
        raise RuntimeError(f"Error fetching schedule data: {e}")

    now = datetime.utcnow().isoformat()
    return [
        {**record, "scrapedOn": now, "source": "NHL Schedule API"}
        for record in data
        if isinstance(record, dict)
    ]

def scrapeSchedule(team: str = "MTL", season: Union[str, int] = "20252026", output_format: str = "pandas") -> pd.DataFrame | pl.DataFrame:
    """
    Scrapes NHL schedule data for a given team and season.

    Parameters:
    - team (str): Team abbreviation (e.g., "MTL")
    - season (str or int): Season ID (e.g., "20242025")
    - output_format (str): One of ["pandas", "polars"]

    Returns:
    - pd.DataFrame or pl.DataFrame: Schedule data with metadata in the specified format.
    """
    raw_data = getScheduleData(team, season)
    return json_normalize(raw_data, output_format)

# Scrape NHL Standings
def getStandingsData(date: str = None) -> List[Dict]:
    """
    Scrapes NHL standings data for a given date.

    Parameters:
    - date (str, optional): Date in 'YYYY-MM-DD' format. Defaults to None (previous years' new year).

    Returns:
    - List[Dict]: Raw standings records with metadata
    """

    # If no date is provided, use the previous year's new year's date
    if date is None:
        date = f"{(datetime.utcnow() - pd.DateOffset(years=1)).strftime('%Y')}-01-01"

    url = f"https://api-web.nhle.com/v1/standings/{date}"

    try:
        response = fetch_json(url)

        if isinstance(response, dict) and "standings" in response:
            data = response["standings"]
        elif isinstance(response, list):
            data = response
        else:
            raise ValueError(f"Unexpected response format: {response}")

    except Exception as e:
        raise RuntimeError(f"Error fetching standings data: {e}")

    now = datetime.utcnow().isoformat()
    return [
        {**record, "scrapedOn": now, "source": "NHL Standings API"}
        for record in data
        if isinstance(record, dict)
    ]

def scrapeStandings(date: str = None, output_format: str = "pandas") -> pd.DataFrame | pl.DataFrame:
    """
    Scrapes NHL standings data for a given date.

    Parameters:
    - date (str, optional): Date in 'YYYY-MM-DD' format. Defaults to None (previous years' new year).
    - output_format (str): One of ["pandas", "polars"]

    Returns:
    - pd.DataFrame or pl.DataFrame: Standings data with metadata in the specified format.
    """
    raw_data = getStandingsData(date)
    return json_normalize(raw_data, output_format)

# Scrape NHL Roster
def getRosterData(team: str = "MTL", season: Union[str, int] = "20242025") -> List[Dict]:
    """
    Scrapes NHL roster data for a given team and season.

    Parameters:
    - team (str): Team abbreviation (e.g., "MTL")
    - season (str or int): Season ID (e.g., "20242025")

    Returns:
    - List[Dict]: Raw roster records with metadata
    """
    season = str(season)
    url = f"https://api-web.nhle.com/v1/roster/{team}/{season}"

    try:
        response = fetch_json(url)

        data = [
            {**record}  # optional: create a shallow copy
            for key, value in response.items()
            if isinstance(value, list)
            for record in value
            if isinstance(record, dict)
        ]

    except Exception as e:
        raise RuntimeError(f"Error fetching roster data: {e}")

    now = datetime.utcnow().isoformat()
    return [
        {**record, "scrapedOn": now, "source": "NHL Roster API"}
        for record in data
        if isinstance(record, dict)
    ]

def scrapeRoster(team: str = "MTL", season: Union[str, int] = "20242025", output_format: str = "pandas") -> pd.DataFrame | pl.DataFrame:
    """
    Scrapes NHL roster data for a given team and season.

    Parameters:
    - team (str): Team abbreviation (e.g., "MTL")
    - season (str or int): Season ID (e.g., "20242025")
    - output_format (str): One of ["pandas", "polars"]

    Returns:
    - pd.DataFrame or pl.DataFrame: Roster data with metadata in the specified format.
    """
    raw_data = getRosterData(team, season)
    return json_normalize(raw_data, output_format)


# Scrape Team Stats
def getTeamStatsData(
    team: str = "MTL",
    season: Union[str, int] = "20252026",
    session: Union[str, int] = 2,
    goalies: bool = False,
) -> List[Dict]:
    """
    Scrapes NHL team statistics for a given team and season.

    Parameters:
    - team (str): Team abbreviation (e.g., "MTL")
    - season (str or int): Season ID (e.g., "20242025")
    - session (str or int): Session ID (default is 2) - 1 for pre-season, 2 for regular season, 3 for playoffs

    Returns:
    - List[Dict]: Raw team statistics records with metadata
    """
    season = str(season)
    url = f"https://api-web.nhle.com/v1/club-stats/{team}/{season}/{session}"

    key = "goalies" if goalies else "skaters"

    # print(f"Fetching team stats for team: {team}, season: {season}, session: {session} from {url}")

    try:
        response = fetch_json(url)

        if isinstance(response, dict) and key in response:
            data = response[key]
        elif isinstance(response, list):
            data = response
        else:
            raise ValueError(f"Unexpected response format: {response}")

    except Exception as e:
        raise RuntimeError(f"Error fetching team stats data: {e}")

    now = datetime.utcnow().isoformat()
    return [
        {**record, "scrapedOn": now, "source": "NHL Team Stats API"}
        for record in data
        if isinstance(record, dict)
    ]

def scrapeTeamStats(
    team: str = "MTL",
    season: Union[str, int] = "20252026",
    session: Union[str, int] = 2,
    goalies: bool = False,
    output_format: str = "pandas",
) -> pd.DataFrame | pl.DataFrame:
    """
    Scrapes NHL team statistics for a given team and season.

    Parameters:
    - team (str): Team abbreviation (e.g., "MTL")
    - season (str or int): Season ID (e.g., "20242025")
    - session (str or int): Session ID (default is 2) - 1 for pre-season, 2 for regular season, 3 for playoffs
    - goalies (bool): Whether to fetch goalie stats (default is False for skaters)
    - output_format (str): One of ["pandas", "polars"]

    Returns:
    - pd.DataFrame or pl.DataFrame: Team statistics data with metadata in the specified format.
    """
    raw_data = getTeamStatsData(team, season, session, goalies)
    return json_normalize(raw_data, output_format)


# Scrape NHL Draft Data
def getDraftDataData(year: Union[str, int] = "2024", round: Union[str, int] = "all") -> List[Dict]:
    """
    Scrapes NHL draft data for a given season.

    Parameters:
    - season (str or int): Season ID (e.g., "2024")
    - round (str or int): Round number (default is "all" for all rounds)

    Returns:
    - List[Dict]: Raw draft records with metadata
    """
    year = str(year)
    url = f"https://api-web.nhle.com/v1/draft/picks/{year}/{round}"

    # print(f"Fetching draft data for season: {year} round: {round} from {url}")

    try:
        response = fetch_json(url)

        if isinstance(response, dict) and "picks" in response:
            data = response["picks"]
        elif isinstance(response, list):
            data = response
        else:
            raise ValueError(f"Unexpected response format: {response}")

    except Exception as e:
        raise RuntimeError(f"Error fetching draft data: {e}")

    now = datetime.utcnow().isoformat()
    return [
        {**record, "year": year, "scrapedOn": now, "source": "NHL Draft API"}
        for record in data
        if isinstance(record, dict)
    ]

def scrapeDraftData(year: Union[str, int] = "2024", round: Union[str, int] = "all", output_format: str = "pandas") -> pd.DataFrame | pl.DataFrame:
    """
    Scrapes NHL draft data for a given season.

    Parameters:
    - season (str or int): Season ID (e.g., "2024")
    - round (str or int): Round number (default is "all" for all rounds)
    - output_format (str): One of ["pandas", "polars"]

    Returns:
    - pd.DataFrame or pl.DataFrame: Draft data with metadata in the specified format.
    """
    raw_data = getDraftDataData(year, round)
    return json_normalize(raw_data, output_format)


# Scrape NHL Draft Records
def getRecordsDraftData(year: Union[str, int] = "2025") -> List[Dict]:
    """
    Scrapes NHL draft records for a given season from NHL Records API.

    Parameters:
    - year (str or int): Season ID (e.g., "2024")

    Returns:
    - List[Dict]: Raw draft records with metadata
    """
    year = str(year)
    url = f"https://records.nhl.com/site/api/draft?include=draftProspect.id&include=player.birthStateProvince&include=player.birthCountry&include=player.position&include=player.onRoster&include=player.yearsPro&include=player.firstName&include=player.lastName&include=player.id&include=team.id&include=team.placeName&include=team.commonName&include=team.fullName&include=team.triCode&include=team.logos&include=franchiseTeam.franchise.mostRecentTeamId&include=franchiseTeam.franchise.teamCommonName&include=franchiseTeam.franchise.teamPlaceName&cayenneExp=%20draftYear%20=%20{year}&start=0&limit=500"

    try:
        response = fetch_json(url)

        if isinstance(response, dict) and "data" in response:
            data = response["data"]
        elif isinstance(response, list):
            data = response
        else:
            raise ValueError(f"Unexpected response format: {response}")

    except Exception as e:
        raise RuntimeError(f"Error fetching draft records: {e}")

    now = datetime.utcnow().isoformat()
    return [
        {**record, "year": year, "scrapedOn": now, "source": "NHL Draft Records API"}
        for record in data
        if isinstance(record, dict)
    ]

def scrapeDraftRecords(year: Union[str, int] = "2025", output_format: str = "pandas") -> pd.DataFrame | pl.DataFrame:
    """
    Scrapes NHL draft records for a given season from NHL Records API.

    Parameters:
    - year (str or int): Season ID (e.g., "2024")
    - output_format (str): One of ["pandas", "polars"]

    Returns:
    - pd.DataFrame or pl.DataFrame: Draft records data with metadata in the specified format.
    """
    raw_data = getRecordsDraftData(year)
    return json_normalize(raw_data, output_format)


# Scrape NHL Team Draft History
def getRecordsTeamDraftHistoryData(franchise: Union[str, int] = 1) -> List[Dict]:
    """
    Scrapes NHL team draft history for a given franchise.

    Parameters:
    - franchise (str or int): Franchise ID

    Returns:
    - List[Dict]: Raw draft history records with metadata
    """
    franchise = str(franchise)
    url = f"https://records.nhl.com/site/api/draft?include=draftProspect.id&include=franchiseTeam&include=player.birthStateProvince&include=player.birthCountry&include=player.position&include=player.onRoster&include=player.yearsPro&include=player.firstName&include=player.lastName&include=player.id&include=team.id&include=team.placeName&include=team.commonName&include=team.fullName&include=team.triCode&include=team.logos&cayenneExp=franchiseTeam.franchiseId=%22{franchise}%22"
    LOG.info(f"Fetching team draft history for franchise: {franchise} from {url}")

    try:
        response = fetch_json(url)

        if isinstance(response, dict) and "data" in response:
            data = response["data"]
        elif isinstance(response, list):
            data = response
        else:
            raise ValueError(f"Unexpected response format: {response}")

    except Exception as e:
        raise RuntimeError(f"Error fetching team draft history: {e}")

    now = datetime.utcnow().isoformat()
    return [
        {**record, "scrapedOn": now, "source": "NHL Team Draft History API"}
        for record in data
        if isinstance(record, dict)
    ]

def scrapeTeamDraftHistory(franchise: Union[str, int] = 1, output_format: str = "pandas") -> pd.DataFrame | pl.DataFrame:
    """
    Scrapes NHL team draft history for a given franchise from NHL Records API.

    Parameters:
    - franchise (str or int): Franchise ID
    - output_format (str): One of ["pandas", "polars"]

    Returns:
    - pd.DataFrame or pl.DataFrame: Team draft history data with metadata in the specified format.
    """
    raw_data = getRecordsTeamDraftHistoryData(franchise)
    return json_normalize(raw_data, output_format)


def getGameData(game: Union[str, int], addGoalReplayData: bool = False) -> Dict:
    """Scrape NHL play-by-play data and enrich with metadata."""
    game = str(game)
    url = f"https://api-web.nhle.com/v1/gamecenter/{game}/play-by-play"
    now = datetime.utcnow().isoformat()
    data = {}

    try:
        response = fetch_json(url)
        if not isinstance(response, dict) or not response:
            raise ValueError(f"Unexpected response format: {response}")
        
        data = response
        extra_keys = ['gameDate', 'gameType', 'startTimeUTC', 'easternUTCOffset', 'venueUTCOffset']

        enriched_plays = []
        for play in data.get('plays', []):
            ppt_data = None
            if addGoalReplayData and play.get('pptReplayUrl'):
                ppt_data = getGoalReplayData(play['pptReplayUrl'])

            enriched_play = {
                **play,
                'pptReplayData': ppt_data,
                'gameId': data.get('id'),
                'venue': data.get('venue', {}).get('default'),
                'venueLocation': data.get('venueLocation', {}).get('default'),
                'scrapedOn': now,
                'source': 'NHL Play-by-Play API',
                **{key: data.get(key) for key in extra_keys}
            }
            enriched_plays.append(enriched_play)

        # data['plays'] = _add_normalized_coordinates(enriched_plays)

    except Exception as e:
        raise RuntimeError(f"Error fetching play-by-play data: {e}")

    data['scrapedOn'] = now
    data['source'] = 'NHL Play-by-Play API'
    return data

@lru_cache(maxsize=1000)
def scrapePlays(game: Union[str, int], addGoalReplayData: bool = False, output_format: str = "pandas") -> pd.DataFrame | pl.DataFrame:
    """
    Scrapes NHL game data from API for a given game ID.

    Parameters:
    - game (str or int): Game ID
    - output_format (str): One of ["pandas", "polars"]

    Returns:
    - pd.DataFrame or pl.DataFrame: Play-by-play data including enriched play records with metadata in the specified format.
    """
    raw_data = getGameData(game, addGoalReplayData)
    plays = raw_data.get('plays', [])
    return json_normalize(plays, output_format)


def scrapeHtmlPbp(game: Union[str, int]) -> Dict:
    """
    Synchronously fetches NHL play-by-play data from HTML for a given game ID.

    Parameters:
    - game (str or int): Game ID

    Returns:
    - Dict: Contains both home and away team HTML play-by-play data
    """
    game_id = str(game)

    
    short_id = game_id[-6:].zfill(6)
    first_year = game_id[:4]
    second_year = str(int(first_year) + 1)

    url = f"https://www.nhl.com/scores/htmlreports/{first_year}{second_year}/PL{short_id}.HTM"

    # print(f"Fetching play-by-play HTML data for game: {game_id}")
    

    try:
        # Fetch both home and away team HTML play-by-play data
        game_html = fetch_html(url)

        if not game_html:
            raise ValueError(f"No HTML play-by-play data found for game {game_id}")

        # Return structured data with keys expected by pipeline
        result = {
            "data": game_html,
            "urls": {"home": url, "away": url},
            "game_id": game_id,
            "scraped_on": datetime.utcnow().isoformat(),
            "source": "NHL HTML Play-by-Play Reports",
        }

        # print(f"✅ Successfully fetched HTML play-by-play data for game {game_id}")
        return result

    except Exception as e:
        raise RuntimeError(f"Error fetching HTML play-by-play data for game {game_id}: {e}")

async def scrapeHtmlPbp_async(game: Union[str, int]) -> Dict:
    """
    Asynchronously fetches NHL play-by-play data from HTML for a given game ID.

    Parameters:
    - game (str or int): Game ID

    Returns:
    - Dict: Contains both home and away team HTML play-by-play data
    """
    game_id = str(game)

    
    short_id = game_id[-6:].zfill(6)
    first_year = game_id[:4]
    second_year = str(int(first_year) + 1)

    url = f"https://www.nhl.com/scores/htmlreports/{first_year}{second_year}/PL{short_id}.HTM"

    # print(f"Fetching play-by-play HTML data for game: {game_id}")
    

    try:
        # Fetch both home and away team HTML play-by-play data
        game_html = await fetch_html_async(url)

        if not game_html:
            raise ValueError(f"No HTML play-by-play data found for game {game_id}")

        # Return structured data with keys expected by pipeline
        result = {
            "data": game_html,
            "urls": {"home": url, "away": url},
            "game_id": game_id,
            "scraped_on": datetime.utcnow().isoformat(),
            "source": "NHL HTML Play-by-Play Reports",
        }

        # print(f"✅ Successfully fetched HTML play-by-play data for game {game_id}")
        return result

    except Exception as e:
        raise RuntimeError(f"Error fetching HTML play-by-play data for game {game_id}: {e}")
  
  
def scrapeHTMLShifts(game: Union[str, int]) -> Dict:
    """
    Scrapes NHL shifts data from HTML for a given game ID.

    This scraper fetches HTML shift reports for both home and away teams,
    following the pattern from scraper_pandas.py for comprehensive shift data.

    Parameters:
    - game (str or int): Game ID

    Returns:
    - Dict: Contains both home and away team HTML shift data
    """
    game_id = str(game)

    # Generate URLs for home (TH) and away (TV) team shift reports
    short_id = game_id[-6:].zfill(6)
    first_year = game_id[:4]
    second_year = str(int(first_year) + 1)

    url_home = f"https://www.nhl.com/scores/htmlreports/{first_year}{second_year}/TH{short_id}.HTM"
    url_away = f"https://www.nhl.com/scores/htmlreports/{first_year}{second_year}/TV{short_id}.HTM"

    # print(f"Fetching shifts HTML data for game: {game_id}")
    # print(f"  Home team URL: {url_home}")
    # print(f"  Away team URL: {url_away}")

    try:
        # Fetch both home and away team HTML shift data
        html_home = fetch_html(url_home)
        html_away = fetch_html(url_away)

        if not html_home and not html_away:
            raise ValueError(f"No HTML shifts data found for game {game_id}")

        # Return structured data with keys expected by pipeline
        result = {
            "home": html_home,
            "away": html_away,
            "urls": {"home": url_home, "away": url_away},
            "game_id": game_id,
            "scraped_on": datetime.utcnow().isoformat(),
            "source": "NHL HTML Shifts Reports",
        }

        # print(f"✅ Successfully fetched HTML shifts data for game {game_id}")
        return result

    except Exception as e:
        raise RuntimeError(f"Error fetching HTML shifts data for game {game_id}: {e}")  

async def scrapeHTMLShifts_async(game: Union[str, int]) -> Dict:
    """
    Async version: Scrapes NHL shifts data from HTML for a given game ID.

    This scraper fetches HTML shift reports for both home and away teams,
    following the pattern from scraper_pandas.py for comprehensive shift data.

    Parameters:
    - game (str or int): Game ID

    Returns:
    - Dict: Contains both home and away team HTML shift data
    """
    game_id = str(game)

    # Generate URLs for home (TH) and away (TV) team shift reports
    short_id = game_id[-6:].zfill(6)
    first_year = game_id[:4]
    second_year = str(int(first_year) + 1)

    url_home = f"https://www.nhl.com/scores/htmlreports/{first_year}{second_year}/TH{short_id}.HTM"
    url_away = f"https://www.nhl.com/scores/htmlreports/{first_year}{second_year}/TV{short_id}.HTM"

    # print(f"Fetching shifts HTML data for game: {game_id}")
    # print(f"  Home team URL: {url_home}")
    # print(f"  Away team URL: {url_away}")

    try:
        # Fetch both home and away team HTML shift data
        html_home = await fetch_html_async(url_home)
        html_away = await fetch_html_async(url_away)

        if not html_home and not html_away:
            raise ValueError(f"No HTML shifts data found for game {game_id}")

        # Return structured data with keys expected by pipeline
        result = {
            "home": html_home,
            "away": html_away,
            "urls": {"home": url_home, "away": url_away},
            "game_id": game_id,
            "scraped_on": datetime.utcnow().isoformat(),
            "source": "NHL HTML Shifts Reports",
        }

        # print(f"✅ Successfully fetched HTML shifts data for game {game_id}")
        return result

    except Exception as e:
        raise RuntimeError(f"Error fetching HTML shifts data for game {game_id}: {e}")  

# Parse HTML PBP using Lexbor
def parse_html_pbp(html: str) -> Dict[str, Any]:
    """
    Parse HTML content using Lexbor HTML parser to extract PBP event data and on-ice info.

    Args:
        html (str): The HTML content to parse.

    Returns:
        dict: Parsed data including events, columns, and on-ice/goalie information.
    """
    if not html or not html.strip():
        raise ValueError("HTML content cannot be empty")

    try:
        parser = LexborHTMLParser(html)
        table = parser.css("tr.oddColor, tr.evenColor")

        if not table:
            LOG.warning("No play-by-play rows found in HTML")
            return _empty_result()

        data = []
        home_on_ice, away_on_ice = [], []
        home_goalie, away_goalie = [], []

        for row in table:
            cells = [td.text(strip=True) for td in row.css("td")]

            # Find embedded tables indicating on-ice players
            on_ice_raw = [
                el.text(strip=True)
                for el in row.css("td > table > tbody")
                if len(el.text(strip=True)) > 5
            ]

            skater_lists, goalie_lists = _parse_on_ice_players(on_ice_raw)

            # Ensure we have exactly 2 teams or handle missing data
            if len(skater_lists) == 2 and len(goalie_lists) == 2:
                away_on_ice.append(skater_lists[0])
                home_on_ice.append(skater_lists[1])
                home_goalie.append(goalie_lists[1])
                away_goalie.append(goalie_lists[0])
            else:
                # Handle missing or incomplete on-ice data
                away_on_ice.append([])
                home_on_ice.append([])
                home_goalie.append([])
                away_goalie.append([])

            # Process cell data with proper bounds checking
            if cells and len(cells) > 0:
                cells_data = _clean_cell_data(cells)
                if cells_data:  # Only add if we have valid data
                    data.append(cells_data)

        columns = ["#", "Per", "Str", "Time:Elapsed Game", "Event", "Description"]

        return {
            "data": data,
            "columns": columns,
            "home_on_ice": home_on_ice,
            "away_on_ice": away_on_ice,
            "home_goalie": home_goalie,
            "away_goalie": away_goalie,
        }

    except Exception as e:
        raise RuntimeError(f"Error parsing HTML play-by-play data: {e}")


def _parse_on_ice_players(on_ice_raw: List[str]) -> tuple[List[List[str]], List[List[str]]]:
    """
    Parse on-ice player strings to extract skater and goalie numbers.

    Args:
        on_ice_raw: List of raw on-ice player strings (usually 2 teams)

    Returns:
        tuple: (skater_lists, goalie_lists) for each team
    """
    skater_lists, goalie_lists = [], []

    for team_str in on_ice_raw:
        if not team_str.strip():
            continue

        # NHL HTML format is like: "18C71C7L3D72D35G"
        # Where numbers+letter indicate: 18C (center), 71C (center), 7L (left wing),
        # 3D (defense), 72D (defense), 35G (goalie)

        # Split by position letters to get individual players
        # Pattern: number + letter (C|L|R|D|G)
        players = re.findall(r"(\d+)([CLRDG])", team_str)

        skaters = []
        goalies = []

        for number, position in players:
            if position == "G":  # Goalie
                goalies.append(number)
            else:  # Skater (C, L, R, D)
                skaters.append(number)

        skater_lists.append(skaters)
        goalie_lists.append(goalies if goalies else [])  # Ensure list even if empty

    return skater_lists, goalie_lists


def _clean_cell_data(cells: List[str]) -> List[str]:
    """
    Clean and validate cell data from play-by-play rows.

    Args:
        cells: Raw cell data

    Returns:
        Cleaned cell data (first 6 columns)
    """
    if not cells:
        return []

    # Clean each cell and take first 6 columns
    cleaned_cells = []
    for i, cell in enumerate(cells[:6]):  # Limit to 6 columns
        if cell:
            # Replace various types of non-breaking spaces and clean
            cleaned_cell = (
                cell.replace("\xa0", " ").replace("\u00a0", " ").replace("\u2009", " ").strip()
            )
            cleaned_cells.append(cleaned_cell)
        else:
            cleaned_cells.append("")  # Ensure we maintain column structure

    # Pad to 6 columns if needed
    while len(cleaned_cells) < 6:
        cleaned_cells.append("")

    return cleaned_cells


def _empty_result() -> Dict[str, Any]:
    """Return empty result structure when no data is found."""
    return {
        "data": [],
        "columns": ["#", "Per", "Str", "Time:Elapsed Game", "Event", "Description"],
        "home_on_ice": [],
        "away_on_ice": [],
        "home_goalie": [],
        "away_goalie": [],
    }


def parse_html_rosters(html: str) -> Dict[str, Any]:
    """
    Parse HTML content to extract NHL game roster information.

    Args:
        html (str): The HTML content from NHL roster report.

    Returns:
        dict: Parsed roster data including home/away players, scratches, coaches, and game info.
    """
    if not html or not html.strip():
        raise ValueError("HTML content cannot be empty")

    try:
        parser = LexborHTMLParser(html)

        # Extract game information
        game_info = _parse_game_info(parser)

        # Extract rosters for both teams
        home_roster = _parse_team_roster(parser, "home")
        away_roster = _parse_team_roster(parser, "away")

        # Extract officials information
        officials = _parse_officials(parser)

        return {
            "home": home_roster,
            "away": away_roster,
            "officials": officials,
            "gameInfo": game_info,
        }

    except Exception as e:
        raise ValueError(f"Failed to parse roster HTML: {e}")


def _parse_game_info(parser: LexborHTMLParser) -> Dict[str, str]:
    """Extract game information from the HTML."""
    try:
        import re
        from datetime import datetime

        # Game info is typically in a table with ID "GameInfo"
        game_info = {}

        # Try to find the game info table
        game_table = parser.css_first("#GameInfo")
        if game_table:
            rows = game_table.css("tr")
            for row in rows:
                cells = row.css("td")
                if len(cells) >= 2:
                    label = cells[0].text(strip=True)
                    value = cells[1].text(strip=True)
                    if label and value:
                        game_info[label.lower().replace(" ", "_")] = value

        # Fallback to specific selectors if the table approach doesn't work
        if not game_info:
            selectors = {
                "date": "#GameInfo > tbody > tr:nth-child(4) > td",
                "attendance_venue": "#GameInfo > tbody > tr:nth-child(5) > td",
                "start_end": "#GameInfo > tbody > tr:nth-child(6) > td",
            }

            for key, selector in selectors.items():
                element = parser.css_first(selector)
                if element:
                    game_info[key] = element.text(strip=True)

        # Parse and convert date to datetime object
        if "date" in game_info:
            date_text = game_info["date"]
            try:
                # Parse date like "Friday, November 1, 2024"
                parsed_date = datetime.strptime(date_text, "%A, %B %d, %Y")
                game_info["date"] = parsed_date.isoformat()
                game_info["date_raw"] = date_text  # Keep original for reference
            except ValueError:
                # If parsing fails, keep original text
                game_info["date_raw"] = date_text
                LOG.warning(f"Could not parse date '{date_text}'")

        # Parse and separate attendance and venue
        if "attendance_venue" in game_info:
            attendance_venue_text = game_info["attendance_venue"]
            # Pattern: "Attendance 18,006 at Madison Square Garden"
            attendance_match = re.search(
                r"Attendance\s+([\d,]+)", attendance_venue_text, re.IGNORECASE
            )
            venue_match = re.search(r"at\s+(.+)$", attendance_venue_text, re.IGNORECASE)

            if attendance_match:
                # Remove commas and convert to clean number string
                game_info["attendance"] = attendance_match.group(1).replace(",", "")
            if venue_match:
                game_info["venue"] = venue_match.group(1).strip()

            # Remove the combined field
            del game_info["attendance_venue"]

        # Parse and separate start and end times with datetime conversion
        if "start_end" in game_info:
            start_end_text = game_info["start_end"]
            # Pattern: "Start 7:08 EDT; End 9:38 EDT" or "Start 7:08 PM EDT; End 9:38 PM EDT"
            start_match = re.search(r"Start\s+([^;]+)", start_end_text, re.IGNORECASE)
            end_match = re.search(r"End\s+(.+)$", start_end_text, re.IGNORECASE)

            if start_match:
                start_time_text = start_match.group(1).strip()
                game_info["start_time_raw"] = start_time_text

                # Try to parse the time to datetime (assuming current date as base)
                try:
                    # Extract time and timezone
                    time_pattern = r"(\d{1,2}:\d{2})(?:\s*(AM|PM))?\s*([A-Z]{3,4})?"
                    time_tz_match = re.search(time_pattern, start_time_text, re.IGNORECASE)
                    if time_tz_match:
                        time_str = time_tz_match.group(1)
                        am_pm = time_tz_match.group(2)
                        timezone = time_tz_match.group(3)

                        # Create a time format string
                        if am_pm:
                            time_format = f"{time_str} {am_pm}"
                            parsed_time = datetime.strptime(time_format, "%I:%M %p")
                        else:
                            parsed_time = datetime.strptime(time_str, "%H:%M")

                        # Combine with game date if available
                        if "date" in game_info and game_info["date"] != game_info.get("date_raw"):
                            game_date = datetime.fromisoformat(game_info["date"])
                            combined_datetime = game_date.replace(
                                hour=parsed_time.hour, minute=parsed_time.minute
                            )
                            game_info["start_time"] = combined_datetime.isoformat()
                        else:
                            # Just store the time part
                            game_info["start_time"] = parsed_time.time().isoformat()

                        if timezone:
                            game_info["start_timezone"] = timezone
                except ValueError:
                    # If parsing fails, keep original text
                    game_info["start_time"] = start_time_text
                    LOG.warning(f"Could not parse start time '{start_time_text}'")

            if end_match:
                end_time_text = end_match.group(1).strip()
                game_info["end_time_raw"] = end_time_text

                # Try to parse the end time
                try:
                    time_pattern = r"(\d{1,2}:\d{2})(?:\s*(AM|PM))?\s*([A-Z]{3,4})?"
                    time_tz_match = re.search(time_pattern, end_time_text, re.IGNORECASE)
                    if time_tz_match:
                        time_str = time_tz_match.group(1)
                        am_pm = time_tz_match.group(2)
                        timezone = time_tz_match.group(3)

                        if am_pm:
                            time_format = f"{time_str} {am_pm}"
                            parsed_time = datetime.strptime(time_format, "%I:%M %p")
                        else:
                            parsed_time = datetime.strptime(time_str, "%H:%M")

                        # Combine with game date if available
                        if "date" in game_info and game_info["date"] != game_info.get("date_raw"):
                            game_date = datetime.fromisoformat(game_info["date"])
                            combined_datetime = game_date.replace(
                                hour=parsed_time.hour, minute=parsed_time.minute
                            )
                            game_info["end_time"] = combined_datetime.isoformat()
                        else:
                            game_info["end_time"] = parsed_time.time().isoformat()

                        if timezone:
                            game_info["end_timezone"] = timezone
                except ValueError:
                    game_info["end_time"] = end_time_text
                    LOG.warning(f"Could not parse end time '{end_time_text}'")

            # Remove the combined field
            del game_info["start_end"]

        return game_info

    except Exception as e:
        LOG.warning(f"Could not parse game info: {e}")
        return {}


def _parse_team_roster(parser: LexborHTMLParser, team: str) -> Dict[str, Any]:
    """Extract roster information for a specific team."""
    try:
        team_data = {"roster": [], "scratches": [], "head_coach": "", "goalies": [], "skaters": []}

        # Find tables that contain player roster data
        # These tables typically have rows with 3 columns: #, Pos, Name
        all_tables = parser.css("table")
        roster_tables = []

        for table in all_tables:
            table_text = table.text(strip=True)
            # Check if this table contains roster structure and player names
            has_roster_header = "#PosName" in table_text or (
                "Pos" in table_text and "Name" in table_text
            )
            has_senators = any(name in table_text for name in ["TKACHUK", "STÜTZLE", "CHABOT"])
            has_rangers = any(name in table_text for name in ["PANARIN", "ZIBANEJAD", "SHESTERKIN"])

            # Count 3-column player rows
            player_row_count = 0
            rows = table.css("tr")
            for row in rows:
                cells = row.css("td")
                if len(cells) == 3:
                    cell_texts = [cell.text(strip=True) for cell in cells]
                    if (
                        cell_texts[0].isdigit()
                        and cell_texts[1] in "CLDGR"
                        and len(cell_texts[2]) > 3
                    ):
                        player_row_count += 1

            # Table is a roster table if it has the header and significant player rows
            if has_roster_header and player_row_count >= 15:
                is_away_table = has_senators and not has_rangers
                is_home_table = has_rangers and not has_senators

                if (team == "away" and is_away_table) or (team == "home" and is_home_table):
                    roster_tables.append(table)

        # Parse the roster table for this team
        if roster_tables:
            roster_table = roster_tables[0]  # Should only be one per team

            # Parse players from the roster table
            player_rows = roster_table.css("tr")

            for row in player_rows:
                cells = row.css("td")
                if len(cells) == 3:  # Number, Position, Name
                    number_text = cells[0].text(strip=True)
                    position_text = cells[1].text(strip=True)
                    name_text = cells[2].text(strip=True)

                    # Skip header row
                    if number_text == "#" or position_text == "Pos":
                        continue

                    # Only process if we have valid data
                    if number_text.isdigit() and position_text in "CLDGR" and name_text:
                        player_info = {
                            "number": number_text,
                            "position": position_text,
                            "name": name_text,
                        }

                        # Categorize by position
                        if position_text == "G":
                            team_data["goalies"].append(player_info)
                        else:
                            team_data["skaters"].append(player_info)

                        team_data["roster"].append(player_info)

        # Parse scratches - look for tables with ID "Scratches"
        scratch_table = parser.css_first("#Scratches")
        if scratch_table:
            # Scratches table has two columns, we need the right one for the team
            scratch_columns = scratch_table.css("td")
            if len(scratch_columns) >= 2:
                # Away team scratches in first column, home team in second
                scratch_column = scratch_columns[1] if team == "home" else scratch_columns[0]

                # Look for a table within this column
                scratch_player_table = scratch_column.css_first("table")
                if scratch_player_table:
                    scratch_rows = scratch_player_table.css("tr")
                    for row in scratch_rows:
                        cells = row.css("td")
                        if len(cells) == 3:
                            number_text = cells[0].text(strip=True)
                            position_text = cells[1].text(strip=True)
                            name_text = cells[2].text(strip=True)

                            # Skip header row
                            if number_text == "#" or position_text == "Pos":
                                continue

                            if number_text.isdigit() and position_text in "CLDGR" and name_text:
                                team_data["scratches"].append(
                                    {
                                        "number": number_text,
                                        "position": position_text,
                                        "name": name_text,
                                    }
                                )

        # Parse head coach - look for "Head Coaches" section
        coaches_table = parser.css_first("#HeadCoaches")
        if coaches_table:
            coach_columns = coaches_table.css("td")
            if len(coach_columns) >= 2:
                # Away coach in first column, home coach in second
                coach_column = coach_columns[1] if team == "home" else coach_columns[0]
                coach_text = coach_column.text(strip=True)

                # Clean up the coach name (remove extra whitespace/formatting)
                if coach_text and len(coach_text) > 1:
                    team_data["head_coach"] = coach_text

        return team_data

    except Exception as e:
        LOG.warning(f"Could not parse {team} roster: {e}")
        return {"roster": [], "scratches": [], "head_coach": "", "goalies": [], "skaters": []}


def _parse_officials(parser: LexborHTMLParser) -> Dict[str, List[str]]:
    """Extract officials information from the HTML."""
    try:
        officials = {"referees": [], "linesmen": [], "standby": []}

        # Try to find officials table
        officials_table = parser.css_first("#Officials")
        if officials_table:
            rows = officials_table.css("tr")
            current_type = None

            for row in rows:
                row_text = row.text(strip=True).lower()

                if "referee" in row_text:
                    current_type = "referees"
                elif "linesmen" in row_text or "linesman" in row_text:
                    current_type = "linesmen"
                elif "standby" in row_text:
                    current_type = "standby"
                elif current_type and row_text:
                    # This is likely an official's name
                    officials[current_type].append(row_text)

        return officials

    except Exception as e:
        LOG.warning(f"Could not parse officials: {e}")
        return {"referees": [], "linesmen": [], "standby": []}


def parse_html_shifts(html_home: str, html_away: str) -> Dict[str, Any]:
    """
    Parse HTML shifts data for both home and away teams.

    This parser follows the sophisticated approach from scraper_pandas.py to extract
    detailed shift information including individual player shifts and summary statistics.

    Args:
        html_home (str): HTML content for home team shifts
        html_away (str): HTML content for away team shifts

    Returns:
        Dict[str, Any]: Parsed shifts data with structure:
        {
            "home": {
                "shifts": [list of individual shift records],
                "summary": [list of summary records],
                "team_name": str,
                "metadata": dict
            },
            "away": {
                "shifts": [list of individual shift records],
                "summary": [list of summary records],
                "team_name": str,
                "metadata": dict
            },
            "parsing_metadata": dict
        }
    """

    def _parse_team_shifts(html_content: str, team_type: str) -> Dict[str, Any]:
        """Parse shifts data for a single team."""
        if not html_content or not html_content.strip():
            return {
                "shifts": [],
                "summary": [],
                "team_name": f"Unknown {team_type}",
                "metadata": {"parsing_error": "Empty HTML content"},
            }

        try:
            parser = LexborHTMLParser(html_content)

            # Extract team name
            team_name_selector = (
                "body > div.pageBreakAfter > table > tbody > tr:nth-child(3) "
                "> td > table > tbody > tr > td"
            )
            team_name_element = parser.css(team_name_selector)
            team_name = (
                team_name_element[0].text(strip=True)
                if team_name_element
                else f"Unknown {team_type}"
            )

            # Extract player names
            player_rows_selector = (
                "body > div.pageBreakAfter > table > tbody > tr:nth-child(4) "
                "> td > table > tbody > tr"
            )
            n_trs = len(parser.css(player_rows_selector))

            players = []
            for i in range(n_trs):
                player_selector = (
                    f"body > div.pageBreakAfter > table > tbody > "
                    f"tr:nth-child(4) > td > table > tbody > "
                    f"tr:nth-child({i + 1}) > td.playerHeading"
                )
                player_element = parser.css(player_selector)
                if player_element:
                    players.append(player_element[0].text(strip=True))

            # Extract shift data rows
            rows = parser.css("tr.oddColor, tr.evenColor")
            raw_data = []
            for row in rows:
                cells = [td.text(strip=True) for td in row.css("td")]
                if cells:  # Only add non-empty rows
                    raw_data.append(cells)

            # Group data by player (separated by TOT rows)
            player_data_groups = []
            current_group = []

            for row in raw_data:
                if row and row[0] == "TOT":
                    if current_group:
                        player_data_groups.append(current_group)
                    current_group = []
                else:
                    current_group.append(row)

            # Add the last group if it exists
            if current_group:
                player_data_groups.append(current_group)

            # Match players to their data
            player_shifts_dict = {}
            for player, player_shifts in zip(players, player_data_groups):
                player_shifts_dict[player] = player_shifts

            # Define columns for different data types
            shift_columns = [
                "shift_number",
                "period",
                "start_time_elapsed_game",
                "end_time_elapsed_game",
                "duration",
                "event",
            ]

            summary_columns = [
                "period",
                "shifts_count",
                "average_duration",
                "total_ice_time",
                "even_strength_total",
                "power_play_total",
                "short_handed_total",
            ]

            # Process shifts data
            all_shifts = []
            all_summary = []

            for player_name, shifts_data in player_shifts_dict.items():
                # Extract jersey number from player name (first part before space)
                jersey_number = None
                if " " in player_name:
                    try:
                        jersey_number = int(player_name.split(" ")[0])
                    except (ValueError, IndexError):
                        jersey_number = None

                # Separate shift records (6 columns) from summary records (7 columns)
                shift_records = [row for row in shifts_data if len(row) == 6]
                summary_records = [row for row in shifts_data if len(row) == 7]

                # Process individual shifts
                for shift_row in shift_records:
                    shift_record = dict(zip(shift_columns, shift_row))
                    shift_record["player_name"] = player_name
                    shift_record["jersey_number"] = jersey_number
                    shift_record["team_type"] = team_type
                    shift_record["team_name"] = team_name

                    # Parse time fields
                    if "/" in shift_record["start_time_elapsed_game"]:
                        start_parts = shift_record["start_time_elapsed_game"].split(" / ")
                        shift_record["start_time_in_period"] = (
                            start_parts[0] if len(start_parts) > 0 else ""
                        )
                        shift_record["start_time_remaining"] = (
                            start_parts[1] if len(start_parts) > 1 else ""
                        )

                    if "/" in shift_record["end_time_elapsed_game"]:
                        end_parts = shift_record["end_time_elapsed_game"].split(" / ")
                        shift_record["end_time_in_period"] = (
                            end_parts[0] if len(end_parts) > 0 else ""
                        )
                        shift_record["end_time_remaining"] = (
                            end_parts[1] if len(end_parts) > 1 else ""
                        )

                    # Convert duration to seconds
                    if ":" in shift_record["duration"]:
                        try:
                            duration_parts = shift_record["duration"].split(":")
                            duration_seconds = int(duration_parts[0]) * 60 + int(duration_parts[1])
                            shift_record["duration_seconds"] = duration_seconds
                        except (ValueError, IndexError):
                            shift_record["duration_seconds"] = None

                    # Convert shift number and period
                    try:
                        shift_record["shift_number"] = int(shift_record["shift_number"])
                    except (ValueError, TypeError):
                        shift_record["shift_number"] = None

                    try:
                        # Handle OT periods
                        period_value = shift_record["period"]
                        if period_value == "OT":
                            shift_record["period_number"] = 4
                        else:
                            shift_record["period_number"] = int(period_value)
                    except (ValueError, TypeError):
                        shift_record["period_number"] = None

                    all_shifts.append(shift_record)

                # Process summary records
                for summary_row in summary_records:
                    summary_record = dict(zip(summary_columns, summary_row))
                    summary_record["player_name"] = player_name
                    summary_record["jersey_number"] = jersey_number
                    summary_record["team_type"] = team_type
                    summary_record["team_name"] = team_name

                    # Convert time fields to seconds
                    time_fields = [
                        "average_duration",
                        "total_ice_time",
                        "even_strength_total",
                        "power_play_total",
                        "short_handed_total",
                    ]

                    for field in time_fields:
                        if field in summary_record and ":" in str(summary_record[field]):
                            try:
                                time_parts = str(summary_record[field]).split(":")
                                seconds = int(time_parts[0]) * 60 + int(time_parts[1])
                                summary_record[f"{field}_seconds"] = seconds
                            except (ValueError, IndexError):
                                summary_record[f"{field}_seconds"] = None

                    # Convert period and shifts count
                    try:
                        period_value = summary_record["period"]
                        if period_value == "OT":
                            summary_record["period_number"] = 4
                        else:
                            summary_record["period_number"] = int(period_value)
                    except (ValueError, TypeError):
                        summary_record["period_number"] = None

                    try:
                        summary_record["shifts_count"] = int(summary_record["shifts_count"])
                    except (ValueError, TypeError):
                        summary_record["shifts_count"] = None

                    all_summary.append(summary_record)

            return {
                "shifts": all_shifts,
                "summary": all_summary,
                "team_name": team_name,
                "metadata": {
                    "players_count": len(players),
                    "total_shifts": len(all_shifts),
                    "total_summary_records": len(all_summary),
                    "parsing_successful": True,
                },
            }

        except Exception as e:
            return {
                "shifts": [],
                "summary": [],
                "team_name": f"Unknown {team_type}",
                "metadata": {"parsing_error": str(e), "parsing_successful": False},
            }

    # Parse both teams
    home_data = _parse_team_shifts(html_home, "Home")
    away_data = _parse_team_shifts(html_away, "Away")

    # Combine results
    result = {
        "home": home_data,
        "away": away_data,
        "parsing_metadata": {
            "total_shifts": len(home_data["shifts"]) + len(away_data["shifts"]),
            "total_summary_records": (len(home_data["summary"]) + len(away_data["summary"])),
            "home_parsing_successful": home_data["metadata"].get("parsing_successful", False),
            "away_parsing_successful": away_data["metadata"].get("parsing_successful", False),
            "parsed_on": datetime.utcnow().isoformat() if "datetime" in globals() else None,
        },
    }

    return result

def _split_time_range(value: Optional[str]) -> pd.Series:
    """Split a time range string like '12:34 15:45' into two zero-padded time strings."""
    if not isinstance(value, str):
        return pd.Series([None, None])
    m = re.match(r"(\d{1,2}:\d{2})(\d{1,2}:\d{2})", value)
    return pd.Series([m.group(1).zfill(5), m.group(2).zfill(5)]) if m else pd.Series([None, None])

def scrape_html_pbp(game_id: int, return_raw: bool = False) -> pd.DataFrame | tuple[pd.DataFrame, Mapping[str, Any]]:
    raw = scrapeHtmlPbp(game_id)
    parsed = parse_html_pbp(raw["data"])  # {'data': [...], 'columns': [...], 'home_on_ice': [...], ...}
    df = pd.DataFrame(data=parsed["data"], columns=parsed["columns"])
    df[["timeInPeriod", "timeRemaining"]] = df["Time:Elapsed Game"].apply(_split_time_range)
    df["timeInPeriodSec"] = df["timeInPeriod"].apply(time_str_to_seconds)
    df["timeRemainingSec"] = df["timeRemaining"].apply(time_str_to_seconds)
    for col in ["home_on_ice", "away_on_ice", "home_goalie", "away_goalie"]:
        df[col] = parsed[col]
    return (df, parsed) if return_raw else df

def _map_numbers(list_of_lists: list[Any], roster: pd.DataFrame, key: str) -> list[list[Any]]:
    if not isinstance(list_of_lists, list) or roster.empty:
        return list_of_lists
    if "sweaterNumber" not in roster.columns or key not in roster.columns:
        return list_of_lists
    mp = roster.assign(sweaterNumber=roster["sweaterNumber"].astype(str)).set_index("sweaterNumber")[key].to_dict()
    out: list[list[Any]] = []
    for sub in list_of_lists:
        if isinstance(sub, list):
            out.append([mp.get(str(x), x) for x in sub])
        else:
            out.append([sub])
    return out

def scrape_shifts(game_id: int) -> pd.DataFrame:
    html = scrapeHTMLShifts(game_id)
    parsed = parse_html_shifts(html["home"], html["away"])
    api = getGameData(game_id)
    home_abbrev = api.get("homeTeam", {}).get("abbrev", "")
    away_abbrev = api.get("awayTeam", {}).get("abbrev", "")

    rosters = pd.json_normalize(api.get("rosterSpots", []), sep=".")
    home_shifts = pd.json_normalize(parsed["home"]["shifts"])
    away_shifts = pd.json_normalize(parsed["away"]["shifts"])
    shifts = pd.concat([home_shifts, away_shifts], ignore_index=True)
    home_id = api.get("homeTeam", {}).get("id")
    rosters["isHome"] = (rosters["teamId"] == home_id).astype(int)
    rosters["fullName"] = rosters["firstName.default"] + " " + rosters["lastName.default"]
    shifts["isHome"] = (shifts["team_type"] == "Home").astype(int)
    shifts = shifts.merge(
        rosters, left_on=["jersey_number","isHome"], right_on=["sweaterNumber","isHome"], how="left"
    )

    for col in ["start_time_in_period","start_time_remaining","end_time_in_period","end_time_remaining"]:
        shifts[f"{col}_seconds"] = shifts[col].apply(lambda x: time_str_to_seconds(x) if isinstance(x, str) else x)

    if api["gameType"] not in (3, "3"):  # not playoff
        shifts["elapsed_time_start"] = np.where(
            shifts["period_number"] != 5,
            shifts["start_time_in_period_seconds"] + (shifts["period_number"] - 1) * 20 * 60,
            np.nan,
        )
        shifts["elapsed_time_end"] = np.where(
            shifts["period_number"] != 5,
            shifts["end_time_in_period_seconds"] + (shifts["period_number"] - 1) * 20 * 60,
            np.nan,
        )
    else:
        shifts["elapsed_time_start"] = shifts["start_time_in_period_seconds"] + (shifts["period_number"] - 1) * 20 * 60
        shifts["elapsed_time_end"] = shifts["end_time_in_period_seconds"] + (shifts["period_number"] - 1) * 20 * 60

    shifts["gameId"] = game_id
    shifts["homeTeam"] = home_abbrev
    shifts["awayTeam"] = away_abbrev
    return shifts

async def scrape_shifts_async(game_id: int) -> pd.DataFrame:
    html = await  scrapeHTMLShifts_async(game_id)
    parsed = parse_html_shifts(html["home"], html["away"])
    api = getGameData(game_id)
    home_abbrev = api.get("homeTeam", {}).get("abbrev", "")
    away_abbrev = api.get("awayTeam", {}).get("abbrev", "")

    rosters = pd.json_normalize(api.get("rosterSpots", []), sep=".")
    home_shifts = pd.json_normalize(parsed["home"]["shifts"])
    away_shifts = pd.json_normalize(parsed["away"]["shifts"])
    shifts = pd.concat([home_shifts, away_shifts], ignore_index=True)
    home_id = api.get("homeTeam", {}).get("id")
    rosters["isHome"] = (rosters["teamId"] == home_id).astype(int)
    rosters["fullName"] = rosters["firstName.default"] + " " + rosters["lastName.default"]
    shifts["isHome"] = (shifts["team_type"] == "Home").astype(int)
    shifts = shifts.merge(
        rosters, left_on=["jersey_number","isHome"], right_on=["sweaterNumber","isHome"], how="left"
    )

    for col in ["start_time_in_period","start_time_remaining","end_time_in_period","end_time_remaining"]:
        shifts[f"{col}_seconds"] = shifts[col].apply(lambda x: time_str_to_seconds(x) if isinstance(x, str) else x)

    if api["gameType"] not in (3, "3"):  # not playoff
        shifts["elapsed_time_start"] = np.where(
            shifts["period_number"] != 5,
            shifts["start_time_in_period_seconds"] + (shifts["period_number"] - 1) * 20 * 60,
            np.nan,
        )
        shifts["elapsed_time_end"] = np.where(
            shifts["period_number"] != 5,
            shifts["end_time_in_period_seconds"] + (shifts["period_number"] - 1) * 20 * 60,
            np.nan,
        )
    else:
        shifts["elapsed_time_start"] = shifts["start_time_in_period_seconds"] + (shifts["period_number"] - 1) * 20 * 60
        shifts["elapsed_time_end"] = shifts["end_time_in_period_seconds"] + (shifts["period_number"] - 1) * 20 * 60

    shifts["gameId"] = game_id
    shifts["homeTeam"] = home_abbrev
    shifts["awayTeam"] = away_abbrev
    return shifts

def build_shifts_events(shifts: pd.DataFrame) -> pd.DataFrame:

    names_cols = [c for c in shifts.columns if c.startswith("firstName.") or c.startswith("lastName.")]
    drops = ["start_time_elapsed_game","end_time_elapsed_game", *names_cols]
    shifts = shifts.drop(columns=[c for c in drops if c in shifts.columns])

    on_cols = {
        "start_time_in_period_seconds": "timeInPeriodSec",
        "start_time_remaining_seconds": "timeRemainingSec",
        "start_time_remaining": "timeRemaining",
        "start_time_in_period": "timeInPeriod",
        "elapsed_time_start": "elapsedTime",
    }
    off_cols = {
        "end_time_in_period_seconds": "timeInPeriodSec",
        "end_time_remaining_seconds": "timeRemainingSec",
        "end_time_remaining": "timeRemaining",
        "end_time_in_period": "timeInPeriod",
        "elapsed_time_end": "elapsedTime",
    }
    on_df = shifts.rename(columns=on_cols).assign(Event="ON")
    on_df["Time"] = on_df["timeInPeriod"]
    off_df = shifts.rename(columns=off_cols).assign(Event="OFF")
    off_df["Time"] = off_df["timeInPeriod"]

    shared = {
        "period_number": "Per",
        "teamId": "eventOwnerTeamId",
        "playerId": "player1Id",
        "fullName": "player1Name",
    }
    df = pd.concat([on_df, off_df], ignore_index=True).rename(columns=shared)
    drops2 = [
        "start_time_remaining","end_time_in_period","end_time_remaining",
        "end_time_in_period_seconds","end_time_remaining_seconds","elapsed_time_end",
        "start_time_in_period_seconds","start_time_remaining_seconds","elapsed_time_start",
        "start_time_in_period",
    ]
    df = df.drop(columns=[c for c in drops2 if c in df.columns])

    # Robustly guarantee required columns for seconds_matrix
    # Always set 'teamId' from 'eventOwnerTeamId' if present, else fallback to original 'teamId' in shifts
    if "eventOwnerTeamId" in df.columns:
        df["teamId"] = df["eventOwnerTeamId"]
    elif "teamId" in shifts.columns:
        df["teamId"] = shifts["teamId"]
    else:
        df["teamId"] = pd.NA

    # Always set 'isGoalie' to 1 for goalies, else 0 (if positionCode exists)
    if "positionCode" in df.columns:
        df["isGoalie"] = (df["positionCode"] == "G").astype(int)
    else:
        df["isGoalie"] = 0

    # Always set 'isHome' to 1 for home team, else 0 (if isHome exists)
    if "isHome" in df.columns:
        df["isHome"] = df["isHome"].astype(int)
    else:
        df["isHome"] = 0

    # Always set 'eventTeam' to 'teamId' if missing or NA
    if "eventTeam" not in df.columns or df["eventTeam"].isna().all():
        df["eventTeam"] = df["teamId"]

    # Always set 'period' from 'Per' if missing or NA
    if "period" not in df.columns or df["period"].isna().all():
        if "Per" in df.columns:
            df["period"] = df["Per"]
        else:
            df["period"] = 1

    # Guarantee all required columns exist
    required_cols = [
        "player1Id","player1Name","isHome","teamId",
        "eventTeam","elapsedTime","Event","isGoalie","period"
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = pd.NA

    return df

def add_strengths_to_shifts_events(shifts_events: pd.DataFrame, strengths_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds gameStrength and detailedGameStrength columns to ON/OFF shift events.
    Uses strengths_by_second output and matches by elapsedTime.
    """
    # Map elapsedTime to strength columns
    shifts_events = shifts_events.copy()
    shifts_events["elapsedTime"] = pd.to_numeric(shifts_events["elapsedTime"], errors="coerce").astype("Int64")
    # Map strengths by elapsedTime (index of strengths_df)
    shifts_events["gameStrength"] = shifts_events["elapsedTime"].map(strengths_df["team_str_home"])
    shifts_events["detailedGameStrength"] = shifts_events["elapsedTime"].map(
        strengths_df["home_strength"].astype(str) + "v" + strengths_df["away_strength"].astype(str)
    )
    return shifts_events

def build_strength_segments_from_shifts(shifts: pd.DataFrame) -> pd.DataFrame:
    """Compute piecewise-constant strength segments from shift intervals.
    Returns columns: ['t_start','t_end','home_skaters','away_skaters','home_goalie','away_goalie','pulled_home','pulled_away'].
    t_end is exclusive.
    """
    if shifts.empty:
        return pd.DataFrame(columns=[
            "t_start","t_end","home_skaters","away_skaters","home_goalie","away_goalie","pulled_home","pulled_away"
        ])

    req = shifts.copy()
    for c in ("elapsed_time_start","elapsed_time_end"):
        req[c] = pd.to_numeric(req[c], errors="coerce")
    req = req.dropna(subset=["elapsed_time_start","elapsed_time_end"])

    req["is_goalie"] = (req.get("positionCode", "") == "G") | (req.get("isGoalie", 0) == 1)
    req["is_goalie"] = req["is_goalie"].astype(bool)

    changes: dict[int, dict[str, int]] = {}
    def _bump(t: int, key: str, delta: int):
        changes.setdefault(t, {"home_skaters":0,"away_skaters":0,"home_goalie":0,"away_goalie":0})
        changes[t][key] += delta

    for _, r in req.iterrows():
        side = "home" if int(r.get("isHome", 0)) == 1 else "away"
        start = int(r["elapsed_time_start"])
        end = int(r["elapsed_time_end"])
        if end <= start:
            continue
        if r["is_goalie"]:
            _bump(start, f"{side}_goalie", +1)
            _bump(end,   f"{side}_goalie", -1)
        else:
            _bump(start, f"{side}_skaters", +1)
            _bump(end,   f"{side}_skaters", -1)

    if not changes:
        return pd.DataFrame(columns=[
            "t_start","t_end","home_skaters","away_skaters","home_goalie","away_goalie","pulled_home","pulled_away"
        ])

    times = sorted(changes.keys())
    cur = {"home_skaters":0,"away_skaters":0,"home_goalie":0,"away_goalie":0}
    segments = []
    for i, t in enumerate(times):
        for k, v in changes[t].items():
            cur[k] += v
        t_next = times[i+1] if i+1 < len(times) else t
        if t_next > t:
            seg = {
                "t_start": t,
                "t_end": t_next,
                "home_skaters": int(cur["home_skaters"]),
                "away_skaters": int(cur["away_skaters"]),
                "home_goalie": int(cur["home_goalie"]),
                "away_goalie": int(cur["away_goalie"]),
            }
            seg["pulled_home"] = 1 if seg["home_goalie"] == 0 else 0
            seg["pulled_away"] = 1 if seg["away_goalie"] == 0 else 0
            segments.append(seg)
    return pd.DataFrame(segments)


def strengths_by_second_from_segments(segments: pd.DataFrame) -> pd.DataFrame:
    """Expand compact strength segments to a per-second index for joining with events/shifts.
    Index = elapsedTime; columns: team_str_home, home_strength, away_strength.
    """
    if segments.empty:
        return pd.DataFrame(columns=["team_str_home","home_strength","away_strength"]).astype({})

    rows = []
    for _, r in segments.iterrows():
        home = int(r["home_skaters"])  # skaters only
        away = int(r["away_skaters"])
        home_s = f"{home}{'*' if int(r['pulled_home']) else ''}"
        away_s = f"{away}{'*' if int(r['pulled_away']) else ''}"
        team_str_home = f"{home}v{away}"
        for t in range(int(r["t_start"]), int(r["t_end"])):
            rows.append((t, team_str_home, home_s, away_s))
    out = (
        pd.DataFrame(rows, columns=["elapsedTime","team_str_home","home_strength","away_strength"])
        .set_index("elapsedTime")
        .sort_index()
    )
    return out


def build_on_ice_long(df: pd.DataFrame) -> pd.DataFrame:
    """Convert list-based on-ice columns into a tidy long table (no numbered wide columns).
    This is defensive against rows where on-ice columns are NaN, scalars, or string-encoded lists.
    """
    import ast
    records: list[dict] = []

    def _ensure_list(x):
        # Already a list
        if isinstance(x, list):
            return x
        # Accept tuples/sets
        if isinstance(x, (tuple, set)):
            return list(x)
        # Treat NaN/None as empty
        try:
            import numpy as np
            if x is None or (isinstance(x, float) and np.isnan(x)):
                return []
        except Exception:
            pass
        # If string like "[1, 2, 3]" or "Nick, Cole, ..." try to parse
        if isinstance(x, str):
            s = x.strip()
            # try literal list first
            if (s.startswith("[") and s.endswith("]")) or (s.startswith("(") and s.endswith(")")):
                try:
                    val = ast.literal_eval(s)
                    if isinstance(val, (list, tuple, set)):
                        return list(val)
                except Exception:
                    pass
            # fallback: comma-separated
            if "," in s:
                return [item.strip() for item in s.split(",") if item.strip()]
        # Unknown scalar → empty
        return []

    for _, row in df.iterrows():
        for side in ("home", "away"):
            ids = _ensure_list(row.get(f"{side}_on_id"))
            names = _ensure_list(row.get(f"{side}_on_full_name"))

            # If names shorter than ids, pad with None; if longer, zip will trim safely
            if len(names) < len(ids):
                names = names + [None] * (len(ids) - len(names))

            for slot, (pid, pname) in enumerate(zip(ids, names), start=1):
                records.append({
                    "gameId": row.get("gameId", pd.NA),
                    "elapsedTime": row.get("elapsedTime", pd.NA),
                    "Per": row.get("Per", pd.NA),
                    "Event": row.get("Event", pd.NA),
                    "team_side": side,
                    "slot_index": slot,
                    "player_id": pid,
                    "player_name": pname,
                })

    return pd.DataFrame.from_records(records)


# --- Wide on-ice columns (skater_1..N, goalie) --------------------------------
def build_on_ice_wide(
    df: pd.DataFrame,
    *,
    max_skaters: int = 6,
    include_goalie: bool = True,
    drop_list_cols: bool = False,
) -> pd.DataFrame:
    """
    Create wide 'on-ice' columns (skater_1..N and optional goalie) and return
    the original DataFrame with these columns appended.

    Parameters
    ----------
    df : pd.DataFrame
        Source dataframe that contains list-based columns like:
        - home_on_id, home_on_full_name
        - away_on_id, away_on_full_name
        - homeGoalie_on_id, homeGoalie_on_full_name
        - awayGoalie_on_id, awayGoalie_on_full_name
    max_skaters : int
        Number of skater slots (per team side) to materialize as columns.
    include_goalie : bool
        Whether to add single goalie id/name columns per side.
    drop_list_cols : bool
        If True, drop the original list-based columns.

    Returns
    -------
    pd.DataFrame
        Same rows as input with additional wide columns:
          home_skater_id_1..max_skaters
          home_skater_name_1..max_skaters
          away_skater_id_1..max_skaters
          away_skater_name_1..max_skaters
        and, if include_goalie:
          home_goalie_id, home_goalie_name, away_goalie_id, away_goalie_name
    """
    import ast
    import numpy as np

    def _ensure_list(x):
        if isinstance(x, list):
            return x
        if isinstance(x, (tuple, set)):
            return list(x)
        # Treat NaN/None as empty
        if x is None:
            return []
        try:
            if isinstance(x, float) and np.isnan(x):
                return []
        except Exception:
            pass
        if isinstance(x, str):
            s = x.strip()
            # literal
            if (s.startswith("[") and s.endswith("]")) or (s.startswith("(") and s.endswith(")")):
                try:
                    val = ast.literal_eval(s)
                    if isinstance(val, (list, tuple, set)):
                        return list(val)
                except Exception:
                    pass
            # csv fallback
            if "," in s:
                return [t.strip() for t in s.split(",") if t.strip()]
        return []

    # Build rows of new columns
    new_cols_records = []
    for _, row in df.iterrows():
        out = {}

        for side in ("home", "away"):
            ids_all   = _ensure_list(row.get(f"{side}_on_id"))
            names_all = _ensure_list(row.get(f"{side}_on_full_name"))
            goalie_ids   = _ensure_list(row.get(f"{side}Goalie_on_id"))
            goalie_names = _ensure_list(row.get(f"{side}Goalie_on_full_name"))

            # Build id->name lookup when lengths differ
            id_name_pairs = list(zip(ids_all, names_all))
            id_to_name = {pid: pname for pid, pname in id_name_pairs if pid is not None}

            # Remove goalies from skater lists if a separate goalie list exists
            gid_set = set(goalie_ids) if goalie_ids else set()
            skater_ids = [pid for pid in ids_all if pid not in gid_set]

            # Rebuild skater names by id_to_name lookup (handles length mismatch)
            skater_names = [id_to_name.get(pid) for pid in skater_ids]

            # Pad / truncate to max_skaters
            if len(skater_ids) < max_skaters:
                skater_ids = skater_ids + [None] * (max_skaters - len(skater_ids))
                skater_names = skater_names + [None] * (max_skaters - len(skater_names))
            else:
                skater_ids = skater_ids[:max_skaters]
                skater_names = skater_names[:max_skaters]

            # Assign columns
            for i in range(1, max_skaters + 1):
                out[f"{side}_skater_id_{i}"] = skater_ids[i-1]
                out[f"{side}_skater_name_{i}"] = skater_names[i-1]

            if include_goalie:
                g_id = goalie_ids[0] if len(goalie_ids) > 0 else None
                g_nm = goalie_names[0] if len(goalie_names) > 0 else None
                # If not separately provided, infer goalie as the only player with '*' strength context is messy;
                # safest is to take the first id in ids_all that is not duplicated in skaters (if any).
                if g_id is None and ids_all:
                    # heuristic: if exactly 1 player not in skater_ids, treat as goalie
                    leftovers = [pid for pid in ids_all if pid not in skater_ids]
                    if len(leftovers) == 1:
                        g_id = leftovers[0]
                        g_nm = id_to_name.get(g_id)
                out[f"{side}_goalie_id"] = g_id
                out[f"{side}_goalie_name"] = g_nm

        new_cols_records.append(out)

    wide_cols_df = pd.DataFrame.from_records(new_cols_records, index=df.index)

    # Merge back
    out_df = pd.concat([df, wide_cols_df], axis=1)

    if drop_list_cols:
        drop_candidates = [
            "home_on_id","away_on_id","home_on_full_name","away_on_full_name",
            "homeGoalie_on_id","awayGoalie_on_id","homeGoalie_on_full_name","awayGoalie_on_full_name",
        ]
        out_df = out_df.drop(columns=[c for c in drop_candidates if c in out_df.columns], errors="ignore")

    return out_df

def scrape_game(game_id:Union[int,str],
                addGoalReplayData: bool = False,) -> pd.DataFrame | tuple[pd.DataFrame, Dict[str, Any]]:
    """Scrape and parse all data for a given NHL game ID.
    Args:
        game_id (int | str): The NHL game ID to scrape.
    Returns:
        pd.DataFrame: The scraped and parsed game data.
    """
    # HTML PBP Manips
    df_html, html_meta = scrape_html_pbp(game_id, return_raw=True)
    if "Time" not in df_html.columns and "timeInPeriod" in df_html.columns:
        df_html = df_html.rename(columns={"timeInPeriod": "Time"})
    required_html = {"Event", "Per", "Time"}
    missing = required_html - set(df_html.columns)
    if missing:
        raise KeyError(f"HTML PBP missing required columns: {missing}")
    api = getGameData(game_id, addGoalReplayData=addGoalReplayData)
    _meta_vals = {
    "gameId": api.get("id"),
    "venue": (api.get("venue") or {}).get("default"),
    "venueLocation": (api.get("venueLocation") or {}).get("default"),
    "gameDate": api.get("gameDate"),
    "gameType": api.get("gameType"),
    "startTimeUTC": api.get("startTimeUTC"),
    "easternUTCOffset": api.get("easternUTCOffset"),
    "venueUTCOffset": api.get("venueUTCOffset"),
    # stamp these here; they’re not in the API payload
    "scrapedOn": datetime.utcnow().isoformat(),
    "source": "NHL Play-by-Play API",
    }
    pbp = pd.json_normalize(api.get("plays", []), sep=".")
    # Ensure unique column names to avoid InvalidIndexError on concat/merge
    pbp.columns = _dedup_cols(pbp.columns)
    rosters = pd.json_normalize (api.get("rosterSpots", []), sep=".")
    home_id = api.get("homeTeam", {}).get("id")
    # away_id = api.get("awayTeam", {}).get("id")
    home_abbrev = api.get("homeTeam", {}).get("abbrev")
    away_abbrev = api.get("awayTeam", {}).get("abbrev")
    rosters["isHome"] = (rosters["teamId"] == home_id).astype(int)
    rosters["fullName"] = rosters["firstName.default"] + " " + rosters["lastName.default"] 
    shifts = scrape_shifts(game_id=game_id)
    shifts_events = build_shifts_events(shifts)
    
    # flatten API
    pbp.columns = (pbp.columns
                   .str.replace(r"^details\.", "", regex=True)
                   .str.replace(r"^periodDescriptor\.", "", regex=True))
    pbp = pbp.rename(columns={"number": "period", "typeDescKey": "api_event"})
    pbp["isHome"] = (pbp["eventOwnerTeamId"] == home_id).astype(int)
    pbp["eventTeam"] = pbp["isHome"].map({1: home_abbrev, 0: away_abbrev})
    pbp["html_event"] = pbp["api_event"].map(EVENT_MAPPING)
    pbp["Event"] = pbp["html_event"] # 

    # dtype normalization
    for col in ["Event","Per","Time"]:
        df_html[col] = df_html[col].astype(str)
    pbp["html_event"] = pbp["html_event"].astype(str)
    pbp["period"] = pbp["period"].astype(str)
    pbp["timeInPeriod"] = pbp["timeInPeriod"].astype(str)

    # ffill scoreboard cols if missing
    for c in ["awaySOG","homeSOG","homeScore","awayScore"]:
        if c not in pbp.columns:
            pbp[c] = pd.NA
        pbp[c] = pbp[c].ffill().fillna(0).astype(int)

    # robust merge index per (Event, Per, Time) in each table
    df_html["merge_idx"] = _group_merge_index(df_html, ["Event","Per","Time"])
    mask_api = pbp["html_event"].isin(EVENT_MAPPING.values())
    pbp_merge = pbp.loc[mask_api].copy()
    pbp_merge["Event"] = pbp_merge["html_event"]
    pbp_merge["Per"] = pbp_merge["period"]
    pbp_merge["Time"] = pbp_merge["timeInPeriod"]
    pbp["merge_idx"] = 0
    pbp.loc[mask_api, "merge_idx"] = _group_merge_index(pbp_merge, ["Event","Per","Time"]).values

    left_on = ["Event","Per","Time","merge_idx"]
    right_on = ["Event","period","timeInPeriod","merge_idx"]
    df = df_html.merge(pbp, left_on=left_on, right_on=right_on, how="left", suffixes=("","_api"))
    df.columns = _dedup_cols(df.columns)
    

    # on-ice mappings
    home_r = rosters.query("isHome == 1")
    away_r = rosters.query("isHome == 0")
    df["home_on_id"] = _map_numbers(html_meta["home_on_ice"], home_r, "playerId")
    df["away_on_id"] = _map_numbers(html_meta["away_on_ice"], away_r, "playerId")
    df["homeGoalie_on_id"] = _map_numbers(html_meta["home_goalie"], home_r, "playerId")
    df["awayGoalie_on_id"] = _map_numbers(html_meta["away_goalie"], away_r, "playerId")

    df["home_on_full_name"] = _map_numbers(html_meta["home_on_ice"], home_r, "fullName")
    df["away_on_full_name"] = _map_numbers(html_meta["away_on_ice"], away_r, "fullName")
    df["homeGoalie_on_full_name"] = _map_numbers(html_meta["home_goalie"], home_r, "fullName")
    df["awayGoalie_on_full_name"] = _map_numbers(html_meta["away_goalie"], away_r, "fullName")

    # counts & numeric strength fields
    for base in ["home_on","away_on","homeGoalie_on","awayGoalie_on"]:
        df[f"{base}_count"] = df[f"{base}_id"].apply(lambda x: len(x) if isinstance(x, list) else 0)

    df["n_home_skaters"] = df["home_on_count"].sub(df["homeGoalie_on_count"].clip(upper=1))
    df["n_away_skaters"] = df["away_on_count"].sub(df["awayGoalie_on_count"].clip(upper=1))
    df["pulled_home"] = (df["homeGoalie_on_count"] == 0).astype("Int8")
    df["pulled_away"] = (df["awayGoalie_on_count"] == 0).astype("Int8")
    
    
    # compact strength strings
    is_home = df["isHome"].astype(bool)
    home_str = df["home_on_count"].astype("Int64").astype("string")
    away_str = df["away_on_count"].astype("Int64").astype("string")
    home_strength = home_str.mask(df["pulled_home"].eq(1), home_str + "*")
    away_strength = away_str.mask(df["pulled_away"].eq(1), away_str + "*")
    game_left = home_str.where(is_home, away_str)
    game_right = away_str.where(is_home, home_str)
    det_left = home_strength.where(is_home, away_strength)
    det_right = away_strength.where(is_home, home_strength)
    m_valid = df["home_on_count"].gt(0) & df["away_on_count"].gt(0)
    df.loc[m_valid, ["home_strength","away_strength","gameStrength","detailedGameStrength"]] = pd.DataFrame({
        "home_strength": det_left[m_valid],
        "away_strength": det_right[m_valid],
        "gameStrength": game_left[m_valid].str.cat(game_right[m_valid], sep="v"),
        "detailedGameStrength": det_left[m_valid].str.cat(det_right[m_valid], sep="v"),
    })
    
    df["Per"] = pd.to_numeric(df["Per"], errors="coerce").astype("Int16")
    df["timeInPeriodSec"] = pd.to_numeric(df["timeInPeriodSec"], errors="coerce").astype("Int16")

    tip = df["timeInPeriodSec"].astype("Int64")   # allow NA
    per = df["Per"].astype("Int64")

    # Compute elapsed for all non-shootout periods; ignore gameType entirely
    mask_reg_ot = per.ne(5) & tip.notna() & per.notna()
    df.loc[mask_reg_ot, "elapsedTime"] = tip[mask_reg_ot] + (per[mask_reg_ot] - 1) * 1200

    

    # player assignment by event type
    for c in ("player1Id","player2Id","player3Id"):
        if c not in df.columns:
            df[c] = pd.NA
    event_columns = {
        "faceoff": ["winningPlayerId","losingPlayerId"],
        "hit": ["hittingPlayerId","hitteePlayerId"],
        "blocked-shot": ["shootingPlayerId","blockingPlayerId"],
        "shot-on-goal": ["shootingPlayerId", None],
        "missed-shot": ["shootingPlayerId", None],
        "goal": ["scoringPlayerId","assist1PlayerId","assist2PlayerId"],
        "giveaway": ["playerId", None],
        "takeaway": ["playerId", None],
        "penalty": ["committedByPlayerId","drawnByPlayerId","servedByPlayerId"],
        "failed-shot-attempt": ["shootingPlayerId", None],
    }
    api_evt = df["api_event"]
    for evt, cols in event_columns.items():
        m = api_evt.eq(evt)
        if not m.any():
            continue
        for i, src in enumerate(cols[:3], start=1):
            if src and src in df.columns:
                df.loc[m, f"player{i}Id"] = df.loc[m, src].to_numpy()

    name_map = rosters.set_index("playerId")["fullName"]
    for i in (1,2,3):
        df[f"player{i}Id"] = df[f"player{i}Id"].astype("Int64")
        df[f"player{i}Name"] = df[f"player{i}Id"].map(name_map)
        
    # 1) Build compact strength segments from shifts and expand per-second only for join
    df.columns = _dedup_cols(df.columns)
    shifts_events.columns = _dedup_cols(shifts_events.columns)
    segments_df = build_strength_segments_from_shifts(shifts)
    strengths_df = strengths_by_second_from_segments(segments_df)

    # 2) Add strengths to ON/OFF shift events using the per-second index
    shifts_events = add_strengths_to_shifts_events(shifts_events, strengths_df)

    # 3) Concatenate pbp and shifts_events
    # pbp.columns = _dedup_cols(pbp.columns)
    shifts_events.columns = _dedup_cols(shifts_events.columns)
    data = pd.concat([df, shifts_events], ignore_index=True)
    
    dups = data.columns[data.columns.duplicated()].tolist()
    if dups:
        LOG.warning(f"Duplicate columns detected: {dups}")
    data.columns = _dedup_cols(data.columns)
    
    # Stable event ordering
    sort_priority = {
        "PGSTR": 1, "PGEND": 2, "ANTHEM": 3, "EGT": 3, "CHL": 3, "DELPEN": 3,
        "BLOCK": 3, "GIVE": 3, "HIT": 3, "MISS": 3, "SHOT": 3, "TAKE": 3,
        "GOAL": 5, "STOP": 6, "PENL": 7, "PBOX": 7, "PSTR": 7, "ON": 8, "OFF": 8,
        "EISTR": 9, "EIEND": 10, "FAC": 12, "PEND": 13, "SOC": 14, "GEND": 15, "GOFF": 16
    }
    data["Priority"] = data["Event"].map(sort_priority).fillna(99).astype(int)
    strength_col = "Str" if "Str" in data.columns else ("strength" if "strength" in data.columns else None)
    sort_cols = ["elapsedTime", "Priority"] + ([strength_col] if strength_col else [])
    sort_asc = [True, True] + ([True] if strength_col else [])
    data = data.sort_values(by=sort_cols, ascending=sort_asc, kind="mergesort")

    data = (data
            .drop(columns=["Priority"])
                .rename(columns={"eventOwnerTeamId":"teamId_",
                                #  "Per":"period",
                                 "Str":"strength","api_event":"event_api"}))
    

    # attach goalie flag if present
    if {"playerId","positionCode"}.issubset(shifts.columns):
        goalies = shifts.rename(columns={"playerId":"player1Id"})[["player1Id","positionCode"]]
        goalies["isGoalie"] = pd.to_numeric(goalies["positionCode"].eq("G"), errors="coerce").fillna(0).astype(int)
        data = data.merge(goalies[["player1Id","isGoalie"]].drop_duplicates(), on=["player1Id", "isGoalie"], how="left")
        
        
    # Attach game-level metadata (constant across rows)
    for k, v in _meta_vals.items():
        data[k] = v

        
    # drop shift columns that are not relevant anymore shift_number	event	player_name	jersey_number	team_type	team_name	duration_seconds	sweaterNumber	positionCode	headshot
    shift_cols = ["shift_number","event","player_name","jersey_number","team_type","team_name","duration_seconds","sweaterNumber","positionCode","headshot"]
    data = data.drop(columns=shift_cols, errors="ignore")
    
    home_abbrev = data["homeTeam"].dropna().iloc[0] if "homeTeam" in data.columns else ""
    away_abbrev = data["awayTeam"].dropna().iloc[0] if "awayTeam" in data.columns else ""
    
    data["eventTeam"] = data["isHome"].map({1: home_abbrev, 0: away_abbrev})
    data["#"] = np.arange(1, len(data) + 1)
    
    data["homeTeam"] = home_abbrev
    data["awayTeam"] = away_abbrev
    
    for c in ["awaySOG","homeSOG","homeScore","awayScore"]:
        if c not in data.columns:
            data[c] = pd.NA
        data[c] = data[c].ffill().fillna(0).astype(int)

    # Prefer teamId_ from API over teamId from shifts if available
    data.loc[data['teamId'].isna() & data['teamId_'].notnull(), 'teamId'] = data.loc[data['teamId'].isna() & data['teamId_'].notnull(), 'teamId_']
    
    

    # Dynamically build a result tuple
    fields = ["data"]
    values = [data]
    
    # df_html, pbp, rosters, home_id, home_abbrev, away_abbrev, shifts_events, html_meta, df, data
    dups = data.columns[data.columns.duplicated()].tolist()
    if dups:
        LOG.warning(f"Duplicate columns detected: {dups}")
    data.columns = _dedup_cols(data.columns)
    
    return data

async def scrape_game_async(game_id:Union[int,str],
                      addGoalReplayData: bool = False,
                      include_rosters: bool = False,
                      include_shifts: bool = False,
                      include_seconds_matrix: bool = False,
                      include_strengths: bool = False) -> pd.DataFrame | tuple[pd.DataFrame, Dict[str, Any]]:
    """Scrape and parse all data for a given NHL game ID.

    Args:
        game_id (int | str): The NHL game ID to scrape.

    Returns:
        pd.DataFrame: The scraped and parsed game data.
    """
    
    # HTML PBP Manips
    df_html, html_meta = await scrape_html_pbp(game_id, return_raw=True)
    if "Time" not in df_html.columns and "timeInPeriod" in df_html.columns:
        df_html = df_html.rename(columns={"timeInPeriod": "Time"})
    required_html = {"Event", "Per", "Time"}
    missing = required_html - set(df_html.columns)
    if missing:
        raise KeyError(f"HTML PBP missing required columns: {missing}")
    
    api = getGameData(game_id, addGoalReplayData=addGoalReplayData)
    pbp = pd.json_normalize(api.get("plays", []), sep=".")
    # Ensure unique column names to avoid InvalidIndexError on concat/merge
    pbp.columns = _dedup_cols(pbp.columns)
    rosters = pd.json_normalize (api.get("rosterSpots", []), sep=".")
    home_id = api.get("homeTeam", {}).get("id")
    # away_id = api.get("awayTeam", {}).get("id")
    home_abbrev = api.get("homeTeam", {}).get("abbrev")
    away_abbrev = api.get("awayTeam", {}).get("abbrev")
    rosters["isHome"] = (rosters["teamId"] == home_id).astype(int)
    rosters["fullName"] = rosters["firstName.default"] + " " + rosters["lastName.default"]
    
    
    # Shifts 
    shifts = await scrape_shifts(game_id=game_id)
    shifts_events = build_shifts_events(shifts)
    
    
    # flatten API
    pbp.columns = (pbp.columns
                   .str.replace(r"^details\.", "", regex=True)
                   .str.replace(r"^periodDescriptor\.", "", regex=True))
    pbp = pbp.rename(columns={"number": "period", "typeDescKey": "api_event"})
    pbp["isHome"] = (pbp["eventOwnerTeamId"] == home_id).astype(int)
    pbp["eventTeam"] = pbp["isHome"].map({1: home_abbrev, 0: away_abbrev})
    pbp["html_event"] = pbp["api_event"].map(EVENT_MAPPING)
    pbp["Event"] = pbp["html_event"] # 

    # dtype normalization
    for col in ["Event","Per","Time"]:
        df_html[col] = df_html[col].astype(str)
    pbp["html_event"] = pbp["html_event"].astype(str)
    pbp["period"] = pbp["period"].astype(str)
    pbp["timeInPeriod"] = pbp["timeInPeriod"].astype(str)

    # ffill scoreboard cols if missing
    for c in ["awaySOG","homeSOG","homeScore","awayScore"]:
        if c not in pbp.columns:
            pbp[c] = pd.NA
        pbp[c] = pbp[c].ffill().fillna(0).astype(int)

    # robust merge index per (Event, Per, Time) in each table
    df_html["merge_idx"] = _group_merge_index(df_html, ["Event","Per","Time"])
    mask_api = pbp["html_event"].isin(EVENT_MAPPING.values())
    pbp_merge = pbp.loc[mask_api].copy()
    pbp_merge["Event"] = pbp_merge["html_event"]
    pbp_merge["Per"] = pbp_merge["period"]
    pbp_merge["Time"] = pbp_merge["timeInPeriod"]
    pbp["merge_idx"] = 0
    pbp.loc[mask_api, "merge_idx"] = _group_merge_index(pbp_merge, ["Event","Per","Time"]).values

    left_on = ["Event","Per","Time","merge_idx"]
    right_on = ["html_event","period","timeInPeriod","merge_idx"]
    df = df_html.merge(pbp, left_on=left_on, right_on=right_on, how="left", suffixes=("","_api"))
    df.columns = _dedup_cols(df.columns)
    

    # on-ice mappings
    home_r = rosters.query("isHome == 1")
    away_r = rosters.query("isHome == 0")
    df["home_on_id"] = _map_numbers(html_meta["home_on_ice"], home_r, "playerId")
    df["away_on_id"] = _map_numbers(html_meta["away_on_ice"], away_r, "playerId")
    df["homeGoalie_on_id"] = _map_numbers(html_meta["home_goalie"], home_r, "playerId")
    df["awayGoalie_on_id"] = _map_numbers(html_meta["away_goalie"], away_r, "playerId")

    df["home_on_full_name"] = _map_numbers(html_meta["home_on_ice"], home_r, "fullName")
    df["away_on_full_name"] = _map_numbers(html_meta["away_on_ice"], away_r, "fullName")
    df["homeGoalie_on_full_name"] = _map_numbers(html_meta["home_goalie"], home_r, "fullName")
    df["awayGoalie_on_full_name"] = _map_numbers(html_meta["away_goalie"], away_r, "fullName")

    # counts & numeric strength fields
    for base in ["home_on","away_on","homeGoalie_on","awayGoalie_on"]:
        df[f"{base}_count"] = df[f"{base}_id"].apply(lambda x: len(x) if isinstance(x, list) else 0)

    df["n_home_skaters"] = df["home_on_count"].sub(df["homeGoalie_on_count"].clip(upper=1))
    df["n_away_skaters"] = df["away_on_count"].sub(df["awayGoalie_on_count"].clip(upper=1))
    df["pulled_home"] = (df["homeGoalie_on_count"] == 0).astype("Int8")
    df["pulled_away"] = (df["awayGoalie_on_count"] == 0).astype("Int8")
    
    
    # compact strength strings
    is_home = df["isHome"].astype(bool)
    home_str = df["home_on_count"].astype("Int64").astype("string")
    away_str = df["away_on_count"].astype("Int64").astype("string")
    home_strength = home_str.mask(df["pulled_home"].eq(1), home_str + "*")
    away_strength = away_str.mask(df["pulled_away"].eq(1), away_str + "*")
    game_left = home_str.where(is_home, away_str)
    game_right = away_str.where(is_home, home_str)
    det_left = home_strength.where(is_home, away_strength)
    det_right = away_strength.where(is_home, home_strength)
    m_valid = df["home_on_count"].gt(0) & df["away_on_count"].gt(0)
    df.loc[m_valid, ["home_strength","away_strength","gameStrength","detailedGameStrength"]] = pd.DataFrame({
        "home_strength": det_left[m_valid],
        "away_strength": det_right[m_valid],
        "gameStrength": game_left[m_valid].str.cat(game_right[m_valid], sep="v"),
        "detailedGameStrength": det_left[m_valid].str.cat(det_right[m_valid], sep="v"),
    })
    
    # If you want a tidy on-ice table for SQL, call: on_ice_long = build_on_ice_long(df)

    # elapsed time
    # Safe numeric dtypes
    df["Per"] = pd.to_numeric(df["Per"], errors="coerce").astype("Int16")
    df["timeInPeriodSec"] = pd.to_numeric(df["timeInPeriodSec"], errors="coerce").astype("Int16")

    tip = df["timeInPeriodSec"].astype("Int64")   # allow NA
    per = df["Per"].astype("Int64")

    # Compute elapsed for all non-shootout periods; ignore gameType entirely
    mask_reg_ot = per.ne(5) & tip.notna() & per.notna()
    df.loc[mask_reg_ot, "elapsedTime"] = tip[mask_reg_ot] + (per[mask_reg_ot] - 1) * 1200

    # (optional) leave shootout rows as NA or set them to last-reg-time + small offsets if you prefer
    # so_mask = per.eq(5) & tip.notna()
    # df.loc[so_mask, "elapsedTime"] = df["elapsedTime"].max()

    # Avoid blanket fillna(0) which hides missing-computation issues
    # If you really need zeros for display, do it only at the end of your notebook:
    # df["elapsedTime"] = df["elapsedTime"].fillna(0)

    # player assignment by event type
    for c in ("player1Id","player2Id","player3Id"):
        if c not in df.columns:
            df[c] = pd.NA
    event_columns = {
        "faceoff": ["winningPlayerId","losingPlayerId"],
        "hit": ["hittingPlayerId","hitteePlayerId"],
        "blocked-shot": ["shootingPlayerId","blockingPlayerId"],
        "shot-on-goal": ["shootingPlayerId", None],
        "missed-shot": ["shootingPlayerId", None],
        "goal": ["scoringPlayerId","assist1PlayerId","assist2PlayerId"],
        "giveaway": ["playerId", None],
        "takeaway": ["playerId", None],
        "penalty": ["committedByPlayerId","drawnByPlayerId","servedByPlayerId"],
        "failed-shot-attempt": ["shootingPlayerId", None],
    }
    api_evt = df["api_event"]
    for evt, cols in event_columns.items():
        m = api_evt.eq(evt)
        if not m.any():
            continue
        for i, src in enumerate(cols[:3], start=1):
            if src and src in df.columns:
                df.loc[m, f"player{i}Id"] = df.loc[m, src].to_numpy()

    name_map = rosters.set_index("playerId")["fullName"]
    for i in (1,2,3):
        df[f"player{i}Id"] = df[f"player{i}Id"].astype("Int64")
        df[f"player{i}Name"] = df[f"player{i}Id"].map(name_map)
        
    # 1) Build compact strength segments from shifts and expand per-second only for join
    df.columns = _dedup_cols(df.columns)
    shifts_events.columns = _dedup_cols(shifts_events.columns)
    segments_df = build_strength_segments_from_shifts(shifts)
    strengths_df = strengths_by_second_from_segments(segments_df)

    # 2) Add strengths to ON/OFF shift events using the per-second index
    shifts_events = add_strengths_to_shifts_events(shifts_events, strengths_df)

    # 3) Concatenate pbp and shifts_events
    # pbp.columns = _dedup_cols(pbp.columns)
    shifts_events.columns = _dedup_cols(shifts_events.columns)
    data = pd.concat([df, shifts_events], ignore_index=True)

    # Stable event ordering
    sort_priority = {
        "PGSTR": 1, "PGEND": 2, "ANTHEM": 3, "EGT": 3, "CHL": 3, "DELPEN": 3,
        "BLOCK": 3, "GIVE": 3, "HIT": 3, "MISS": 3, "SHOT": 3, "TAKE": 3,
        "GOAL": 5, "STOP": 6, "PENL": 7, "PBOX": 7, "PSTR": 7, "ON": 8, "OFF": 8,
        "EISTR": 9, "EIEND": 10, "FAC": 12, "PEND": 13, "SOC": 14, "GEND": 15, "GOFF": 16
    }
    data["Priority"] = data["Event"].map(sort_priority).fillna(99).astype(int)
    strength_col = "Str" if "Str" in data.columns else ("strength" if "strength" in data.columns else None)
    sort_cols = ["elapsedTime", "Priority"] + ([strength_col] if strength_col else [])
    sort_asc = [True, True] + ([True] if strength_col else [])
    data = data.sort_values(by=sort_cols, ascending=sort_asc, kind="mergesort")

    data = (data
            .drop(columns=["Priority"])
                .rename(columns={"eventOwnerTeamId":"teamId_",
                                #  "Per":"period",
                                 "Str":"strength","api_event":"event_api"}))
    

    # attach goalie flag if present
    if {"playerId","positionCode"}.issubset(shifts.columns):
        goalies = shifts.rename(columns={"playerId":"player1Id"})[["player1Id","positionCode"]]
        goalies["isGoalie"] = pd.to_numeric(goalies["positionCode"].eq("G"), errors="coerce").fillna(0).astype(int)
        data = data.merge(goalies[["player1Id","isGoalie"]].drop_duplicates(), on=["player1Id", "isGoalie"], how="left")
        
        
    # do fills for those cols [gameId	venue	venueLocation	scrapedOn	source	gameDate	gameType	startTimeUTC	easternUTCOffset	venueUTCOffset]
    meta_cols = ["gameId","venue","venueLocation","scrapedOn","source","gameDate","gameType","startTimeUTC","easternUTCOffset","venueUTCOffset"]
    for col in meta_cols:
        if col in data.columns and data[col].notna().any():
            col_val = data.loc[data[col].notna(), col].iloc[0]
        else:
            col_val = None
        data[col] = col_val

        
    # drop shift columns that are not relevant anymore shift_number	event	player_name	jersey_number	team_type	team_name	duration_seconds	sweaterNumber	positionCode	headshot
    shift_cols = ["shift_number","event","player_name","jersey_number","team_type","team_name","duration_seconds","sweaterNumber","positionCode","headshot"]
    data = data.drop(columns=shift_cols, errors="ignore")
    
    home_abbrev = data["homeTeam"].dropna().iloc[0] if "homeTeam" in data.columns else ""
    away_abbrev = data["awayTeam"].dropna().iloc[0] if "awayTeam" in data.columns else ""
    
    data["eventTeam"] = data["isHome"].map({1: home_abbrev, 0: away_abbrev})
    data["#"] = np.arange(1, len(data) + 1)
    
    data["homeTeam"] = home_abbrev
    data["awayTeam"] = away_abbrev
    
    # Prefer teamId_ from API over teamId from shifts if available
    data.loc[data['teamId'].isna() & data['teamId_'].notnull(), 'teamId'] = data.loc[data['teamId'].isna() & data['teamId_'].notnull(), 'teamId_']

    # Dynamically build a result tuple
    fields = ["data"]
    values = [data]
    
    if include_shifts:
        fields.append("shifts")
        values.append(shifts)
    if include_rosters:
        fields.append("rosters")
        values.append(rosters)
    if include_seconds_matrix:
        fields.append("matrix")
        values.append(matrix_df)
    if include_strengths:
        fields.append("strengths")
        values.append(strengths_df)
    
    if len(fields) == 1:
        return data
    
    GameResult = namedtuple("GameResult", fields)
    return GameResult(*values)
    
def seconds_matrix(df: pd.DataFrame, shifts: pd.DataFrame) -> pd.DataFrame:
    """
    Boolean on-ice matrix by second.
    Index (MultiIndex): player1Id, player1Name, isHome, teamId, eventTeam, isGoalie, positionCode
    Columns: 0..max_sec-1 (seconds)
    """
    # ensure ints
    game_length = int(pd.to_numeric(df["elapsedTime"], errors="coerce").max())
    base = (
        df.query("Event in ['ON','OFF']")[[
            "player1Id","player1Name","isHome","teamId","eventTeam","elapsedTime","Event","isGoalie","period"
        ]]
        .assign(elapsedTime=lambda x: pd.to_numeric(x["elapsedTime"], errors="coerce").astype("Int32"))
        .sort_values(["player1Id","elapsedTime","period"])
    )

    intervals = (
        base.assign(
            endTime=lambda x: x.groupby(
                ["player1Id","player1Name","isHome","teamId","eventTeam","isGoalie"]
            )["elapsedTime"].transform(lambda s: s.shift(-1).fillna(game_length))
        )
        .query("Event == 'ON'")
        .merge(
            shifts[["playerId","positionCode"]].drop_duplicates().rename(columns={"playerId":"player1Id"}),
            on="player1Id", how="left"
        )
    )

    # index
    idx_cols = ["player1Id","player1Name","isHome","teamId","eventTeam","isGoalie","positionCode"]
    player_index = (
        intervals[idx_cols].drop_duplicates().set_index(idx_cols).sort_index().index
    )
    max_sec = int(intervals["endTime"].max())

    mat = pd.DataFrame(False, index=player_index, columns=range(max_sec), dtype=bool)

    # fill
    for _ , r in intervals.iterrows():
        row_key = (r["player1Id"], r["player1Name"], r["isHome"], r["teamId"], r["eventTeam"], r["isGoalie"], r["positionCode"])
        start = int(r["elapsedTime"])
        end   = int(r["endTime"])
        if end > start:
            mat.loc[row_key, start:end-1] = True
    return mat

def strengths_by_second(matrix_df: pd.DataFrame, sep: str = "v", star: str = "*") -> pd.DataFrame:
    """
    Returns per-second table with the exact fields you asked for:
      home_sktrs_count, away_sktrs_count,
      home_goalies_count, away_goalies_count,
      home_strength, away_strength,
      team_str_home, team_str_away
    """
    is_home   = matrix_df.index.get_level_values("isHome").astype(bool)
    is_goalie = matrix_df.index.get_level_values("isGoalie").astype(bool)

    # skater counts (exclude goalies)
    home_sktrs_count = matrix_df[~is_goalie & is_home].sum(axis=0).astype("Int16")
    away_sktrs_count = matrix_df[~is_goalie & ~is_home].sum(axis=0).astype("Int16")

    # goalie counts
    home_goalies_count = matrix_df[is_goalie & is_home].sum(axis=0).astype("Int8")
    away_goalies_count = matrix_df[is_goalie & ~is_home].sum(axis=0).astype("Int8")

    # side strings: skater count + '*' if that side has 0 goalies
    home_strength = home_sktrs_count.astype(str).mask(home_goalies_count.eq(0), home_sktrs_count.astype(str) + star)
    away_strength = away_sktrs_count.astype(str).mask(away_goalies_count.eq(0), away_sktrs_count.astype(str) + star)

    # team-oriented strings
    team_str_home = home_strength.str.cat(away_strength, sep=sep)  # for HOME players
    team_str_away = away_strength.str.cat(home_strength, sep=sep)  # for AWAY players

    out = pd.DataFrame({
        "home_sktrs_count": home_sktrs_count,
        "away_sktrs_count": away_sktrs_count,
        "home_goalies_count": home_goalies_count,
        "away_goalies_count": away_goalies_count,
        "home_strength": home_strength,
        "away_strength": away_strength,
        "team_str_home": team_str_home,
        "team_str_away": team_str_away,
    })
    out.index.name = "second"
    return out



# ============================================================
# 2) Player TOI by (player-perspective) strength
# ============================================================
def toi_by_strength_all(matrix_df: pd.DataFrame, strengths_df: pd.DataFrame, in_seconds: bool = False) -> pd.DataFrame:
    """
    For every player: total TOI by the strength string from THEIR team perspective.
    Output columns: [player-index-levels...], Strength, time_on_ice
    """
    idx_names = list(matrix_df.index.names)
    is_home   = matrix_df.index.get_level_values("isHome").astype(bool).to_numpy()

    # choose proper per-second label for each side
    # (map columns 'team_str_home' or 'team_str_away' onto seconds)
    sec_label_home = strengths_df["team_str_home"]
    sec_label_away = strengths_df["team_str_away"]

    results = []
    for (i, row) in enumerate(matrix_df.itertuples(index=True, name=None)):
        idx = row[0]                 # MultiIndex key
        on  = np.asarray(row[1:], dtype=bool)  # boolean seconds for this player

        # decide side
        player_is_home = is_home[i]
        labels = sec_label_home if player_is_home else sec_label_away

        counts = pd.Series(labels[on]).value_counts()
        counts.name = idx
        results.append(counts)

    out = pd.DataFrame(results).fillna(0)
    if not in_seconds:
        out = out / 60.0
    out.index.names = idx_names
    out = out.reset_index().melt(
        id_vars=idx_names, var_name="Strength", value_name="time_on_ice"
    )
    return out


def _stack_all_columns_to_series(df: pd.DataFrame) -> pd.Series:
    try:
        return df.stack(df.columns.names, future_stack=True)
    except TypeError:
        return df.stack(list(range(df.columns.nlevels)))

def _square_to_long(co: np.ndarray, idx: pd.MultiIndex, right_prefix: str) -> pd.DataFrame:
    co_df = pd.DataFrame(co, index=idx, columns=idx)
    ser = _stack_all_columns_to_series(co_df)
    ser = ser[ser > 0]
    if ser.empty:
        return pd.DataFrame(columns=list(idx.names) + [f"{right_prefix}_{n}" for n in idx.names] + ["TOI_sec"])
    ser.index = ser.index.set_names(list(idx.names) + [f"{right_prefix}_{n}" for n in idx.names])
    return ser.rename("TOI_sec").reset_index()

def _rect_to_long(cross: np.ndarray, left_idx: pd.MultiIndex, right_idx: pd.MultiIndex, right_prefix: str) -> pd.DataFrame:
    df = pd.DataFrame(cross, index=left_idx, columns=right_idx)
    ser = _stack_all_columns_to_series(df)
    ser = ser[ser > 0]
    if ser.empty:
        return pd.DataFrame(columns=list(left_idx.names) + [f"{right_prefix}_{n}" for n in right_idx.names] + ["TOI_sec"])
    ser.index = ser.index.set_names(list(left_idx.names) + [f"{right_prefix}_{n}" for n in right_idx.names])
    return ser.rename("TOI_sec").reset_index()



def shared_toi_teammates_by_strength(
    matrix_df: pd.DataFrame,
    strengths_df: pd.DataFrame,
    *,
    in_seconds: bool = False,
) -> pd.DataFrame:
    """
    Output columns: [player-index...], [tm_* index...], Strength, TOI
    Strength is team-oriented:
      - Home players use strengths_df['team_str_home']
      - Away players use strengths_df['team_str_away']
    Goalies ARE included in pairs and seconds.
    """
    idx_names = list(matrix_df.index.names)
    is_home   = matrix_df.index.get_level_values("isHome").astype(bool).to_numpy()
    all_rows  = np.ones(len(matrix_df), dtype=bool)

    pieces = []

    # HOME players → group seconds by team_str_home
    rows = all_rows & is_home
    if np.any(rows):
        r_idx = matrix_df.index[rows]
        groups = strengths_df["team_str_home"].dropna().groupby(strengths_df["team_str_home"]).groups
        for s, sec_idx in groups.items():
            secs = list(sec_idx)
            if not secs: continue
            M = matrix_df.loc[rows, secs].to_numpy(dtype=np.uint8)
            co = M @ M.T
            np.fill_diagonal(co, 0)
            if co.sum() == 0: continue
            long = _square_to_long(co, r_idx, right_prefix="tm")
            long["Strength"] = s
            pieces.append(long)

    # AWAY players → group seconds by team_str_away
    rows = all_rows & (~is_home)
    if np.any(rows):
        r_idx = matrix_df.index[rows]
        groups = strengths_df["team_str_away"].dropna().groupby(strengths_df["team_str_away"]).groups
        for s, sec_idx in groups.items():
            secs = list(sec_idx)
            if not secs: continue
            M = matrix_df.loc[rows, secs].to_numpy(dtype=np.uint8)
            co = M @ M.T
            np.fill_diagonal(co, 0)
            if co.sum() == 0: continue
            long = _square_to_long(co, r_idx, right_prefix="tm")
            long["Strength"] = s
            pieces.append(long)

    if not pieces:
        cols = idx_names + [f"tm_{n}" for n in idx_names] + ["Strength","TOI"]
        return pd.DataFrame(columns=cols)

    res = pd.concat(pieces, ignore_index=True)
    res["TOI"] = res.pop("TOI_sec") if in_seconds else (res.pop("TOI_sec")/60.0)
    order = idx_names + [f"tm_{n}" for n in idx_names] + ["Strength","TOI"]
    for nm in order:
        if nm not in res.columns: res[nm] = pd.Series(dtype="object")
    return res[order]


def shared_toi_opponents_by_strength(
    matrix_df: pd.DataFrame,
    strengths_df: pd.DataFrame,
    *,
    in_seconds: bool = False,
) -> pd.DataFrame:
    """
    Output columns: [player-index...], [opp_* index...], playerStrength, oppStrength, TOI
    Goalies included.
    """
    idx_names = list(matrix_df.index.names)
    is_home   = matrix_df.index.get_level_values("isHome").astype(bool).to_numpy()
    all_rows  = np.ones(len(matrix_df), dtype=bool)

    # pair both team strings per second
    pair_series = pd.Series(
        list(zip(strengths_df["team_str_home"], strengths_df["team_str_away"])),
        index=strengths_df.index
    )
    groups = {k: idxs for k, idxs in pair_series.groupby(pair_series).groups.items()
              if pd.notna(k[0]) and pd.notna(k[1])}

    pieces = []
    rows_home = all_rows & is_home
    rows_away = all_rows & (~is_home)

    for (home_label, away_label), sec_idx in groups.items():
        secs = list(sec_idx)
        if not secs or not (np.any(rows_home) and np.any(rows_away)):
            continue
        Mh = matrix_df.loc[rows_home, secs].to_numpy(dtype=np.uint8)
        Ma = matrix_df.loc[rows_away, secs].to_numpy(dtype=np.uint8)
        cross = Mh @ Ma.T
        if cross.sum() == 0:
            continue

        # Home perspective
        long1 = _rect_to_long(cross, matrix_df.index[rows_home], matrix_df.index[rows_away], right_prefix="opp")
        long1["playerStrength"] = home_label
        long1["oppStrength"]    = away_label
        pieces.append(long1)

        # Away perspective
        long2 = _rect_to_long(cross.T, matrix_df.index[rows_away], matrix_df.index[rows_home], right_prefix="opp")
        long2["playerStrength"] = away_label
        long2["oppStrength"]    = home_label
        pieces.append(long2)

    if not pieces:
        cols = idx_names + [f"opp_{n}" for n in idx_names] + ["playerStrength","oppStrength","TOI"]
        return pd.DataFrame(columns=cols)

    res = pd.concat(pieces, ignore_index=True)
    res["TOI"] = res.pop("TOI_sec") if in_seconds else (res.pop("TOI_sec")/60.0)
    order = idx_names + [f"opp_{n}" for n in idx_names] + ["playerStrength","oppStrength","TOI"]
    for nm in order:
        if nm not in res.columns: res[nm] = pd.Series(dtype="object")
    return res[order]

# --- small utilities ---------------------------------------------------------
def _expand_mi_tuple(mi_tuple, names, prefix):
    """Expand a MultiIndex tuple -> dict of {f'{prefix}_{name}': value}."""
    return {f"{prefix}_{n}": v for n, v in zip(names, mi_tuple)}

def _mk_rows_df(keys, counts, idx_names, member_prefix="p"):
    """
    keys: list of (Strength, (rowpos1,rowpos2,...)) ; counts: seconds
    returns long dataframe with p1_*, p2_*, ... + Strength + TOI
    """
    rows = []
    for (strength, combo_pos), sec in zip(keys, counts):
        row = {"Strength": strength, "TOI_sec": sec}
        for k, pos in enumerate(combo_pos, start=1):
            row.update(_expand_mi_tuple(pos, idx_names, f"{member_prefix}{k}"))
        rows.append(row)
    return pd.DataFrame(rows)

# --- 1) TEAMMATE COMBOS (same side) -----------------------------------------
def combos_teammates_by_strength(
    matrix_df: pd.DataFrame,
    strengths_df: pd.DataFrame,           # from strengths_by_second(...)
    *,
    N: int = 3,
    include_goalies: bool = False,        # combos are usually skaters; set True to allow goalies in combos
    in_seconds: bool = False,
    min_seconds: int = 1,                 # drop combos with less TOI than this (in seconds)
) -> pd.DataFrame:
    """
    Returns one row per N-player same-side combo per team-strength:
      p1_*, p2_*, ... pN_* (player bio from matrix index), Strength, TOI

    Strength is **from that team's perspective**:
      - home seconds use strengths_df['team_str_home']
      - away seconds use strengths_df['team_str_away']
    """
    idx_names = list(matrix_df.index.names)
    is_home   = matrix_df.index.get_level_values("isHome").astype(bool).to_numpy()
    is_goalie = matrix_df.index.get_level_values("isGoalie").astype(bool).to_numpy()

    # choose rows to consider for combos
    row_mask_home = (is_home)   & (include_goalies | (~is_goalie))
    row_mask_away = (~is_home)  & (include_goalies | (~is_goalie))

    out_keys, out_counts = [], []

    # --- HOME side
    if row_mask_home.any():
        M  = matrix_df.loc[row_mask_home]            # players x secs
        Mi = matrix_df.index[row_mask_home]
        sec_groups = strengths_df["team_str_home"].dropna().groupby(strengths_df["team_str_home"]).groups
        # precompute boolean numpy for speed
        M_np = M.to_numpy(dtype=bool)
        for label, secs_idx in sec_groups.items():
            secs = np.fromiter(secs_idx, dtype=int)
            if secs.size == 0: 
                continue
            sub = M_np[:, secs]                       # P x T'
            # iterate seconds: build combos from active players
            counts = {}
            for t in range(sub.shape[1]):
                on = np.flatnonzero(sub[:, t])
                if on.size < N: 
                    continue
                for combo in combinations(on.tolist(), N):
                    # represent combo as tuple of MultiIndex tuples (for stable identity)
                    combo_key = tuple(Mi[i] for i in combo)
                    counts[combo_key] = counts.get(combo_key, 0) + 1
            # push to output
            for combo_key, sec in counts.items():
                if sec >= min_seconds:
                    out_keys.append((label, combo_key))
                    out_counts.append(sec)

    # --- AWAY side
    if row_mask_away.any():
        M  = matrix_df.loc[row_mask_away]
        Mi = matrix_df.index[row_mask_away]
        M_np = M.to_numpy(dtype=bool)
        sec_groups = strengths_df["team_str_away"].dropna().groupby(strengths_df["team_str_away"]).groups
        for label, secs_idx in sec_groups.items():
            secs = np.fromiter(secs_idx, dtype=int)
            if secs.size == 0:
                continue
            sub = M_np[:, secs]
            counts = {}
            for t in range(sub.shape[1]):
                on = np.flatnonzero(sub[:, t])
                if on.size < N:
                    continue
                for combo in combinations(on.tolist(), N):
                    combo_key = tuple(Mi[i] for i in combo)
                    counts[combo_key] = counts.get(combo_key, 0) + 1
            for combo_key, sec in counts.items():
                if sec >= min_seconds:
                    out_keys.append((label, combo_key))
                    out_counts.append(sec)

    if not out_keys:
        # build empty frame with the expected columns
        cols = []
        for k in range(1, N+1):
            cols += [f"p{k}_{n}" for n in idx_names]
        cols += ["Strength", "TOI"]
        return pd.DataFrame(columns=cols)

    df = _mk_rows_df(out_keys, out_counts, idx_names, member_prefix="p")
    df["TOI"] = df.pop("TOI_sec") if in_seconds else (df.pop("TOI_sec")/60.0)
    # column order
    ordered = []
    for k in range(1, N+1):
        ordered += [f"p{k}_{n}" for n in idx_names]
    ordered += ["Strength", "TOI"]
    return df[ordered].sort_values(["Strength","TOI"], ascending=[True, False]).reset_index(drop=True)

# --- 2) OPPONENT COMBOS (vs each player) ------------------------------------
def combos_opponents_by_strength(
    matrix_df: pd.DataFrame,
    strengths_df: pd.DataFrame,           # from strengths_by_second(...)
    *,
    N: int = 2,
    include_goalies: bool = False,        # usually False (opponent skater combos)
    in_seconds: bool = False,
    min_seconds: int = 1,
) -> pd.DataFrame:
    """
    For every player, returns the TOI he shared with **N opponents at once** per strength.
    Columns:
      player_* (bio), opp1_* … oppN_* (bio), Strength, TOI

    Strength is from the **player's team perspective**:
      - home player → strengths_df['team_str_home']
      - away player → strengths_df['team_str_away']
    """
    idx_names = list(matrix_df.index.names)
    is_home   = matrix_df.index.get_level_values("isHome").astype(bool).to_numpy()
    is_goalie = matrix_df.index.get_level_values("isGoalie").astype(bool).to_numpy()

    # masks
    player_rows_home = is_home
    player_rows_away = ~is_home

    opp_rows_home = (~is_home) & (include_goalies | (~is_goalie))   # opponents for HOME players are AWAY rows
    opp_rows_away = ( is_home) & (include_goalies | (~is_goalie))   # opponents for AWAY players are HOME rows

    # numpy views
    M_all   = matrix_df.to_numpy(dtype=bool)
    idx_all = matrix_df.index

    out_rows = []

    # Helper to process one side
    def _process_side(player_mask, opp_mask, label_series):
        P_idx = np.flatnonzero(player_mask)
        O_idx = np.flatnonzero(opp_mask)
        if P_idx.size == 0 or O_idx.size == 0:
            return
        # split matrices
        M_players = M_all[P_idx, :]    # P x T
        M_opps    = M_all[O_idx, :]    # O x T

        # group seconds by strength label
        groups = label_series.dropna().groupby(label_series).groups
        for label, secs_idx in groups.items():
            secs = np.fromiter(secs_idx, dtype=int)
            if secs.size == 0:
                continue
            P_sub = M_players[:, secs]            # P x T'
            O_sub = M_opps[:, secs]               # O x T'
            # precompute opponent presence per second → combinations once per second,
            # then attribute those seconds to every present player.
            for t in range(P_sub.shape[1]):
                opp_on = np.flatnonzero(O_sub[:, t])
                if opp_on.size < N:
                    continue
                opp_combos = list(combinations(opp_on.tolist(), N))
                if not opp_combos:
                    continue
                ply_on = np.flatnonzero(P_sub[:, t])
                if ply_on.size == 0:
                    continue
                for p in ply_on:
                    p_key  = idx_all[P_idx[p]]
                    for comb in opp_combos:
                        opp_keys = tuple(idx_all[O_idx[c]] for c in comb)
                        out_rows.append((label, p_key, opp_keys))

    # home and away perspectives
    _process_side(player_rows_home, opp_rows_home, strengths_df["team_str_home"])
    _process_side(player_rows_away, opp_rows_away, strengths_df["team_str_away"])

    if not out_rows:
        cols = [f"player_{n}" for n in idx_names]
        for k in range(1, N+1):
            cols += [f"opp{k}_{n}" for n in idx_names]
        cols += ["Strength", "TOI"]
        return pd.DataFrame(columns=cols)

    # aggregate seconds
    # key: (Strength, player_mi_tuple, opp_combo_mi_tuple)
    c = Counter(out_rows)  # each occurrence is 1 second
    keys, secs = zip(*c.items())

    # build rows
    records = []
    for (label, player_mi, opp_combo_mi), sec in zip(keys, secs):
        row = {"Strength": label, "TOI_sec": sec}
        row.update(_expand_mi_tuple(player_mi, idx_names, "player"))
        for k, mi in enumerate(opp_combo_mi, start=1):
            row.update(_expand_mi_tuple(mi, idx_names, f"opp{k}"))
        records.append(row)

    df = pd.DataFrame.from_records(records)
    # units / order
    df["TOI"] = df.pop("TOI_sec") if in_seconds else (df.pop("TOI_sec")/60.0)
    ordered = [f"player_{n}" for n in idx_names]
    for k in range(1, N+1):
        ordered += [f"opp{k}_{n}" for n in idx_names]
    ordered += ["Strength", "TOI"]
    # filter by min_seconds
    if not in_seconds and min_seconds > 1:
        df = df[df["TOI"]*60 >= min_seconds]
    elif in_seconds and min_seconds > 1:
        df = df[df["TOI"] >= min_seconds]
    return df[ordered].sort_values(["Strength","TOI"], ascending=[True, False]).reset_index(drop=True)


# --- small helpers -----------------------------------------------------------
def _expand_mi_tuple(mi_tuple, names, prefix):
    return {f"{prefix}_{n}": v for n, v in zip(names, mi_tuple)}

def _build_empty_cols(idx_names, n_team, m_opp):
    cols = [f"p{k}_{n}" for k in range(1, n_team+1) for n in idx_names]
    cols += [f"opp{k}_{n}" for k in range(1, m_opp+1) for n in idx_names]
    cols += ["Strength", "TOI"]
    return cols

# --- the general combo function ---------------------------------------------
def combo_toi_by_strength(
    matrix_df: pd.DataFrame,
    strengths_df: pd.DataFrame,
    *,
    n_team: int = 2,                 # size of your-team combo
    m_opp: int  = 0,                 # size of opponent combo (0 = ignore opps)
    include_goalies_team: bool = False,
    include_goalies_opp: bool  = False,
    side: str = "both",              # "home", "away", or "both"
    in_seconds: bool = False,
    min_seconds: int = 1,
) -> pd.DataFrame:
    """
    Count seconds where an n_team-player combo (same side) was on-ice
    simultaneously with an m_opp-player combo (opposite side), grouped by
    the *team-perspective* strength label.

    Strength used:
      - For HOME combos -> strengths_df['team_str_home']
      - For AWAY  combos -> strengths_df['team_str_away']
    """
    assert side in {"home", "away", "both"}

    idx_names = list(matrix_df.index.names)
    is_home   = matrix_df.index.get_level_values("isHome").astype(bool).to_numpy()
    is_goalie = matrix_df.index.get_level_values("isGoalie").astype(bool).to_numpy()

    # team-side row masks
    team_home_mask = is_home & (include_goalies_team | (~is_goalie))
    team_away_mask = (~is_home) & (include_goalies_team | (~is_goalie))

    # opponent-side row masks
    opp_for_home_mask = (~is_home) & (include_goalies_opp | (~is_goalie))
    opp_for_away_mask = (is_home)  & (include_goalies_opp | (~is_goalie))

    # numpy views
    M_all   = matrix_df.to_numpy(dtype=bool)
    idx_all = matrix_df.index

    def _process(team_mask, opp_mask, label_series):
        keys = []
        # precompute indices
        T_idx = np.flatnonzero(team_mask)
        O_idx = np.flatnonzero(opp_mask)
        if T_idx.size == 0:
            return Counter()

        M_team = M_all[T_idx, :]                # T x S
        M_opp  = M_all[O_idx, :] if m_opp > 0 else None

        # seconds grouped by team-perspective strength
        groups = label_series.dropna().groupby(label_series).groups
        cnt = Counter()

        for label, sec_idx in groups.items():
            secs = np.fromiter(sec_idx, dtype=int)
            if secs.size == 0:
                continue

            T_sub = M_team[:, secs]             # T x S'
            O_sub = M_opp[:, secs] if m_opp > 0 else None

            # iterate seconds; make combos only from active players
            for t in range(T_sub.shape[1]):
                t_on = np.flatnonzero(T_sub[:, t])
                if t_on.size < n_team:
                    continue
                team_combos = list(combinations(t_on.tolist(), n_team))

                if m_opp == 0:
                    for tc in team_combos:
                        team_key = tuple(idx_all[T_idx[i]] for i in tc)
                        cnt[(label, team_key, tuple())] += 1
                else:
                    o_on = np.flatnonzero(O_sub[:, t])
                    if o_on.size < m_opp:
                        continue
                    opp_combos = list(combinations(o_on.tolist(), m_opp))
                    if not opp_combos:
                        continue
                    for tc in team_combos:
                        team_key = tuple(idx_all[T_idx[i]] for i in tc)
                        for oc in opp_combos:
                            opp_key = tuple(idx_all[O_idx[j]] for j in oc)
                            cnt[(label, team_key, opp_key)] += 1
        return cnt

    counts = Counter()

    if side in {"home", "both"}:
        counts.update(_process(team_home_mask, opp_for_home_mask, strengths_df["team_str_home"]))
    if side in {"away", "both"}:
        counts.update(_process(team_away_mask, opp_for_away_mask, strengths_df["team_str_away"]))

    if not counts:
        return pd.DataFrame(columns=_build_empty_cols(idx_names, n_team, m_opp))

    # materialize to rows
    records = []
    for (label, team_combo, opp_combo), secs in counts.items():
        if secs < min_seconds:
            continue
        row = {"Strength": label, "TOI_sec": secs}
        for k, mi in enumerate(team_combo, start=1):
            row.update(_expand_mi_tuple(mi, idx_names, f"p{k}"))
        for k, mi in enumerate(opp_combo, start=1):
            row.update(_expand_mi_tuple(mi, idx_names, f"opp{k}"))
        records.append(row)

    if not records:
        return pd.DataFrame(columns=_build_empty_cols(idx_names, n_team, m_opp))

    df = pd.DataFrame.from_records(records)

    # units + column order
    df["TOI"] = df.pop("TOI_sec") if in_seconds else (df.pop("TOI_sec")/60.0)

    order = []
    for k in range(1, n_team+1):
        order += [f"p{k}_{n}" for n in idx_names]
    for k in range(1, m_opp+1):
        order += [f"opp{k}_{n}" for n in idx_names]
    order += ["Strength", "TOI"]

    # ensure all expected columns exist (in case some positions never appear)
    for col in order:
        if col not in df.columns:
            df[col] = pd.Series(dtype="object")

    return df[order].sort_values(["Strength","TOI"], ascending=[True, False]).reset_index(drop=True)



def combo_shot_metrics_by_strength(
    matrix_df: pd.DataFrame,
    strengths_df: pd.DataFrame,
    pbp_df: pd.DataFrame,
    *,
    n_team: int = 2,
    m_opp: int  = 0,
    include_goalies_team: bool = False,
    include_goalies_opp: bool  = False,
    side: str = "both",
    include_toi: bool = True,
    toi_in_seconds: bool = False,         # if True, TOI values are seconds; else minutes
    precomputed_toi: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    One row per combo (team N + optional opponent M) per strength:
      p1_*,...,pN_*, [opp1_*,...,oppM_*], Strength,
      ShotsFor, ShotsAgainst, FenwickFor, FenwickAgainst, CorsiFor, CorsiAgainst,
      [TOI, rates/60, shares]
    """

    assert side in {"home", "away", "both"}

    idx_names = list(matrix_df.index.names)
    is_home   = matrix_df.index.get_level_values("isHome").astype(bool).to_numpy()
    is_goalie = matrix_df.index.get_level_values("isGoalie").astype(bool).to_numpy()

    team_home_mask = is_home   & (include_goalies_team | (~is_goalie))
    team_away_mask = (~is_home) & (include_goalies_team | (~is_goalie))
    opp_for_home_mask = (~is_home) & (include_goalies_opp | (~is_goalie))
    opp_for_away_mask = ( is_home)  & (include_goalies_opp | (~is_goalie))

    M = matrix_df.to_numpy(dtype=bool)
    _, S = M.shape
    idx_all = matrix_df.index

    H_team_idx = np.flatnonzero(team_home_mask)
    A_team_idx = np.flatnonzero(team_away_mask)
    H_opp_idx  = np.flatnonzero(opp_for_away_mask)  # home as opponents for away team
    A_opp_idx  = np.flatnonzero(opp_for_home_mask)  # away as opponents for home team

    # --- PBP: filter to attempts ---
    events = pbp_df.loc[pbp_df["Event"].isin(["SHOT","GOAL","MISS","BLOCK"]),
                        ["Event","elapsedTime","isHome"]].copy()
    events["sec"] = pd.to_numeric(events["elapsedTime"], errors="coerce")
    events = events[events["sec"].notna()]
    events["sec"] = events["sec"].astype("Int32").clip(lower=0, upper=S-1)

    evt_is_home  = events["isHome"].astype(int).astype(bool).to_numpy()
    evt_is_block = events["Event"].eq("BLOCK").to_numpy()
    attempt_home = np.where(evt_is_block, ~evt_is_home, evt_is_home)

    is_shot = events["Event"].isin(["SHOT","GOAL"]).to_numpy()
    is_fen  = events["Event"].isin(["SHOT","GOAL","MISS"]).to_numpy()

    home_label = strengths_df["team_str_home"]
    away_label = strengths_df["team_str_away"]

    # SF, SA, FF, FA, CF, CA
    counts = defaultdict(lambda: np.zeros(6, dtype=int))

    # --- Attribute each event to relevant combos on that second ---
    for e_idx, r in events.reset_index(drop=True).iterrows():
        s = int(r["sec"])
        att_home = bool(attempt_home[e_idx])

        label_for = home_label.iloc[s] if att_home else away_label.iloc[s]
        label_def = away_label.iloc[s] if att_home else home_label.iloc[s]

        w_shot = int(is_shot[e_idx])
        w_fen  = int(is_fen[e_idx])
        w_cor  = 1

        # FOR side
        if (att_home and side in {"home","both"}) or ((not att_home) and side in {"away","both"}):
            T_idx = H_team_idx if att_home else A_team_idx
            O_idx = A_opp_idx  if att_home else H_opp_idx

            team_on = T_idx[np.flatnonzero(M[T_idx, s])]
            if team_on.size >= n_team:
                team_combos = combinations(sorted(team_on.tolist()), n_team)
                if m_opp > 0:
                    opp_on = O_idx[np.flatnonzero(M[O_idx, s])]
                    opp_combos = list(combinations(sorted(opp_on.tolist()), m_opp)) if opp_on.size >= m_opp else []
                else:
                    opp_combos = [()]

                for tc in team_combos:
                    tc_keys = tuple(idx_all[i] for i in tc)
                    if m_opp == 0:
                        key = (label_for, tc_keys, ())
                        counts[key][0] += w_shot; counts[key][2] += w_fen; counts[key][4] += w_cor
                    else:
                        for oc in opp_combos:
                            oc_keys = tuple(idx_all[j] for j in oc)
                            key = (label_for, tc_keys, oc_keys)
                            counts[key][0] += w_shot; counts[key][2] += w_fen; counts[key][4] += w_cor

        # AGAINST side
        if (att_home and side in {"away","both"}) or ((not att_home) and side in {"home","both"}):
            T_idx = A_team_idx if att_home else H_team_idx
            O_idx = H_opp_idx  if att_home else A_opp_idx

            team_on = T_idx[np.flatnonzero(M[T_idx, s])]
            if team_on.size >= n_team:
                team_combos = combinations(sorted(team_on.tolist()), n_team)
                if m_opp > 0:
                    opp_on = O_idx[np.flatnonzero(M[O_idx, s])]
                    opp_combos = list(combinations(sorted(opp_on.tolist()), m_opp)) if opp_on.size >= m_opp else []
                else:
                    opp_combos = [()]

                for tc in team_combos:
                    tc_keys = tuple(idx_all[i] for i in tc)
                    if m_opp == 0:
                        key = (label_def, tc_keys, ())
                        counts[key][1] += w_shot; counts[key][3] += w_fen; counts[key][5] += w_cor
                    else:
                        for oc in opp_combos:
                            oc_keys = tuple(idx_all[j] for j in oc)
                            key = (label_def, tc_keys, oc_keys)
                            counts[key][1] += w_shot; counts[key][3] += w_fen; counts[key][5] += w_cor

    # --- Build DataFrame ---
    rows = []
    for (label, team_keys, opp_keys), vec in counts.items():
        row = {"Strength": label,
               "ShotsFor": vec[0], "ShotsAgainst": vec[1],
               "FenwickFor": vec[2], "FenwickAgainst": vec[3],
               "CorsiFor": vec[4], "CorsiAgainst": vec[5]}
        for i, mi in enumerate(team_keys, start=1):
            for n, v in zip(idx_names, mi): row[f"p{i}_{n}"] = v
        for i, mi in enumerate(opp_keys, start=1):
            for n, v in zip(idx_names, mi): row[f"opp{i}_{n}"] = v
        rows.append(row)

    if not rows:
        base_cols = []
        for k in range(1, n_team+1): base_cols += [f"p{k}_{n}" for n in idx_names]
        for k in range(1, m_opp+1):  base_cols += [f"opp{k}_{n}" for n in idx_names]
        base_cols += ["Strength","ShotsFor","ShotsAgainst","FenwickFor","FenwickAgainst","CorsiFor","CorsiAgainst"]
        if include_toi: base_cols += ["TOI"]
        # include differentials / shares / per60 columns as empty too
        base_cols += ["ShotsDifferential","FenwickDifferential","CorsiDifferential",
                      "ShotsFor%","FenwickFor%","CorsiFor%"]
        if include_toi:
            for col in ["ShotsFor","ShotsAgainst","ShotsDifferential",
                        "FenwickFor","FenwickAgainst","FenwickDifferential",
                        "CorsiFor","CorsiAgainst","CorsiDifferential"]:
                base_cols.append(f"{col}/60")
        return pd.DataFrame(columns=base_cols)

    df = pd.DataFrame(rows)

    # Ensure combo columns exist (some MI fields may be missing if constant)
    for k in range(1, n_team+1):
        for n in idx_names:
            col = f"p{k}_{n}"
            if col not in df.columns: df[col] = pd.NA
    for k in range(1, m_opp+1):
        for n in idx_names:
            col = f"opp{k}_{n}"
            if col not in df.columns: df[col] = pd.NA

    # ----- Optional TOI attach -----
    if include_toi:
        key_cols = []
        for k in range(1, n_team+1): key_cols += [f"p{k}_{n}" for n in idx_names]
        for k in range(1, m_opp+1):  key_cols += [f"opp{k}_{n}" for n in idx_names]
        key_cols += ["Strength"]

        if precomputed_toi is None:
            toi_df = combo_toi_by_strength(
                matrix_df, strengths_df,
                n_team=n_team, m_opp=m_opp,
                include_goalies_team=include_goalies_team,
                include_goalies_opp=include_goalies_opp,
                side=side,
                in_seconds=toi_in_seconds,
            )
        else:
            toi_df = precomputed_toi.copy()

        df = df.merge(toi_df[key_cols + ["TOI"]], on=key_cols, how="left")

    # Add +/- and shares
    df["ShotsDifferential"]   = df["ShotsFor"]   - df["ShotsAgainst"]
    df["FenwickDifferential"] = df["FenwickFor"] - df["FenwickAgainst"]
    df["CorsiDifferential"]   = df["CorsiFor"]   - df["CorsiAgainst"]

    denom = (df["ShotsFor"]   + df["ShotsAgainst"]).replace(0, np.nan)
    df["ShotsFor%"]   = df["ShotsFor"]   / denom
    denom = (df["FenwickFor"] + df["FenwickAgainst"]).replace(0, np.nan)
    df["FenwickFor%"] = df["FenwickFor"] / denom
    denom = (df["CorsiFor"]   + df["CorsiAgainst"]).replace(0, np.nan)
    df["CorsiFor%"]   = df["CorsiFor"]   / denom

    # Per-60 (if TOI attached)
    ordered = []
    for k in range(1, n_team+1): ordered += [f"p{k}_{n}" for n in idx_names]
    for k in range(1, m_opp+1):  ordered += [f"opp{k}_{n}" for n in idx_names]
    ordered += ["Strength",
                "ShotsFor","ShotsAgainst","ShotsDifferential","ShotsFor%",
                "FenwickFor","FenwickAgainst","FenwickDifferential","FenwickFor%",
                "CorsiFor","CorsiAgainst","CorsiDifferential","CorsiFor%"]
    if include_toi:
        ordered += ["TOI"]
        # convert TOI to minutes for per-60 math if needed
        toi_minutes = df["TOI"] if not toi_in_seconds else (df["TOI"] / 60.0)
        toi_minutes = toi_minutes.replace(0, np.nan)
        for col in ["ShotsFor","ShotsAgainst","ShotsDifferential",
                    "FenwickFor","FenwickAgainst","FenwickDifferential",
                    "CorsiFor","CorsiAgainst","CorsiDifferential"]:
            df[f"{col}/60"] = df[col] * 60.0 / toi_minutes
        ordered += [f"{c}/60" for c in ["ShotsFor","ShotsAgainst","ShotsDifferential",
                                        "FenwickFor","FenwickAgainst","FenwickDifferential",
                                        "CorsiFor","CorsiAgainst","CorsiDifferential"]]
        

    # Guarantee all columns exist (edge cases)
    for col in ordered:
        if col not in df.columns: df[col] = pd.NA
        
        

    return df[ordered].sort_values(["Strength","CorsiFor","CorsiAgainst"], ascending=[True, False, False]).reset_index(drop=True)


def engineer_xg_features(
    pbp_df: pd.DataFrame,
    *,
    goal_x: float = 89.0,
    goal_y: float = 0.0,
    rebound_window_s: int = 3,
    on_off_events: tuple = ("ON", "OFF"),
    shot_like_events_prev: tuple = ("SHOT",),   # previous-event types that can parent a rebound
) -> pd.DataFrame:
    """
    Add all xG feature columns to a copy of pbp_df and return it.

    Creates/updates:
      x_norm, y_norm, distanceFromGoal, angle_signed,
      isHome, strengthDiff, scoreDiff,
      previousEvent, previousTeam, previousElapsedTime, timeDiff, isRebound,
      shooterSkaters, defendingSkaters, shootEmptyNet,
      isGoal

    Assumes columns like: gameId, elapsedTime, Event, eventTeam, homeTeam, awayTeam,
    xCoord, yCoord, homeScore, awayScore, home_on_count, away_on_count,
    pulled_home, pulled_away.
    Missing ones are created as NA and handled gracefully.
    """
    df = pbp_df.copy()

    # --- Ensure required columns exist to avoid KeyErrors ---
    need_cols = [
        "gameId", "elapsedTime", "Event", "eventTeam", "homeTeam", "awayTeam",
        "xCoord", "yCoord", "homeScore", "awayScore",
        "home_on_count", "away_on_count", "pulled_home", "pulled_away"
    ]
    for c in need_cols:
        if c not in df.columns:
            df[c] = pd.NA

    # ============================================
    # Geometry: normalize coords to attack +x, preserve handedness
    # ============================================
    x_raw = df["xCoord"].to_numpy(dtype="float64")
    y_raw = df["yCoord"].to_numpy(dtype="float64")

    sign = np.where(np.isfinite(x_raw) & (x_raw < 0), -1.0, 1.0)  # mirror to +x; NaNs -> +1
    x_norm = sign * x_raw
    y_norm = sign * y_raw

    df["x_norm"] = x_norm
    df["y_norm"] = y_norm

    dx = goal_x - x_norm
    dy = goal_y - y_norm
    df["distanceFromGoal"] = np.hypot(dx, dy)
    df["angle_signed"] = np.degrees(np.arctan2(y_norm, dx))  # ~[-90, 90]

    # ============================================
    # Home/away role for this event
    # ============================================
    df["isHome"] = df["eventTeam"].eq(df["homeTeam"]).astype("boolean")

    # ============================================
    # Strength diff from shooter's perspective (skaters on ice)
    # ============================================
    home_on = pd.to_numeric(df["home_on_count"], errors="coerce")
    away_on = pd.to_numeric(df["away_on_count"], errors="coerce")

    df["strengthDiff"] = np.where(
        df["eventTeam"].eq(df["homeTeam"]),
        (home_on - away_on).to_numpy(dtype="float64"),
        np.where(
            df["eventTeam"].eq(df["awayTeam"]),
            (away_on - home_on).to_numpy(dtype="float64"),
            np.nan
        )
    )

    # ============================================
    # ScoreDiff from shooter's perspective (pre-shot; undo goal on this row)
    # ============================================
    home_sc = pd.to_numeric(df["homeScore"], errors="coerce")
    away_sc = pd.to_numeric(df["awayScore"], errors="coerce")

    is_goal = df["Event"].eq("GOAL")
    is_home_team = df["eventTeam"].eq(df["homeTeam"])
    is_away_team = df["eventTeam"].eq(df["awayTeam"])

    # undo the increment on GOAL rows so score diff is the state *before* the shot
    home_sc_pre = np.where(is_goal & is_home_team, home_sc - 1, home_sc)
    away_sc_pre = np.where(is_goal & is_away_team, away_sc - 1, away_sc)

    df["scoreDiff"] = np.where(
        is_home_team,
        home_sc_pre - away_sc_pre,
        np.where(is_away_team, away_sc_pre - home_sc_pre, np.nan)
    ).astype("float64")

    # ============================================
    # Shooter/defender skater counts
    # ============================================
    df["shooterSkaters"] = np.where(is_home_team, home_on, np.where(is_away_team, away_on, np.nan))
    df["defendingSkaters"] = np.where(is_home_team, away_on, np.where(is_away_team, home_on, np.nan))

    # ============================================
    # Empty-net (the net being attacked has no goalie)
    # Using pulled_* flags from feed
    # ============================================
    pulled_home = (pd.to_numeric(df["pulled_home"], errors="coerce") == 1)
    pulled_away = (pd.to_numeric(df["pulled_away"], errors="coerce") == 1)

    df["shootEmptyNet"] = (
        (is_home_team & pulled_away) | (is_away_team & pulled_home)
    ).astype("boolean")

    # ============================================
    # Rebounds: previous shot-like by same team within window
    # ============================================
    df["elapsedTime"] = pd.to_numeric(df["elapsedTime"], errors="coerce")
    df = df.sort_values(["gameId", "elapsedTime"], kind="mergesort")

    is_play = ~df["Event"].isin(on_off_events)
    is_shot_like = df["Event"].isin(shot_like_events_prev)

    df.loc[is_play, "previousEvent"] = df.loc[is_play].groupby("gameId")["Event"].shift(1)
    df.loc[is_play, "previousTeam"] = df.loc[is_play].groupby("gameId")["eventTeam"].shift(1)
    df.loc[is_play, "previousEventSameTeam"] = df.loc[is_play]['previousTeam'] == df.loc[is_play]['eventTeam']
    df.loc[is_play, "previousElapsedTime"] = df.loc[is_play].groupby("gameId")["elapsedTime"].shift(1)
    df.loc[is_play, "previousEventDistanceFromGoal"] = df.loc[is_play].groupby("gameId")["distanceFromGoal"].shift(1)
    df.loc[is_play, "previousEventAngleSigned"] = df.loc[is_play].groupby("gameId")["angle_signed"].shift(1)
    df.loc[is_play, "previousEventXNorm"] = df.loc[is_play].groupby("gameId")["x_norm"].shift(1)
    df.loc[is_play, "previousEventYNorm"] = df.loc[is_play].groupby("gameId")["y_norm"].shift(1)
    

    df["timeDiff"] = df["elapsedTime"] - df["previousElapsedTime"]

    mask_rebound = (
        df["previousEvent"].isin(shot_like_events_prev)
        & is_shot_like
        & df["previousTeam"].eq(df["eventTeam"])
        & df["timeDiff"].between(0, rebound_window_s, inclusive="both")
    )

    df["isRebound"] = pd.Series(mask_rebound, index=df.index).where(is_play, pd.NA).astype("boolean")

    # ============================================
    # Goal flag
    # ============================================
    df["isGoal"] = df["Event"].eq("GOAL").astype("boolean")

    return df

def _ensure_columns(df, cols, fill_val=np.nan):
    """Create any missing columns so the pipeline won't crash."""
    missing = [c for c in cols if c not in df.columns]
    if missing:
        df = df.copy()
        for c in missing:
            df[c] = fill_val
    return df

def build_shots_design_matrix(pbp_df: pd.DataFrame) -> pd.DataFrame:
    """
    Slice shots, coerce dtypes, and one-hot encode categoricals.
    Returns a design matrix with columns ready to align to training features.
    """
    # Ensure required columns exist (no-ops if already there)
    pbp_df = _ensure_columns(pbp_df, BASE_NUM + BASE_BOOL + CAT_COLS + ["Event"])

    # Filter to shot-like events used in training
    shots = pbp_df.loc[pbp_df["Event"].isin(EVENTS_FOR_XG)].copy()

    # Dtype coercion (safe)
    for c in BASE_NUM:
        shots[c] = pd.to_numeric(shots[c], errors="coerce")
    for c in BASE_BOOL:
        shots[c] = shots[c].fillna(False).astype("int8")  # model expects numeric
    for c in CAT_COLS:
        shots[c] = shots[c].astype("string").str.strip().fillna("<NA>")

    # One-hot encode categoricals
    X = pd.get_dummies(
        shots[BASE_NUM + BASE_BOOL + CAT_COLS],
        columns=CAT_COLS,
        dummy_na=True,   # keep NA bucket consistent with training
        drop_first=False
    )
    return shots, X

def predict_xg_for_pbp(pbp_df: pd.DataFrame,
                       model_path: str = MODEL_PATH,
                       feat_path: str = FEAT_PATH,
                       xg_colname: str = "xG") -> pd.DataFrame:
    """
    Returns a copy of pbp_df with an 'xG' column filled only for shot rows.
    """
    # Build design matrix from PBP
    shots, X = build_shots_design_matrix(pbp_df)

    # Load model + training feature order
    booster = xgb.Booster()
    booster.load_model(model_path)
    train_cols = joblib.load(feat_path)  # list of column names used during training (after one-hot)

    # Align columns to training (create missing, keep order)
    X_aligned = _align_to_training_columns(X, feat_path)

    # Predict
    dmat = xgb.DMatrix(X_aligned.to_numpy(dtype=np.float32))
    shots[xg_colname] = booster.predict(dmat)

    # Merge xG back to PBP on index (shots keeps original index)
    out = pbp_df.copy()
    out[xg_colname] = np.nan
    out.loc[shots.index, xg_colname] = shots[xg_colname].values
    return out

def _align_to_training_columns(X: pd.DataFrame, feat_path: str) -> pd.DataFrame:
    """Safely align feature matrix X to the training column list stored at feat_path."""
    import joblib
    train_cols = joblib.load(feat_path)  # list of column names used during training (after one-hot)

    # Ensure train_cols are unique (defensive)
    if len(train_cols) != len(pd.Index(train_cols).unique()):
        # Keep first occurrence to preserve model column order
        train_cols = list(pd.Index(train_cols).unique())

    # Make sure all column labels are strings (avoids 1 vs "1" collisions later)
    X = X.copy()
    X.columns = X.columns.astype(str)

    # === 1) DEDUPE ===
    if not X.columns.is_unique:
        # For one-hot/dummy duplicates, max() is correct (any 1 wins).
        # For numeric engineered duplicates, consider swapping to .mean() if that’s more appropriate.
        X = X.T.groupby(level=0).max().T

    # === 2) FILL MISSING ===
    missing = [c for c in train_cols if c not in X.columns]
    if missing:
        # Add as float to match model’s expected dtype
        for c in missing:
            X[c] = 0.0

    # === 3) DROP EXTRAS ===
    extras = [c for c in X.columns if c not in train_cols]
    if extras:
        X = X.drop(columns=extras)

    # === 4) ORDER ===
    X = X[train_cols]

    # Final sanity checks
    assert X.columns.is_unique, "Post-alignment columns are still non-unique."
    assert list(X.columns) == list(train_cols), "Column order/contents don’t match training features."
    return X

def pipeline(game_id):
    """
    Full pipeline: scrape game PBP, engineer xG features, predict xG,
    build on-ice wide dataset, and scrape shifts + player info.
    Returns (pbp_with_xg_wide, players_df).
    """

    # game_id = 2025020110

    cols = ["player_name", "jersey_number", "team_type", "team_name", "isHome", "teamId", "playerId", "sweaterNumber", "positionCode",
        "headshot", "firstName.default", "lastName.default", "fullName", "gameId", "homeTeam", "awayTeam"]
    shifts_df = scrape_shifts(game_id)
    players_df = shifts_df[cols].drop_duplicates().reset_index(drop=True)
    players_df["team"] = np.where(players_df["isHome"], players_df["homeTeam"], players_df["awayTeam"])
    players_df["position"] = np.where(~players_df["positionCode"].isin(["G", "D"]), "F", players_df["positionCode"])

    game = scrape_game(game_id)
    pbp_df = engineer_xg_features(game)
    pbp_with_xg = predict_xg_for_pbp(pbp_df)
    pbp_with_xg_wide = build_on_ice_wide(pbp_with_xg, max_skaters=6, include_goalie=True, drop_list_cols=False)
    # pbp_with_xg_wide.sample(10)
    return pbp_with_xg_wide, players_df
    
def toi_by_strength(pbp_change_events: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate total time on ice per game-strength state (e.g., 5v5, 6*v5, 4v6*).

    Parameters
    ----------
    pbp_change_events : pd.DataFrame
        Must contain:
        ['Event', 'Per', 'elapsedTime', 'eventTeam', 'player1Id', 'isGoalie']
        with 'Event' in ['ON', 'OFF'].

    Returns
    -------
    pd.DataFrame
        Columns: ['strength', 'seconds', 'minutes']
    """

    df = (pbp_change_events
          .loc[pbp_change_events['Event'].isin(['ON', 'OFF']),
               ['Event', 'Per', 'elapsedTime', 'eventTeam', 'player1Id', 'isGoalie']]
          .copy())

    # Process OFFs before ONs if simultaneous
    df['event_order'] = np.where(df['Event'].eq('OFF'), 0, 1)
    df = df.sort_values(['elapsedTime', 'event_order'], kind='mergesort').drop(columns='event_order')

    # Identify the two teams
    teams = df['eventTeam'].dropna().unique().tolist()
    if len(teams) != 2:
        raise ValueError(f"Expected exactly 2 teams, found {len(teams)}: {teams}")
    t1, t2 = sorted(teams)

    # Track on-ice skaters and goalies
    skaters_present = {t1: set(), t2: set()}
    goalies_present = {t1: set(), t2: set()}

    def strength_label():
        s1 = len(skaters_present[t1])
        s2 = len(skaters_present[t2])
        g1 = len(goalies_present[t1]) > 0
        g2 = len(goalies_present[t2]) > 0
        left  = f"{s1}{'' if g1 else '*'}"
        right = f"{s2}{'' if g2 else '*'}"
        return f"{left}v{right}"

    segments = []
    prev_t = 0

    for ts, grp in df.groupby('elapsedTime', sort=True):
        # Accumulate segment for current state
        if ts > prev_t:
            segments.append({
                'start': prev_t,
                'end': ts,
                'seconds': ts - prev_t,
                'strength': strength_label()
            })

        # Apply changes at this timestamp
        off = grp.loc[grp['Event'].eq('OFF')]
        on  = grp.loc[grp['Event'].eq('ON')]

        for _, r in off.iterrows():
            team, pid = r['eventTeam'], int(r['player1Id'])
            if int(r['isGoalie']) == 1:
                goalies_present[team].discard(pid)
            else:
                skaters_present[team].discard(pid)

        for _, r in on.iterrows():
            team, pid = r['eventTeam'], int(r['player1Id'])
            if int(r['isGoalie']) == 1:
                goalies_present[team].add(pid)
            else:
                skaters_present[team].add(pid)

        prev_t = ts

    # Use final event time as end of game
    game_end = int(df['elapsedTime'].max())
    if game_end > prev_t:
        segments.append({
            'start': prev_t,
            'end': game_end,
            'seconds': game_end - prev_t,
            'strength': strength_label()
        })

    seg_df = pd.DataFrame(segments)
    if seg_df.empty:
        return pd.DataFrame(columns=['strength', 'seconds', 'minutes'])

    out = (seg_df.groupby('strength', as_index=False)['seconds']
                 .sum()
                 .sort_values('seconds', ascending=False))
    out['minutes'] = out['seconds'] / 60
    return out

def toi_by_player_and_strength(pbp_change_events: pd.DataFrame) -> pd.DataFrame:
    """
    Compute time on ice per player per game-strength (e.g., 5v5, 6*v5).

    Parameters
    ----------
    pbp_change_events : DataFrame
        Must have ['Event','Per','elapsedTime','eventTeam','player1Id','player1Name','isGoalie'].
        Only ON/OFF events should be included.

    Returns
    -------
    DataFrame with columns:
        ['player1Id','player1Name','eventTeam','strength','seconds','minutes']
    """

    df = (pbp_change_events
          .loc[pbp_change_events['Event'].isin(['ON', 'OFF']),
               ['Event','Per','elapsedTime','eventTeam','player1Id','player1Name','isGoalie']]
          .copy())

    # Ensure proper event order: OFF before ON at same timestamp
    df['event_order'] = np.where(df['Event'].eq('OFF'), 0, 1)
    df = df.sort_values(['elapsedTime','event_order'], kind='mergesort').drop(columns='event_order')

    # Identify teams
    teams = df['eventTeam'].dropna().unique().tolist()
    if len(teams) != 2:
        raise ValueError(f"Expected 2 teams, got {len(teams)}: {teams}")
    t1, t2 = sorted(teams)

    # Track current on-ice skaters/goalies per team
    on_ice = {t1: set(), t2: set()}
    goalies = {t1: set(), t2: set()}

    # Time accumulator
    toi = defaultdict(lambda: defaultdict(float))  # toi[player_id][strength] = seconds
    player_info = {}  # player_id -> (name, team)

    def strength_label():
        s1 = len(on_ice[t1])
        s2 = len(on_ice[t2])
        g1 = len(goalies[t1]) > 0
        g2 = len(goalies[t2]) > 0
        left  = f"{s1}{'' if g1 else '*'}"
        right = f"{s2}{'' if g2 else '*'}"
        return f"{left}v{right}"

    prev_t = 0

    for ts, grp in df.groupby('elapsedTime', sort=True):
        # Record time for current strength
        if ts > prev_t:
            dt = ts - prev_t
            str_label = strength_label()
            for team in [t1, t2]:
                for pid in list(on_ice[team]) + list(goalies[team]):
                    toi[pid][str_label] += dt
        # Apply OFF events
        off = grp.loc[grp['Event'].eq('OFF')]
        for _, r in off.iterrows():
            team, pid = r['eventTeam'], int(r['player1Id'])
            player_info[pid] = (r['player1Name'], team)
            if int(r['isGoalie']) == 1:
                goalies[team].discard(pid)
            else:
                on_ice[team].discard(pid)
        # Apply ON events
        on = grp.loc[grp['Event'].eq('ON')]
        for _, r in on.iterrows():
            team, pid = r['eventTeam'], int(r['player1Id'])
            player_info[pid] = (r['player1Name'], team)
            if int(r['isGoalie']) == 1:
                goalies[team].add(pid)
            else:
                on_ice[team].add(pid)

        prev_t = ts

    # Add final segment up to end of game
    game_end = int(df['elapsedTime'].max())
    if game_end > prev_t:
        dt = game_end - prev_t
        str_label = strength_label()
        for team in [t1, t2]:
            for pid in list(on_ice[team]) + list(goalies[team]):
                toi[pid][str_label] += dt

    # Convert to DataFrame
    records = []
    for pid, strengths in toi.items():
        name, team = player_info[pid]
        for strength, seconds in strengths.items():
            records.append({
                'player1Id': pid,
                'player1Name': name,
                'eventTeam': team,
                'strength': strength,
                'seconds': seconds,
                'minutes': seconds / 60
            })

    out = pd.DataFrame(records)
    return (out.sort_values(['eventTeam','player1Name','strength'])
               .reset_index(drop=True))

def on_ice_stats_by_player_strength(
    pbp: pd.DataFrame,
    *,
    xg_col_candidates=("xG","xg","shot_xg","xg_shot"),
    include_goalies: bool = False,
    rates: bool = False,          # if True, add per60 rates for key metrics
    rate_fields=("CF","CA","FF","FA","SF","SA","xG","xGA","GF","GA","PF","PA")
) -> pd.DataFrame:
    """
    Compute per-player, per-strength on-ice stats + TOI (and optional per60 rates).

    Required columns in `pbp` (ON/OFF, plus play rows like SHOT/GOAL/BLOCK/MISS/PENL/GIVE/TAKE):
        'Event', 'elapsedTime', 'eventTeam'
    For ON/OFF rows also provide:
        'player1Id', 'player1Name', 'isGoalie' (isGoalie in {0,1})

    Event semantics assumed (typical NHL feeds):
        - GOAL/SHOT/MISS: eventTeam is *shooting* team.
        - BLOCK: eventTeam is the *blocking* (defending) team.
                 The attempt counts for the opponent (shooting) team.
        - PENL: eventTeam is the *penalized* team.
                On-ice players of the *other* team get PF; penalized team gets PA.
        - GIVE/TAKE (or GIVEAWAY/TAKEAWAY): carried as *_for for eventTeam, *_against for opponent.

    Naming:
        CF/CA  = Corsi (all attempts incl. blocks)
        FF/FA  = Fenwick (attempts excl. blocks)
        SF/SA  = Shots on goal (SHOT + GOAL)
        xG/xGA = expected goals for/against
        GF/GA  = goals
        PF/PA  = penalties drawn/taken while on-ice

    Parameters
    ----------
    include_goalies : include goalies in attributions (strength counts still exclude them).
    rates           : if True, appends <field>_per60 for fields in `rate_fields`.

    Returns
    -------
    One row per player per strength with TOI + on-ice stats (and optional per60 rates).
    """

    # ---- Prep & ordering ----
    need_cols = ['Event','elapsedTime','eventTeam']
    for c in need_cols:
        if c not in pbp.columns:
            raise ValueError(f"pbp must contain column '{c}'")

    xg_col = next((c for c in xg_col_candidates if c in pbp.columns), None)

    df = pbp.copy()
    # gameplay rows first at a timestamp, then OFF, then ON
    df['_chg'] = np.where(df['Event'].isin(['OFF','ON']), 1, 0)
    df['_off_on'] = np.where(df['Event'].eq('OFF'), 0, np.where(df['Event'].eq('ON'), 1, 2))
    df = df.sort_values(['elapsedTime','_chg','_off_on'], kind='mergesort')

    # Identify teams
    teams = df['eventTeam'].dropna().unique().tolist()
    if len(teams) != 2:
        raise ValueError(f"Expected 2 teams, found {len(teams)}: {teams}")
    t1, t2 = sorted(teams)
    other = {t1: t2, t2: t1}

    # On-ice tracking
    skaters_on = {t1: set(), t2: set()}
    goalies_on = {t1: set(), t2: set()}
    player_info = {}  # pid -> (name, team)

    # Accumulators
    toi = defaultdict(float)  # (pid,strength) -> seconds
    stats = defaultdict(lambda: defaultdict(float))  # (pid,strength) -> metric -> value

    def strength_label():
        s1, s2 = len(skaters_on[t1]), len(skaters_on[t2])
        g1, g2 = len(goalies_on[t1]) > 0, len(goalies_on[t2]) > 0
        left  = f"{s1}{'' if g1 else '*'}"
        right = f"{s2}{'' if g2 else '*'}"
        return f"{left}v{right}"

    def iter_players(team):
        return (list(skaters_on[team]) + list(goalies_on[team])) if include_goalies else list(skaters_on[team])

    # Attribute a *play* row at current strength
    def attribute_play(row, s):
        evt = str(row['Event'])
        team = row.get('eventTeam')
        if team not in (t1, t2):  # ignore rows without a valid team
            return

        # SHOT/GOAL/MISS/ BLOCK  -> CF/CA, FF/FA, SF/SA, GF/GA, xG/xGA
        if evt in ('GOAL','SHOT','MISS','BLOCK'):
            if evt == 'BLOCK':
                off_team, def_team = other[team], team
                is_block, is_shot, is_goal, is_miss = True, False, False, False
                xg = 0.0
            else:
                off_team, def_team = team, other[team]
                is_goal = (evt == 'GOAL')
                is_shot = (evt == 'SHOT') or is_goal
                is_miss = (evt == 'MISS')
                is_block = False
                xg = float(row[xg_col]) if xg_col and pd.notna(row.get(xg_col)) else 0.0

            # FOR (offense)
            for pid in iter_players(off_team):
                # Corsi/Fenwick/Shots/Goals
                stats[(pid,s)]['CF'] += 1
                if not is_block: stats[(pid,s)]['FF'] += 1
                if is_shot:      stats[(pid,s)]['SF'] += 1
                if is_goal:      stats[(pid,s)]['GF'] += 1
                # xG
                stats[(pid,s)]['xG'] += xg

            # AGAINST (defense)
            for pid in iter_players(def_team):
                stats[(pid,s)]['CA'] += 1
                if not is_block: stats[(pid,s)]['FA'] += 1
                if is_shot:      stats[(pid,s)]['SA'] += 1
                if is_goal:      stats[(pid,s)]['GA'] += 1
                stats[(pid,s)]['xGA'] += xg
            return

        # PENALTIES  -> PF/PA (penalized team is eventTeam)
        if evt in ('PENL','PEN','PENALTY'):
            penalized = team
            benefited = other[team]
            for pid in iter_players(benefited):
                stats[(pid,s)]['PF'] += 1
            for pid in iter_players(penalized):
                stats[(pid,s)]['PA'] += 1
            return

        # Giveaways / Takeaways if you still want them in this table (optional)
        if evt in ('GIVE','GIVEAWAY'):
            gw, op = team, other[team]
            for pid in iter_players(gw): stats[(pid,s)]['GIVE_for'] += 1
            for pid in iter_players(op): stats[(pid,s)]['GIVE_against'] += 1
            return
        if evt in ('TAKE','TAKEAWAY'):
            tk, op = team, other[team]
            for pid in iter_players(tk): stats[(pid,s)]['TAKE_for'] += 1
            for pid in iter_players(op): stats[(pid,s)]['TAKE_against'] += 1
            return

    # ---- Main timeline sweep ----
    times = df['elapsedTime'].dropna().astype(int).unique()
    prev_t = 0

    for ts in times:
        # 1) Attribute gameplay at this time using current on-ice
        plays = df[(df['elapsedTime']==ts) & (~df['Event'].isin(['ON','OFF']))]
        if not plays.empty:
            s = strength_label()
            for _, r in plays.iterrows():
                attribute_play(r, s)

        # 2) TOI from prev_t -> ts
        if ts > prev_t:
            dt = ts - prev_t
            s = strength_label()
            for tm in (t1, t2):
                for pid in iter_players(tm):
                    toi[(pid,s)] += dt
            prev_t = ts

        # 3) Apply roster changes (OFF then ON; already ordered)
        chg = df[(df['elapsedTime']==ts) & (df['Event'].isin(['ON','OFF']))]
        for _, r in chg.iterrows():
            evt = r['Event']; team = r.get('eventTeam'); pid = r.get('player1Id')
            if pd.isna(team) or pd.isna(pid):
                continue
            pid = int(pid)
            player_info[pid] = (r.get('player1Name', str(pid)), team)
            is_g = int(r.get('isGoalie', 0)) == 1
            if evt == 'OFF':
                (goalies_on if is_g else skaters_on)[team].discard(pid)
            else:
                (goalies_on if is_g else skaters_on)[team].add(pid)

    # Close final segment
    game_end = int(df['elapsedTime'].max())
    if game_end > prev_t:
        dt = game_end - prev_t
        s = strength_label()
        for tm in (t1, t2):
            for pid in iter_players(tm):
                toi[(pid,s)] += dt

    # ---- Build output ----
    rows = []
    for (pid, s), sec in toi.items():
        name, team = player_info.get(pid, (str(pid), None))
        base = {
            'player1Id': pid,
            'player1Name': name,
            'eventTeam': team,
            'strength': s,
            'seconds': sec,
            'minutes': sec/60.0,
            # standard metrics initialized to 0
            'CF':0,'CA':0,'FF':0,'FA':0,'SF':0,'SA':0,'GF':0,'GA':0,'xG':0.0,'xGA':0.0,
            'PF':0,'PA':0
        }
        base.update(stats[(pid,s)])  # merges any counted fields
        rows.append(base)

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    # Optional per60 rates
    if rates:
        # avoid division by zero: only compute for positive TOI
        per60 = (out['seconds'].replace(0, np.nan) / 3600.0)
        for f in rate_fields:
            if f in out.columns:
                out[f + '_per60'] = out[f] / per60
        out = out.fillna({c:0 for c in out.columns if c.endswith('_per60')})

    # Pretty order
    order_cols = ['player1Id','player1Name','eventTeam','strength','seconds','minutes',
                  'CF','CA','FF','FA','SF','SA','GF','GA','xG','xGA','PF','PA']
    rate_cols = [c for c in out.columns if c.endswith('_per60')]
    out = out.reindex(columns=order_cols + rate_cols).sort_values(
        ['eventTeam','player1Name','strength']
    ).reset_index(drop=True)

    return out

def combo_on_ice_stats(
    pbp: pd.DataFrame,
    *,
    focus_team: str,                 # tri-code of the team whose combos you want (e.g., "MTL")
    n_team: int = 2,                 # size of focus-team combos
    m_opp: int  = 0,                 # size of opponent combos (0 => aggregate vs ANY)
    min_TOI: int = 15,               # minimum seconds to keep a row
    include_goalies: bool = False,   # include goalies in combos/on-ice attributions (strength still excludes them)
    rates: bool = False,             # add per-60 for the key fields
    xg_col_candidates=("xG","xg","shot_xg","xg_shot")
) -> pd.DataFrame:
    """
    Compute on-ice stats for combinations of players on `focus_team`, optionally
    against combinations of size `m_opp` from the opponent, grouped by strength.

    Required columns in `pbp`:
      'Event','elapsedTime','eventTeam'  (all rows)
      'player1Id','player1Name','isGoalie' (for ON/OFF rows)

    Events handled:
      GOAL/SHOT/MISS/BLOCK -> CF/CA, FF/FA, SF/SA, GF/GA, xG/xGA
      PENL                 -> PF/PA   (eventTeam is penalized team)
    """
    # --- prep ---
    need_cols = ['Event','elapsedTime','eventTeam']
    for c in need_cols:
        if c not in pbp.columns:
            raise ValueError(f"pbp must contain column '{c}'")
    xg_col = next((c for c in xg_col_candidates if c in pbp.columns), None)

    df = pbp.copy()
    # gameplay rows BEFORE roster changes at the same timestamp; OFF before ON
    df['_chg'] = np.where(df['Event'].isin(['OFF','ON']), 1, 0)
    df['_off_on'] = np.where(df['Event'].eq('OFF'), 0, np.where(df['Event'].eq('ON'), 1, 2))
    df = df.sort_values(['elapsedTime','_chg','_off_on'], kind='mergesort')

    # teams
    teams = df['eventTeam'].dropna().unique().tolist()
    if len(teams) != 2:
        raise ValueError(f"Expected 2 teams, found {len(teams)}: {teams}")
    t1, t2 = sorted(teams)
    other = {t1: t2, t2: t1}
    if focus_team not in (t1, t2):
        raise ValueError(f"focus_team must be one of {t1},{t2}")

    opp_team = other[focus_team]

    # on-ice tracking (skaters for strength; goalies optional for combo membership)
    sk_on  = {t1: set(), t2: set()}
    g_on   = {t1: set(), t2: set()}
    player_info = {}  # pid -> (name, team)

    # helpers
    def strength_label():
        l = len(sk_on[t1]); r = len(sk_on[t2])
        g1 = len(g_on[t1]) > 0; g2 = len(g_on[t2]) > 0
        return f"{l}{'' if g1 else '*'}v{r}{'' if g2 else '*'}"

    def players_for_combo(team):
        base = list(sk_on[team])
        if include_goalies:
            base += list(g_on[team])
        return base

    # accumulators: (team_combo_ids, opp_combo_ids_or_None, strength) -> stats
    TOI = defaultdict(float)
    ST  = defaultdict(lambda: defaultdict(float))

    def add_toi_and_stats(dt, str_lab, play=None):
        """
        dt >= 0 seconds for the open interval.
        play: dict with keys (type, for_team, xg) for shots/goals/miss/block/penl,
              or None to just accrue time.
        """
        team_players = players_for_combo(focus_team)
        opp_players  = players_for_combo(opp_team)

        if len(team_players) < n_team:
            return  # not enough on-ice to form the requested combo size
        if m_opp > 0 and len(opp_players) < m_opp:
            return

        team_combos = list(combinations(sorted(team_players), n_team))
        if m_opp == 0:
            opp_combos = [None]  # aggregate vs any
        else:
            opp_combos = list(combinations(sorted(opp_players), m_opp))

        for tc in team_combos:
            for oc in opp_combos:
                key = (tc, oc, str_lab)
                if dt > 0:
                    TOI[key] += dt

                if play is None:
                    continue

                ptype = play['type']
                for_team = play['for_team']
                xg = play.get('xg', 0.0)

                # map to FOR/AGAINST from focus_team perspective
                if ptype in ('GOAL','SHOT','MISS','BLOCK'):
                    # who is on offense?
                    if ptype == 'BLOCK':
                        off = other[for_team]      # eventTeam is blocking team
                        # block is still a Corsi attempt *for* the offense
                        is_block, is_miss = True, False
                        is_goal, is_shot = False, False
                        xg_eff = 0.0
                    else:
                        off = for_team
                        is_goal = (ptype == 'GOAL')
                        is_shot = (ptype in ('SHOT','GOAL'))
                        is_miss = (ptype == 'MISS')
                        is_block = False
                        xg_eff = xg

                    # if offense is focus_team, it's FOR; else AGAINST
                    if off == focus_team:
                        ST[key]['CF'] += 1
                        if not is_block: ST[key]['FF'] += 1
                        if is_shot:      ST[key]['SF'] += 1
                        if is_goal:      ST[key]['GF'] += 1
                        ST[key]['xG'] += xg_eff
                    else:
                        ST[key]['CA'] += 1
                        if not is_block: ST[key]['FA'] += 1
                        if is_shot:      ST[key]['SA'] += 1
                        if is_goal:      ST[key]['GA'] += 1
                        ST[key]['xGA'] += xg_eff

                elif ptype == 'PENL':
                    penalized = for_team
                    if penalized == focus_team:
                        ST[key]['PA'] += 1
                    else:
                        ST[key]['PF'] += 1

    # sweep timeline
    times = df['elapsedTime'].dropna().astype(int).unique()
    prev_t = 0
    for ts in times:
        str_lab = strength_label()

        # 1) plays at ts
        plays = df[(df['elapsedTime']==ts) & (~df['Event'].isin(['ON','OFF']))]
        if not plays.empty:
            for _, row in plays.iterrows():
                evt = str(row['Event'])
                team = row.get('eventTeam')
                if team not in (t1, t2): 
                    continue
                payload = None
                if evt in ('GOAL','SHOT','MISS'):
                    xg = float(row[xg_col]) if xg_col and pd.notna(row.get(xg_col)) else 0.0
                    payload = {'type': evt, 'for_team': team, 'xg': xg}
                elif evt == 'BLOCK':
                    payload = {'type': 'BLOCK', 'for_team': team}  # team is blocking
                elif evt in ('PENL','PEN','PENALTY'):
                    payload = {'type': 'PENL', 'for_team': team}    # penalized
                if payload:
                    add_toi_and_stats(0, str_lab, payload)

        # 2) accumulate TOI prev_t -> ts
        if ts > prev_t:
            add_toi_and_stats(ts - prev_t, str_lab, None)
            prev_t = ts

        # 3) apply roster changes at ts (OFF then ON; already ordered)
        chg = df[(df['elapsedTime']==ts) & (df['Event'].isin(['ON','OFF']))]
        for _, r in chg.iterrows():
            team = r.get('eventTeam'); pid = r.get('player1Id')
            if pd.isna(team) or pd.isna(pid):
                continue
            pid = int(pid)
            player_info[pid] = (r.get('player1Name', str(pid)), team)
            is_g = int(r.get('isGoalie', 0)) == 1
            if r['Event'] == 'OFF':
                (g_on if is_g else sk_on)[team].discard(pid)
            else:
                (g_on if is_g else sk_on)[team].add(pid)

    # close final segment
    game_end = int(df['elapsedTime'].max())
    if game_end > prev_t:
        add_toi_and_stats(game_end - prev_t, strength_label(), None)

    # --- build output ---
    rows = []
    for (tc, oc, s), sec in TOI.items():
        if sec < min_TOI:
            continue
        # pretty labels
        def lab(ids):
            if ids is None: return "ANY"
            names = [player_info[i][0] if i in player_info else str(i) for i in ids]
            return " / ".join(names)
        team_combo_names = lab(tc)
        opp_combo_names  = lab(oc)

        row = {
            'team': focus_team,
            'opp': opp_team,
            'team_combo_ids': tuple(tc),
            'opp_combo_ids': None if oc is None else tuple(oc),
            'team_combo': team_combo_names,
            'opp_combo': opp_combo_names,
            'strength': s,
            'seconds': sec,
            'minutes': sec/60.0,
            # standard stats (init to 0)
            'CF':0,'CA':0,'FF':0,'FA':0,'SF':0,'SA':0,'GF':0,'GA':0,'xG':0.0,'xGA':0.0,'PF':0,'PA':0
        }
        for k, v in ST[(tc, oc, s)].items():
            row[k] = v
        rows.append(row)

    out = pd.DataFrame(rows)
    if out.empty:
        # Return the schema even if nothing meets min_TOI
        return pd.DataFrame(columns=[
            'team','opp','team_combo_ids','opp_combo_ids','team_combo','opp_combo','strength',
            'seconds','minutes','CF','CA','FF','FA','SF','SA','GF','GA','xG','xGA','PF','PA'
        ])

    # per-60 (optional)
    if rates:
        denom = out['seconds'].replace(0, np.nan) / 3600.0
        for f in ('CF','CA','FF','FA','SF','SA','GF','GA','xG','xGA','PF','PA'):
            out[f + '_per60'] = out[f] / denom
        out = out.fillna({c:0 for c in out.columns if c.endswith('_per60')})

    # nice ordering
    base_cols = ['team','opp','team_combo','opp_combo','strength','seconds','minutes',
                 'CF','CA','FF','FA','SF','SA','GF','GA','xG','xGA','PF','PA']
    rate_cols = [c for c in out.columns if c.endswith('_per60')]
    out = out.sort_values(['strength','team_combo','opp_combo']).reset_index(drop=True)
    return out[base_cols + rate_cols]


def combo_on_ice_stats_both_teams(
    pbp: pd.DataFrame,
    *,
    n_team: int = 2,
    m_opp: int  = 0,            # 0 => vs ANY opponent mix
    min_TOI: int = 15,          # seconds
    include_goalies: bool = False,
    rates: bool = False,
    xg_col_candidates=("xG","xg","shot_xg","xg_shot"),
    player_df: pd.DataFrame | None = None,   # to enrich Name/Position/Number/Headshot
) -> pd.DataFrame:
    """
    On-ice combo stats for BOTH teams with exploded player columns for both sides.

    Required in `pbp`:
      - All rows: ['Event','elapsedTime','eventTeam']
      - ON/OFF rows: add ['player1Id','player1Name','isGoalie'] (isGoalie in {0,1})

    Event assumptions (typical NHL):
      - GOAL/SHOT/MISS: eventTeam = shooting team
      - BLOCK: eventTeam = blocking (defending) team; attempt credited to the OTHER team
      - PENL: eventTeam = penalized team (PF for opponents, PA for penalized)

    Returns one row per (team combo vs opp combo/ANY, strength, team) with:
      TOI, CF/CA, FF/FA, SF/SA, xG/xGA, GF/GA, PF/PA, optional per-60,
      and exploded player columns for both sides:
        player1..N (Id, Name, Position, Number, Headshot)
        oppPlayer1..M (same fields; empty if m_opp == 0)
    """
    # ---- prep/order ----------------------------------------------------------
    for c in ('Event','elapsedTime','eventTeam'):
        if c not in pbp.columns:
            raise ValueError(f"pbp must contain column '{c}'")

    xg_col = next((c for c in xg_col_candidates if c in pbp.columns), None)

    df = pbp.copy()

    # At identical timestamps: (1) gameplay rows, (2) OFF, (3) ON
    df['_chg'] = np.where(df['Event'].isin(['OFF','ON']), 1, 0)
    df['_off_on'] = np.where(df['Event'].eq('OFF'), 0,
                      np.where(df['Event'].eq('ON'), 1, 2))
    df = df.sort_values(['elapsedTime','_chg','_off_on'], kind='mergesort')

    # identify teams
    teams = df['eventTeam'].dropna().unique().tolist()
    if len(teams) != 2:
        raise ValueError(f"Expected 2 teams, found {len(teams)}: {teams}")
    t1, t2 = sorted(teams)
    other = {t1: t2, t2: t1}

    # on-ice tracking
    sk_on = {t1: set(), t2: set()}     # skaters only (used for strength counts)
    g_on  = {t1: set(), t2: set()}     # goalies

    # player metadata maps -----------------------------------------------------
    # pid -> dict(name, team, pos, number, headshot)
    player_info: dict[int, dict] = {}
    pos_map: dict[int, str] = {}

    if player_df is not None and len(player_df):
        # Try to find likely column names (flexible to your inputs)
        id_col   = next((c for c in ['player1Id','playerId','id'] if c in player_df.columns), None)
        team_col = next((c for c in ['eventTeam','team','Team'] if c in player_df.columns), None)
        pos_col  = next((c for c in ['position','positionCode','pos','Position'] if c in player_df.columns), None)
        num_col  = next((c for c in ['sweaterNumber','number','jerseyNumber'] if c in player_df.columns), None)
        name_col = next((c for c in ['fullName','player1Name','Player','name'] if c in player_df.columns), None)
        head_col = next((c for c in ['headshot','Headshot'] if c in player_df.columns), None)

        for _, r in player_df.dropna(subset=[id_col]).iterrows():
            pid = int(r[id_col])
            tm  = r.get(team_col)
            # Normalize position to one of F/D/G if possible
            raw_pos = r.get(pos_col)
            pos = str(raw_pos).upper()[:1] if pd.notna(raw_pos) else None
            pos = pos if pos in ('F','D','G') else None
            num = int(r[num_col]) if (num_col and pd.notna(r.get(num_col))) else None
            nm  = str(r[name_col]) if (name_col and pd.notna(r.get(name_col))) else None
            hd  = str(r[head_col]) if (head_col and pd.notna(r.get(head_col))) else None

            player_info[pid] = {
                'name': nm,
                'team': tm,
                'pos': pos,
                'number': num,
                'headshot': hd
            }
            pos_map[pid] = pos

    # also capture names/teams from ON/OFF rows if not in player_df
    def remember_player_from_change_row(r):
        pid = r.get('player1Id')
        if pd.isna(pid): return
        pid = int(pid)
        if pid not in player_info:
            player_info[pid] = {
                'name': r.get('player1Name', str(pid)),
                'team': r.get('eventTeam'),
                'pos': pos_map.get(pid, None),
                'number': player_info.get(pid, {}).get('number', None),
                'headshot': player_info.get(pid, {}).get('headshot', None)
            }

    # helpers ------------------------------------------------------------------
    def strength_label():
        l = len(sk_on[t1]); r = len(sk_on[t2])
        g1 = len(g_on[t1]) > 0; g2 = len(g_on[t2]) > 0
        return f"{l}{'' if g1 else '*'}v{r}{'' if g2 else '*'}"

    def on_ice_players(team):
        base = list(sk_on[team])
        if include_goalies:
            base += list(g_on[team])
        return base

    # accumulators: key = (focus_team, team_combo_ids, opp_combo_ids_or_None, strength)
    TOI = defaultdict(float)
    ST  = defaultdict(lambda: defaultdict(float))  # stats

    def add_toi_for_both(dt, s):
        # Attribute time to both sides' combo rows
        for focus in (t1, t2):
            opp = other[focus]
            tp = on_ice_players(focus)
            op = on_ice_players(opp)
            if len(tp) < n_team:
                continue
            if m_opp > 0 and len(op) < m_opp:
                continue
            t_combos = list(combinations(sorted(tp), n_team))
            o_combos = [None] if m_opp == 0 else list(combinations(sorted(op), m_opp))
            for tc in t_combos:
                for oc in o_combos:
                    TOI[(focus, tc, oc, s)] += dt

    def add_play_for_both(evt, evt_team, xg_val, s):
        # Attribute a play to both sides' rows
        if evt not in ('GOAL','SHOT','MISS','BLOCK','PENL','PEN','PENALTY'):
            return
        for focus in (t1, t2):
            opp = other[focus]
            tp = on_ice_players(focus)
            op = on_ice_players(opp)
            if len(tp) < n_team:
                continue
            if m_opp > 0 and len(op) < m_opp:
                continue
            t_combos = list(combinations(sorted(tp), n_team))
            o_combos = [None] if m_opp == 0 else list(combinations(sorted(op), m_opp))

            if evt in ('GOAL','SHOT','MISS','BLOCK'):
                if evt == 'BLOCK':
                    # eventTeam is the blocking team; offense is the other
                    off = other[evt_team]
                    is_block, is_shot, is_goal = True, False, False
                    xg_eff = 0.0
                else:
                    off = evt_team
                    is_goal = (evt == 'GOAL')
                    is_shot = (evt in ('SHOT','GOAL'))
                    is_block = False
                    xg_eff = float(xg_val) if xg_val is not None else 0.0

                for tc in t_combos:
                    for oc in o_combos:
                        key = (focus, tc, oc, s)
                        if off == focus:
                            ST[key]['CF'] += 1
                            if not is_block: ST[key]['FF'] += 1
                            if is_shot:      ST[key]['SF'] += 1
                            if is_goal:      ST[key]['GF'] += 1
                            ST[key]['xG'] += xg_eff
                        else:
                            ST[key]['CA'] += 1
                            if not is_block: ST[key]['FA'] += 1
                            if is_shot:      ST[key]['SA'] += 1
                            if is_goal:      ST[key]['GA'] += 1
                            ST[key]['xGA'] += xg_eff

            else:  # penalties
                penalized = evt_team
                for tc in t_combos:
                    for oc in o_combos:
                        key = (focus, tc, oc, s)
                        if penalized == focus:
                            ST[key]['PA'] += 1
                        else:
                            ST[key]['PF'] += 1

    # ---- timeline sweep ------------------------------------------------------
    times = df['elapsedTime'].dropna().astype(int).unique()
    prev_t = 0

    for ts in times:
        s = strength_label()

        # Play events at ts (use current on-ice state)
        plays = df[(df['elapsedTime']==ts) & (~df['Event'].isin(['ON','OFF']))]
        if not plays.empty:
            for _, r in plays.iterrows():
                evt = str(r['Event'])
                team = r.get('eventTeam')
                if team not in (t1, t2):
                    continue
                xg_val = float(r[xg_col]) if xg_col and pd.notna(r.get(xg_col)) else None
                add_play_for_both(evt, team, xg_val, s)

        # TOI from prev_t -> ts
        if ts > prev_t:
            add_toi_for_both(ts - prev_t, s)
            prev_t = ts

        # Apply OFF/ON at ts (already ordered: OFF then ON)
        chg = df[(df['elapsedTime']==ts) & (df['Event'].isin(['ON','OFF']))]
        for _, r in chg.iterrows():
            team = r.get('eventTeam'); pid = r.get('player1Id')
            if pd.isna(team) or pd.isna(pid):
                continue
            remember_player_from_change_row(r)
            pid = int(pid)
            is_g = int(r.get('isGoalie', 0)) == 1
            if r['Event'] == 'OFF':
                (g_on if is_g else sk_on)[team].discard(pid)
            else:
                (g_on if is_g else sk_on)[team].add(pid)

    # Close final segment
    game_end = int(df['elapsedTime'].max())
    if game_end > prev_t:
        add_toi_for_both(game_end - prev_t, strength_label())

    # -------- build base output ---------------------------------------------
    def combo_label(ids):
        return " / ".join(
            (player_info[i]['name'] if i in player_info and player_info[i].get('name') else str(i))
            for i in ids
        )

    def combo_pos(ids):
        cnt = {'F':0,'D':0,'G':0}
        for i in ids:
            p = pos_map.get(i)
            if p in cnt: cnt[p] += 1
        parts = []
        if cnt['F']: parts.append(f"{cnt['F']}F")
        if cnt['D']: parts.append(f"{cnt['D']}D")
        if cnt['G']: parts.append(f"{cnt['G']}G")
        return "".join(parts)

    rows = []
    for (focus, tc, oc, s), sec in TOI.items():
        if sec < min_TOI:
            continue
        opp = other[focus]
        row = {
            'team': focus,
            'opp': opp,
            'team_combo_ids': tuple(tc),
            'opp_combo_ids': None if oc is None else tuple(oc),
            'team_combo': combo_label(tc),
            'opp_combo': "ANY" if oc is None else combo_label(oc),
            'team_combo_pos': combo_pos(tc) if pos_map else "",
            'opp_combo_pos': "" if oc is None else (combo_pos(oc) if pos_map else ""),
            'strength': s,
            'seconds': float(sec),
            'minutes': float(sec)/60.0,
            # standard stats initialized to 0
            'CF':0,'CA':0,'FF':0,'FA':0,'SF':0,'SA':0,'GF':0,'GA':0,'xG':0.0,'xGA':0.0,'PF':0,'PA':0
        }
        row.update(ST[(focus, tc, oc, s)])
        rows.append(row)

    out = pd.DataFrame(rows)
    if out.empty:
        # return schema with exploded cols
        cols = ['team','opp','team_combo','opp_combo','team_combo_pos','opp_combo_pos','strength',
                'seconds','minutes','CF','CA','FF','FA','SF','SA','GF','GA','xG','xGA','PF','PA']
        for i in range(1, n_team+1):
            cols += [f'player{i}Id', f'player{i}Name', f'player{i}Position', f'player{i}Number', f'player{i}Headshot']
        if m_opp > 0:
            for j in range(1, m_opp+1):
                cols += [f'oppPlayer{j}Id', f'oppPlayer{j}Name', f'oppPlayer{j}Position', f'oppPlayer{j}Number', f'oppPlayer{j}Headshot']
        return pd.DataFrame(columns=cols)

    # -------- explode player columns (both sides) ---------------------------
    def explode_side(df_in: pd.DataFrame, id_col: str, prefix: str, size: int):
        df_out = df_in.copy()
        for i in range(size):
            col_id = f'{prefix}{i+1}Id'
            col_nm = f'{prefix}{i+1}Name'
            col_ps = f'{prefix}{i+1}Position'
            col_nb = f'{prefix}{i+1}Number'
            col_hd = f'{prefix}{i+1}Headshot'

            df_out[col_id] = df_out[id_col].apply(lambda ids: ids[i] if ids and len(ids) > i else np.nan)
            # map metadata
            df_out[col_nm] = df_out[col_id].map(lambda pid: player_info.get(pid, {}).get('name') if pd.notna(pid) else np.nan)
            df_out[col_ps] = df_out[col_id].map(lambda pid: player_info.get(pid, {}).get('pos') if pd.notna(pid) else np.nan)
            df_out[col_nb] = df_out[col_id].map(lambda pid: player_info.get(pid, {}).get('number') if pd.notna(pid) else np.nan)
            df_out[col_hd] = df_out[col_id].map(lambda pid: player_info.get(pid, {}).get('headshot') if pd.notna(pid) else np.nan)
        return df_out

    out = explode_side(out, 'team_combo_ids', 'player', n_team)
    if m_opp > 0:
        out = explode_side(out, 'opp_combo_ids', 'oppPlayer', m_opp)

    # per-60 (optional)
    if rates:
        denom = out['seconds'].replace(0, np.nan) / 3600.0
        for f in ('CF','CA','FF','FA','SF','SA','GF','GA','xG','xGA','PF','PA'):
            out[f + '_per60'] = out[f] / denom
        out = out.fillna({c:0 for c in out.columns if c.endswith('_per60')})

    # final tidy ordering
    base_cols = ['team','opp','team_combo','opp_combo','team_combo_pos','opp_combo_pos',
                 'strength','seconds','minutes',
                 'CF','CA','FF','FA','SF','SA','GF','GA','xG','xGA','PF','PA']
    rate_cols = [c for c in out.columns if c.endswith('_per60')]
    team_player_cols = sum(([f'player{i}Id', f'player{i}Name', f'player{i}Position', f'player{i}Number', f'player{i}Headshot'] for i in range(1, n_team+1)), [])
    opp_player_cols  = sum(([f'oppPlayer{j}Id', f'oppPlayer{j}Name', f'oppPlayer{j}Position', f'oppPlayer{j}Number', f'oppPlayer{j}Headshot'] for j in range(1, m_opp+1)), []) if m_opp>0 else []

    # keep ids arrays for debugging; drop at the end if you don't need them
    ordered = base_cols + rate_cols + team_player_cols + opp_player_cols + ['team_combo_ids','opp_combo_ids']
    out = out[ordered].sort_values(['team','strength','team_combo','opp_combo']).reset_index(drop=True)
    return out
    
    
def team_strength_aggregates(
    pbp: pd.DataFrame,
    *,
    include_goalies: bool = False,
    rates: bool = False,
    min_TOI: int = 0,                       # filter out team-strength rows with < this many seconds
    xg_col_candidates=("xG","xg","shot_xg","xg_shot"),
) -> pd.DataFrame:
    """
    Aggregate BOTH teams' on-ice stats at the TEAM x STRENGTH level.

    Expects the same pbp schema as combo_on_ice_stats_both_teams:
      - General: ['Event','elapsedTime','eventTeam']
      - ON/OFF rows include: ['player1Id','player1Name','isGoalie'] (isGoalie in {0,1})

    Event semantics (NHL-style):
      - GOAL/SHOT/MISS: eventTeam is the SHOOTING team
      - BLOCK: eventTeam is the BLOCKING team; credit the attempt to the OTHER team
      - PENL/PEN/PENALTY: eventTeam is the penalized team (PF for opponents, PA for penalized)

    Returns one row per (team, strength) with TOI and standard counts; optional per-60 columns.
    """
    # --- basic checks & prep
    for c in ('Event','elapsedTime','eventTeam'):
        if c not in pbp.columns:
            raise ValueError(f"pbp must contain column '{c}'")
    xg_col = next((c for c in xg_col_candidates if c in pbp.columns), None)

    df = pbp.copy()

    # Ensure correct ordering at identical timestamps: gameplay rows, then OFF, then ON
    df['_chg'] = np.where(df['Event'].isin(['OFF','ON']), 1, 0)
    df['_off_on'] = np.where(df['Event'].eq('OFF'), 0,
                      np.where(df['Event'].eq('ON'), 1, 2))
    df = df.sort_values(['elapsedTime','_chg','_off_on'], kind='mergesort')

    # identify the two teams
    teams = df['eventTeam'].dropna().unique().tolist()
    if len(teams) != 2:
        raise ValueError(f"Expected 2 teams, found {len(teams)}: {teams}")
    t1, t2 = sorted(teams)
    other = {t1: t2, t2: t1}

    # track on-ice skaters/goalies by team
    sk_on = {t1: set(), t2: set()}
    g_on  = {t1: set(), t2: set()}

    # helpers
    def strength_label():
        l = len(sk_on[t1]); r = len(sk_on[t2])
        g1 = len(g_on[t1]) > 0; g2 = len(g_on[t2]) > 0
        # add '*' if no goalie on that side (optional but handy for pulled-goalie states)
        return f"{l}{'' if g1 else '*'}v{r}{'' if g2 else '*'}"

    # accumulators keyed by (team, strength)
    TOI = defaultdict(float)
    ST  = defaultdict(lambda: defaultdict(float))

    def add_toi(dt, s):
        TOI[(t1, s)] += dt
        TOI[(t2, s)] += dt

    def add_play(evt, evt_team, xg_val, s):
        if evt not in ('GOAL','SHOT','MISS','BLOCK','PENL','PEN','PENALTY'):
            return

        if evt in ('GOAL','SHOT','MISS','BLOCK'):
            if evt == 'BLOCK':
                # blocking team recorded; attempt belongs to the other team
                off = other[evt_team]
                is_goal = False
                is_shot = False
                is_block = True
                xg_eff = 0.0
            else:
                off = evt_team
                is_goal = (evt == 'GOAL')
                is_shot = (evt in ('SHOT','GOAL'))
                is_block = False
                xg_eff = float(xg_val) if xg_val is not None else 0.0

            def push(team_key, for_offense: bool):
                if for_offense:
                    ST[(team_key, s)]['CF'] += 1
                    if not is_block: ST[(team_key, s)]['FF'] += 1
                    if is_shot:      ST[(team_key, s)]['SF'] += 1
                    if is_goal:      ST[(team_key, s)]['GF'] += 1
                    ST[(team_key, s)]['xG'] += xg_eff
                else:
                    ST[(team_key, s)]['CA'] += 1
                    if not is_block: ST[(team_key, s)]['FA'] += 1
                    if is_shot:      ST[(team_key, s)]['SA'] += 1
                    if is_goal:      ST[(team_key, s)]['GA'] += 1
                    ST[(team_key, s)]['xGA'] += xg_eff

            # offense team gets "for", defense gets "against"
            push(off, True)
            push(other[off], False)

        else:
            # penalties: eventTeam is penalized
            penalized = evt_team
            ST[(penalized, s)]['PA'] += 1
            ST[(other[penalized], s)]['PF'] += 1

    # ---- sweep the timeline
    times = df['elapsedTime'].dropna().astype(int).unique()
    prev_t = 0

    for ts in times:
        s = strength_label()

        # apply all non-ON/OFF events at ts
        plays = df[(df['elapsedTime']==ts) & (~df['Event'].isin(['ON','OFF']))]
        if not plays.empty:
            for _, r in plays.iterrows():
                evt = str(r['Event'])
                team = r.get('eventTeam')
                if team not in (t1, t2):
                    continue
                xg_val = float(r[xg_col]) if xg_col and pd.notna(r.get(xg_col)) else None
                add_play(evt, team, xg_val, s)

        # accrue TOI from prev_t to ts
        if ts > prev_t:
            add_toi(ts - prev_t, s)
            prev_t = ts

        # process OFF then ON at ts (already ordered)
        chg = df[(df['elapsedTime']==ts) & (df['Event'].isin(['ON','OFF']))]
        for _, r in chg.iterrows():
            team = r.get('eventTeam'); pid = r.get('player1Id')
            if pd.isna(team) or pd.isna(pid):
                continue
            pid = int(pid)
            is_g = int(r.get('isGoalie', 0)) == 1
            if r['Event'] == 'OFF':
                (g_on if is_g else sk_on)[team].discard(pid)
            else:
                (g_on if is_g else sk_on)[team].add(pid)

    # close final segment
    game_end = int(df['elapsedTime'].max())
    if game_end > prev_t:
        add_toi(game_end - prev_t, strength_label())

    # ---- build output
    rows = []
    for (team, s), sec in TOI.items():
        if sec < min_TOI:
            continue
        row = {
            'team': team,
            'opp': other[team],
            'strength': s,
            'seconds': float(sec),
            'minutes': float(sec)/60.0,
            'CF':0,'CA':0,'FF':0,'FA':0,'SF':0,'SA':0,'GF':0,'GA':0,
            'xG':0.0,'xGA':0.0,'PF':0,'PA':0
        }
        row.update(ST[(team, s)])
        rows.append(row)

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=[
            'team','opp','strength','seconds','minutes',
            'CF','CA','FF','FA','SF','SA','GF','GA','xG','xGA','PF','PA'
        ])

    # optional per-60s
    if rates:
        denom_hours = out['seconds'].replace(0, np.nan) / 3600.0
        for f in ('CF','CA','FF','FA','SF','SA','GF','GA','xG','xGA','PF','PA'):
            out[f + '_per60'] = out[f] / denom_hours
        out = out.fillna({c:0 for c in out.columns if c.endswith('_per60')})

    # tidy order
    base_cols = ['team','opp','strength','seconds','minutes',
                 'CF','CA','FF','FA','SF','SA','GF','GA','xG','xGA','PF','PA']
    rate_cols = [c for c in out.columns if c.endswith('_per60')]
    out = out[base_cols + rate_cols].sort_values(['team','strength']).reset_index(drop=True)

    return out
    