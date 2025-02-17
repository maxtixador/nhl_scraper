"""
NHL Teams Data Scraper Module.

This module provides functions to scrape NHL team data including:
- Team information
- Rosters
- Schedules
- Statistics

Functions:
    scrapeTeams: Get basic team data
    scrapeTeamRoster: Get team roster data
    scrapeTeamStats: Get team statistics
    scrapeTeamProspects: Get team prospects
    scrapeTeamSchedule: Get team schedule
    scrapeTeamDraftHistory: Get team draft history
    scrapeTeamRosterLegacy: Get team roster data from NHL Records API
"""

import warnings
from typing import Union

import numpy as np
import pandas as pd
import requests

warnings.filterwarnings("ignore")


# Scrape Teams
def scrapeTeams() -> pd.DataFrame:
    """
    Scrapes basic team data from the NHL website, primarily for team IDs.

    Returns:
        pd.DataFrame: A DataFrame containing team data with columns:
            - teamId: Unique identifier for each team
            - fullName: Full team name
            - firstSeasonId: First season ID
            - lastSeasonId: Last season ID (null for active teams)
            - meta columns: datetime of scraping

    Raises:
        requests.HTTPError: If the API request fails
        ValueError: If there's an error processing the data
    """
    try:
        url = (
            "https://api.nhle.com/stats/rest/en/franchise?"
            "sort=fullName"
            "&include=lastSeason.id"
            "&include=firstSeason.id"
        )

        response = requests.get(url)
        response.raise_for_status()  # Check for HTTP errors
        data = response.json()

        # Validate response data
        if "data" not in data:
            raise ValueError("Unexpected API response format: 'data' key not found")

        # Create DataFrame and rename columns
        teams_df = pd.json_normalize(data["data"])
        teams_df = teams_df.rename(columns={"id": "teamId"})

        # Add metadata
        teams_df["meta_datetime"] = pd.to_datetime("now")
        teams_df["meta_source"] = "NHL Stats API"

        # Validate essential columns
        required_columns = ["teamId", "fullName"]
        missing_columns = [col for col in required_columns if col not in teams_df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        return teams_df

    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to fetch teams data: {str(e)}")
    except (KeyError, ValueError) as e:
        raise ValueError(f"Error processing teams data: {str(e)}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {str(e)}")


# Team Roster
def scrapeTeamRoster(team: str, season: Union[str, int]) -> pd.DataFrame:
    """
    Scrapes roster data from the NHL website for a given team and season.

    Parameters:
        team (str): Team abbreviation (e.g., 'MTL', 'TOR', 'BOS')
        season (Union[str, int]): Season in 'YYYYYYYY' format (e.g., '20232024' or 20232024)

    Returns:
        pd.DataFrame: A DataFrame containing team roster data with columns:
            - playerId: Unique identifier for each player
            - fullName: Player's full name
            - position: Player's position (F/D/G)
            - positionCode: Detailed position code
            - shootsCatches: Player's shooting/catching hand
            - team: Team abbreviation
            - season: Season identifier
            - meta columns: datetime of scraping

    Raises:
        requests.HTTPError: If the API request fails
        ValueError: If there's an error processing the data or invalid parameters
    """
    try:
        # Validate inputs
        if not isinstance(team, str) or len(team) < 2:
            raise ValueError(f"Invalid team abbreviation: {team}")

        # Convert season to string if it's an integer
        season_str = str(season)
        if not len(season_str) == 8:
            raise ValueError(f"Invalid season format: {season}. Must be 'YYYYYYYY' format.")

        # Make API request
        url = f"https://api-web.nhle.com/v1/roster/{team}/{season_str}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Process roster data
        df = pd.concat([pd.json_normalize(data[key]) for key in data.keys()]).reset_index(drop=True)

        # Create full name and rename ID column
        df["fullName"] = df["firstName.default"] + " " + df["lastName.default"]
        df = df.rename(columns={"id": "playerId"})

        # Process position data
        df["position"] = np.where(~df["positionCode"].isin(["G", "D"]), "F", df["positionCode"])
        df["positionD"] = np.where(
            df["position"] == "D", df["shootsCatches"] + df["position"], df["position"]
        )

        # Add team and season info
        df["team"] = team
        df["season"] = season_str

        # Add metadata
        df["meta_datetime"] = pd.to_datetime("now")
        df["meta_source"] = "NHL API"

        # Validate essential columns
        required_columns = ["playerId", "fullName", "position", "team", "season"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        return df

    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to fetch roster data for {team} ({season}): {str(e)}")
    except (KeyError, ValueError) as e:
        raise ValueError(f"Error processing roster data: {str(e)}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {str(e)}")


def scrapeTeamRosterLegacy(teamId: Union[str, int]) -> pd.DataFrame:
    """
    Scrapes roster data from the NHL website for a given team and season.

    Parameters:
        teamId (Union[str, int]): The team ID from NHL Records API.
            Used to identify the team for roster scraping.

    Returns:
        pd.DataFrame: A DataFrame containing team roster data with columns:
            - playerId: Unique identifier for each player
            - fullName: Player's full name
            - position: Player's position (F/D/G)
            - positionCode: Detailed position code
            - shootsCatches: Player's shooting/catching hand
            - team: Team abbreviation
            - season: Season identifier
            - meta columns: datetime of scraping

    Raises:
        ValueError: If parameters are invalid or data processing fails
        requests.HTTPError: If the API request fails
        KeyError: If the API response format is unexpected
    """
    try:
        # Validate inputs
        if not isinstance(teamId, (str, int)):
            raise ValueError("Invalid team ID: Must be a non-empty string or integer")

        # Make API request
        url = (
            "https://records.nhl.com/site/api/roster",
            f"/byTeam/{teamId}?",
            "include=id&include=firstName&include=lastName",
            "include=sweaterNumber&include=position",
            "include=height&include=weight",
            "include=birthDate&include=birthCountry",
            "include=birthCity&include=birthStateProvince",
            "include=onRoster",
        )

        response = requests.get(url).json()

        roster_df = pd.json_normalize(response["data"])

        roster_df["fullName"] = roster_df["firstName"] + " " + roster_df["lastName"]
        roster_df = roster_df.rename(columns={"id": "playerId"})
        roster_df["teamId"] = teamId

        roster_df["meta_datetime"] = pd.to_datetime("now")
        roster_df["meta_source"] = "NHL Records API"

        return roster_df

    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to fetch roster data for {teamId}: {str(e)}")
    except (KeyError, ValueError) as e:
        raise ValueError(f"Error processing roster data: {str(e)}")


# Team Stats
def scrapeTeamStats(
    team: str, season: Union[str, int], session: Union[str, int] = 2, goalies: bool = False
) -> pd.DataFrame:
    """
    Scrapes team stats data from the NHL website for a given team and season.

    Parameters:
        team (str): Team abbreviation (e.g., 'MTL', 'TOR', 'BOS')
        season (str): Season in 'YYYYYYYY' format (e.g., '20232024')
        session (Union[str, int]): Game session type. Default is 2 (regular season).
            Values can be:
            - 1 or 'preseason': Preseason games
            - 2 or 'regular': Regular season games
            - 3 or 'playoffs': Playoff games
        goalies (bool): Whether to return goalie stats (True) or skater stats (False).
            Default is False (skater stats).

    Returns:
        pd.DataFrame: A DataFrame containing team statistics with columns varying by type:
            For skaters (goalies=False):
                - playerId, fullName, position, games played, goals, assists, etc.
            For goalies (goalies=True):
                - playerId, fullName, games played, saves, goals against, etc.
            Common columns:
                - team: Team abbreviation
                - season: Season identifier
                - session: Session type
                - meta columns: datetime of scraping

    Raises:
        ValueError: If parameters are invalid or data processing fails
        requests.HTTPError: If the API request fails
    """
    try:
        # Session mapping
        SESSION_DICT = {"preseason": 1, "regular": 2, "playoffs": 3, 1: 1, 2: 2, 3: 3}

        # Validate team
        if not isinstance(team, str) or len(team) < 2:
            raise ValueError(f"Invalid team abbreviation: {team}")

        # Validate season
        season_str = str(season)
        if not len(season_str) == 8:
            raise ValueError(f"Invalid season format: {season}. Must be 'YYYYYYYY' format.")

        # Validate and process session
        if session not in SESSION_DICT:
            raise ValueError(
                "Session must be either 'preseason' (1), 'regular' (2), 'playoffs' (3) "
                "or their corresponding integer values."
            )
        session_value = SESSION_DICT[session]
        session_key = [
            k for k, v in SESSION_DICT.items() if v == session_value and isinstance(k, str)
        ][0]

        # Make API request
        url = f"https://api-web.nhle.com/v1/club-stats/{team}/{season}/{session_value}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Process player data
        key = "goalies" if goalies else "skaters"
        if key not in data:
            raise KeyError(f"No {key} data found in API response")

        df = pd.json_normalize(data[key])

        # Create full name and process basic info
        df["fullName"] = df["firstName.default"] + " " + df["lastName.default"]
        df = df.rename(columns={"id": "playerId"})

        # Process position for skaters
        if not goalies:
            df["position"] = np.where(~df["positionCode"].isin(["G", "D"]), "F", df["positionCode"])

        # Add team and season info
        df["team"] = team
        df["season"] = season
        df["session"] = session_key
        df["sessionCode"] = session_value

        # Add metadata
        df["meta_datetime"] = pd.to_datetime("now")
        df["meta_source"] = "NHL API"

        # Validate essential columns
        required_columns = ["playerId", "fullName", "team", "season"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        return df

    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to fetch stats data for {team} ({season}): {str(e)}")
    except (KeyError, ValueError) as e:
        raise ValueError(f"Error processing stats data: {str(e)}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {str(e)}")


# Prospects
def scrapeTeamProspects(team: str) -> pd.DataFrame:
    """
    Scrapes prospect data from the NHL website for a given team.

    Parameters:
        team (str): Team abbreviation (e.g., 'MTL', 'TOR', 'BOS')

    Returns:
        pd.DataFrame: A DataFrame containing prospect data with columns:
            - playerId: Unique identifier for each prospect
            - fullName: Prospect's full name
            - position: Player's position (F/D/G)
            - positionCode: Detailed position code
            - shootsCatches: Player's shooting/catching hand
            - team: Team abbreviation
            - meta columns: datetime of scraping

    Raises:
        ValueError: If team abbreviation is invalid
        requests.HTTPError: If the API request fails
        KeyError: If the API response format is unexpected
    """
    try:
        # Validate team
        if not isinstance(team, str) or len(team) < 2:
            raise ValueError(f"Invalid team abbreviation: {team}")

        # Make API request
        url = f"https://api-web.nhle.com/v1/prospects/{team}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Check if data exists
        if not data:
            raise ValueError(f"No prospect data found for team: {team}")

        # Process prospect data
        df = pd.concat([pd.json_normalize(data[key]) for key in data.keys()]).reset_index(drop=True)

        # Create full name and process basic info
        df["fullName"] = df["firstName.default"] + " " + df["lastName.default"]
        df = df.rename(columns={"id": "playerId"})

        # Process position data
        df["position"] = np.where(~df["positionCode"].isin(["G", "D"]), "F", df["positionCode"])
        df["positionD"] = np.where(
            df["position"] == "D", df["shootsCatches"] + df["position"], df["position"]
        )

        # Add team info
        df["team"] = team

        # Add metadata
        df["meta_datetime"] = pd.to_datetime("now")
        df["meta_source"] = "NHL API"

        # Validate essential columns
        required_columns = ["playerId", "fullName", "position", "team"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        return df

    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to fetch prospect data for {team}: {str(e)}")
    except (KeyError, ValueError) as e:
        raise ValueError(f"Error processing prospect data: {str(e)}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {str(e)}")


# Schedule
def scrapeTeamSchedule(team: str, season: Union[str, int]) -> pd.DataFrame:
    """
    Scrapes schedule data from the NHL website for a given team and season.

    Parameters:
        team (str): Team abbreviation (e.g., 'MTL', 'TOR', 'BOS')
        season (Union[str, int]): Season in 'YYYYYYYY' format (e.g., '20232024' or 20232024)

    Returns:
        pd.DataFrame: A DataFrame containing schedule data with columns:
            - id: Game ID
            - gameType: Type of game (regular season, playoffs, etc.)
            - season: Season identifier
            - gameDate: Date of the game
            - awayTeam: Away team information
            - homeTeam: Home team information
            - venue: Game venue information
            - startTimeUTC: Game start time in UTC
            - teamAbbrev: Requested team's abbreviation
            - meta columns: datetime of scraping

    Raises:
        ValueError: If team or season parameters are invalid
        requests.HTTPError: If the API request fails
        KeyError: If the API response format is unexpected
    """
    try:
        # Validate team
        if not isinstance(team, str) or len(team) < 2:
            raise ValueError(f"Invalid team abbreviation: {team}")

        # Validate season
        season_str = str(season)
        if not len(season_str) == 8:
            raise ValueError(f"Invalid season format: {season}. Must be 'YYYYYYYY' format.")

        # Make API request
        url = f"https://api-web.nhle.com/v1/club-schedule-season/{team}/{season_str}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Check if games data exists
        if "games" not in data:
            raise KeyError("Unexpected API response format: 'games' key not found")

        # Process schedule data
        schedule_df = pd.json_normalize(data["games"])

        # Add team identifier
        schedule_df["teamAbbrev"] = team

        # Add metadata
        schedule_df["meta_datetime"] = pd.to_datetime("now")
        schedule_df["meta_source"] = "NHL API"

        # Convert date columns to datetime
        date_columns = ["gameDate", "startTimeUTC"]
        for col in date_columns:
            if col in schedule_df.columns:
                schedule_df[col] = pd.to_datetime(schedule_df[col])

        # Validate essential columns
        required_columns = ["id", "gameType", "season", "gameDate"]
        missing_columns = [col for col in required_columns if col not in schedule_df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        return schedule_df

    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to fetch schedule data for {team} ({season}): {str(e)}")
    except (KeyError, ValueError) as e:
        raise ValueError(f"Error processing schedule data: {str(e)}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {str(e)}")


# Teams
def scrapeTeamDetails() -> pd.DataFrame:
    """
    Scrape detailed team data from NHL Records API.

    Use this function for detailed team data including franchise history,
    team codes, locations, and conference/division information.

    Returns:
        pd.DataFrame: A DataFrame containing detailed team data including:
            - Franchise information
            - Team codes and abbreviations
            - Place names and common names
            - Conference and division affiliations
            - First and last season IDs
            - Team logos

    Raises:
        requests.HTTPError: If the API request fails
        ValueError: If there's an error processing the data
    """
    try:
        url = (
            "https://records.nhl.com/site/api/franchise?"
            "include=teams.id&include=teams.active&include=teams.triCode"
            "&include=teams.placeName&include=teams.commonName"
            "&include=teams.fullName&include=teams.logos"
            "&include=teams.conference.name&include=teams.division.name"
            "&include=teams.franchiseTeam.firstSeason.id"
            "&include=teams.franchiseTeam.lastSeason.id"
        )

        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if "data" not in data:
            raise ValueError("Unexpected API response format: 'data' key not found")

        team_df = pd.json_normalize(data["data"])

        # Add metadata
        team_df["meta_datetime"] = pd.to_datetime("now")
        team_df["meta_source"] = "NHL Records API"

        return team_df

    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to fetch team data: {str(e)}")
    except (KeyError, ValueError) as e:
        raise ValueError(f"Error processing team data: {str(e)}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {str(e)}")


# AHL and ECHL affiliates of active teams
def scrapeTeamAffiliates() -> pd.DataFrame:
    """
    Scrape active team affiliate data (AHL and ECHL) from NHL Records API.

    Originally contributed by @Hamalytics on Twitter/X. This function retrieves
    information about NHL teams' minor league affiliates, including both AHL
    and ECHL teams.

    Returns:
        pd.DataFrame: A DataFrame containing affiliate data with columns:
            - franchiseId: NHL franchise ID
            - teamId: NHL team ID
            - teamAffiliateId: Affiliate team ID
            - teamAffiliate.fullName: Affiliate team name
            - teamAffiliate.officialSiteUrl: Affiliate team website
            - teamAffiliate.league.abbreviation: League abbreviation (AHL/ECHL)
            - meta columns: datetime of scraping

    Raises:
        requests.HTTPError: If the API request fails
        ValueError: If there's an error processing the data
    """
    try:
        url = (
            "https://records.nhl.com/site/api/team-affiliate?"
            "cayenneExp=active=true"
            "&include=franchiseId"
            "&include=teamId"
            "&include=teamAffiliateId"
            "&include=teamAffiliate.fullName"
            "&include=teamAffiliate.officialSiteUrl"
            "&include=teamAffiliate.league.abbreviation"
        )

        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Validate response data
        if "data" not in data:
            raise ValueError("Unexpected API response format: 'data' key not found")

        # Create DataFrame
        df = pd.json_normalize(data["data"])

        # Add metadata
        df["meta_datetime"] = pd.to_datetime("now")
        df["meta_source"] = "NHL Records API"

        # Validate essential columns
        required_columns = ["teamId", "teamAffiliateId", "teamAffiliate.fullName"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        return df

    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to fetch affiliate data: {str(e)}")
    except (KeyError, ValueError) as e:
        raise ValueError(f"Error processing affiliate data: {str(e)}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {str(e)}")


# Active Teams
def scrapeActiveTeams() -> pd.DataFrame:
    """
    Scrapes active team data from the NHL website.

    This function is used to scrape active team data from the NHL website.
    Originally contributed by @Hamalytics on Twitter/X.

    Returns:
        pd.DataFrame: A DataFrame containing active team data with columns:
            - teamId: Unique team identifier
            - abbrev: Team abbreviation (e.g., 'MTL', 'TOR')
            - name: Full team name
            - commonName: Team name without city
            - placeNameWithPreposition: City name with preposition
            - logo: URL to team's primary logo
            - darkLogo: URL to team's dark logo
            - meta columns: datetime of scraping

    Raises:
        requests.HTTPError: If the API request fails
        ValueError: If there's an error processing the data
    """
    try:
        # Make API request
        url = "https://api-web.nhle.com/v1/schedule-calendar/now"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Check if teams data exists
        if "teams" not in data:
            raise KeyError("Unexpected API response format: 'teams' key not found")

        # Process teams data
        df = pd.json_normalize(data["teams"])

        # Select and rename columns
        columns = [
            "id",
            "abbrev",
            "name.default",
            "commonName.default",
            "placeNameWithPreposition.default",
            "logo",
            "darkLogo",
        ]
        df = df[columns]
        df = df.rename(
            columns={
                "id": "teamId",
                "name.default": "name",
                "commonName.default": "commonName",
                "placeNameWithPreposition.default": "placeNameWithPreposition",
            }
        )

        # Add metadata
        df["meta_datetime"] = pd.to_datetime("now")
        df["meta_source"] = "NHL API"

        # Validate essential columns
        required_columns = ["teamId", "abbrev", "name"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        return df

    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to fetch active teams data: {str(e)}")
    except (KeyError, ValueError) as e:
        raise ValueError(f"Error processing active teams data: {str(e)}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {str(e)}")


# Draft History
def scrapeTeamDraftHistory(franchiseId: Union[str, int]) -> pd.DataFrame:
    """
    Scrapes draft history for a given franchise from the NHL website.

    This function is used to scrape draft history for a given franchise from the NHL website.
    The franchise ID can be found in the NHL Records API.

    Parameters:
        franchiseId (Union[str, int]): The franchise ID from NHL Records API.
            Used to identify the team for draft history scraping.

    Returns:
        pd.DataFrame: A DataFrame containing the scraped draft history.

    Raises:
        ValueError: If the franchise ID is invalid
        requests.HTTPError: If the API request fails
        KeyError: If the API response format is unexpected
    """
    franchiseId = str(franchiseId)
    if not franchiseId:
        raise ValueError("Invalid franchise ID: Must be a non-empty string or integer")

    try:
        # Base URL for NHL Records API
        base_url = "https://records.nhl.com/site/api/draft"

        # Include parameters for the API request
        includes = [
            "draftProspect.id",
            "franchiseTeam",
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
        ]

        # Build the URL with parameters
        url = (
            f"{base_url}"
            f"?{'&'.join(f'include={include}' for include in includes)}"
            f"&cayenneExp=franchiseTeam.franchiseId=%22{franchiseId}%22"
        )
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        draft_df = pd.json_normalize(data["data"])

        draft_df["meta_datetime"] = pd.to_datetime("now")
        draft_df["meta_source"] = "NHL Records API"

        return draft_df

    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to fetch draft history: {str(e)}")
    except (KeyError, ValueError) as e:
        raise ValueError(f"Error processing draft history: {str(e)}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {str(e)}")
