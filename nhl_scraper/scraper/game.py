"""
NHL Game Data Scraper Module.

This module provides functions to scrape NHL game data including:
- Play-by-play events
- Game rosters
- Player shifts
- Game statistics

Functions:
    scrapeGamePlayByPlay: Get play-by-play data
    scrapeGameComplete: Get comprehensive game data
    scrapeGameRosters: Get game rosters
    scrapeGameShifts: Get player shifts
    scrapeGameShiftsLegacy: Get legacy shift data

"""

import re
import warnings
from functools import lru_cache
from typing import Tuple, Union

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

# from lxml.etree import _Element
from pandas import DataFrame

from nhl_scraper.scraper.tools.helper import process_play_by_play

# from nhl_scraper.scraper.utils.types import EventDict, XPathResult
from nhl_scraper.scraper.tools.utils import (  # fix_coordinates,; process_event_players,; ; process_event,; clean_player_name_etree,
    calculate_shift_stats,
    convert_time_to_seconds,
    etree,
    fix_json_encoding,
    scrape_game_data,
    validate_game_id,
    validate_roster_data,
    validate_shift_data,
)

# from nhl_scraper.scraper.tools.constants import PBP_DF_RENAME, PBP_DEFAULT_COLUMNS

warnings.filterwarnings("ignore")


# Play-by-Play
def scrapeGamePlayByPlay(game_id: Union[str, int]) -> DataFrame:
    """
    Scrapes play-by-play data for an NHL game.

    Args:
        game_id: NHL game ID (10-digit number)

    Returns:
        pd.DataFrame: Play-by-play data with columns including:
            - Event details (type, time, period, coordinates)
            - Player information
            - Team information
            - Score information
            - Metadata

    Raises:
        ValueError: If game ID is invalid
        requests.HTTPError: If API request fails
        KeyError: If response format is unexpected
    """
    try:
        # Validate game ID
        game_id = validate_game_id(game_id)

        # Make API request
        data = scrape_game_data(game_id)

        # Create initial DataFrame
        pbp_df = pd.json_normalize(data["plays"])

        rosters_df = scrapeGameRosters(game_id)

        # Process play by play data
        pbp_df = process_play_by_play(data, game_id, rosters_df)

        # Add game ID and metadata
        pbp_df["gameId"] = game_id
        pbp_df["meta_datetime"] = pd.to_datetime("now")
        pbp_df["meta_source"] = "NHL API"
        return pbp_df

    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to fetch play-by-play data: {str(e)}")
    except (KeyError, ValueError) as e:
        raise ValueError(f"Error processing play-by-play data: {str(e)}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {str(e)}")


@lru_cache(maxsize=1000)
def scrapeGameComplete(game_id: Union[str, int]) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Scrapes comprehensive game data including play-by-play, rosters, and shifts.

    Args:
        game_id: NHL game ID (10-digit number)

    Returns:
        Tuple containing:
            - Play-by-play DataFrame
            - Rosters DataFrame
            - Shifts DataFrame
    """
    try:
        # Validate game ID and get base data
        game_id = validate_game_id(game_id)
        data = scrape_game_data(game_id)

        # Get all required data
        rosters_df = scrapeGameRosters(game_id)
        pbp_df = process_play_by_play(data, game_id, rosters_df)

        # Try modern shifts API first, fallback to legacy if needed
        try:
            shifts_df = scrapeGameShifts(game_id)
        except Exception as e:
            print(f"Warning: Using legacy shifts data due to: {e}")
            shifts_df = scrapeGameShiftsLegacy(game_id)

        return pbp_df, rosters_df, shifts_df

    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to fetch game data: {str(e)}")
    except ValueError as e:
        raise ValueError(f"Error processing game data: {str(e)}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {str(e)}")


# Rosters
def scrapeGameRosters(game_id: Union[str, int]) -> DataFrame:
    """
    Scrapes roster information for both teams in an NHL game.

    Args:
        game_id: NHL game ID (10-digit number)

    Returns:
        pd.DataFrame: Roster information including:
            - Player details (ID, name, position, number)
            - Team information
            - Game status (starter, scratched, etc.)
            - Metadata

    Raises:
        ValueError: If game ID is invalid
        requests.HTTPError: If API request fails
        KeyError: If response format is unexpected
    """
    try:
        # Validate game ID
        game_id = validate_game_id(game_id)

        # Make API request
        data = scrape_game_data(game_id)

        roster_df = pd.json_normalize(data["rosterSpots"])

        abbrev_dict = {
            data["awayTeam"]["id"]: data["awayTeam"]["abbrev"],
            data["homeTeam"]["id"]: data["homeTeam"]["abbrev"],
        }

        roster_df["teamAbbrev"] = roster_df["teamId"].map(abbrev_dict)

        roster_df["is_home"] = (roster_df["teamId"] == data["homeTeam"]["id"]).astype(int)

        # Add game information
        roster_df["gameId"] = game_id
        roster_df["gameDatetime"] = pd.to_datetime(data.get("gameDate"))
        roster_df["gameType"] = data.get("gameType")
        roster_df["season"] = data.get("season")

        # Add metadata
        roster_df["meta_datetime"] = pd.to_datetime("now")
        roster_df["meta_source"] = "NHL API"

        # Validate data
        roster_df = validate_roster_data(roster_df)

        # Sort DataFrame
        sort_columns = ["teamAbbrev", "positionCode", "sweaterNumber"]
        roster_df = roster_df.sort_values(sort_columns)

        return roster_df

    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to fetch roster data: {str(e)}")
    except (KeyError, ValueError) as e:
        raise ValueError(f"Error processing roster data: {str(e)}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {str(e)}")


# Shifts
def scrapeGameShifts(game_id: Union[str, int]) -> DataFrame:
    """
    Scrapes shift data for all players in an NHL game.

    Args:
        game_id: NHL game ID (10-digit number)

    Returns:
        pd.DataFrame: Shift data including:
            - Player information (ID, name, team)
            - Shift details (start, end, duration)
            - Period information
            - Calculated statistics (game time, etc.)
            - Metadata

    Raises:
        ValueError: If game ID is invalid
        requests.HTTPError: If API request fails
        KeyError: If response format is unexpected
    """
    try:
        # Validate game ID
        game_id = validate_game_id(game_id)

        # Make API requests
        shifts_url = f"https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={game_id}"
        game_data = scrape_game_data(game_id)
        shifts_response = requests.get(shifts_url)

        shifts_response.raise_for_status()

        shifts_data = shifts_response.json()

        # Fix encoding issues in the JSON object
        shifts_data = fix_json_encoding(shifts_data)

        # Get roster information for player details
        roster_df = scrapeGameRosters(game_id)
        roster_df = validate_roster_data(roster_df)

        player_info = roster_df[
            [
                "teamId",
                "playerId",
                "sweaterNumber",
                "positionCode",
                "position",
                "headshot",
                "teamAbbrev",
                "is_home",
            ]
        ]

        # Process shifts data
        if "data" not in shifts_data or not shifts_data["data"]:
            raise ValueError(f"No shift data found for game {game_id}")

        shifts_df = pd.json_normalize(shifts_data["data"])

        # Validate shift data
        shifts_df = validate_shift_data(shifts_df)

        # # Merge with player information
        shifts_df = shifts_df.merge(
            player_info, on=["playerId", "teamId", "teamAbbrev"], how="left"
        )

        # Add full name
        shifts_df["fullName"] = shifts_df["firstName"] + " " + shifts_df["lastName"]

        # Calculate additional statistics
        shifts_df = calculate_shift_stats(shifts_df)

        # Add game information
        shifts_df["gameId"] = game_id
        shifts_df["gameDatetime"] = pd.to_datetime(game_data.get("gameDate"))
        shifts_df["gameType"] = game_data.get("gameType")
        shifts_df["season"] = game_data.get("season")

        # Add metadata
        shifts_df["meta_datetime"] = pd.to_datetime("now")
        shifts_df["meta_source"] = "NHL API"

        # Validate data
        # validate_shift_data(shifts_df)

        # Sort DataFrame
        sort_columns = [
            "teamAbbrev",
            "period",
        ]  # "startTimeInSeconds", "playerId"]
        shifts_df = shifts_df.sort_values(sort_columns)

        # Add shift numbers
        shifts_df["shiftNumber"] = shifts_df.groupby("playerId").cumcount() + 1

        return shifts_df

    except requests.RequestException as e:
        raise requests.HTTPError(f"Failed to fetch shift data: {str(e)}")
    except (KeyError, ValueError) as e:
        raise ValueError(f"Error processing shift data: {str(e)}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {str(e)}")


def scrapeGameShiftsLegacy(game_id: Union[str, int]) -> DataFrame:
    """
    Scrape shift data from the NHL's legacy HTML reports using lxml.etree.

    Use this method for historical games where the API data is unavailable.

    Args:
        game_id: NHL game ID (10-digit number)

    Returns:
        pd.DataFrame: Shift data including:
            - Player information (name, number, team)
            - Shift details (start, end, duration)
            - Period information
            - Calculated statistics

    Raises:
        ValueError: If game ID is invalid
        requests.HTTPError: If HTML request fails
        ValueError: If HTML parsing fails
    """
    try:
        # Validate game ID
        game_id = validate_game_id(game_id)

        year1 = str(game_id)[:4]
        year2 = int(year1) + 1
        year1_2 = f"{year1}{year2}"

        shortId = str(game_id)[4:]

        url_template = f"https://www.nhl.com/scores/htmlreports/{year1}{year2}/T{{HV}}{shortId}.HTM"

        team_shifts_dfs = []

        try:
            for side in ["H", "V"]:
                url = url_template.format(HV=side)

                # Fetch the webpage content
                response = requests.get(url)
                response.raise_for_status()

                # Use BeautifulSoup with lxml parser
                soup = BeautifulSoup(response.content, "lxml")

                # Convert BeautifulSoup object to lxml etree
                tree = etree.HTML(str(soup))

                # Extract player names using XPath
                player_names = tree.xpath('.//td[@class="playerHeading + border"]/text()')

                # Extract shift details using XPath
                shift_rows = tree.xpath('.//tr[@class="evenColor"] | .//tr[@class="oddColor"]')

                # Store extracted data
                shift_data = []
                for row in shift_rows:
                    shift_number = (
                        row.xpath("./td[1]/text()")[0].strip()
                        if row.xpath("./td[1]/text()")
                        else ""
                    )
                    period = (
                        row.xpath("./td[2]/text()")[0].strip()
                        if row.xpath("./td[2]/text()")
                        else ""
                    )
                    start_time = (
                        row.xpath("./td[3]/text()")[0].strip()
                        if row.xpath("./td[3]/text()")
                        else ""
                    )
                    end_time = (
                        row.xpath("./td[4]/text()")[0].strip()
                        if row.xpath("./td[4]/text()")
                        else ""
                    )
                    duration = (
                        row.xpath("./td[5]/text()")[0].strip()
                        if row.xpath("./td[5]/text()")
                        else ""
                    )
                    event = (
                        row.xpath("./td[6]/text()")[0].strip()
                        if row.xpath("./td[6]/text()")
                        else ""
                    )

                    shift_data.append(
                        {
                            "Shift Number": shift_number,
                            "Period": period,
                            "Start Time": start_time,
                            "End Time": end_time,
                            "Duration": duration,
                            "Event": event,
                        }
                    )

                # Output extracted data
                shifts_df = pd.DataFrame(shift_data)

                shifts_df["is_home"] = 1 if side == "H" else 0

                # Replace 'OT' with 4 ### TO FIX EVENTUALLY BECAUSE OF PLAYOFFS
                shifts_df["Period"] = shifts_df["Period"].replace("OT", 4)
                shifts_df["Period"] = pd.to_numeric(shifts_df["Period"], errors="coerce")

                shifts_df["Shift Number"] = pd.to_numeric(
                    shifts_df["Shift Number"], errors="coerce"
                )

                # Assign a row with 1 where "Start Time" has a / in it and filter out 0s
                shifts_df["dummy"] = np.where(shifts_df["Start Time"].str.contains("/"), 1, 0)
                shifts_df = shifts_df[shifts_df["dummy"] == 1]
                shifts_df = shifts_df.drop(columns=["dummy"])

                # Assign player names
                shifts_df["Player Name"] = None
                player_index = 0

                shifts_df["Shift Number"] = pd.to_numeric(
                    shifts_df["Shift Number"], errors="coerce"
                )
                shifts_df = shifts_df.reset_index(drop=True)
                # Iterate through shifts and assign player names
                for i in range(len(shifts_df)):
                    if player_index < len(player_names):
                        shifts_df.loc[i, "Player Name"] = player_names[player_index]

                    # If shift number decreases, move to the next player
                    if (
                        i > 0
                        and shifts_df.loc[i, "Shift Number"] < shifts_df.loc[i - 1, "Shift Number"]
                    ):
                        player_index += 1  # Move to the next player

                # Split the "Player Name" column into "Player Number" and "Player Name"
                shifts_df[["Player Number", "Player Name"]] = shifts_df["Player Name"].str.split(
                    " ", n=1, expand=True
                )

                # Convert "Player Number" to numeric for sorting or analysis
                shifts_df["Player Number"] = pd.to_numeric(
                    shifts_df["Player Number"], errors="coerce"
                )
                shifts_df["firstName"] = shifts_df["Player Name"].str.split(", ").str[1]

                shifts_df["lastName"] = shifts_df["Player Name"].str.split(", ").str[0]
                # Remove number from firstName
                shifts_df["lastName"] = shifts_df["lastName"].str.replace(r"\d+", "")
                shifts_df["lastName"] = shifts_df["lastName"].str.strip()

                shifts_df[["Start Time (Elapsed)", "Start Time (Remaining)"]] = shifts_df[
                    "Start Time"
                ].str.split(" ", n=1, expand=True)
                shifts_df[["End Time (Elapsed)", "End Time (Remaining)"]] = shifts_df[
                    "End Time"
                ].str.split(" ", n=1, expand=True)

                # Strip "/ " in remaining cols
                shifts_df["Start Time (Remaining)"] = shifts_df[
                    "Start Time (Remaining)"
                ].str.replace("/ ", "")
                shifts_df["End Time (Remaining)"] = shifts_df["End Time (Remaining)"].str.replace(
                    "/ ", ""
                )

                shifts_df = shifts_df.drop(columns=["Start Time", "End Time"])

                for col in [
                    "Start Time (Elapsed)",
                    "Start Time (Remaining)",
                    "End Time (Elapsed)",
                    "End Time (Remaining)",
                    "Duration",
                ]:
                    col_with_seconds = col + " (Seconds)"
                    shifts_df[col_with_seconds] = shifts_df[col].apply(convert_time_to_seconds)

                team_shifts_dfs.append(shifts_df)

        except requests.RequestException as e:
            raise requests.HTTPError(f"Failed to fetch HTML shift report: {str(e)}")
        except (ValueError, etree.ParseError) as e:
            raise ValueError(f"Error processing HTML shift data: {str(e)}")
        except Exception as e:
            print(f"Error processing HTML shift data: {str(e)}")
            return pd.DataFrame()

        shifts_df = pd.concat(team_shifts_dfs)
        shifts_df = shifts_df.reset_index(drop=True)

        shifts_df["startTimeInSeconds"] = (
            shifts_df["Start Time (Elapsed) (Seconds)"] + (shifts_df["Period"] - 1) * 60 * 20
        )
        shifts_df["endTimeInSeconds"] = (
            shifts_df["End Time (Elapsed) (Seconds)"] + (shifts_df["Period"] - 1) * 60 * 20
        )

        shifts_df["sweaterNumber"] = shifts_df["Player Number"]
        shifts_df["gameId"] = game_id

        # Add game information
        shifts_df["gameId"] = game_id
        shifts_df["source"] = "HTML"
        shifts_df["season"] = year1_2

        # Add metadata
        shifts_df["meta_datetime"] = pd.to_datetime("now")
        shifts_df["meta_source"] = "NHL HTML Reports"

        # Sort DataFrame
        sort_columns = ["Period", "startTimeInSeconds", "Player Number"]
        shifts_df = shifts_df.sort_values(sort_columns)

        return shifts_df
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {str(e)}")


def hs_strip_html(td):
    """
    Function from Harry Shomer's Github

    Strip html tags and such

    :param td: pbp

    :return: list of plays (which contain a list of info) stripped of html
    """
    for y in range(len(td)):
        # Get the 'br' tag for the time column...this get's us time remaining instead
        # of elapsed and remaining combined
        if y == 3:
            td[y] = td[y].get_text()  # This gets us elapsed and remaining combined-< 3:0017:00
            index = td[y].find(":")
            td[y] = td[y][: index + 3]
        elif (y == 6 or y == 7) and td[0] != "#":
            # 6 & 7-> These are the player 1 ice one's
            # The second statement controls for when it's just a header
            baz = td[y].find_all("td")
            bar = [
                baz[z] for z in range(len(baz)) if z % 4 != 0
            ]  # Because of previous step we get repeats...delete some

            # The setup in the list is now: Name/Number->Position->Blank...and repeat
            # Now strip all the html
            players = []
            for i in range(len(bar)):
                if i % 3 == 0:
                    try:
                        # name = return_name_html(bar[i].find("font")["title"])
                        name = bar[i].get_text().strip("\n")
                        number = (
                            bar[i].get_text().strip("\n")
                        )  # Get number and strip leading/trailing newlines
                    except KeyError:
                        name = ""
                        number = ""
                elif i % 3 == 1:
                    if name != "":
                        position = bar[i].get_text()
                        players._append([name, number, position])

            td[y] = players
        else:
            td[y] = td[y].get_text()

    return td


def scrape_html_events(season, game_id):
    # global game
    url = "http://www.nhl.com/scores/htmlreports/" + season + "/PL0" + game_id + ".HTM"
    page = requests.get(url)
    # if int(season)<20092010:
    #   soup = BeautifulSoup(page.content, 'html.parser')
    # else:
    #   soup = BeautifulSoup(page.content, 'lxml')
    soup = BeautifulSoup(page.content.decode("ISO-8859-1"), "lxml")
    tds = soup.find_all("td", {"class": re.compile(".*bborder.*")})
    # global stripped_html
    # global eventdf
    stripped_html = hs_strip_html(tds)
    length = int(len(stripped_html) / 8)
    eventdf = pd.DataFrame(np.array(stripped_html).reshape(length, 8)).rename(
        columns={
            0: "index",
            1: "period",
            2: "strength",
            3: "time",
            4: "event",
            5: "description",
            6: "away_skaters",
            7: "home_skaters",
        }
    )
    split = eventdf.time.str.split(":")
    # game_date = soup.find_all(
    #     "td", {"align": "center", "style": "font-size: 10px;font-weight:bold"}
    # )[2].get_text()

    potentialnames = soup.find_all(
        "td", {"align": "center", "style": "font-size: 10px;font-weight:bold"}
    )

    for i in range(0, 999):
        away = potentialnames[i].get_text()
        if ("Away Game") in away or ("tr./Away") in away:
            away = re.split("Match|Game", away)[0]
            break

    for i in range(0, 999):
        home = potentialnames[i].get_text()
        if ("Home Game") in home or ("Dom./Home") in home:
            home = re.split("Match|Game", home)[0]
            break

    game = eventdf.assign(
        away_skaters=eventdf.away_skaters.str.replace("\n", ""),
        home_skaters=eventdf.home_skaters.str.replace("\n", ""),
        original_time=eventdf.time,
        time=split.str[0] + ":" + split.str[1].str[:2],
        home_team=home,
        away_team=away,
    )

    game = game.assign(
        away_team_abbreviated=game.away_skaters[0].split(" ")[0],
        home_team_abbreviated=game.home_skaters[0].split(" ")[0],
    )

    game = game[game.period != "Per"]

    game = game.assign(index=game.index.astype(int)).rename(columns={"index": "event_index"})

    game = game.assign(event_team=game.description.str.split(" ").str[0])

    game = game.assign(event_team=game.event_team.str.split("\xa0").str[0])

    game = game.assign(
        event_team=np.where(
            ~game.event_team.isin(
                [game.home_team_abbreviated.iloc[0], game.away_team_abbreviated.iloc[0]]
            ),
            "\xa0",
            game.event_team,
        )
    )

    game = game.assign(
        other_team=np.where(
            game.event_team == "",
            "\xa0",
            np.where(
                game.event_team == game.home_team_abbreviated.iloc[0],
                game.away_team_abbreviated.iloc[0],
                game.home_team_abbreviated.iloc[0],
            ),
        )
    )

    game["event_player_str"] = (
        game.description
        # .apply(lambda x: re.findall("(#)(\d\d)|(#)(\d)|(-) (\d\d)|(-) (\d)", x))
        .astype(str)
        .str.replace("#", "")
        .str.replace("-", "")
        .str.replace("'", "")
        .str.replace(",", "")
        .str.replace("(", "")
        .str.replace(")", "")
        .astype(str)
        .str.replace("[", "")
        .str.replace("]", "")
        .apply(lambda x: re.sub(" +", " ", x))
        .str.strip()
    )

    game = game.assign(
        event_player_1=game.event_player_str.str.split(" ").str[0],
        event_player_2=game.event_player_str.str.split(" ").str[1],
        event_player_3=game.event_player_str.str.split(" ").str[2],
    )

    if len(game[game.description.str.contains("Drawn By")]) > 0:

        game = game.assign(
            event_player_2=np.where(
                game.description.str.contains("Drawn By"),
                game.description.str.split("Drawn By")
                .str[1]
                .str.split("#")
                .str[1]
                .str.split(" ")
                .str[0]
                .str.strip(),
                game.event_player_2,
            ),
            event_player_3=np.where(
                game.description.str.contains("Served By"), "\xa0", game.event_player_3
            ),
        )

    game = game.assign(
        event_player_1=np.where(
            (~pd.isna(game.event_player_1)) & (game.event_player_1 != ""),
            np.where(game.event == "FAC", game.away_team_abbreviated, game.event_team)
            + (game.event_player_1.astype(str)),
            game.event_player_1,
        ),
        event_player_2=np.where(
            (~pd.isna(game.event_player_2)) & (game.event_player_2 != ""),
            np.where(
                game.event == "FAC",
                game.home_team_abbreviated,
                np.where(
                    game.event.isin(["BLOCK", "HIT", "PENL"]), game.other_team, game.event_team
                ),
            )
            + (game.event_player_2.astype(str)),
            game.event_player_2,
        ),
        event_player_3=np.where(
            (~pd.isna(game.event_player_3)) & (game.event_player_3 != ""),
            game.event_team + (game.event_player_3.astype(str)),
            game.event_player_3,
        ),
    )

    # game = game.assign(
    #     event_player_1=np.where(
    #         (game.event == "FAC") & (game.event_team == game.home_team_abbreviated),
    #         game.event_player_2,
    #         game.event_player_1,
    #     ),
    #     event_player_2=np.where(
    #         (game.event == "FAC") & (game.event_team == game.home_team_abbreviated),
    #         game.event_player_1,
    #         game.event_player_2,
    #     ),
    # )
    return game
