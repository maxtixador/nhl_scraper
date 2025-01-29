import requests
import pandas as pd
import numpy as np
from functools import lru_cache
from functools import lru_cache
from lxml import etree
from bs4 import BeautifulSoup
import warnings
warnings.filterwarnings('ignore')

pd.set_option('display.max_columns', None)

def convert_str_to_seconds(time_str):

    number_of_separators = time_str.count(':')
    
    if number_of_separators == 0:
        return int(time_str)
    elif number_of_separators == 1:
        minutes, seconds = map(int, time_str.split(':'))
        total_seconds = minutes * 60 + seconds
        return total_seconds
    elif number_of_separators == 2:
        hours, minutes, seconds = map(int, time_str.split(':'))
        total_seconds = hours * 3600 + minutes * 60 + seconds
        return total_seconds
    
    else:
        raise ValueError("Invalid time format")

# Scrape Draft
def scrapeDraft(year = 2023, round = "all"):
    """
    Scrapes draft data from the NHL website for a given year and round.

    Parameters :
      - year (int) : The year of the Draft you want to scrape the data from. Default is set to 2023.
      - round (int/str) : The round of the Draft you want to scrape the data from. It generally takes integers, but you can have "all" to get all rounds. Default is set to "all".

    Returns :
      - draft_df (pd.DataFrame) : A DataFrame containing the scraped draft data.

    """


    url = f"https://api-web.nhle.com/v1/draft/picks/{year}/{round}"

    response = requests.get(url).json()

    draft_df = pd.json_normalize(response["picks"])

    # Add meta data (datetime of the execution)
    draft_df["meta_datetime"] = pd.to_datetime("now")



    return draft_df

# Central of Scouting Rankings
def scrapeRankings(year=2025, category=1):
    """
    Scrapes draft rankings from the NHL website for a given year and category.

    Parameters :
      - year (int) : The year of the Draft you want to scrape the data from. Default is set to 2024.
      - category (int) : The category of the Draft you want to scrape the data from. Default is set to 1.
        1 = North American Skaters
        2 = International Skaters
        3 = North American Goalies
        4 = International Goalies

    Returns :
      - draft_rankings_df (pd.DataFrame) : A DataFrame containing the scraped draft rankings data.

    """

    # By the Central of Scouting of the NHL

    url = f"https://api-web.nhle.com/v1/draft/rankings/{year}/{category}"

    # Which category is what id
    categoryDict = {
        "north-american-skater": 1,
        "international-skater": 2,
        "north-american-goalie": 3,
        "international-goalie": 4
    }



    response = requests.get(url).json()

    draft_rankings_df = pd.json_normalize(response["rankings"])

    # Add meta data (datetime of the execution)
    draft_rankings_df["meta_datetime"] = pd.to_datetime("now")

    return draft_rankings_df


# Teams
def scrapeTeams():
    """
    Scrapes team data from the NHL website. Generally to get team IDs for future use.

    Parameters :
      - None

    Returns :
      - teams_df (pd.DataFrame) : A DataFrame containing the scraped team data.

    *** It does not contain team abbreviations

    """


    url = "https://api.nhle.com/stats/rest/en/franchise?sort=fullName&include=lastSeason.id&include=firstSeason.id"

    response = requests.get(url).json()

    teams_df = pd.json_normalize(response["data"])
    teams_df = teams_df.rename(columns={"id": "teamId"})

    # Add meta data (datetime of the execution)
    teams_df["meta_datetime"] = pd.to_datetime("now")

    return teams_df


# Schedule
def scrapeSchedule(team_slug, season):
    """
    Scrapes schedule data from the NHL website for a given team and season.

    Parameters :
      - team_slug (str) : The slug of the team you want to scrape the schedule data for.
      - season (str/int) : The season you want to scrape the schedule data for in the format of "YYYYYYYY".

    Returns :
      - schedule_df (pd.DataFrame) : A DataFrame containing the scraped schedule data.

    """

    url = f"https://api-web.nhle.com/v1/club-schedule-season/{team_slug}/{season}"

    response = requests.get(url).json()

    schedule_df = pd.json_normalize(response["games"])
    schedule_df["teamAbbrev"] = team_slug

    # Add meta data (datetime of the execution)
    schedule_df["meta_datetime"] = pd.to_datetime("now")

    return schedule_df


# Game

## Play-by-Play
def scrapePlayByPlay(gameId):

    """
    Scrapes play-by-play data from the NHL website for a given game ID.

    Parameters :
      - game_id (int) : The ID of the game you want to scrape the play-by-play data for.

      Returns :
      - pbp_df (pd.DataFrame) : A DataFrame containing the scraped play-by-play data.

    """



    url = f"https://api-web.nhle.com/v1/gamecenter/{gameId}/play-by-play"

    response = requests.get(url).json()

    pbp_df = pd.json_normalize(response["plays"])


    pbp_df["gameId"] = gameId


    # Add meta data (datetime of the execution)
    pbp_df["meta_datetime"] = pd.to_datetime("now")

    return pbp_df


## Play-by-Play with Roster and Shifts
@lru_cache(maxsize=1000)  # Cache up to 1000 unique gameIds
def scrape_pbp(gameId):
    """
    Scrapes play-by-play data from the NHL website for a given game ID.

    Parameters :
      - game_id (int) : The ID of the game you want to scrape the play-by-play data for.

      Returns :
      - pbp_df (pd.DataFrame) : A DataFrame containing the scraped play-by-play data.
      - rosters_df (pd.DataFrame) : A DataFrame containing the scraped roster data.
      - shifts_df (pd.DataFrame) : A DataFrame containing the scraped shift data.


    """



    url = f"https://api-web.nhle.com/v1/gamecenter/{gameId}/play-by-play"

    response = requests.get(url).json()

    pbp_df = pd.json_normalize(response["plays"])

    rosters_df = scrapeRosters(gameId)
    rosters_df['fullName'] = rosters_df['firstName.default'] + ' ' + rosters_df['lastName.default']

    rosters_dict = dict(zip(rosters_df['playerId'], rosters_df['fullName']))

    shifts_url = f"https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={gameId}"

    shifts_response = requests.get(shifts_url).json()

    # shifts_df = pd.json_normalize(shifts_response["data"])
    # shifts_df['startTime_s'] = shifts_df['startTime'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) + (shifts_df['period'] -1)* 20 * 60
    # shifts_df['endTime_s'] = shifts_df['endTime'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1])) + (shifts_df['period'] -1)* 20 * 60
    # shifts_df['fullName'] = shifts_df['firstName'] + ' ' + shifts_df['lastName']

    shifts_df = scrapeShiftsHTML(gameId)
    shifts_df = shifts_df.merge(rosters_df, on=['is_home', 'sweaterNumber'], how='left')
    shifts_df.loc[shifts_df['positionCode'].isin(["L", "C", "R", "W"]), 'position'] = 'F'
    shifts_df.loc[shifts_df['positionCode'].isin(["D"]), 'position'] = 'D'
    shifts_df.loc[shifts_df['positionCode'].isin(["G"]), 'position'] = 'G'

    shifts_df = shifts_df.reset_index(drop=False)
    shifts_df=shifts_df.rename(columns={'index': 'id'})

 

    shifts_df['side'] = np.where(shifts_df['is_home'] == 1, 'home', 'away')


    # Compare columns
    og_cols = ['eventId', 'timeInPeriod', 'timeRemaining', 'situationCode',
       'homeTeamDefendingSide', 'typeCode', 'typeDescKey', 'sortOrder',
       'periodDescriptor.number', 'periodDescriptor.periodType',
       'periodDescriptor.maxRegulationPeriods', 'details.eventOwnerTeamId',
       'details.losingPlayerId', 'details.winningPlayerId', 'details.xCoord',
       'details.yCoord', 'details.zoneCode', 'details.hittingPlayerId',
       'details.hitteePlayerId', 'details.blockingPlayerId',
       'details.shootingPlayerId', 'details.reason', 'details.shotType',
       'details.goalieInNetId', 'details.awaySOG', 'details.homeSOG',
       'details.playerId', 'details.typeCode', 'details.descKey',
       'details.duration', 'details.committedByPlayerId',
       'details.drawnByPlayerId', 'pptReplayUrl', 'details.scoringPlayerId',
       'details.scoringPlayerTotal', 'details.assist1PlayerId',
       'details.assist1PlayerTotal', 'details.assist2PlayerId',
       'details.assist2PlayerTotal', 'details.awayScore', 'details.homeScore',
       'details.highlightClipSharingUrl', 'details.highlightClipSharingUrlFr',
       'details.highlightClip', 'details.highlightClipFr',
       'details.discreteClip', 'details.discreteClipFr',
       'details.secondaryReason', 'details.servedByPlayerId', 'zoneStartSide_1','zoneStartSideDetail_1']
    pbp_columns = set(pbp_df.columns.tolist())
    expected_columns = set(og_cols)

    if pbp_columns != expected_columns:
        new_columns = pbp_columns - expected_columns
        missing_columns = expected_columns - pbp_columns
        if new_columns:
            print(f"New columns in the dataset: {new_columns}")

        if missing_columns:
            # print(f"Missing columns from the dataset: {missing_columns}. We will create them.")
            for col in missing_columns:
                pbp_df[col] = np.nan

        # raise ValueError("Column mismatch detected. See details above.")

    # Life's always better when you have team abbreviation in the dataframe instead of just the teamId
    abbrev_dict = {
        response['awayTeam']['id'] : response['awayTeam']['abbrev'],
        response['homeTeam']['id'] : response['homeTeam']['abbrev']
    }
    pbp_df['eventTeam'] = pbp_df['details.eventOwnerTeamId'].map(abbrev_dict)
    pbp_df["gameId"] = gameId

    pbp_df['period'] = pd.to_numeric(pbp_df['periodDescriptor.number'])

    # Test to see if events we are not aware of
    expected_events = ['period-start', 'faceoff', 'hit', 'blocked-shot', 'shot-on-goal',
       'stoppage', 'giveaway', 'delayed-penalty', 'penalty', 'failed-shot-attempt',
       'missed-shot', 'goal', 'takeaway', 'period-end',
       'shootout-complete', 'game-end', ]

    actual_events = pbp_df['typeDescKey'].unique()

    missing_events = set(actual_events) - set(expected_events)

    if missing_events:
        raise ValueError(f"The following events are not in the dataset: {missing_events}")

    # Rename cols
    # pbp_df

    # # Make playerId_1, playerId_2, playerId_3
    # pbp_df['playerId_1'] = np.nan
    # pbp_df['playerName_1'] = np.nan

    # pbp_df['playerId_2'] = np.nan
    # pbp_df['playerName_2'] = np.nan

    # pbp_df['playerId_3'] = np.nan
    # pbp_df['playerName_3'] = np.nan

    # ## if faceoff
    # pbp_df.loc[pbp_df["typeDescKey"] == 'faceoff','playerId_1'] = pbp_df.loc[pbp_df["typeDescKey"] == 'faceoff','details.winningPlayerId']
    # pbp_df.loc[pbp_df["typeDescKey"] == 'faceoff','playerId_2'] = pbp_df.loc[pbp_df["typeDescKey"] == 'faceoff','details.losingPlayerId']

    # pbp_df.loc[pbp_df["typeDescKey"] == 'hit','playerId_1'] = pbp_df.loc[pbp_df["typeDescKey"] == 'hit','details.hittingPlayerId']
    # pbp_df.loc[pbp_df["typeDescKey"] == 'hit','playerId_2'] = pbp_df.loc[pbp_df["typeDescKey"] == 'hit','details.hitteePlayerId']

    # pbp_df.loc[pbp_df["typeDescKey"] == 'blocked-shot','playerId_1'] = pbp_df.loc[pbp_df["typeDescKey"] == 'blocked-shot','details.shootingPlayerId']
    # pbp_df.loc[pbp_df["typeDescKey"] == 'blocked-shot','playerId_2'] = pbp_df.loc[pbp_df["typeDescKey"] == 'blocked-shot','details.blockingPlayerId']

    # pbp_df.loc[pbp_df["typeDescKey"].isin(['shot-on-goal', 'missed-shot']) ,'playerId_1'] = pbp_df.loc[pbp_df["typeDescKey"].isin(['shot-on-goal', 'missed-shot']),'details.shootingPlayerId']

    # pbp_df.loc[pbp_df["typeDescKey"] == 'goal' ,'playerId_1'] = pbp_df.loc[pbp_df["typeDescKey"] == 'goal','details.scoringPlayerId']
    # pbp_df.loc[pbp_df["typeDescKey"] == 'goal' ,'playerId_2'] = pbp_df.loc[pbp_df["typeDescKey"] == 'goal','details.assist1PlayerId']
    # pbp_df.loc[pbp_df["typeDescKey"] == 'goal' ,'playerId_3'] = pbp_df.loc[pbp_df["typeDescKey"] == 'goal','details.assist2PlayerId']

    # pbp_df.loc[pbp_df["typeDescKey"].isin(['giveaway', 'takeaway']),'playerId_1'] = pbp_df.loc[pbp_df["typeDescKey"].isin(['giveaway', 'takeaway']),'details.playerId']

    # pbp_df.loc[pbp_df["typeDescKey"] == 'penalty','playerId_1'] = pbp_df.loc[pbp_df["typeDescKey"] == 'penalty','details.committedByPlayerId']
    # pbp_df.loc[pbp_df["typeDescKey"] == 'penalty','playerId_2'] = pbp_df.loc[pbp_df["typeDescKey"] == 'penalty','details.drawnByPlayerId']
    # # pbp_df.loc[pbp_df["typeDescKey"] == 'penalty','playerId_3'] = pbp_df.loc[pbp_df["typeDescKey"] == 'penalty','details.servedByPlayerId']

    # Player mapping and event assignments
    event_columns = {
        'faceoff': ('details.winningPlayerId', 'details.losingPlayerId'),
        'hit': ('details.hittingPlayerId', 'details.hitteePlayerId'),
        'blocked-shot': ('details.shootingPlayerId', 'details.blockingPlayerId'),
        'shot-on-goal': ('details.shootingPlayerId', None),
        'missed-shot': ('details.shootingPlayerId', None),
        'goal': ('details.scoringPlayerId', 'details.assist1PlayerId', 'details.assist2PlayerId'),
        'giveaway': ('details.playerId', None),
        'takeaway': ('details.playerId', None),
        'penalty': ('details.committedByPlayerId', 'details.drawnByPlayerId', 'details.servedByPlayerId'),
        'failed-shot-attempt': ('details.shootingPlayerId', None)
    }

    # Initialize player columns
    pbp_df[['playerId_1', 'playerId_2', 'playerId_3']] = np.nan
    pbp_df[['playerName_1', 'playerName_2', 'playerName_3']] = np.nan

    # Assign player data based on event type
    for event, columns in event_columns.items():
        for i, col in enumerate(columns, start=1):
            if col:
                pbp_df.loc[pbp_df['typeDescKey'] == event, f'playerId_{i}'] = pbp_df.loc[pbp_df['typeDescKey'] == event, col]


    for col in ['details.awaySOG', 'details.homeSOG', 'details.awayScore', 'details.homeScore'] :
        pbp_df[col] = pbp_df[col].ffill().fillna(0)



    # Dynamically find columns that contain the word 'Clip'
    clip_columns = pbp_df.filter(like='Clip').columns.tolist()


    # Define the columns to drop, including dynamically found clip columns
    columns_to_drop = [
        # 'details.eventOwnerTeamId',
        'details.losingPlayerId', 'details.winningPlayerId',
        'details.hittingPlayerId', 'details.hitteePlayerId', 'details.shootingPlayerId',
        'details.blockingPlayerId', 'details.playerId', 'details.committedByPlayerId',
        'details.drawnByPlayerId', 'periodDescriptor.maxRegulationPeriods', 'situationCode',
        'typeCode', 'pptReplayUrl',
        'details.scoringPlayerId', 'details.assist1PlayerId', 'details.assist2PlayerId',
        'details.servedByPlayerId'

    ] + clip_columns

    # Drop the specified columns
    pbp_df = pbp_df.drop(columns=columns_to_drop)

    pbp_df['playerName_1'] = pbp_df['playerId_1'].map(rosters_dict)
    pbp_df['playerName_2'] = pbp_df['playerId_2'].map(rosters_dict)
    pbp_df['playerName_3'] = pbp_df['playerId_3'].map(rosters_dict)

    pbp_df = pbp_df.rename(columns={
        'typeDescKey' : 'event',
        'periodDescriptor.number' : 'periodNumber',
        'periodDescriptor.periodType' : 'periodType',
        'details.eventOwnerTeamId' : 'teamId',
        'details.xCoord' : 'xCoord',
        'details.yCoord' : 'yCoord',
        'details.zoneCode' : 'zoneCode',
        'details.reason' : 'reason',
        'details.shotType' : 'shotType',
        'details.goalieInNetId' : 'goalieInNetId',
        'details.awaySOG' : 'awaySOG',
        'details.homeSOG' : 'homeSOG',
        'details.typeCode' :	'typeCode',
        'details.descKey' : 'descKey',
        'details.duration' : 'duration',
        'details.scoringPlayerTotal' : 'scoringPlayerTotal',
        'details.assist1PlayerTotal' : 'assist1PlayerTotal',
        'details.assist2PlayerTotal' : 'assist2PlayerTotal',
        'details.awayScore' : 'awayScore',
        'details.homeScore' : 'homeScore',
        'details.secondaryReason' : 'secondaryReason'


        })

    # Elapsed time in seconds
    pbp_df['timeInPeriod_s'] = pbp_df['timeInPeriod'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1]))
    pbp_df['timeRemaining_s'] = pbp_df['timeRemaining'].str.split(':').apply(lambda x: int(x[0]) * 60 + int(x[1]))
    pbp_df['elapsedTime'] = (pbp_df['period']- 1) * 20 * 60 + pbp_df['timeInPeriod_s']


    pbp_df['homeTeam'] = response['homeTeam']['abbrev']
    pbp_df['awayTeam'] = response['awayTeam']['abbrev']

    pbp_df['homeTeamId'] = response['homeTeam']['id']
    pbp_df['awayTeamId'] = response['awayTeam']['id']

    pbp_df['eventTeamType'] = np.where(pbp_df['eventTeam'] == pbp_df['homeTeam'], 'home', 'away')

    pbp_df['xFixed'] = np.where(
    ((pbp_df['eventTeamType'] == "home") & (pbp_df['homeTeamDefendingSide'] == "right")) |
    ((pbp_df['eventTeamType'] == "away") & (pbp_df['homeTeamDefendingSide'] == "right")),
    0 - pbp_df['xCoord'],
    pbp_df['xCoord']
    )

    pbp_df['yFixed'] = np.where(
        ((pbp_df['eventTeamType'] == "home") & (pbp_df['homeTeamDefendingSide'] == "right")) |
        ((pbp_df['eventTeamType'] == "away") & (pbp_df['homeTeamDefendingSide'] == "right")),
        0 - pbp_df['yCoord'],
        pbp_df['yCoord']
    )

    stoppages_times = pbp_df.query("event == 'faceoff'")['elapsedTime'].to_list()

    shifts_df['type_on'] = np.where(
        shifts_df['startTime_s'].isin(stoppages_times),
        'SIP', # Stoppage-in-Play
        'OTF' # On-the-Fly
    )
    shifts_df['type_off'] = np.where(
        shifts_df['endTime_s'].isin(stoppages_times),
        'SIP', # Stoppage-in-Play
        'OTF' # On-the-Fly
    )

    # faceoff_dots = {
    #     "center_ice": {
    #         "description": "The center ice faceoff dot, located at the middle of the rink.",
    #         "coordinates": (0, 0),
    #     },
    #     "neutral_zone_offside": {
    #         "description": "The four faceoff dots near the blue lines in the neutral zone.",
    #         "dots": [
    #             {"location": "Right Blue Line, Left Side", "coordinates": (25, 22)},
    #             {"location": "Right Blue Line, Right Side", "coordinates": (25, -22)},
    #             {"location": "Left Blue Line, Left Side", "coordinates": (-25, 22)},
    #             {"location": "Left Blue Line, Right Side", "coordinates": (-25, -22)},
    #         ],
    #     },
    #     "offensive_zone": {
    #         "description": "The two main faceoff dots in the offensive zone.",
    #         "dots": [
    #             {"location": "Left Offensive Dot", "coordinates": (69, 22)},
    #             {"location": "Right Offensive Dot", "coordinates": (69, -22)},
    #         ],
    #     },
    #     "defensive_zone": {
    #         "description": "The two main faceoff dots in the defensive zone, mirrored from the offensive zone.",
    #         "dots": [
    #             {"location": "Left Defensive Dot", "coordinates": (-69, 22)},
    #             {"location": "Right Defensive Dot", "coordinates": (-69, -22)},
    #         ],
    #     },
    #     "low_zone": {
    #         "description": "Faceoff dots near the goal crease, used for plays closer to the goal.",
    #         "dots": [
    #             {"location": "Low Dot, Right Offensive Zone", "coordinates": (69, 5)},
    #             {"location": "Low Dot, Left Offensive Zone", "coordinates": (69, -5)},
    #             {"location": "Low Dot, Right Defensive Zone", "coordinates": (-69, 5)},
    #             {"location": "Low Dot, Left Defensive Zone", "coordinates": (-69, -5)},
    #         ],
    #     },
    # }

    # Create the initial data for the faceoff dots
    fac = {
    'name': [
        'Center Ice', 'Neutral Zone Right Blue Line Left Side',
        'Neutral Zone Right Blue Line Right Side',
        'Neutral Zone Left Blue Line Left Side',
        'Neutral Zone Left Blue Line Right Side',
        'Offensive Zone Left Dot', 'Offensive Zone Right Dot',
        'Defensive Zone Left Dot', 'Defensive Zone Right Dot',
        'Low Zone Offensive Right Dot', 'Low Zone Offensive Left Dot',
        'Low Zone Defensive Right Dot', 'Low Zone Defensive Left Dot'
    ],
    'x_coord': [0, 20, 20, -20, -20, 69, 69, -69, -69, 69, 69, -69, -69],
    'y_coord': [0, 22, -22, 22, -22, 22, -22, 22, -22, 5, -5, 5, -5],
    'home_team_side': [
        'Centre', 'Neutral Zone', 'Neutral Zone', 'Neutral Zone', 'Neutral Zone',
        'Offensive Zone', 'Offensive Zone', 'Defensive Zone', 'Defensive Zone',
        'Offensive Zone', 'Offensive Zone', 'Defensive Zone', 'Defensive Zone'
    ],
    'away_team_side': [
        'Centre', 'Neutral Zone', 'Neutral Zone', 'Neutral Zone', 'Neutral Zone',
        'Defensive Zone', 'Defensive Zone', 'Offensive Zone', 'Offensive Zone',
        'Defensive Zone', 'Defensive Zone', 'Offensive Zone', 'Offensive Zone'
    ],
    'home_team_side_detail': [
        'Centre', 'Neutral Offensive Zone Right',
        'Neutral Offensive Zone Left', 'Neutral Defensive Zone Right',
        'Neutral Defensive Zone Left', 'Offensive Zone Right',
        'Offensive Zone Left', 'Defensive Zone Right',
        'Defensive Zone Left', 'Offensive Zone Right',
        'Offensive Zone Left', 'Defensive Zone Right', 'Defensive Zone Left'
    ],
    'away_team_side_detail': [
        'Centre', 'Neutral Defensive Zone Left',
        'Neutral Defensive Zone Right', 'Neutral Offensive Zone Left',
        'Neutral Offensive Zone Right', 'Defensive Zone Left',
        'Defensive Zone Right', 'Offensive Zone Left',
        'Offensive Zone Right', 'Defensive Zone Left',
        'Defensive Zone Right', 'Offensive Zone Left', 'Offensive Zone Right'
    ]
    }
    # Create the DataFrame directly from the dictionary
    faceoff_df = pd.DataFrame(fac)
    faceoff_df['xFixed'] = faceoff_df['x_coord']
    faceoff_df['yFixed'] = faceoff_df['y_coord']

    pbp_df = pbp_df.merge(faceoff_df.drop(columns=['name', 'x_coord', 'y_coord']),
                          on=['xFixed', 'yFixed'],how='left')


    # Where was the faceoff taken (if player has a SIP Start)
    shifts_df = shifts_df.merge(
        pbp_df.query("event == 'faceoff'")[['elapsedTime', 'home_team_side', 'away_team_side',
                                            'home_team_side_detail', 'away_team_side_detail']],
        left_on='startTime_s',
        right_on='elapsedTime',
        how='left'
    )

    shifts_df["zoneStartSide"] = np.where(
        shifts_df["side"] == "home",
        shifts_df["home_team_side"],
        shifts_df["away_team_side"])

    shifts_df["zoneStartSideDetail"] = np.where(
        shifts_df["side"] == "home",
        shifts_df["home_team_side_detail"],
        shifts_df["away_team_side_detail"])

    shifts_df = shifts_df.drop(columns=['elapsedTime', 'home_team_side', 'away_team_side',
                                        'home_team_side_detail', 'away_team_side_detail'])
    shifts_df['is_home'] = np.where(shifts_df['side'] == 'home', 1, 0)


    # rosters_df.loc[rosters_df['positionCode'].isin(["L", "C", "R", "W"]), 'position'] = 'F'
    # rosters_df.loc[rosters_df['positionCode'].isin(["D"]), 'position'] = 'D'
    # rosters_df.loc[rosters_df['positionCode'].isin(["G"]), 'position'] = 'G'
    # shifts_df = shifts_df.merge(
    #     rosters_df[['playerId', 'position']],
    #     on='playerId',
    #     how='left'
    # )

    rosters_df['is_home'] = (rosters_df['teamId'] == response.get('homeTeam', {}).get('id', "")).astype(int)
    # Prepare filtered DataFrames
    home_skaters_df = shifts_df.query("position != 'G' and is_home == 1")
    away_skaters_df = shifts_df.query("position != 'G' and is_home == 0")
    home_goalies_df = shifts_df.query("position == 'G' and is_home == 1")
    away_goalies_df = shifts_df.query("position == 'G' and is_home == 0")

    # Precompute elapsed times
    elapsed_times = pbp_df['elapsedTime']

    # Determine skater IDs and goalie IDs by elapsed time
    home_sktrs_id = [
        home_skaters_df.loc[(home_skaters_df['startTime_s'] <= second) & (home_skaters_df['endTime_s'] > second), 'playerId'].unique().tolist()
        for second in elapsed_times
    ]

    away_sktrs_id = [
        away_skaters_df.loc[(away_skaters_df['startTime_s'] <= second) & (away_skaters_df['endTime_s'] > second), 'playerId'].unique().tolist()
        for second in elapsed_times
    ]

    home_goalie_id = [
        home_goalies_df.loc[(home_goalies_df['startTime_s'] <= second) & (home_goalies_df['endTime_s'] > second), 'playerId'].unique()
        for second in elapsed_times
    ]
    home_goalie_id = [ids[0] if len(ids) == 1 else np.nan for ids in home_goalie_id]

    away_goalie_id = [
        away_goalies_df.loc[(away_goalies_df['startTime_s'] <= second) & (away_goalies_df['endTime_s'] > second), 'playerId'].unique()
        for second in elapsed_times
    ]
    away_goalie_id = [ids[0] if len(ids) == 1 else np.nan for ids in away_goalie_id]

    # Count skaters
    n_home_sktrs = [len(ids) for ids in home_sktrs_id]
    n_away_sktrs = [len(ids) for ids in away_sktrs_id]

    # Assign skater counts and goalie IDs
    pbp_df['home_skaters'] = n_home_sktrs
    pbp_df['away_skaters'] = n_away_sktrs
    pbp_df['home_goalie_id'] = home_goalie_id
    pbp_df['away_goalie_id'] = away_goalie_id

    # Determine game strength
    pbp_df['is_home'] = pbp_df['eventTeamType'].map({'home': 1, 'away': 0})
    pbp_df['game_strength'] = pbp_df.apply(
        lambda row: f"{row['home_skaters']}v{row['away_skaters']}" if row['is_home'] == 1 else f"{row['away_skaters']}v{row['home_skaters']}",
        axis=1
    )

    # Determine max number of skaters on ice
    max_column_index = max(len(ids) for ids in home_sktrs_id + away_sktrs_id)

    # Prepare column names
    columns_to_add = [f"home_skater_id{j+1}" for j in range(max_column_index)] + \
                    [f"away_skater_id{j+1}" for j in range(max_column_index)] + \
                    [f"home_skater_fullName{j+1}" for j in range(max_column_index)] + \
                    [f"away_skater_fullName{j+1}" for j in range(max_column_index)]

    # Initialize columns with proper type (object)
    for column in columns_to_add:
        pbp_df[column] = ""

    id_name_dict = rosters_df.set_index('playerId')['fullName'].to_dict()
    # Assign values to the DataFrame for skater IDs and full names
    # Assign skater IDs and names
    for i, (home_skater_ids, away_skater_ids) in enumerate(zip(home_sktrs_id, away_sktrs_id)):
        for j, skater_id in enumerate(home_skater_ids):
            pbp_df.at[i, f"home_skater_id{j+1}"] = skater_id
            pbp_df.at[i, f"home_skater_fullName{j+1}"] = id_name_dict.get(skater_id, "")
        for j, skater_id in enumerate(away_skater_ids):
            pbp_df.at[i, f"away_skater_id{j+1}"] = skater_id
            pbp_df.at[i, f"away_skater_fullName{j+1}"] = id_name_dict.get(skater_id, "")


    # pbp_df = pbp_df.replace('NaN', np.nan, inplace=False)

    pbp_df['home_goalie_id'] = home_goalie_id
    pbp_df['away_goalie_id'] = away_goalie_id

    pbp_df.loc[(pbp_df['goalieInNetId'].notnull()) & (pbp_df['home_goalie_id'].isna()) & (pbp_df['away_goalie_id'].isna()) & (pbp_df['is_home']== 1), "away_goalie_id"] = pbp_df['goalieInNetId']
    pbp_df.loc[(pbp_df['goalieInNetId'].notnull()) & (pbp_df['home_goalie_id'].isna()) & (pbp_df['away_goalie_id'].isna()) & (pbp_df['is_home']== 0), "home_goalie_id"] = pbp_df['goalieInNetId']



    pbp_df['home_goalie_fullName'] = pbp_df['home_goalie_id'].map(id_name_dict)
    pbp_df['away_goalie_fullName'] = pbp_df['away_goalie_id'].map(id_name_dict)


    # Add shifts to pbp
    reshaped_df = pd.melt(
    shifts_df,
    id_vars=['id', 'playerId','teamAbbrev','teamId', 'fullName', 'zoneStartSide', 'zoneStartSideDetail', 'is_home', 'type_on', 'type_off'],
    value_vars=['startTime_s', 'endTime_s'],
    var_name='event_type',
    value_name='event_time_s'
    )

    reshaped_df['typeCode'] = reshaped_df['event_type'].map({'startTime_s': 'ON', 'endTime_s': 'OFF'})


    reshaped_df['type'] = reshaped_df.apply(
        lambda row: row['type_on'] if row['typeCode'] == 'ON' else row['type_off'], axis=1
    )


    reshaped_df = reshaped_df.drop(columns=['event_type', 'type_on', 'type_off'])


    reshaped_df = reshaped_df.sort_values(['playerId', 'event_time_s', 'typeCode']).reset_index(drop=True)
    reshaped_df['event'] = 'line-change'
    reshaped_df = reshaped_df.rename(columns={'event_time_s': 'time_s',
                                          'playerId':'playerId_1',
                                          'fullName':'playerName_1',
                                          'zoneStartSide':'zoneStartSide_1',
                                          'zoneStartSideDetail':'zoneStartSideDetail_1',
                                          'event_time_s' : 'elapsedTime',
                                          'teamAbbrev' : 'eventTeam',
                                          'type' : 'descKey',
                                          'id' : 'eventId'

                                          })


    for col in pbp_df.columns.tolist():
        if col not in reshaped_df.columns.tolist():
            reshaped_df[col] = np.nan

    pbp_df = pd.concat([pbp_df, reshaped_df], ignore_index=True)

    pbp_df['eventTeamType'] = np.where(pbp_df['eventTeam'] == pbp_df['homeTeam'], 'home', 'away')
    pbp_df['eventTeamId'] = np.where(pbp_df['eventTeam'] == pbp_df['homeTeam'], pbp_df['homeTeamId'], pbp_df['awayTeamId'])

    event_priority = {
    'goal': 1,
    'penalty': 2,
    'delayed-penalty': 3,
    'shot-on-goal': 4,
    'missed-shot': 5,
    'failed-shot-attempt': 5,
    'blocked-shot': 7,
    'hit': 8,
    'takeaway': 9,
    'giveaway': 10,
    'stoppage': 11,
    'line-change': 12,
    'period-start': 13,
    'period-end': 14,
    'game-end': 15,
    'faceoff': 16
    }

    pbp_df['priority'] = pbp_df['event'].map(event_priority)
    pbp_df= pbp_df.sort_values(by=['elapsedTime', 'priority']).reset_index(drop=True)

    pbp_df['season'] = response['season']
    pbp_df['gameType'] = response['gameType']
    pbp_df['limitedScoring'] = response['limitedScoring']
    pbp_df['gameDate'] = pd.to_datetime(response['gameDate']).strftime('%Y-%m-%d')
    pbp_df['venue'] = response['venue']['default']
    pbp_df['venueLocation'] = response['venueLocation']['default']
    pbp_df['startTimeUTC'] = pd.to_datetime(response['startTimeUTC']).strftime('%Y-%m-%d %H:%M:%S')
    pbp_df['easternUTCOffset'] = response['easternUTCOffset']
    pbp_df['venueUTCOffset'] = response['venueUTCOffset']
    # pbp_df['tvBroadcasts'] = response['tvBroadcasts']
    pbp_df['gameState'] = response['gameState']
    pbp_df['gameScheduleState'] = response['gameScheduleState']
    pbp_df['gameOutcome'] = response['gameOutcome']

    # Bbfill columns
    pbp_df[['homeTeam', 'awayTeam', 'homeTeamId', 'awayTeamId', 'timeRemaining_s',
            'awayScore', 'homeScore', 'awaySOG', 'homeSOG', 'sortOrder', 'periodNumber',
            'timeInPeriod', 'timeRemaining', 'homeTeamDefendingSide']] =  pbp_df[['homeTeam', 'awayTeam', 'homeTeamId', 'awayTeamId', 'timeRemaining_s',
                                                                                  'awayScore', 'homeScore', 'awaySOG', 'homeSOG', 'sortOrder', 'periodNumber',
                                                                                  'timeInPeriod', 'timeRemaining', 'homeTeamDefendingSide']].bfill(axis ='rows')


    # Add meta data (datetime of the execution)
    current_time = pd.to_datetime("now")
    pbp_df['meta_datetime'] = current_time
    shifts_df['meta_datetime'] = current_time
    rosters_df['meta_datetime'] = current_time

    return pbp_df, rosters_df, shifts_df

## Rosters
def scrapeRosters(gameId):

    """
    Scrapes roster data from the NHL website for a given game ID.

    Parameters :
      - game_id (int) : The ID of the game you want to scrape the roster data for.

      Returns :
      - rosters_df (pd.DataFrame) : A DataFrame containing the scraped roster data.

    """



    url = f"https://api-web.nhle.com/v1/gamecenter/{gameId}/play-by-play"

    response = requests.get(url).json()

    rosters_df = pd.json_normalize(response['rosterSpots'])

    # Life's always better when you have team abbreviation in the dataframe instead of just the teamId
    abbrev_dict = {
        response['awayTeam']['id'] : response['awayTeam']['abbrev'],
        response['homeTeam']['id'] : response['homeTeam']['abbrev']
    }
    rosters_df['teamAbbrev'] = rosters_df['teamId'].map(abbrev_dict)
    rosters_df['is_home'] = (rosters_df['teamId'] == response['homeTeam']['id']).astype(int)

    rosters_df["gameId"] = gameId

    # Add meta data (datetime of the execution)
    rosters_df["meta_datetime"] = pd.to_datetime("now")


    return rosters_df

## Shifts
def scrapeShifts(gameId):

    """
    Scrapes shift data from the NHL website for a given game ID.

    Parameters :
      - game_id (int) : The ID of the game you want to scrape the shift data for.

    Returns :
      - shifts_df (pd.DataFrame) : A DataFrame containing the scraped shift data.

    """


    url = f"https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={gameId}"

    response = requests.get(url).json()

    shifts_df = pd.json_normalize(response["data"])

    # Add meta data (datetime of the execution)
    shifts_df["meta_datetime"] = pd.to_datetime("now")

    return shifts_df

def scrapeShiftsHTML(gameId):

    """
    Scrapes shift data from the NHL TOI Reports for a given game ID.

    Parameters :
      - game_id (int) : The ID of the game you want to scrape the shift data for.

    Returns :
      - big_df (pd.DataFrame) : A DataFrame containing the scraped shift data.

    """


    year1 = str(gameId)[:4]
    year2 = int(year1) + 1

    shortId = str(gameId)[4:]

    # if url has TH, it's home, if TV away


    url_template = f"https://www.nhl.com/scores/htmlreports/{year1}{year2}/T{{HV}}{shortId}.HTM"

    dfs_list = []

    for side in ['H', 'V']:
        url = url_template.format(HV=side)



        # Fetch the webpage content
        response = requests.get(url)
        response.raise_for_status()

        # Use BeautifulSoup with lxml parser
        soup = BeautifulSoup(response.content, 'lxml')

        # Convert BeautifulSoup object to lxml etree
        tree = etree.HTML(str(soup))

        # Example: Extract player names using XPath
        player_names = tree.xpath('.//td[@class="playerHeading + border"]/text()')

        # Example: Extract shift details using XPath
        shift_rows = tree.xpath('.//tr[@class="evenColor"] | .//tr[@class="oddColor"]')

        # Store extracted data
        shift_data = []
        for row in shift_rows:
            shift_number = row.xpath('./td[1]/text()')[0].strip() if row.xpath('./td[1]/text()') else ''
            period = row.xpath('./td[2]/text()')[0].strip() if row.xpath('./td[2]/text()') else ''
            start_time = row.xpath('./td[3]/text()')[0].strip() if row.xpath('./td[3]/text()') else ''
            end_time = row.xpath('./td[4]/text()')[0].strip() if row.xpath('./td[4]/text()') else ''
            duration = row.xpath('./td[5]/text()')[0].strip() if row.xpath('./td[5]/text()') else ''
            event = row.xpath('./td[6]/text()')[0].strip() if row.xpath('./td[6]/text()') else ''

            shift_data.append({
                'Shift Number': shift_number,
                'Period': period,
                'Start Time': start_time,
                'End Time': end_time,
                'Duration': duration,
                'Event': event
            })

        # Output extracted data
        shifts_df = pd.DataFrame(shift_data)

        shifts_df['is_home'] = 1 if side == 'H' else 0

        # Replace 'OT' with 4 ### TO FIX EVENTUALLY BECAUSE OF PLAYOFFS
        shifts_df['Period'] = shifts_df['Period'].replace('OT', 4)
        shifts_df['Period'] = pd.to_numeric(shifts_df['Period'], errors='coerce')

        shifts_df['Shift Number'] = pd.to_numeric(shifts_df['Shift Number'], errors='coerce')

        #Assign a row with 1 where 	Start Time has a / in it and filter out 0s
        shifts_df['dummy'] = np.where(shifts_df['Start Time'].str.contains('/'), 1, 0)
        shifts_df = shifts_df[shifts_df['dummy'] == 1]

        shifts_df = shifts_df.drop(columns=['dummy'])




        # Assign player names
        shifts_df["Player Name"] = None
        player_index = 0

        shifts_df['Shift Number'] = pd.to_numeric(shifts_df['Shift Number'], errors='coerce')
        shifts_df = shifts_df.reset_index(drop=True)
        # Iterate through shifts and assign player names
        for i in range(len(shifts_df)):
            if player_index < len(player_names):
                shifts_df.loc[i, "Player Name"] = player_names[player_index]

            # If shift number decreases, move to the next player
            if i > 0 and shifts_df.loc[i, "Shift Number"] < shifts_df.loc[i - 1, "Shift Number"]:
                player_index += 1  # Move to th



        # Split the "Player Name" column into "Player Number" and "Player Name"
        shifts_df[['Player Number', 'Player Name']] = shifts_df['Player Name'].str.split(' ', n=1, expand=True)

        # Convert "Player Number" to numeric for sorting or analysis
        shifts_df['Player Number'] = pd.to_numeric(shifts_df['Player Number'], errors='coerce')
        shifts_df['firstName'] = shifts_df['Player Name'].str.split(', ').str[1]

        shifts_df['lastName'] = shifts_df['Player Name'].str.split(', ').str[0]
        # Remove number from firstName
        shifts_df['lastName'] = shifts_df['lastName'].str.replace(r'\d+', '')
        shifts_df['lastName'] = shifts_df['lastName'].str.strip()
        
        

        shifts_df[['Start Time (Elapsed)', 'Start Time (Remaining)']] = shifts_df['Start Time'].str.split(' ', n=1, expand=True)
        shifts_df[['End Time (Elapsed)', 'End Time (Remaining)']] = shifts_df['End Time'].str.split(' ', n=1, expand=True)

        # Strip "/ " in remaining cols
        shifts_df['Start Time (Remaining)'] = shifts_df['Start Time (Remaining)'].str.replace('/ ', '')
        shifts_df['End Time (Remaining)'] = shifts_df['End Time (Remaining)'].str.replace('/ ', '')


        shifts_df = shifts_df.drop(columns=['Start Time', 'End Time'])

        for col in ['Start Time (Elapsed)', 'Start Time (Remaining)', 'End Time (Elapsed)', 'End Time (Remaining)', 'Duration']:
            col_with_seconds = col + ' (Seconds)'
            shifts_df[col_with_seconds] = shifts_df[col].apply(convert_str_to_seconds)

        dfs_list.append(shifts_df)
    
    big_df = pd.concat(dfs_list)
    big_df = big_df.reset_index(drop=True)


    big_df['startTime_s'] = big_df['Start Time (Elapsed) (Seconds)'] + (big_df['Period']-1)*60*20
    big_df['endTime_s'] = big_df['End Time (Elapsed) (Seconds)'] + (big_df['Period']-1)*60*20



    big_df['sweaterNumber'] = big_df['Player Number']
    big_df['gameId'] = gameId
    

    return big_df

# Standings
def scrapeStandings(date):
    """
    Scrapes standings data from the NHL website for a given date.

    Parameters :
      - date (str) : The date you want to scrape the standings data for in the format of "YYYY-MM-DD".

    Returns :
      - standings_df (pd.DataFrame) : A DataFrame containing the scraped standings data.

    """



    url = f"https://api-web.nhle.com/v1/standings/{date}"

    response = requests.get(url).json()

    standings_df = pd.json_normalize(response["standings"])

    # Add meta data (datetime of the execution)
    standings_df["meta_datetime"] = pd.to_datetime("now")

    return standings_df

# Player
def scrapePlayer(playerId, key=None):

    """
    Scrapes player data from the NHL website for a given player ID.

    Parameters :
      - playerId (int) : The ID of the player you want to scrape the data for.
      - key (str) : The key to use to extract the data from the response.

    Returns :
      - response (dict) : A dictionary containing the scraped player data.

      Data in dict :
        - playerId
        - isActive
        - currentTeamId
        - currentTeamAbbrev
        - fullTeamName
        - teamCommonName
        - teamPlaceNameWithPreposition
        - firstName
        - lastName
        - teamLogo
        - sweaterNumber
        - position
        - headshot
        - heroImage
        - heightInInches
        - heightInCentimeters
        - weightInPounds
        - weightInKilograms
        - birthDate
        - birthCity
        - birthStateProvince
        - birthCountry
        - shootsCatches
        - draftDetails
        - playerSlug
        - inTop100AllTime
        - inHHOF
        - featuredStats
        - careerTotals
        - shopLink
        - twitterLink
        - watchLink
        - last5Games
        - seasonTotals
        - currentTeamRoster

    """


    url = f"https://api-web.nhle.com/v1/player/{playerId}/landing"

    response = requests.get(url).json()

    if key in response.keys():
        response = response[key]
    else:
        response = response

    return response

## Records

# Teams
def scrapeTeams1():

    """
    Scrapes team data from the NHL website (NHL Records) and returns a DataFrame.

    Returns:
    - team_df (pd.DataFrame): A DataFrame containing the scraped team data.
    """

    url = "https://records.nhl.com/site/api/franchise?include=teams.id&include=teams.active&include=teams.triCode&include=teams.placeName&include=teams.commonName&include=teams.fullName&include=teams.logos&include=teams.conference.name&include=teams.division.name&include=teams.franchiseTeam.firstSeason.id&include=teams.franchiseTeam.lastSeason.id"

    response = requests.get(url).json()

    team_df = pd.json_normalize(response['data'])

    # Add meta data (datetime of the execution)
    team_df["meta_datetime"] = pd.to_datetime("now")

    return team_df


