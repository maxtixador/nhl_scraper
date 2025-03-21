import asyncio
import json
import re
from datetime import datetime

import aiohttp
import nest_asyncio
import polars as pl
from playwright.async_api import async_playwright
from selectolax.lexbor import LexborHTMLParser
from tqdm import tqdm

# Apply nest_asyncio to avoid runtime errors in environments like Colab
nest_asyncio.apply()

### MAKE SURE TO MAKE THE PERSON RUN PLAYWRIGHT INSTALL IN THE TERMINAL
### playwright install

init_message = """
Welcome to the NHL Scraper!

My name is Max and I'm the developer of this project and hockey fan. I hope you enjoy using this tool!

This is a work in progress and not all functionalities are available yet.

*** IMPORTANT ***

Make sure to run the following command in the terminal (it might take a while, but it will ensure that the playwright browser is installed):

$ playwright install
or
$ playwright install chromium

***

If you have any questions, please contact me on X: @woumaxx[https://x.com/woumaxx]

Thank you for using the NHL Scraper!


"""

print(init_message)


### NOT MENDATORY : INCLUDE LAZYFRAME IN THE COLLECT METHOD TO INCREASE SPEED ###

### PRETTY SURE THIS IS NOT USED ###
SESSION_MAP = {
    "preseason": 1,
    "regular": 2,
    "playoffs": 3,
}

_SESSION_MAP = {
    1: "preseason",
    2: "regular",
    3: "playoffs",
}

CATEGORY_MAP = {
    1: "north-american-skater",
    2: "international-skater",
    3: "north-american-goalie",
    4: "international-goalie",
}

_CATEGORY_MAP = {
    1: "north-american-skater",
    2: "international-skater",
    3: "north-american-goalie",
    4: "international-goalie",
}


class NHLBaseScraper:
    """Base class for NHL data scraping with shared async functionality."""

    async def fetch_json(self, url):
        """Fetch JSON data from a URL asynchronously."""
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    raise Exception(f"Failed to fetch {url}: Status {response.status}")

    async def fetch_html(self, url):
        """Fetch HTML content from a URL asynchronously using Playwright."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(url)
            html = await page.content()
            await browser.close()
            return html


class NHLTeamScraper(NHLBaseScraper):
    """Scraper for NHL team data."""

    def __init__(self):
        self.base_url = "https://api.nhle.com/stats/rest/en/franchise?sort=fullName&include=lastSeason.id&include=firstSeason.id"
        self.calendar_url = "https://api-web.nhle.com/v1/schedule-calendar/now"
        self.records_url = "https://records.nhl.com/site/api/franchise?include=teams.id&include=teams.active&include=teams.triCode&include=teams.placeName&include=teams.commonName&include=teams.fullName&include=teams.logos&include=teams.conference.name&include=teams.division.name&include=teams.franchiseTeam.firstSeason.id&include=teams.franchiseTeam.lastSeason.id"

    async def scrape_teams(self, source=None):
        """Fetch basic team information."""

        source = source if source else "default"

        if source == "active":
            try:
                data = await self.fetch_json(self.calendar_url)
                teams = data.get("teams", [])
                return (
                    pl.LazyFrame(teams)
                    .with_columns(
                        [
                            pl.col("commonName").struct.field("default").alias("commonName"),
                            pl.col("name").struct.field("default").alias("name"),
                            pl.col("placeNameWithPreposition")
                            .struct.field("default")
                            .alias("placeNameWithPreposition"),
                        ]
                    )
                    .collect()
                )
            except Exception as e:
                print(f"Error scraping teams: {str(e)}")
                return pl.DataFrame()

        elif source == "records":
            try:
                data = await self.fetch_json(self.records_url)
                teams = data.get("data", [])
                return pl.LazyFrame(teams).drop(["teams"]).collect()
            except Exception as e:
                print(f"Error scraping teams: {str(e)}")
                return pl.DataFrame()

        else:
            try:
                data = await self.fetch_json(self.base_url)
                teams = data.get("data", [])
                return (
                    pl.LazyFrame(teams)
                    .with_columns(
                        [
                            pl.col("id").cast(pl.Int32).alias("teamId"),
                            pl.col("firstSeason").struct.field("id").alias("firstSeasonId"),
                            pl.col("lastSeason").struct.field("id").alias("lastSeasonId"),
                        ]
                    )
                    .drop(["firstSeason", "lastSeason", "id"])
                    .collect()
                )
            except Exception as e:
                print(f"Error scraping teams: {str(e)}")
                return pl.DataFrame()


class NHLScheduleScraper(NHLBaseScraper):
    """Scraper for NHL game schedules."""

    def __init__(self, teams=None, seasons=None):
        self.teams = teams if isinstance(teams, (list, tuple)) else [teams]
        self.seasons = seasons if isinstance(seasons, (list, tuple)) else [seasons]
        self.base_url = f"https://api-web.nhle.com/v1/club-schedule-season/"

    async def fetch_schedule(self, team=None, season=None):
        """Fetch game schedule for a given season or date range."""
        url = f"https://api-web.nhle.com/v1/club-schedule-season/{team}/{season}"
        try:
            data = await self.fetch_json(url)
            if data:
                games = []
                for game in data.get("games", []):
                    game["team_key"] = team
                    game["season_key"] = season
                    games.append(game)
                return games
            else:
                print(f"Warning: No schedule data found for {team} in {season}.")
                return None
        except Exception as e:
            print(f"Error fetching schedule: {str(e)}")
            return None

    async def scrape_schedule(self, progress=True):
        """Scrape schedule data for multiple teams and seasons concurrently."""
        tasks = [
            self.fetch_schedule(team=team, season=season)
            for team in self.teams
            for season in self.seasons
        ]
        results = []
        if progress:
            with tqdm(total=len(tasks), desc="Scraping NHL Schedule") as pbar:
                for task in asyncio.as_completed(tasks):
                    result = await task
                    results.append(result)
                    pbar.update(1)
        else:
            results = await asyncio.gather(*tasks, return_exceptions=True)

        schedule_data = [game for result in results if result is not None for game in result]
        if not schedule_data:
            print("No schedule data found.")
            return pl.DataFrame()

        df = (
            pl.LazyFrame(schedule_data)
            .with_columns(
                [
                    pl.col("gameDate").str.to_date("%Y-%m-%d").alias("gameDate"),
                    pl.col("venue").struct.field("default").alias("venue"),
                    pl.col("awayTeam").struct.field("id").alias("awayTeamId"),
                    pl.col("homeTeam").struct.field("id").alias("homeTeamId"),
                    pl.col("awayTeam").struct.field("score").alias("awayTeamScore"),
                    pl.col("homeTeam").struct.field("score").alias("homeTeamScore"),
                    pl.col("gameOutcome").struct.field("lastPeriodType").alias("gameOutcome"),
                    pl.col("awayTeam")
                    .struct.field("commonName")
                    .struct.field("default")
                    .alias("awayTeamCommonName"),
                    pl.col("homeTeam")
                    .struct.field("commonName")
                    .struct.field("default")
                    .alias("homeTeamCommonName"),
                    pl.col("awayTeam")
                    .struct.field("placeName")
                    .struct.field("default")
                    .alias("placeNameHome"),
                    pl.col("homeTeam")
                    .struct.field("placeName")
                    .struct.field("default")
                    .alias("placeNameAway"),
                    pl.col("awayTeam").struct.field("abbrev").alias("awayTeamAbbrev"),
                    pl.col("homeTeam").struct.field("abbrev").alias("homeTeamAbbrev"),
                    pl.col("awayTeam").struct.field("logo").alias("awayTeamLogo"),
                    pl.col("homeTeam").struct.field("logo").alias("homeTeamLogo"),
                    pl.col("awayTeam").struct.field("darkLogo").alias("awayTeamDarkLogo"),
                    pl.col("homeTeam").struct.field("darkLogo").alias("homeTeamDarkLogo"),
                    pl.col("winningGoalie").struct.field("playerId").alias("winningGoalieId"),
                    pl.col("winningGoalie")
                    .struct.field("firstInitial")
                    .struct.field("default")
                    .alias("winningGoalieFirstInitial"),
                    pl.col("winningGoalie")
                    .struct.field("lastName")
                    .struct.field("default")
                    .alias("winningGoalieLastName"),
                    pl.col("winningGoalScorer")
                    .struct.field("playerId")
                    .alias("winningGoalScorerId"),
                    pl.col("winningGoalScorer")
                    .struct.field("firstInitial")
                    .struct.field("default")
                    .alias("winningGoalScorerFirstInitial"),
                    pl.col("winningGoalScorer")
                    .struct.field("lastName")
                    .struct.field("default")
                    .alias("winningGoalScorerLastName"),
                    pl.col("gameType").cast(pl.Utf8).replace(_SESSION_MAP).alias("_gameType"),
                    pl.lit(datetime.now()).alias("meta_datetime"),
                ]
            )
            .drop(
                [
                    "tvBroadcasts",
                    "awayTeam",
                    "homeTeam",
                    "periodDescriptor",
                    "winningGoalie",
                    "winningGoalScorer",
                    "threeMinRecapFr",
                    "condensedGameFr",
                    "gameCenterLink",
                    "threeMinRecap",
                    "condensedGame",
                    "ticketsLink",
                    "ticketsLinkFr",
                ]
            )
        )
        return df.collect()

    ### THIS CODE IS NOT WORKING, BUT I'M KEEPING IT HERE FOR REFERENCE ###
    # async def scrape_schedule(self, date_range=None):
    #     """Fetch game schedule for a given season or date range."""
    #     try:
    #         if date_range:
    #             start_date, end_date = date_range
    #             url = f"https://api-web.nhle.com/v1/schedule/{self.season}?startDate={start_date}&endDate={end_date}"
    #         else:
    #             url = self.base_url

    #         data = await self.fetch_json(url)
    #         games = []
    #         for week in data.get('gameWeek', []):
    #             for game in week.get('games', []):
    #                 games.append({
    #                     "gameId": game["id"],
    #                     "date": week["date"],
    #                     "homeTeamId": game["homeTeam"]["id"],
    #                     "awayTeamId": game["awayTeam"]["id"],
    #                     "gameType": game["gameType"],
    #                     "gameState": game["gameState"],
    #                     "venue": game["venue"]["default"],
    #                     "startTimeUTC": game["startTimeUTC"]
    #                 })

    #         return pl.DataFrame(games).with_columns([
    #             pl.col("gameId").cast(pl.Int64),
    #             pl.col("homeTeamId").cast(pl.Int32),
    #             pl.col("awayTeamId").cast(pl.Int32),
    #             pl.col("date").str.to_date("%Y-%m-%d"),
    #             pl.col("startTimeUTC").str.to_datetime()
    #         ])
    #     except Exception as e:
    #         print(f"Error scraping schedule: {str(e)}")
    #         return pl.DataFrame()
    ### END OF NOT WORKING CODE ###

    async def scrape_team_schedule(self, team=None, season=None):
        """Fetch game schedule for a given team and season."""
        try:
            team = team if team else self.team
            season = season if season else self.season

            url = f"https://api-web.nhle.com/v1/club-schedule-season/{team}/{season}"
            data = await self.fetch_json(url)
            games = []
            for game in data.get("games", []):
                game["team_key"] = team
                game["season_key"] = season
                games.append(game)

            return (
                pl.DataFrame(games)
                .with_columns(
                    pl.col("gameDate").str.to_date("%Y-%m-%d").alias("gameDate"),
                    pl.col("venue").struct.field("default").alias("venue"),
                    pl.col("awayTeam").struct.field("id").alias("awayTeamId"),
                    pl.col("homeTeam").struct.field("id").alias("homeTeamId"),
                    pl.col("awayTeam").struct.field("score").alias("awayTeamScore"),
                    pl.col("homeTeam").struct.field("score").alias("homeTeamScore"),
                    pl.col("gameOutcome").struct.field("lastPeriodType").alias("gameOutcome"),
                    pl.col("awayTeam")
                    .struct.field("commonName")
                    .struct.field("default")
                    .alias("awayTeamCommonName"),
                    pl.col("homeTeam")
                    .struct.field("commonName")
                    .struct.field("default")
                    .alias("homeTeamCommonName"),
                    pl.col("awayTeam")
                    .struct.field("placeName")
                    .struct.field("default")
                    .alias("placeNameHome"),
                    pl.col("homeTeam")
                    .struct.field("placeName")
                    .struct.field("default")
                    .alias("placeNameAway"),
                    pl.col("awayTeam").struct.field("abbrev").alias("awayTeamAbbrev"),
                    pl.col("homeTeam").struct.field("abbrev").alias("homeTeamAbbrev"),
                    pl.col("awayTeam").struct.field("logo").alias("awayTeamLogo"),
                    pl.col("homeTeam").struct.field("logo").alias("homeTeamLogo"),
                    pl.col("awayTeam").struct.field("darkLogo").alias("awayTeamDarkLogo"),
                    pl.col("homeTeam").struct.field("darkLogo").alias("homeTeamDarkLogo"),
                    pl.col("winningGoalie").struct.field("playerId").alias("winningGoalieId"),
                    pl.col("winningGoalie")
                    .struct.field("firstInitial")
                    .struct.field("default")
                    .alias("winningGoalieFirstInitial"),
                    pl.col("winningGoalie")
                    .struct.field("lastName")
                    .struct.field("default")
                    .alias("winningGoalieLastName"),
                    pl.col("winningGoalScorer")
                    .struct.field("playerId")
                    .alias("winningGoalScorerId"),
                    pl.col("winningGoalScorer")
                    .struct.field("firstInitial")
                    .struct.field("default")
                    .alias("winningGoalScorerFirstInitial"),
                    pl.col("winningGoalScorer")
                    .struct.field("lastName")
                    .struct.field("default")
                    .alias("winningGoalScorerLastName"),
                    pl.lit(datetime.now()).alias("meta_datetime"),
                )
                .drop(
                    [
                        "tvBroadcasts",
                        "awayTeam",
                        "homeTeam",
                        "periodDescriptor",
                        "winningGoalie",
                        "winningGoalScorer",
                        "threeMinRecapFr",
                        "condensedGameFr",
                        "gameCenterLink",
                        "threeMinRecap",
                        "condensedGame",
                        "ticketsLink",
                        "ticketsLinkFr",
                    ]
                )
            )
        except Exception as e:
            print(f"Error scraping team schedule: {str(e)}")
            return pl.DataFrame()


class NHLStandingsScraper(NHLBaseScraper):
    """Scraper for NHL standings data."""

    def __init__(self, date=None):
        self.date = date if date else datetime.now().strftime("%Y-%m-%d")
        self.base_url = f"https://api-web.nhle.com/v1/standings/"

    async def scrape_standings(self, date=None):
        """Fetch standings for a given season or specific date."""
        try:
            url = f"{self.base_url}{date}" if date else f"{self.base_url}{self.date}"
            data = await self.fetch_json(url)
            standings = data.get("standings", [])
            return pl.DataFrame(standings).with_columns(
                [
                    # pl.col("teamId").cast(pl.Int32),
                    pl.col("teamName").struct.field("default").alias("teamName"),
                    pl.col("teamAbbrev").struct.field("default").alias("abbreviation"),
                    pl.col("gamesPlayed").cast(pl.Int32),
                    pl.col("wins").cast(pl.Int32),
                    pl.col("losses").cast(pl.Int32),
                    pl.col("otLosses").cast(pl.Int32).alias("otLosses"),
                    pl.col("points").cast(pl.Int32),
                    pl.col("goalFor").cast(pl.Int32),
                    pl.col("goalAgainst").cast(pl.Int32),
                    pl.col("divisionName").alias("division"),
                    pl.col("conferenceName").alias("conference"),
                ]
            )
        except Exception as e:
            print(f"Error scraping standings: {str(e)}")
            return pl.DataFrame()


class NHLTeamRosterScraper(NHLBaseScraper):
    """Scraper for NHL team roster data."""

    def __init__(self, teams, seasons):
        self.teams = teams if isinstance(teams, (list, tuple)) else [teams]
        self.seasons = seasons if isinstance(seasons, (list, tuple)) else [seasons]

    async def fetch_team_roster(self, team, season):
        """Fetch roster data for a single team and season."""
        url = f"https://api-web.nhle.com/v1/roster/{team}/{season}"
        try:
            data = await self.fetch_json(url)
            if data:
                for key in data.keys():
                    for player in data[key]:
                        player["team"] = team
                        player["season"] = season
                return [player for key in data.keys() for player in data[key]]
            else:
                print(f"Warning: No roster data found for {team} in {season}.")
                return None
        except Exception as e:
            print(f"Error fetching roster for {team}, season {season}: {str(e)}")
            return None

    async def scrape_team_rosters(self, progress=True):
        """Scrape roster data for multiple teams and seasons concurrently."""
        tasks = [
            self.fetch_team_roster(team, season) for team in self.teams for season in self.seasons
        ]
        results = []

        if progress:
            with tqdm(total=len(tasks), desc="Scraping NHL Team Rosters") as pbar:
                for task in asyncio.as_completed(tasks):
                    result = await task
                    results.append(result)
                    pbar.update(1)
        else:
            results = await asyncio.gather(*tasks, return_exceptions=True)

        roster_data = [player for result in results if result is not None for player in result]
        if not roster_data:
            print("No team roster data found.")
            return pl.DataFrame()

        return pl.DataFrame(roster_data).with_columns(
            [
                pl.concat_str(
                    pl.col("firstName").struct.field("default"),
                    pl.col("lastName").struct.field("default"),
                    separator=" ",
                ).alias("fullName"),
                pl.col("firstName").struct.field("default").alias("firstName"),
                pl.col("lastName").struct.field("default").alias("lastName"),
                pl.col("birthCity").struct.field("default").alias("birthCity"),
                pl.col("birthStateProvince").struct.field("default").alias("birthStateProvince"),
                pl.when(pl.col("positionCode").is_in(["G", "D"]))
                .then(pl.col("positionCode"))
                .otherwise(pl.lit("F"))
                .alias("position"),
                pl.lit(datetime.now()).alias("meta_datetime"),
            ]
        )


class NHLTeamStatsScraper(NHLBaseScraper):
    """Scraper for NHL team statistics data."""

    def __init__(self, teams, seasons, sessions, goalies=False):
        self.teams = teams if isinstance(teams, (list, tuple)) else [teams]
        self.seasons = seasons if isinstance(seasons, (list, tuple)) else [seasons]
        self.sessions = [
            SESSION_MAP[session] if isinstance(session, str) and session in SESSION_MAP else session
            for session in (sessions if isinstance(sessions, (list, tuple)) else [sessions])
        ]
        self.goalies = goalies

    async def fetch_team_stats(self, team, season, session):
        """Fetch stats data for a single team, season, and session."""
        url = f"https://api-web.nhle.com/v1/club-stats/{team}/{season}/{session}"
        try:
            data = await self.fetch_json(url)
            key = "goalies" if self.goalies else "skaters"
            if key in data:
                for player in data[key]:
                    player["team"] = team
                    player["season"] = season
                    player["session"] = session
                return data[key]
            else:
                print(f"Warning: No {key} data found for {team} in {season} (Session {session}).")
                return None
        except Exception as e:
            print(f"Error fetching stats for {team}, season {season}, session {session}: {str(e)}")
            return None

    async def scrape_team_stats(self, progress=True):
        """Scrape stats data for multiple teams, seasons, and sessions concurrently."""
        tasks = [
            self.fetch_team_stats(team, season, session)
            for team in self.teams
            for season in self.seasons
            for session in self.sessions
        ]
        results = []

        if progress:
            with tqdm(
                total=len(tasks),
                desc=f"Scraping NHL {'Goalie' if self.goalies else 'Skater'} Stats",
            ) as pbar:
                for task in asyncio.as_completed(tasks):
                    result = await task
                    results.append(result)
                    pbar.update(1)
        else:
            results = await asyncio.gather(*tasks, return_exceptions=True)

        stats_data = [player for result in results if result is not None for player in result]
        if not stats_data:
            print("No team stats data found.")
            return pl.DataFrame()

        return (
            pl.LazyFrame(stats_data)
            .with_columns(
                [
                    pl.concat_str(
                        pl.col("firstName").struct.field("default"),
                        pl.col("lastName").struct.field("default"),
                        separator=" ",
                    ).alias("fullName"),
                    pl.col("firstName").struct.field("default").alias("firstName"),
                    pl.col("lastName").struct.field("default").alias("lastName"),
                    pl.when(pl.col("positionCode").is_in(["G", "D"]))
                    .then(pl.col("positionCode"))
                    .otherwise(pl.lit("F"))
                    .alias("position"),
                    pl.lit(datetime.now()).alias("meta_datetime"),
                ]
            )
            .collect()
        )


class NHLDraftScraper(NHLBaseScraper):
    """Scraper for NHL draft data."""

    def __init__(self, years, round_="all"):
        self.years = years if isinstance(years, (list, tuple)) else [years]
        self.round_ = round_

    async def fetch_draft_data(self, year):
        """Fetch draft data for a single year and round."""
        url = f"https://api-web.nhle.com/v1/draft/picks/{year}/{self.round_}"
        try:
            data = await self.fetch_json(url)
            if "picks" in data:
                for item in data["picks"]:
                    item["draftYear"] = year
                return data["picks"]
            else:
                print(f"Warning: No draft data found for {year}, round {self.round_}.")
                return None
        except Exception as e:
            print(f"Error fetching draft data for {year}, round {self.round_}: {str(e)}")
            return None

    async def scrape_draft(self, progress=True):
        """Scrape draft data for multiple years concurrently."""
        tasks = [self.fetch_draft_data(year) for year in self.years]
        results = []

        if progress:
            with tqdm(total=len(self.years), desc="Scraping NHL Draft Data") as pbar:
                for task in asyncio.as_completed(tasks):
                    result = await task
                    results.append(result)
                    pbar.update(1)
        else:
            results = await asyncio.gather(*tasks, return_exceptions=True)

        draft_data = [item for result in results if result is not None for item in result]
        if not draft_data:
            print("No draft data found.")
            return pl.DataFrame()

        return pl.DataFrame(draft_data).with_columns(
            [
                pl.concat_str(
                    pl.col("firstName").struct.field("default"),
                    pl.col("lastName").struct.field("default"),
                    separator=" ",
                ).alias("fullName"),
                pl.col("firstName").struct.field("default").alias("firstName"),
                pl.col("lastName").struct.field("default").alias("lastName"),
                pl.col("teamName").struct.field("default").alias("teamName"),
                pl.col("teamCommonName").struct.field("default").alias("teamCommonName"),
                pl.col("teamPlaceNameWithPreposition")
                .struct.field("default")
                .alias("teamPlaceNameWithPreposition"),
                pl.col("displayAbbrev").struct.field("default").alias("displayAbbrev"),
                pl.lit(datetime.now()).alias("meta_datetime"),
            ]
        )


class NHLDraftRankingsScraper(NHLBaseScraper):
    """Scraper for NHL draft rankings data."""

    def __init__(self, years, categories):
        self.years = years if isinstance(years, (list, tuple)) else [years]
        self.categories = categories if isinstance(categories, (list, tuple)) else [categories]

    async def fetch_rankings(self, year, category):
        """Fetch rankings data for a single year and category."""
        url = f"https://api-web.nhle.com/v1/draft/rankings/{year}/{category}"
        try:
            data = await self.fetch_json(url)
            if "rankings" in data:
                for ranking in data["rankings"]:
                    ranking["category"] = CATEGORY_MAP[category]
                    ranking["year"] = year
                return data["rankings"]
            else:
                print(f"Warning: No rankings found for {year}, category {category}.")
                return None
        except Exception as e:
            print(f"Error fetching draft rankings for {year}, category {category}: {str(e)}")
            return None

    async def scrape_rankings(self, progress=True):
        """Scrape rankings data for multiple years and categories concurrently."""
        tasks = [
            self.fetch_rankings(year, category)
            for year in self.years
            for category in self.categories
        ]
        results = []

        if progress:
            with tqdm(total=len(tasks), desc="Scraping NHL Draft Rankings") as pbar:
                for task in asyncio.as_completed(tasks):
                    result = await task
                    results.append(result)
                    pbar.update(1)
        else:
            results = await asyncio.gather(*tasks, return_exceptions=True)

        rankings_data = [item for result in results if result is not None for item in result]
        if not rankings_data:
            print("No draft rankings data found.")
            return pl.DataFrame()

        return pl.DataFrame(rankings_data).with_columns(
            [
                pl.concat_str(pl.col("firstName"), pl.col("lastName"), separator=" ").alias(
                    "fullName"
                ),
                pl.lit(datetime.now()).alias("meta_datetime"),
            ]
        )


class NHLTOIScraper(NHLBaseScraper):
    """Scraper for NHL Time on Ice (TOI) data."""

    def __init__(self, game_ids):
        self.game_ids = (
            [str(game_id) for game_id in game_ids]
            if isinstance(game_ids, (list, tuple))
            else [str(game_ids)]
        )

    async def scrape_toi_single_game(self, game_id):
        """Scrape TOI data for a single game."""
        game_id = str(game_id)
        short_id = game_id[-6:].zfill(6)
        first_year = game_id[:4]
        second_year = str(int(first_year) + 1)

        final_dict = {
            "GameId": game_id,
            "FirstYear": first_year,
            "SecondYear": second_year,
            "ShortId": short_id,
            "season": f"{first_year}-{second_year}",
            "Away": "",
            "Home": "",
            "homeTOI": None,
            "awayTOI": None,
            "homeSummary": None,
            "awaySummary": None,
        }

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            tasks = []
            for team_l in ["H", "V"]:
                url = f"https://www.nhl.com/scores/htmlreports/{first_year}{second_year}/T{team_l}{short_id}.HTM"
                tasks.append(self._fetch_and_parse_team_toi(url, team_l, final_dict, browser))

            await asyncio.gather(*tasks)
            await browser.close()

        final_dict["TOI_combined"] = pl.concat(
            [df for df in [final_dict["awayTOI"], final_dict["homeTOI"]] if df is not None],
            how="diagonal",
        ).with_columns(pl.lit(final_dict["GameId"]).alias("gameId"))
        final_dict["Summary_combined"] = pl.concat(
            [df for df in [final_dict["awaySummary"], final_dict["homeSummary"]] if df is not None],
            how="diagonal",
        ).with_columns(pl.lit(final_dict["GameId"]).alias("gameId"))
        return final_dict

    async def _fetch_and_parse_team_toi(self, url, team_l, final_dict, browser):
        """Helper method to fetch and parse TOI data for a team."""
        try:
            page = await browser.new_page()
            await page.goto(url)
            html = await page.content()
            parser = LexborHTMLParser(html)

            n_trs = len(
                parser.css(
                    "body > div.pageBreakAfter > table > tbody > tr:nth-child(4) > td > table > tbody > tr"
                )
            )
            players = [
                parser.css(
                    f"body > div.pageBreakAfter > table > tbody > tr:nth-child(4) > td > table > tbody > tr:nth-child({i+1}) > td.playerHeading"
                )[0].text()
                for i in range(n_trs)
                if parser.css(
                    f"body > div.pageBreakAfter > table > tbody > tr:nth-child(4) > td > table > tbody > tr:nth-child({i+1}) > td.playerHeading"
                )
            ]
            team = parser.css(
                "body > div.pageBreakAfter > table > tbody > tr:nth-child(3) > td > table > tbody > tr > td"
            )[0].text()

            if team_l == "H":
                final_dict["Home"] = team
            else:
                final_dict["Away"] = team

            rows = parser.css("tr.oddColor, tr.evenColor")
            data = [[td.text(strip=True) for td in row.css("td")] for row in rows if row.css("td")]

            data_test = []
            current_list = []
            for row in data:
                if row[0] == "TOT":
                    if current_list:
                        data_test.append(current_list)
                    current_list = []
                else:
                    current_list.append(row)
            if current_list:
                data_test.append(current_list)

            player_dict = {player: player_data for player, player_data in zip(players, data_test)}
            columns_1 = [
                "Shift #",
                "Per",
                "Start of Shift - Elapsed / Game",
                "End of Shift - Elapsed / Game",
                "Duration",
                "Event",
            ]
            columns_2 = ["Per", "SHF", "AVG", "TOI", "EV TOT", "PP TOT", "SH TOT"]

            toi_df1, toi_df2 = [], []
            for key, shifts in player_dict.items():
                df_1 = pl.DataFrame(
                    [i for i in shifts if len(i) == 6], schema=columns_1, orient="row"
                ).with_columns(pl.lit(key).alias("Player"))
                df_2 = pl.DataFrame(
                    [i for i in shifts if len(i) == 7], schema=columns_2, orient="row"
                ).with_columns(pl.lit(key).alias("Player"))
                if not df_1.is_empty():
                    toi_df1.append(df_1)
                if not df_2.is_empty():
                    toi_df2.append(df_2)

            toi_df1 = pl.concat(toi_df1, how="diagonal") if toi_df1 else None
            if toi_df1 is not None:
                toi_df1 = (
                    toi_df1.with_columns(
                        [
                            pl.col("Player")
                            .str.split(" ")
                            .list.get(0)
                            .cast(pl.Int32, strict=False)
                            .alias("#"),
                            pl.col("Shift #").cast(pl.Int32, strict=False),
                            pl.col("Per").cast(pl.Int32, strict=False),
                            pl.col("Start of Shift - Elapsed / Game")
                            .str.split(" / ")
                            .alias("Start Elapsed"),
                            pl.col("End of Shift - Elapsed / Game")
                            .str.split(" / ")
                            .alias("End Elapsed"),
                        ]
                    )
                    .with_columns(
                        [
                            pl.col("Start Elapsed")
                            .list.get(0)
                            .alias("Start Elapsed Time - Legacy"),
                            pl.col("Start Elapsed").list.get(1).alias("Start Game Time - Legacy"),
                            pl.col("End Elapsed").list.get(0).alias("End Elapsed Time - Legacy"),
                            pl.col("End Elapsed").list.get(1).alias("End Game Time - Legacy"),
                        ]
                    )
                    .drop(
                        [
                            "Start Elapsed",
                            "End Elapsed",
                            "Start of Shift - Elapsed / Game",
                            "End of Shift - Elapsed / Game",
                        ]
                    )
                    .with_columns(
                        (
                            pl.col("Duration")
                            .str.split(":")
                            .list.get(0)
                            .cast(pl.Int32, strict=False)
                            * 60
                            + pl.col("Duration")
                            .str.split(":")
                            .list.get(1)
                            .cast(pl.Int32, strict=False)
                        ).alias("Duration (Seconds)")
                    )
                    .with_columns(pl.lit(team).alias("Team"), pl.lit(team_l).alias("H/V"))
                )

            toi_df2 = pl.concat(toi_df2, how="diagonal") if toi_df2 else None
            if toi_df2 is not None:
                toi_df2 = toi_df2.with_columns(
                    pl.col("Player")
                    .str.split(" ")
                    .list.get(0)
                    .cast(pl.Int32, strict=False)
                    .alias("#")
                ).with_columns(
                    [
                        pl.col("Per").cast(pl.Int32, strict=False),
                        pl.col("SHF").cast(pl.Int32, strict=False),
                    ]
                )
                for col in ["AVG", "TOI", "EV TOT", "PP TOT", "SH TOT"]:
                    toi_df2 = toi_df2.with_columns(
                        (
                            pl.col(col).str.split(":").list.get(0).cast(pl.Int32, strict=False) * 60
                            + pl.col(col).str.split(":").list.get(1).cast(pl.Int32, strict=False)
                        ).alias(f"{col} (Seconds)")
                    )
                toi_df2 = toi_df2.with_columns(
                    pl.lit(team).alias("Team"), pl.lit(team_l).alias("H/V")
                )

            if team_l == "H":
                final_dict["homeTOI"] = toi_df1
                final_dict["homeSummary"] = toi_df2
            else:
                final_dict["awayTOI"] = toi_df1
                final_dict["awaySummary"] = toi_df2

        except Exception as e:
            print(f"Error processing TOI for game {final_dict['GameId']} ({team_l}): {str(e)}")
        finally:
            await page.close()

    async def scrape_toi_multiple_games(self, progress=True):
        """Scrape TOI data for multiple games concurrently."""
        tasks = [self.scrape_toi_single_game(game_id) for game_id in self.game_ids]
        results = []

        if progress:
            with tqdm(total=len(self.game_ids), desc="Scraping NHL TOI Data") as pbar:
                for task in asyncio.as_completed(tasks):
                    result = await task
                    results.append(result)
                    pbar.update(1)
        else:
            results = await asyncio.gather(*tasks, return_exceptions=True)

        valid_results = [
            r
            for r in results
            if isinstance(r, dict)
            and r.get("TOI_combined") is not None
            and not r["TOI_combined"].is_empty()
        ]
        if valid_results:
            combined_toi = pl.concat([r["TOI_combined"] for r in valid_results], how="diagonal")
            combined_summary = pl.concat(
                [r["Summary_combined"] for r in valid_results], how="diagonal"
            )
            return {
                "TOI_combined": combined_toi,
                "Summary_combined": combined_summary,
                "game_details": {
                    r["GameId"]: {
                        k: v for k, v in r.items() if k not in ["TOI_combined", "Summary_combined"]
                    }
                    for r in valid_results
                },
            }
        else:
            print("No valid TOI data retrieved from any games.")
            return {
                "TOI_combined": pl.DataFrame(),
                "Summary_combined": pl.DataFrame(),
                "game_details": {},
            }


class NHLGameURLGenerator:
    """Generates URLs for NHL game data based on game ID."""

    def __init__(self, game_id):
        self.game_id = str(game_id)
        self.short_id = self.game_id[-6:].zfill(6)
        self.first_year = self.game_id[:4]
        self.second_year = str(int(self.first_year) + 1)

    def get_html_url(self):
        """Generate HTML report URL for play-by-play data."""
        return f"https://www.nhl.com/scores/htmlreports/{self.first_year}{self.second_year}/PL{self.short_id}.HTM"

    def get_api_url(self):
        """Generate API URL for play-by-play data."""
        return f"https://api-web.nhle.com/v1/gamecenter/{self.game_id}/play-by-play"


class NHLDataScraper(NHLBaseScraper):
    """Handles asynchronous scraping of NHL data from web pages."""

    async def scrape_html(self, url):
        """Scrape HTML content from a URL."""
        return await self.fetch_html(url)

    async def scrape_api(self, url):
        """Scrape JSON data from an API URL."""
        return await self.fetch_json(url)


class NHLPlayByPlayParser:
    """Parses play-by-play data from HTML and processes it into structured format."""

    def __init__(self):
        self.event_mapping = {
            "BLOCK": "blocked-shot",
            "DELPEN": "delayed-penalty",
            "FAC": "faceoff",
            "GIVE": "giveaway",
            "GOAL": "goal",
            "HIT": "hit",
            "MISS": "missed-shot",
            "PENL": "penalty",
            "SHOT": "shot-on-goal",
            "STOP": "stoppage",
            "TAKE": "takeaway",
            "GEND": "game-end",
            "PEND": "period-end",
            "PSTR": "period-start",
            "SOC": "shootout-completed",
        }
        self.event_columns = {
            "faceoff": ["details.winningPlayerId", "details.losingPlayerId"],
            "hit": ["details.hittingPlayerId", "details.hitteePlayerId"],
            "blocked-shot": ["details.shootingPlayerId", "details.blockingPlayerId"],
            "shot-on-goal": ["details.shootingPlayerId", None],
            "missed-shot": ["details.shootingPlayerId", None],
            "goal": [
                "details.scoringPlayerId",
                "details.assist1PlayerId",
                "details.assist2PlayerId",
            ],
            "giveaway": ["details.playerId", None],
            "takeaway": ["details.playerId", None],
            "penalty": [
                "details.committedByPlayerId",
                "details.drawnByPlayerId",
                "details.servedByPlayerId",
            ],
            "failed-shot-attempt": ["details.shootingPlayerId", None],
        }

    def parse_html(self, html):
        """Parse HTML play-by-play data into a DataFrame and on-ice player lists."""
        parser = LexborHTMLParser(html)
        table = parser.css("tr.oddColor, tr.evenColor")
        data, home_on_ice, away_on_ice, home_goalie, away_goalie = [], [], [], [], []

        for row in table:
            cells = [td.text(strip=True) for td in row.css("td")]
            on_ice = [
                el.text() for el in row.css("td > table > tbody") if len(el.text(strip=True)) > 5
            ]

            on_list_cleaned, goalie_cleaned = [], []
            for team in on_ice:
                players = ["".join(item.split()).strip() for item in team.split("\xa0")]
                goalies = [re.sub(r"[A-Za-z]", "", item) for item in players if "G" in item] or [""]
                skaters = [re.sub(r"[A-Za-z]", "", item) for item in players if "G" not in item]
                on_list_cleaned.append(skaters)
                goalie_cleaned.append(goalies)

            if len(on_list_cleaned) == 2:
                away_on_ice.append(on_list_cleaned[0])
                home_on_ice.append(on_list_cleaned[1])
                home_goalie.append(goalie_cleaned[1])
                away_goalie.append(goalie_cleaned[0])
            else:
                away_on_ice.append([])
                home_on_ice.append([])
                home_goalie.append([])
                away_goalie.append([])

            if cells:
                data.append(cells[:6])

        columns = ["#", "Per", "Str", "Time:Elapsed Game", "Event", "Description"]
        df = pl.DataFrame(data, schema=columns, orient="row")
        return df, home_on_ice, away_on_ice, home_goalie, away_goalie

    def process_rosters(self, api_data):
        """Process roster data from API response."""
        rosters = pl.json_normalize(api_data["rosterSpots"])
        abbrev_dict = {
            api_data["awayTeam"]["id"]: api_data["awayTeam"]["abbrev"],
            api_data["homeTeam"]["id"]: api_data["homeTeam"]["abbrev"],
        }
        home_away = {api_data["awayTeam"]["id"]: 0, api_data["homeTeam"]["id"]: 1}

        return rosters.with_columns(
            [
                pl.col("teamId")
                .replace_strict(abbrev_dict, default="unknown")
                .alias("Team")
                .cast(pl.Utf8),
                pl.col("teamId")
                .replace_strict(home_away, default="unknown")
                .alias("Home/Away")
                .cast(pl.Int32),
                (pl.col("firstName.default") + " " + pl.col("lastName.default")).alias("fullName"),
            ]
        )

    def create_mappings(self, rosters):
        """Create mappings for player names and IDs from rosters."""
        if rosters.is_empty():
            return {"home_names": {}, "away_names": {}, "home_ids": {}, "away_ids": {}}

        home_roster = rosters.filter(pl.col("Home/Away") == 1)
        away_roster = rosters.filter(pl.col("Home/Away") == 0)

        return {
            "home_names": dict(
                zip(home_roster["sweaterNumber"].cast(str), home_roster["fullName"])
            ),
            "away_names": dict(
                zip(away_roster["sweaterNumber"].cast(str), away_roster["fullName"])
            ),
            "home_ids": dict(zip(home_roster["sweaterNumber"].cast(str), home_roster["playerId"])),
            "away_ids": dict(zip(away_roster["sweaterNumber"].cast(str), away_roster["playerId"])),
        }

    def map_players(self, df, mappings):
        """Map player names and IDs to on-ice data."""

        def replace_list(series, mapping_dict):
            return [
                (
                    [mapping_dict.get(str(i), None) for i in row if i]
                    if row is not None and len(row) > 0
                    else None
                )
                for row in series
            ]

        return df.with_columns(
            [
                pl.Series("homeNames", replace_list(df["homeON"], mappings["home_names"])),
                pl.Series("awayNames", replace_list(df["awayON"], mappings["away_names"])),
                pl.Series("homeIds", replace_list(df["homeON"], mappings["home_ids"])),
                pl.Series("awayIds", replace_list(df["awayON"], mappings["away_ids"])),
                pl.Series("homeGName", replace_list(df["homeG"], mappings["home_names"])),
                pl.Series("awayGName", replace_list(df["awayG"], mappings["away_names"])),
                pl.Series("homeGId", replace_list(df["homeG"], mappings["home_ids"])),
                pl.Series("awayGId", replace_list(df["awayG"], mappings["away_ids"])),
            ]
        )

    def merge_with_api(self, df, api_data):
        """Merge HTML play-by-play data with API data."""
        df = df.filter(~pl.col("Event").is_in(["ANTHEM", "PGEND", "PGSTR"]))
        api_df = pl.json_normalize(api_data["plays"]).filter(pl.col("typeDescKey").is_not_null())

        df = df.with_columns(
            [
                pl.arange(0, df.height).alias("index"),
                pl.col("Event")
                .replace_strict(self.event_mapping, default="unknown")
                .alias("event"),
            ]
        )
        api_df = api_df.with_columns(
            [pl.arange(0, api_df.height).alias("index"), pl.col("typeDescKey").alias("event")]
        ).with_columns(pl.col("details.eventOwnerTeamId").cast(pl.Utf8))

        return df.join(api_df, on=["index", "event"], how="left")

    def add_action_players(self, df, rosters):
        """Add player IDs and names for specific actions."""
        if rosters.is_empty():
            return df

        rosters_dict = dict(zip(rosters["playerId"].cast(str), rosters["fullName"]))
        df = df.with_columns(
            [pl.lit(None).alias(f"playerId_{i}") for i in range(1, 4)]
            + [pl.lit(None).alias(f"playerName_{i}") for i in range(1, 4)]
        )

        for event, columns in self.event_columns.items():
            for i, col in enumerate(columns[:3], start=1):
                if col and col in df.columns:
                    df = df.with_columns(
                        pl.when(pl.col("typeDescKey") == event)
                        .then(pl.col(col))
                        .otherwise(pl.col(f"playerId_{i}"))
                        .alias(f"playerId_{i}")
                    )

        columns_to_fill = [
            "details.awaySOG",
            "details.homeSOG",
            "details.awayScore",
            "details.homeScore",
        ]
        columns_to_fill = [col for col in columns_to_fill if col in df.columns]
        df = df.with_columns([pl.col(col).forward_fill().fill_null(0) for col in columns_to_fill])

        df = df.with_columns(
            [
                pl.col("details.goalieInNetId")
                .cast(str, strict=False)
                .replace(rosters_dict)
                .alias("goalieInNetName")
            ]
        ).with_columns(
            [
                pl.col(f"playerId_{i}")
                .cast(str, strict=False)
                .replace(rosters_dict)
                .alias(f"playerName_{i}")
                for i in range(1, 4)
            ]
        )
        return df

    def add_on_columns(self, df, rosters, max_column_index):
        """Add columns for on-ice skaters and goalies."""
        if rosters.is_empty():
            return df

        rosters_dict = dict(zip(rosters["playerId"], rosters["fullName"]))
        home_ids = df["homeIds"].to_list()
        away_ids = df["awayIds"].to_list()

        columns_to_add = (
            [f"home_skater_id{j+1}" for j in range(max_column_index)]
            + [f"away_skater_id{j+1}" for j in range(max_column_index)]
            + [f"home_skater_fullName{j+1}" for j in range(max_column_index)]
            + [f"away_skater_fullName{j+1}" for j in range(max_column_index)]
        )

        df = df.with_columns(
            [
                pl.Series(
                    f"home_skater_id{j+1}",
                    [row[j] if row and j < len(row) else None for row in home_ids],
                )
                for j in range(max_column_index)
            ]
            + [
                pl.Series(
                    f"away_skater_id{j+1}",
                    [row[j] if row and j < len(row) else None for row in away_ids],
                )
                for j in range(max_column_index)
            ]
            + [
                pl.Series(
                    f"home_skater_fullName{j+1}",
                    [
                        rosters_dict.get(row[j]) if row and j < len(row) and row[j] else None
                        for row in home_ids
                    ],
                )
                for j in range(max_column_index)
            ]
            + [
                pl.Series(
                    f"away_skater_fullName{j+1}",
                    [
                        rosters_dict.get(row[j]) if row and j < len(row) and row[j] else None
                        for row in away_ids
                    ],
                )
                for j in range(max_column_index)
            ]
        )

        return df.with_columns(
            [
                pl.col("homeGName").list.first().alias("homeGName"),
                pl.col("awayGName").list.first().alias("awayGName"),
                pl.col("homeGId").list.first().alias("homeGId"),
                pl.col("awayGId").list.first().alias("awayGId"),
                pl.col("homeIds").list.len().alias("homeSktrs"),
                pl.col("awayIds").list.len().alias("awaySktrs"),
            ]
        )

    def add_normalized_coords(self, df):
        """Add normalized x, y coordinates based on defending side."""
        df = df.with_columns(
            [pl.col("details.xCoord").cast(pl.Float64), pl.col("details.yCoord").cast(pl.Float64)]
        )
        df = df.with_columns(
            pl.when(
                (
                    (pl.col("details.eventOwnerTeamId") == pl.col("homeTeamId"))
                    & (pl.col("homeTeamDefendingSide") == "right")
                )
                | (
                    (pl.col("details.eventOwnerTeamId") == pl.col("awayTeamId"))
                    & (pl.col("homeTeamDefendingSide") == "right")
                )
            )
            .then(0 - pl.col("details.xCoord"))
            .otherwise(pl.col("details.xCoord"))
            .alias("x")
        ).with_columns(
            pl.when(
                (
                    (pl.col("details.eventOwnerTeamId") == pl.col("homeTeamId"))
                    & (pl.col("homeTeamDefendingSide") == "right")
                )
                | (
                    (pl.col("details.eventOwnerTeamId") == pl.col("awayTeamId"))
                    & (pl.col("homeTeamDefendingSide") == "right")
                )
            )
            .then(0 - pl.col("details.yCoord"))
            .otherwise(pl.col("details.yCoord"))
            .alias("y")
        )
        return df

    def add_game_info(self, api_data, df):
        """Add game metadata to the DataFrame."""
        abbrev_id_dict = {
            api_data["awayTeam"]["id"]: api_data["awayTeam"]["abbrev"],
            api_data["homeTeam"]["id"]: api_data["homeTeam"]["abbrev"],
        }
        return df.with_columns(
            [
                pl.col("details.eventOwnerTeamId").replace(abbrev_id_dict).alias("eventOwnerTeam"),
                pl.lit(abbrev_id_dict[api_data["awayTeam"]["id"]]).alias("awayTeam"),
                pl.lit(abbrev_id_dict[api_data["homeTeam"]["id"]]).alias("homeTeam"),
                pl.lit(api_data["awayTeam"]["id"]).cast(pl.String).alias("awayTeamId"),
                pl.lit(api_data["homeTeam"]["id"]).cast(pl.String).alias("homeTeamId"),
                pl.lit(api_data["season"]).alias("season"),
                pl.lit(api_data["gameType"]).alias("gameType"),
                pl.lit(api_data["limitedScoring"]).alias("limitedScoring"),
                pl.lit(datetime.strptime(api_data["gameDate"], "%Y-%m-%d")).alias("gameDate"),
                pl.lit(api_data["venue"]["default"]).alias("venue"),
                pl.lit(api_data["venueLocation"]["default"]).alias("venueLocation"),
                pl.lit(datetime.strptime(api_data["startTimeUTC"], "%Y-%m-%dT%H:%M:%SZ")).alias(
                    "startTimeUTC"
                ),
                pl.lit(api_data["easternUTCOffset"]).alias("easternUTCOffset"),
                pl.lit(api_data["venueUTCOffset"]).alias("venueUTCOffset"),
                pl.lit(api_data["gameState"]).alias("gameState"),
                pl.lit(api_data["gameScheduleState"]).alias("gameScheduleState"),
                pl.lit(api_data["gameOutcome"]["lastPeriodType"]).alias("gameOutcome"),
            ]
        )

    def add_elapsed_time(self, df):
        """Add elapsed time in seconds to the DataFrame."""

        def convert_str_to_sec(time_str):
            if isinstance(time_str, str) and ":" in time_str:
                try:
                    minutes, seconds = map(int, time_str.split(":"))
                    return minutes * 60 + seconds
                except ValueError:
                    return None
            return None

        df = df.with_columns(
            pl.col("timeInPeriod")
            .map_elements(convert_str_to_sec, return_dtype=pl.Int64)
            .alias("timeInPeriod_s")
        )
        df = df.with_columns(
            pl.when(pl.col("periodDescriptor.periodType") == "SO")
            .then(pl.lit(3900).alias("elapsedTime"))
            .otherwise(
                (
                    pl.col("timeInPeriod_s") + (pl.col("periodDescriptor.number") - 1) * 60 * 20
                ).alias("elapsedTime")
            )
        )
        return df

    def add_strength(self, df):
        """Add strength state (e.g., 5v5) to the DataFrame."""
        return df.with_columns(
            [
                pl.when(pl.col("details.eventOwnerTeamId").is_null())
                .then(pl.lit(None))
                .otherwise(
                    pl.when(pl.col("details.eventOwnerTeamId") == pl.col("homeTeamId"))
                    .then(pl.concat_str([pl.col("homeSktrs"), pl.lit("v"), pl.col("awaySktrs")]))
                    .otherwise(
                        pl.concat_str([pl.col("awaySktrs"), pl.lit("v"), pl.col("homeSktrs")])
                    )
                )
                .alias("strength")
            ]
        )

    def rename_columns(self, df):
        """Rename columns for consistency and clarity."""
        df = df.rename({col: col.replace(".", "_") for col in df.columns})
        rename_dict = {
            "typeDescKey": "api_event",
            "periodDescriptor_number": "periodNumber",
            "periodDescriptor_periodType": "periodType",
            "details_eventOwnerTeamId": "eventOwnerTeamId",
            "details_yCoord": "yCoord",
            "details_xCoord": "xCoord",
            "details_zoneCode": "zoneCode",
            "details_reason": "reason",
            "details_shotType": "shotType",
            "details_awaySOG": "awaySOG",
            "details_homeSOG": "homeSOG",
            "details_awayScore": "awayScore",
            "details_homeScore": "homeScore",
            "details_goalieInNetId": "goalieInNetId",
        }

        # Only rename columns that exist in the DataFrame
        rename_dict = {k: v for k, v in rename_dict.items() if k in df.columns}
    


        return df.rename(rename_dict)


class NHLPBPProcessor:
    """Processor for NHL play-by-play data across multiple games."""

    def __init__(self, game_ids):
        self.game_ids = [str(game_id) for game_id in game_ids]
        self.scraper = NHLDataScraper()
        self.parser = NHLPlayByPlayParser()

    async def process_single_game(self, game_id):
        """Process play-by-play data for a single game."""
        try:
            url_generator = NHLGameURLGenerator(game_id)
            html_task = self.scraper.scrape_html(url_generator.get_html_url())
            api_task = self.scraper.scrape_api(url_generator.get_api_url())
            html, api_data = await asyncio.gather(html_task, api_task)

            df, home_on_ice, away_on_ice, home_goalie, away_goalie = self.parser.parse_html(html)
            df = df.with_columns(
                [
                    pl.Series("homeON", home_on_ice),
                    pl.Series("awayON", away_on_ice),
                    pl.Series("homeG", home_goalie),
                    pl.Series("awayG", away_goalie),
                ]
            )

            max_column_index = (
                max(
                    len(plyrs)
                    for plyrs in home_on_ice + away_on_ice
                    if plyrs is not None and len(plyrs) > 0
                )
                if any(home_on_ice + away_on_ice)
                else 6
            )
            rosters = self.parser.process_rosters(api_data)
            mappings = self.parser.create_mappings(rosters)

            df = self.parser.map_players(df, mappings)
            df = self.parser.merge_with_api(df, api_data)
            df = self.parser.add_action_players(df, rosters)
            df = self.parser.add_on_columns(df, rosters, max_column_index)
            df = self.parser.add_game_info(api_data, df)
            df = self.parser.add_normalized_coords(df)
            df = self.parser.add_elapsed_time(df)
            df = self.parser.add_strength(df)

            # ### NEED TO WORK ON THE SEQUENCING OF THESE TASKS : DROP COLUMNS, RENAME COLUMNS, ADD META DATA ###
            cols_to_drop = [
                "periodDescriptor.maxRegulationPeriods",
                "homeON",
                "awayON",
                "homeG",
                "awayG",
                "homeNames",
                "awayNames",
                "homeIds",
                "awayIds",
                "details.highlightClipSharingUrl",
                "details.highlightClipSharingUrlFr",
                "details.highlightClip",
                "details.highlightClipFr",
                "details.discreteClip",
                "details.discreteClipFr",
                "pptReplayUrl",
                "Time:Elapsed Game",
                "typeCode",
                "eventId",
                "sortOrder",
                "situationCode",
                "details.hittingPlayerId",
                "details.hitteePlayerId",
                "details.losingPlayerId",
                "details.winningPlayerId",
                "details.blockingPlayerId",
                "details.assist1PlayerId",
                "details.assist2PlayerId",
                "details.scoringPlayerId",
                "details.committedByPlayerId",
                "details.shootingPlayerId",
            ]
            cols_to_drop = [col for col in cols_to_drop if col in df.columns]
            df = df.drop(cols_to_drop)
            df = self.parser.rename_columns(df)
            df = (df.with_columns(
                [
                    pl.lit(str(game_id)).alias("gameId"),
                    pl.lit(datetime.now()).alias("meta_datetime"),
                ]
            ))

            return df

        except Exception as e:
            print(f"Error processing game {game_id}: {str(e)}")
            return pl.DataFrame()

    async def fetch_and_process_multiple(self, progress=True):
        """Process play-by-play data for multiple games concurrently."""
        tasks = [self.process_single_game(game_id) for game_id in self.game_ids]
        results = []

        if progress:
            with tqdm(total=len(self.game_ids), desc="Scraping NHL PBP Data") as pbar:
                for task in asyncio.as_completed(tasks):
                    result = await task
                    results.append(result)
                    pbar.update(1)
        else:
            results = await asyncio.gather(*tasks, return_exceptions=True)

        valid_dfs = [df for df in results if isinstance(df, pl.DataFrame) and not df.is_empty()]
        if valid_dfs:
            return pl.concat(valid_dfs, how="diagonal")
        else:
            print("No valid data retrieved from any games.")
            return pl.DataFrame()


class Scraper:
    """Main class to orchestrate scraping of various NHL data types."""

    def __init__(self):
        """Initialize the Scraper with optional season parameter."""
        # self.season = season
        # self.team = team
        self.team_scraper = NHLTeamScraper()
        self.schedule_scraper = None
        self.standings_scraper = NHLStandingsScraper()
        self.roster_scraper = None
        self.stats_scraper = None
        self.toi_scraper = None
        self.pbp_processor = None
        self.draft_scraper = None
        self.rankings_scraper = None

    async def scrape_teams(self, source=None, progress=True):
        """Scrape team data."""
        if progress:
            with tqdm(total=1, desc="Scraping NHL Teams") as pbar:
                df = await self.team_scraper.scrape_teams(source)
                pbar.update(1)
        else:
            df = await self.team_scraper.scrape_teams(source)
        return df

    async def scrape_standings(self, date=None, progress=True):
        """Scrape standings data."""
        if progress:
            with tqdm(total=1, desc="Scraping NHL Standings") as pbar:
                df = await self.standings_scraper.scrape_standings(date)
                pbar.update(1)
        else:
            df = await self.standings_scraper.scrape_standings(date)
        return df

    async def scrape_team_rosters(self, teams, seasons, progress=True):
        """Scrape team roster data."""
        self.roster_scraper = NHLTeamRosterScraper(teams, seasons)
        return await self.roster_scraper.scrape_team_rosters(progress=progress)

    async def scrape_team_stats(self, teams, seasons, sessions, goalies=False, progress=True):
        """Scrape team stats data."""
        self.stats_scraper = NHLTeamStatsScraper(teams, seasons, sessions, goalies)
        return await self.stats_scraper.scrape_team_stats(progress=progress)

    async def scrape_schedule(self, teams, seasons, progress=True):
        """Scrape team schedule data."""
        self.schedule_scraper = NHLScheduleScraper(teams, seasons)
        return await self.schedule_scraper.scrape_schedule(progress=progress)

    async def scrape_toi(self, game_ids, progress=True):
        """Scrape TOI data."""
        self.toi_scraper = NHLTOIScraper(game_ids)
        return await self.toi_scraper.scrape_toi_multiple_games(progress=progress)

    async def scrape_pbp(self, game_ids, progress=True):
        """Scrape play-by-play data."""
        self.pbp_processor = NHLPBPProcessor(game_ids)
        return await self.pbp_processor.fetch_and_process_multiple(progress=progress)

    async def scrape_draft(self, years, round_="all", progress=True):
        """Scrape draft data."""
        self.draft_scraper = NHLDraftScraper(years, round_)
        return await self.draft_scraper.scrape_draft(progress=progress)

    async def scrape_rankings(self, years, categories, progress=True):
        """Scrape draft rankings data."""
        self.rankings_scraper = NHLDraftRankingsScraper(years, categories)
        return await self.rankings_scraper.scrape_rankings(progress=progress)

    # async def scrape_all(
    #     self,
    #     teams=None,
    #     seasons=None,
    #     sessions=None,
    #     game_ids=None,
    #     date_range=None,
    #     standings_date=None,
    #     years=None,
    #     categories=None,
    #     round_="all",
    #     goalies=False,
    #     progress=True,
    # ):
    #     """Scrape all specified NHL data types concurrently."""
    #     tasks = [
    #         ("teams", self.scrape_teams(progress=False)),
    #         ("schedule", self.scrape_schedule(date_range, progress=False)),
    #         ("standings", self.scrape_standings(standings_date, progress=False)),
    #     ]
    #     if teams and seasons:
    #         tasks.append(("rosters", self.scrape_team_rosters(teams, seasons, progress=False)))
    #     if teams and seasons and sessions:
    #         tasks.append(
    #             ("stats", self.scrape_team_stats(teams, seasons, sessions, goalies, progress=False))
    #         )
    #     if game_ids:
    #         tasks.append(("toi", self.scrape_toi(game_ids, progress=False)))
    #         tasks.append(("pbp", self.scrape_pbp(game_ids, progress=False)))
    #     if years:
    #         tasks.append(("draft", self.scrape_draft(years, round_, progress=False)))
    #     if years and categories:
    #         tasks.append(("rankings", self.scrape_rankings(years, categories, progress=False)))

    #     results = {}
    #     if progress:
    #         with tqdm(total=len(tasks), desc="Scraping NHL Data") as pbar:
    #             for task_name, task in tasks:
    #                 results[task_name] = await task
    #                 pbar.update(1)
    #     else:
    #         completed_tasks = await asyncio.gather(*[task for _, task in tasks])
    #         results = dict(zip([name for name, _ in tasks], completed_tasks))

    #     return results


# Main execution
async def main():
    """Example usage of the Scraper class."""
    scraper = Scraper(season="20232024")

    # Scrape multiple data types
    data = await scraper.scrape_draft(years=[2023, 2022], round_="all", progress=True)
    print(data)


# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())
