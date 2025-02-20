"""
API Endpoints for the NHL Scraper module.

This module contains the API endpoints for the NHL Scraper module.
"""

# NHL API URLs
NHL_API_BASE_URL = "https://api-web.nhle.com/v1/"

# Draft Rankings
DRAFT_RANKINGS_URL = NHL_API_BASE_URL + "draft/rankings/{year}/{category}"

# Draft Picks
DRAFT_PICKS_URL = NHL_API_BASE_URL + "draft/picks/{year}/{round}"  # or all

# Teams (remove /v1/)
TEAMS_URL = (
    NHL_API_BASE_URL[::3]
    + "/stats/rest/en/franchise?sort=fullName&include=lastSeason.id&include=firstSeason.id"
)

# Roster
ROSTER_URL = NHL_API_BASE_URL + "roster/{team_id}/{season}"

# Team Scoring
TEAM_SCORING_URL = NHL_API_BASE_URL + "club-stats/{team}/{season}/{sessionId}"

# Schedule
SCHEDULE_URL = NHL_API_BASE_URL + "schedule-calendar/now"  # Used to find active teams

# Team Schedule
TEAM_SCHEDULE_URL = NHL_API_BASE_URL + "club-schedule-season/{team}/{season}"

# Shifts
SHIFTS_URL = NHL_API_BASE_URL[::3] + "stats/rest/en/shiftcharts?cayenneExp=gameId={game_id}"

# Game data
GAME_DATA = NHL_API_BASE_URL + "gamecenter/{gameId}/play-by-play"

# Standings
STANDINGS_URL = NHL_API_BASE_URL + "standings/{date}"  # or "now"

# Prospects
PROSPECTS_URL = NHL_API_BASE_URL + "prospects/{team}"
