# 🏒 NHL Scraper

A Python package for scraping NHL data from both the current NHL API and NHL Records API. This package provides easy access to various NHL statistics and information through multiple NHL data endpoints.

## Features

- Draft data (current and historical)
- Player statistics and rankings
- Team information and rosters
- Game data (play-by-play, shifts, rosters)
- League standings
- Player profiles and game logs

## Installation

```bash
pip install git+https://github.com/maxtixador/nhl_scraper.git
```

## Usage

```python
from nhl_scraper.scraper import (
    draft,
    game,
    player,
    teams,
    standings
)

# Get draft data
draft_data = draft.scrapeDraft(year=2023)  # Current NHL API
legacy_draft = draft.scrapeDraftLegacy(2023)  # NHL Records API
rankings = draft.scrapeRankings(year=2025, category=1)  # Draft rankings

# Get game data
pbp = game.scrapeGamePlayByPlay(2024020858)
shifts = game.scrapeGameShifts(2024020858)
shifts_legacy = game.scrapeGameShiftsLegacy(2024020858)
rosters = game.scrapeGameRosters(2024020858)

# Get player data
profile = player.scrapePlayerProfile(8478402)
stats = player.scrapePlayerStats(8478402, stats_type="seasonTotals")
gamelog = player.scrapePlayerGameLog(8478402, "20232024")

# Get team data
teams_list = teams.scrapeTeams()
team_roster = teams.scrapeTeamRoster("MTL", "20232024")
team_stats = teams.scrapeTeamStats("MTL", "20232024")
team_schedule = teams.scrapeSchedule("MTL", "20232024")

# Get standings
standings = standings.scrapeLeagueStandings("2024-02-20")
```

## Available Functions

### Draft Data
- `scrapeDraft(year=2023, round="all")`: Current season draft data
- `scrapeDraftLegacy(draft_year=2023)`: Historical draft data
- `scrapeRankings(year=2025, category=1)`: NHL Central Scouting rankings

### Game Data
- `scrapeGamePlayByPlay(game_id)`: Detailed play-by-play events
- `scrapeGameRosters(game_id)`: Complete game rosters
- `scrapeGameShifts(game_id)`: Player shift data (current API)
- `scrapeGameShiftsLegacy(game_id)`: Player shift data (HTML reports)

### Player Data
- `scrapePlayerProfile(player_id)`: Basic player information
- `scrapePlayerStats(player_id, season, stats_type)`: Player statistics
- `scrapePlayerGameLog(player_id, season, session_type)`: Game-by-game stats

### Team Data
- `scrapeTeams()`: Basic team information
- `scrapeTeamDetails()`: Detailed franchise information
- `scrapeTeamRoster(team, season)`: Team roster data
- `scrapeTeamStats(team, season, session, goalies)`: Team statistics
- `scrapeSchedule(team, season)`: Team schedule
- `scrapeTeamDraftHistory(franchiseId)`: Historical draft picks

### League Data
- `scrapeLeagueStandings(date)`: League standings for specific date

## Development

### Contributing
1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- NHL API and NHL Records API for providing the data
- Contributors and maintainers
- NHL Stats community

## Disclaimer

This package is not affiliated with or endorsed by the National Hockey League (NHL). All NHL logos and marks are the property of the NHL and its teams.
