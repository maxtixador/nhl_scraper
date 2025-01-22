# NHL Scraper

A Python package for scraping NHL data including draft information, player statistics, game data, and more. This package provides easy access to various NHL statistics and information through the NHL's API.

## Features

- Draft data scraping
- Player rankings
- Team information
- Game schedules
- Play-by-play data
- Player shifts
- Team standings
- Player information
- Game rosters

## Installation

Since this package is not yet available on PyPI, you can install it directly from GitHub:
```bash
pip install git+https://github.com/maxtixador/nhl_scraper.git
```

# How to use it
```bash
from nhl_scraper.scraper.scrape import *

scrapeDraft()
```


## Functions

### Draft Data
- `scrapeDraft(year=2023, round="all")`: Get draft data for specific year and round
- `scrapeRankings(year=2025, category=1)`: Get NHL Central Scouting rankings

### Game Data
- `scrapePlayByPlay(gameId)`: Get detailed play-by-play events
- `scrapeRosters(gameId)`: Get complete game rosters
- `scrapeShifts(gameId)`: Get player shift data
- `scrape_pbp(gameId)`: Get combined play-by-play, roster, and shift data

### Team & Player Data
- `scrapeTeams()`: Get all NHL team information
- `scrapeStandings(date)`: Get standings for specific date
- `scrapePlayer(playerId, key=None)`: Get detailed player information
- `scrapeSchedule(team_slug, season)`: Get team schedule


## Development

#### Contributing
1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- NHL API for providing the data
- Contributors and maintainers
- NHL Stats community

## Disclaimer

This package is not affiliated with or endorsed by the National Hockey League (NHL). All NHL logos and marks are the property of the NHL and its teams.