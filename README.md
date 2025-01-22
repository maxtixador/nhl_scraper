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

## Data Format

Each function returns pandas DataFrames with the following key information:

- Draft data: round, pick numbers, team information
- Rankings: player rankings by category
- Schedule: game dates, times, locations, teams
- Player data: biographical info, statistics, current team
- Game data: play-by-play events, player shifts, rosters

## Development

### Requirements
- Python 3.8+
- pandas
- requests
- numpy

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- NHL API for providing the data
- Contributors and maintainers

## Disclaimer

This package is not affiliated with or endorsed by the National Hockey League (NHL).