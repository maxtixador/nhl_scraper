# Basic Scraping
Several examples of how to use the `scrapernhl` package to scrape NHL data.

## Getting Play-by-Play Data
```python
from scrapernhl import scraper

# Example: Scrape game data
game_data = scraper.get_game_pbp(game_id=2023020001)
print(game_data.head())
```

## Scraping Multiple Games
```python
# Scrape a range of games
game_ids = [2023020001, 2023020002, 2023020003]
for game_id in game_ids:
    data = scraper.get_game_pbp(game_id)
    # Process data...
```