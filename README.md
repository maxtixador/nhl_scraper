# 🏒 NHL Scraper

Welcome to **NHL Scraper** – an asynchronous, high-performance toolkit for scraping NHL data, built by a hockey fan for hockey fans! 🚨

This Python-based project uses modern libraries like `Playwright`, `Polars`, and `aiohttp` to collect a variety of NHL data including team stats, schedules, rosters, TOI (Time on Ice), play-by-play (PBP), draft picks, and rankings.

---

## 📦 Features

- ✅ Active NHL teams
- 📅 Game schedules (by team and season)
- 📊 Standings snapshot
- 👥 Team rosters (positions, birthplace, etc.)
- 🧤 Goalie & skater stats
- 🕰️ TOI reports from official HTML
- 🎯 Play-by-play event parsing (API + HTML merged)
- 🧑‍🎓 Draft data (all rounds & years)
- 📋 Draft rankings by category

---

## ⚙️ Installation

> ⚠️ Python 3.8+ is required

Clone the repo and install dependencies:

```bash
git clone https://github.com/yourusername/nhl-scraper.git
cd nhl-scraper
pip install playwright polars aiohttp nest_asyncio selectolax 
```
Install Playwright and required browser:
```bash
playwright install chromium
```
---
## 📘 Functionality Overview

### ✅ Teams
```python
await scraper.scrape_teams()
```

### 📅 Schedule
```python
await scraper.scrape_schedule(teams=["MTL"], seasons=["20232024"])
```

### 👥 Rosters
```python
await scraper.scrape_team_rosters(teams=["MTL"], seasons=["20232024"])
```

### 📊 Stats
```python
await scraper.scrape_team_stats(
    teams=["MTL"],
    seasons=["20232024"],
    sessions=["regular"],
    goalies=False
)
```

### 🕰️ Time On Ice
```python
await scraper.scrape_toi(game_ids=["2023020012"])
```

### 🎯 Play-by-Play
```python
await scraper.scrape_pbp(game_ids=["2023020012"])
```

### 🧑‍🎓 Draft Picks
```python
await scraper.scrape_draft(years=[2022, 2023], round_="all")
```

### 📋 Draft Rankings
```python
await scraper.scrape_rankings(years=[2023], categories=[1, 2, 3, 4])
```

---
## 🧪 Quick Start
```python
import asyncio
from nhl_scraper.scraper import Scraper
from datetime import datetime

async def main():
    scraper = Scraper()
    standings = await scraper.scrape_standings(date=datetime.now().strftime("%Y-%m-%d"))  # for today's date
    print(standings)

if __name__ == "__main__":
    asyncio.run(main())
````

### 📙Notebook Example
```python
import asyncio
from scraper import Scraper

async def main():
    scraper = Scraper()
    standings = await scraper.scrape_standings(date=datetime.now().strftime("%Y-%m-%d"))  # for today's date
    print(standings)

standings_df = await scraper.scrape_standings(date=datetime.now().strftime("%Y-%m-%d"))
print(standings_df)
````
## 📁 Project Structure
```bash
nhl_scraper/
│
├── scraper.py                # Main scraping classes
├── README.md                 # This file
├── requirements.txt          # Dependencies
├── data/                     # (Optional) Saved output
└── notebooks/                # (Optional) Jupyter usage examples
```

---
## 🤝 Contributing

Pull requests are welcome! For major changes, please open an issue first to discuss what you would like to change.

---
## 📝 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---
## 📚 References

- [NHL API](https://gitlab.com/dword4/nhlapi)
- [Polars](https://pola.rs/)
- [Playwright](https://playwright.dev/)
- [Selectolax](https://github.com/rushter/selectolax)

---
## 📝 Acknowledgements

- Patrick Bacon ([@TopDownHockey](https://x.com/TopDownHockey)) - Inspiration for the project
- Mikael Nahabedian ([@hunterofstats](https://x.com/hunterofstats)) - For teaching me how to code in Python
- Matthew Barlowe (I think he wants to stay anonymous) - Doesn't know it but he inspired me to create this project
- Chace McCallum & Josh Khalfin - For being my friends and for putting up with my bullshit
- François Kik ([@francois_kik](https://x.com/francois_kik)) - For being a good friend and for putting up with my bullshit
- Sorry if I forgot anyone, just ask me and I'll add you to the list


## Next Steps

- Add more functionality to the scraper
- Add more tests
- Add more documentation
- Add more examples
- Add more leagues
- Add aggregate functions
- Add Pandas DataFrame output option



## 📝 Contributors

- 🤓 [Max](https://github.com/maxtixador) | [Follow me on X](https://x.com/woumaxx) | [Follow me on LinkedIn](https://www.linkedin.com/in/max-tixador/)

