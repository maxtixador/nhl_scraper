from nhl_scraper.scraper.scrape import scrape_pbp

# Test with a real game ID (e.g., first game of 2023-24 season)
pbp_df, rosters_df, shifts_df = scrape_pbp(2023020001)

# Print some basic info
print("\nPlay-by-Play Data:")
print(f"Number of events: {len(pbp_df)}")
print(f"Columns: {pbp_df.columns.tolist()}")

print("\nRosters Data:")
print(f"Number of players: {len(rosters_df)}")
print(f"Columns: {rosters_df.columns.tolist()}")

print("\nShifts Data:")
print(f"Number of shifts: {len(shifts_df)}")
print(f"Columns: {shifts_df.columns.tolist()}")
print(pbp_df.head())