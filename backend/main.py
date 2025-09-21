from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd

app = FastAPI(title="ICC Rankings API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global cache
df = None
DATA_URL = "https://raw.githubusercontent.com/ajaychawla07/icc_rankings/main/ICC_Rankings_recent.csv.gz"


# ---------------- STARTUP ----------------
@app.on_event("startup")
def load_data():
    """Load dataset into memory when API starts."""
    global df
    try:
        df = pd.read_csv(DATA_URL, compression="gzip")
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        print(f"✅ Data loaded: {len(df):,} rows")
    except Exception as e:
        print(f"❌ Failed to load data: {e}")
        df = pd.DataFrame()


# ---------------- BASIC ENDPOINTS ----------------
@app.get("/")
def root():
    return {"message": "ICC Rankings API is running"}


@app.get("/players/{name}")
def get_player(name: str, format: str = None, category: str = None):
    """Get ranking history for a player."""
    global df
    data = df[df["Player"].str.contains(name, case=False, na=False)]
    if format:
        data = data[data["Format"] == format]
    if category:
        data = data[data["Category"] == category]
    return data.to_dict(orient="records")


@app.get("/top")
def get_top(date: str, format: str, category: str):
    """Get top 10 players on a given date."""
    global df
    d = pd.to_datetime(date, errors="coerce")
    data = df[
        (df["Format"] == format) &
        (df["Category"] == category) &
        (df["Date"] == d)
    ]
    return data.sort_values("Rank").head(10).to_dict(orient="records")


@app.get("/compare")
def compare(players: str, format: str, category: str):
    """Compare multiple players' ranking history."""
    global df
    names = [p.strip() for p in players.split(",")]
    result = {}
    for name in names:
        pdata = df[
            (df["Format"] == format) &
            (df["Category"] == category) &
            (df["Player"].str.contains(name, case=False, na=False))
        ]
        result[name] = pdata.to_dict(orient="records")
    return result


@app.get("/refresh")
def refresh():
    """Reload dataset from GitHub."""
    load_data()
    return {"message": "Data refreshed"}


# ---------------- CAREER STATS ----------------
@app.get("/summary/{name}")
def player_summary(name: str, format: str = None, category: str = None):
    """Get career summary stats for a player."""
    global df
    data = df[df["Player"].str.contains(name, case=False, na=False)]
    if format:
        data = data[data["Format"] == format]
    if category:
        data = data[data["Category"] == category]

    if data.empty:
        return {"Player": name, "message": "No data found"}

    peak_row = data.loc[data["Rating"].idxmax()]
    peak_rating = int(peak_row["Rating"])
    peak_rank = int(peak_row["Rank"])
    weeks_rank1 = int((data["Rank"] == 1).sum())
    first_date = str(data["Date"].min().date())
    last_date = str(data["Date"].max().date())

    return {
        "Player": name,
        "Format": format if format else "all",
        "Category": category if category else "all",
        "PeakRating": peak_rating,
        "PeakRank": peak_rank,
        "WeeksAtRank1": weeks_rank1,
        "FirstAppearance": first_date,
        "LastAppearance": last_date,
    }


@app.get("/dominance/{name}")
def dominance(name: str, format: str = None, category: str = None):
    """Get number of days a player spent at Rank 1, Top 5, Top 10."""
    global df
    data = df[df["Player"].str.contains(name, case=False, na=False)]
    if format:
        data = data[data["Format"] == format]
    if category:
        data = data[data["Category"] == category]

    if data.empty:
        return {"Player": name, "message": "No data found"}

    days_rank1 = int((data["Rank"] == 1).sum())
    days_top5 = int((data["Rank"] <= 5).sum())
    days_top10 = int((data["Rank"] <= 10).sum())

    return {
        "Player": name,
        "Format": format if format else "all",
        "Category": category if category else "all",
        "DaysAtRank1": days_rank1,
        "DaysInTop5": days_top5,
        "DaysInTop10": days_top10
    }


# ---------------- LEADERBOARDS ----------------
@app.get("/leaders")
def leaders(format: str, category: str, top_n: int = 20):
    """Get leaderboard of players: days at Rank 1, Top 5, and Top 10."""
    global df
    data = df[(df["Format"] == format) & (df["Category"] == category)]
    if data.empty:
        return []

    leaderboard = (
        data.groupby("Player")
        .agg(
            DaysAtRank1=("Rank", lambda x: (x == 1).sum()),
            DaysInTop5=("Rank", lambda x: (x <= 5).sum()),
            DaysInTop10=("Rank", lambda x: (x <= 10).sum()),
        )
        .reset_index()
    )

    leaderboard = leaderboard.sort_values(
        ["DaysAtRank1", "DaysInTop5", "DaysInTop10"],
        ascending=[False, False, False]
    ).head(top_n)

    return leaderboard.to_dict(orient="records")


@app.get("/yearly-top")
def yearly_top(year: int, format: str, category: str):
    """Get Top 10 players at end of a given year."""
    global df
    data = df[
        (df["Format"] == format) &
        (df["Category"] == category) &
        (df["Date"].dt.year == year)
    ]
    if data.empty:
        return []
    last_date = data["Date"].max()
    snapshot = data[data["Date"] == last_date].sort_values("Rank").head(10)
    return snapshot.to_dict(orient="records")


@app.get("/year-leaders")
def year_leaders(year: int, format: str, category: str, top_n: int = 10):
    """Get leaderboard for a given year."""
    global df
    data = df[
        (df["Format"] == format) &
        (df["Category"] == category) &
        (df["Date"].dt.year == year)
    ]
    if data.empty:
        return []

    leaderboard = (
        data.groupby("Player")
        .agg(
            DaysAtRank1=("Rank", lambda x: (x == 1).sum()),
            DaysInTop5=("Rank", lambda x: (x <= 5).sum()),
            DaysInTop10=("Rank", lambda x: (x <= 10).sum()),
        )
        .reset_index()
    )

    leaderboard = leaderboard.sort_values(
        ["DaysAtRank1", "DaysInTop5", "DaysInTop10"],
        ascending=[False, False, False]
    ).head(top_n)

    return leaderboard.to_dict(orient="records")


@app.get("/decade-leaders")
def decade_leaders(format: str, category: str, decade: int, top_n: int = 10):
    """Get leaderboard of players for a given decade (e.g. 2000, 2010, 2020)."""
    global df
    start_year = decade
    end_year = decade + 9

    data = df[
        (df["Format"] == format) &
        (df["Category"] == category) &
        (df["Date"].dt.year >= start_year) &
        (df["Date"].dt.year <= end_year)
    ]
    if data.empty:
        return []

    leaderboard = (
        data.groupby("Player")
        .agg(
            DaysAtRank1=("Rank", lambda x: (x == 1).sum()),
            DaysInTop5=("Rank", lambda x: (x <= 5).sum()),
            DaysInTop10=("Rank", lambda x: (x <= 10).sum()),
        )
        .reset_index()
    )

    leaderboard = leaderboard.sort_values(
        ["DaysAtRank1", "DaysInTop5", "DaysInTop10"],
        ascending=[False, False, False]
    ).head(top_n)

    return {
        "Decade": f"{start_year}s",
        "Format": format,
        "Category": category,
        "Leaders": leaderboard.to_dict(orient="records"),
    }


# ---------------- UTILITIES ----------------
@app.get("/search")
def search(query: str):
    """Search players by partial name (autocomplete)."""
    global df
    players = df["Player"].dropna().unique()
    matches = [p for p in players if query.lower() in p.lower()]
    return matches[:20]

@app.get("/latest")
def latest():
    """Get top 10 players for latest date for each format and category."""
    global df
    results = []
    for format in ["odi", "test"]:
        for category in ["batting", "bowling"]:
            subset = df[(df["Format"] == format) & (df["Category"] == category)]
            if subset.empty:
                continue
            latest_date = subset["Date"].max()
            latest_data = subset[subset["Date"] == latest_date].sort_values("Rank").head(10)
            results.append({
                "Format": format,
                "Category": category,
                "Date": str(latest_date.date()),
                "TopPlayers": latest_data.to_dict(orient="records")
            })
    return results

@app.get("/leaders-summary")
def leaders_summary(top_n: int = 5):
    """Get leaderboard of players by days at Rank 1 for each format and category."""
    global df
    summary = []
    for format in ["odi", "test"]:
        for category in ["batting", "bowling"]:
            subset = df[(df["Format"] == format) & (df["Category"] == category)]
            if subset.empty:
                continue
            leaderboard = (
                subset.groupby("Player")
                .agg(DaysAtRank1=("Rank", lambda x: (x == 1).sum()))
                .reset_index()
                .sort_values("DaysAtRank1", ascending=False)
                .head(top_n)
            )
            summary.append({
                "Format": format,
                "Category": category,
                "Leaders": leaderboard.to_dict(orient="records")
            })
    return summary
