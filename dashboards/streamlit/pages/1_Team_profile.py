"""Page 1: team profile for one season (table: team_season_stats)."""
import pandas as pd
import plotly.express as px
import streamlit as st

from common import fmt, load, select

st.set_page_config(page_title="Team profile | MatchPulse", page_icon="⚽", layout="wide")
st.title("Team profile")

stats = load("team_season_stats")

# --- Selection (sidebar): competition -> season -> team ---------------------------------
st.sidebar.header("Team")
# Open on the first team that has xG data (the richest view), if there is one.
with_xg = stats[stats["xg_for"].notna()].sort_values("points", ascending=False)
start = with_xg.iloc[0] if not with_xg.empty else stats.iloc[0]

competitions = sorted(stats["competition"].unique())
competition = select("Competition", competitions, "team_comp", index=competitions.index(start["competition"]))
in_comp = stats[stats["competition"] == competition]
seasons = sorted(in_comp["season"].unique(), reverse=True)
season = select("Season", seasons, "team_season",
                index=seasons.index(start["season"]) if start["season"] in seasons else 0)
league = in_comp[in_comp["season"] == season]
# Teams with xG data first, then by points, so the interesting teams are at the top.
teams = (league.assign(has_xg=league["xg_for"].notna())
         .sort_values(["has_xg", "points"], ascending=False)["team_name"].tolist())
team = select("Team", teams, "team_name")
row = league[league["team_name"] == team].iloc[0]

st.subheader(f"{team}: {competition} {season}")

# --- KPI cards -----------------------------------------------------------------------
cols = st.columns(6)
cols[0].metric("Position", fmt(row["league_position"]) if pd.notna(row["league_position"]) else "tournament")
cols[1].metric("Points", int(row["points"]), help="3 per win, 1 per draw")
cols[2].metric("W / D / L", f"{row['wins']} / {row['draws']} / {row['losses']}")
cols[3].metric("Goals", f"{row['goals_for']} : {row['goals_against']}", delta=int(row["goal_difference"]))
cols[4].metric("xG for", fmt(row["xg_for"], 1),
               delta=None if pd.isna(row["xg_for"]) else round(row["goals_for"] - row["xg_for"], 1),
               help="Expected goals from shot quality. Delta = goals minus xG (finishing).")
cols[5].metric("xG against", fmt(row["xg_against"], 1))
if pd.isna(row["xg_for"]):
    st.caption("xG and shots are only available when event data covers every match of the team.")

left, right = st.columns(2)

# --- Home vs away ---------------------------------------------------------------------
with left:
    st.markdown("**Home vs away**")
    home_away = pd.DataFrame({
        "metric": ["Matches", "Wins", "Goals for", "Goals against"] * 2,
        "venue": ["Home"] * 4 + ["Away"] * 4,
        "value": [row["home_matches"], row["home_wins"], row["home_goals_for"], row["home_goals_against"],
                  row["away_matches"], row["away_wins"], row["away_goals_for"], row["away_goals_against"]],
    })
    fig = px.bar(home_away, x="metric", y="value", color="venue", barmode="group", text="value",
                 color_discrete_map={"Home": "#1f77b4", "Away": "#ff7f0e"})
    fig.update_layout(height=350, margin=dict(t=10, b=10), legend_title=None, xaxis_title=None, yaxis_title=None)
    st.plotly_chart(fig, width="stretch")

# --- Team vs competition average (per match) -----------------------------------------
with right:
    st.markdown("**Per match, compared with the competition average**")
    per_match = pd.DataFrame({
        "metric": ["Points", "Goals for", "Goals against"],
        team: [row["points"] / row["matches_played"], row["goals_for"] / row["matches_played"],
               row["goals_against"] / row["matches_played"]],
        "Competition average": [
            (league["points"] / league["matches_played"]).mean(),
            (league["goals_for"] / league["matches_played"]).mean(),
            (league["goals_against"] / league["matches_played"]).mean(),
        ],
    }).melt(id_vars="metric", var_name="who", value_name="per match")
    fig = px.bar(per_match, x="metric", y="per match", color="who", barmode="group",
                 text=per_match["per match"].round(2),
                 color_discrete_map={team: "#1f77b4", "Competition average": "#b0b0b0"})
    fig.update_layout(height=350, margin=dict(t=10, b=10), legend_title=None, xaxis_title=None, yaxis_title=None)
    st.plotly_chart(fig, width="stretch")

# --- Seasons of this team in this competition (only if more than one) ----------------
history = in_comp[in_comp["team_name"] == team].sort_values("season")
if len(history) > 1:
    st.markdown("**Across seasons**")
    fig = px.line(history, x="season", y=["points", "goals_for", "goals_against"], markers=True)
    fig.update_layout(height=300, margin=dict(t=10, b=10), legend_title=None, yaxis_title=None)
    st.plotly_chart(fig, width="stretch")

# --- Full table for the season, selected team highlighted -----------------------------
st.markdown("**Table**" if league["league_position"].notna().any() else "**All teams (by points)**")
table = league.sort_values(["points", "goal_difference", "goals_for"], ascending=False)[
    ["league_position", "team_name", "matches_played", "wins", "draws", "losses",
     "goals_for", "goals_against", "goal_difference", "points"]
].rename(columns={"league_position": "pos", "team_name": "team", "matches_played": "P", "wins": "W",
                  "draws": "D", "losses": "L", "goals_for": "GF", "goals_against": "GA",
                  "goal_difference": "GD", "points": "Pts"})
st.dataframe(
    table.style.apply(lambda r: ["background-color: rgba(31,119,180,0.25)" if r["team"] == team else ""
                                 for _ in r], axis=1),
    hide_index=True, width="stretch",
)
