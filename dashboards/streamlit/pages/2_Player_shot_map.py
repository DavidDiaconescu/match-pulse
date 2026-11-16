"""Page 2: player page with a shot map (tables: shots, player_season_stats)."""
import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st
from mplsoccer import VerticalPitch

from common import fmt, load, select

st.set_page_config(page_title="Player shot map | MatchPulse", page_icon="⚽", layout="wide")
st.title("Player shot map")

shots = load("shots")
players = load("player_season_stats")

# --- Selection: competition -> season -> team -> player -------------------------------
st.sidebar.header("Player")
competitions = sorted(shots["competition"].unique())
competition = select("Competition", competitions, "pl_comp")
seasons = sorted(shots.loc[shots["competition"] == competition, "season"].unique(), reverse=True)
season = select("Season", seasons, "pl_season")
in_season = shots[(shots["competition"] == competition) & (shots["season"] == season)]

# Teams ordered by number of shots, players ordered by total xG: best options first.
teams = in_season["team_name"].value_counts().index.tolist()
team = select("Team", teams, "pl_team")
team_shots = in_season[in_season["team_name"] == team]
by_player = team_shots.groupby(["player_id", "player_name"])["xg"].sum().sort_values(ascending=False)
labels = [f"{name} ({xg:.2f} xG)" for (_, name), xg in by_player.items()]
choice = select("Player", labels, "pl_player")
player_id, player_name = by_player.index[labels.index(choice)]

include_penalties = st.sidebar.checkbox("Include penalties", value=True)
player_shots = team_shots[team_shots["player_id"] == player_id]
if not include_penalties:
    player_shots = player_shots[player_shots["shot_type"] != "Penalty"]

st.subheader(f"{player_name}: {team}, {competition} {season}")

# --- KPI cards: from player_season_stats if available, otherwise from the shots ---------
season_row = players[(players["player_id"] == player_id) & (players["competition"] == competition)
                     & (players["season"] == season)]
cols = st.columns(6)
if not season_row.empty:
    p = season_row.iloc[0]
    cols[0].metric("Matches", int(p["matches_played"]))
    cols[1].metric("Minutes", fmt(p["minutes_played"]))
    cols[2].metric("Goals", int(p["goals"]), help="Excludes penalty shoot-outs")
    cols[3].metric("xG", fmt(p["xg"], 2), delta=round(p["goals"] - p["xg"], 2),
                   help="Delta = goals minus xG (finishing above/below expectation)")
    cols[4].metric("xG per 90", fmt(p["xg_per_90"], 2))
    cols[5].metric("Assists / key passes", f"{int(p['assists'])} / {int(p['key_passes'])}")
else:
    cols[0].metric("Shots", len(player_shots))
    cols[1].metric("Goals", int(player_shots["is_goal"].sum()))
    cols[2].metric("xG", fmt(player_shots["xg"].sum(), 2))
    st.caption("Season stats are only available for teams whose every match has event data; "
               "showing totals of the shots below.")

# --- Shot map ------------------------------------------------------------------------
left, right = st.columns([3, 2])
with left:
    # StatsBomb coordinates (120 x 80, attacking towards x = 120) are drawn as-is on a
    # vertical half pitch. Marker area grows with xG; goals are filled, misses hollow.
    pitch = VerticalPitch(pitch_type="statsbomb", half=True, pad_bottom=-10,
                          pitch_color="#22312b", line_color="#c7d5cc")
    fig, ax = pitch.draw(figsize=(7, 6))
    fig.set_facecolor("#22312b")
    goals = player_shots[player_shots["is_goal"]]
    misses = player_shots[~player_shots["is_goal"]]
    pitch.scatter(misses["x"], misses["y"], s=misses["xg"] * 900 + 40, ax=ax,
                  facecolors="none", edgecolors="#c7d5cc", linewidths=1.5, label="No goal")
    pitch.scatter(goals["x"], goals["y"], s=goals["xg"] * 900 + 40, ax=ax,
                  color="#f4a261", edgecolors="white", linewidths=1, label="Goal")
    legend = ax.legend(loc="lower left", facecolor="#22312b", edgecolor="#c7d5cc", labelcolor="#c7d5cc",
                       markerscale=0.5, fontsize=9)
    ax.set_title(f"{len(player_shots)} shots, {int(player_shots['is_goal'].sum())} goals, "
                 f"{player_shots['xg'].sum():.2f} xG  (marker size = xG)", color="#c7d5cc", fontsize=11)
    st.pyplot(fig)
    plt.close(fig)

# --- Shot list ---------------------------------------------------------------------
with right:
    st.markdown("**Shots**")
    st.dataframe(
        player_shots.sort_values(["match_date", "period", "minute", "second"])[
            ["match_date", "opponent_name", "minute", "xg", "outcome", "body_part", "shot_type"]
        ].rename(columns={"match_date": "date", "opponent_name": "opponent", "body_part": "body part",
                          "shot_type": "type"}).style.format({"xg": "{:.2f}"}),
        hide_index=True, width="stretch", height=480,
    )
