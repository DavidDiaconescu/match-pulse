"""Page 3: league trends: goals per season and home advantage (table: league_trends)."""
import plotly.express as px
import streamlit as st

from common import load

st.set_page_config(page_title="League trends | MatchPulse", page_icon="⚽", layout="wide")
st.title("League trends")

trends = load("league_trends")

# --- League selector ------------------------------------------------------------------
# Default: the leagues with the most seasons, so the lines over time have several points.
seasons_per_league = trends.groupby("competition")["season"].nunique().sort_values(ascending=False)
leagues = sorted(trends["competition"].unique())
default = [c for c in seasons_per_league.index[:4]]
selected = st.sidebar.multiselect("Leagues", leagues, default=default)
if not selected:
    st.info("Select at least one league in the sidebar.")
    st.stop()
data = trends[trends["competition"].isin(selected)].sort_values(["competition", "season"])

# --- Headline numbers over the selected leagues (weighted by number of matches) -------
matches = data["matches"].sum()
cols = st.columns(4)
cols[0].metric("Matches", f"{matches:,}")
cols[1].metric("Goals per match", f"{data['total_goals'].sum() / matches:.2f}")
cols[2].metric("Home wins", f"{(data['home_win_pct'] * data['matches']).sum() / matches:.0%}")
cols[3].metric("Away wins", f"{(data['away_win_pct'] * data['matches']).sum() / matches:.0%}",
               help="Draws are the rest")

# --- Goals per match by season --------------------------------------------------------
st.markdown("**Goals per match by season**")
fig = px.line(data, x="season", y="avg_goals_per_match", color="competition", markers=True,
              hover_data={"matches": True, "total_goals": True, "avg_goals_per_match": ":.2f"})
fig.update_layout(height=360, margin=dict(t=10, b=10), legend_title=None,
                  yaxis_title="goals per match", xaxis_title=None)
fig.update_xaxes(type="category", categoryorder="category ascending")
st.plotly_chart(fig, width="stretch")

left, right = st.columns(2)

# --- Home vs away goals ---------------------------------------------------------------
with left:
    st.markdown("**Home vs away goals per match**")
    home_away = data.melt(id_vars=["competition", "season"], value_vars=["avg_home_goals", "avg_away_goals"],
                          var_name="side", value_name="goals per match")
    home_away["side"] = home_away["side"].map({"avg_home_goals": "Home", "avg_away_goals": "Away"})
    home_away["league / season"] = home_away["competition"] + " " + home_away["season"]
    fig = px.bar(home_away, x="league / season", y="goals per match", color="side", barmode="group",
                 color_discrete_map={"Home": "#1f77b4", "Away": "#ff7f0e"})
    fig.update_layout(height=380, margin=dict(t=10, b=10), legend_title=None, xaxis_title=None)
    st.plotly_chart(fig, width="stretch")

# --- Result split: home win / draw / away win -------------------------------------------
with right:
    st.markdown("**Results: home win / draw / away win**")
    results = data.melt(id_vars=["competition", "season"], value_vars=["home_win_pct", "draw_pct", "away_win_pct"],
                        var_name="result", value_name="share")
    results["result"] = results["result"].map({"home_win_pct": "Home win", "draw_pct": "Draw",
                                               "away_win_pct": "Away win"})
    results["league / season"] = results["competition"] + " " + results["season"]
    fig = px.bar(results, x="league / season", y="share", color="result", barmode="stack",
                 color_discrete_map={"Home win": "#1f77b4", "Draw": "#b0b0b0", "Away win": "#ff7f0e"})
    fig.update_layout(height=380, margin=dict(t=10, b=10), legend_title=None, xaxis_title=None,
                      yaxis_tickformat=".0%")
    st.plotly_chart(fig, width="stretch")

st.caption("Tournaments are played mostly at neutral venues, so 'home' there is just the team listed first.")

with st.expander("Data"):
    st.dataframe(data.drop(columns=["updated_at"]), hide_index=True, width="stretch")
