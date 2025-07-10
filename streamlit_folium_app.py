import streamlit as st
import folium
import pandas as pd
from streamlit_folium import st_folium
from folium.plugins import MarkerCluster
from hail_pipeline_folium import run_hail_risk_pipeline  # Pure data logic, no Streamlit

# Page config
st.set_page_config(layout="wide", page_title="Hail Risk Dashboard")
st.title("Hail Risk Dashboard")

# --- Run pipeline only once ---
if "gdf_all" not in st.session_state or "hail_gdf" not in st.session_state:
    with st.spinner("Loading data and generating map..."):
        gdf_all, hail_gdf = run_hail_risk_pipeline()
        st.session_state["gdf_all"] = gdf_all
        st.session_state["hail_gdf"] = hail_gdf
else:
    gdf_all = st.session_state["gdf_all"]
    hail_gdf = st.session_state["hail_gdf"]

# --- Dropdown for state selection ---
available_states = sorted(gdf_all["STATEFP"].unique())
state_name_map = {"29": "Missouri", "20": "Kansas", "19": "Iowa", "31": "Nebraska"}
state_options = [state_name_map[s] for s in available_states if s in state_name_map]
selected_state = st.selectbox("Select State", state_options)

# --- Map state name back to FIPS
reverse_state_map = {v: k for k, v in state_name_map.items()}
selected_statefp = reverse_state_map[selected_state]

# --- Filter data ---
gdf_filtered = gdf_all[gdf_all["STATEFP"] == selected_statefp]
hail_filtered = hail_gdf[hail_gdf["STATEFP"] == selected_statefp] if "STATEFP" in hail_gdf.columns else hail_gdf

# --- Create folium map centered on selected state ---
m = folium.Map(location=[gdf_filtered.geometry.centroid.y.mean(), gdf_filtered.geometry.centroid.x.mean()],
               zoom_start=7, tiles="cartodbpositron")

# --- Vehicle Ownership Density Layer ---
gdf_ownership = gdf_filtered[~gdf_filtered["car_ownership_density"].isna()]
folium.Choropleth(
    geo_data=gdf_ownership,
    data=gdf_ownership,
    columns=["GEOID", "car_ownership_density"],
    key_on="feature.properties.GEOID",
    fill_color="BuGn",
    fill_opacity=0.6,
    line_opacity=0.2,
    legend_name="Car Ownership Density (households/km²)",
    highlight=True
).add_to(m)

# --- Hail Risk Score Layer ---
gdf_risk = gdf_filtered[gdf_filtered["hail_risk_score"] > 0]
folium.Choropleth(
    geo_data=gdf_risk,
    data=gdf_risk,
    columns=["GEOID", "hail_risk_score"],
    key_on="feature.properties.GEOID",
    fill_color="YlOrRd",
    fill_opacity=0.6,
    line_opacity=0.2,
    legend_name="Hail Risk Score",
    highlight=True
).add_to(m)

# --- Hail Reports Markers ---
marker_cluster = MarkerCluster(name="Hail Reports").add_to(m)
for _, row in hail_filtered.iterrows():
    lat, lon = row.get("Lat"), row.get("Lon")
    if pd.notna(lat) and pd.notna(lon):
        folium.CircleMarker(
            location=[lat, lon],
            radius=4,
            color="blue",
            fill=True,
            fill_opacity=0.6,
            popup=str(row.get("Comments", "No comment"))
        ).add_to(marker_cluster)

# --- Layer Control + Map Render ---
folium.LayerControl().add_to(m)
st_folium(m, width=1300, height=750)

# --- Reload button ---
if st.button("🔁 Reload Data"):
    with st.spinner("Reloading data..."):
        gdf_all, hail_gdf = run_hail_risk_pipeline()
        st.session_state["gdf_all"] = gdf_all
        st.session_state["hail_gdf"] = hail_gdf
        st.experimental_rerun()
