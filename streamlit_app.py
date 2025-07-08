import streamlit as st
import folium
from streamlit_folium import st_folium
from folium.plugins import MarkerCluster
from hail_pipeline import run_hail_risk_pipeline  # This must contain ONLY data logic, no Streamlit calls

# Page config
st.set_page_config(layout="wide", page_title="Hail Risk Dashboard")
st.title("Hail Risk Dashboard")

# --- Caching the data pipeline ---
@st.cache_data(ttl=3600)
def get_data():
    return run_hail_risk_pipeline()

# --- Load data ---
with st.spinner("Loading data and generating map..."):
    gdf_all, hail_gdf = get_data()

    # Keep all for car ownership layer
    gdf_all_ownership = gdf_all.copy()

#   Filter only those with hail risk > 0
    gdf_all_risk = gdf_all[gdf_all["hail_risk_score"] > 0].copy()# --- Build base map ---

m = folium.Map(location=[39.5, -96.5], zoom_start=6, tiles="cartodbpositron")

# 1. Car Ownership Layer
folium.Choropleth(
    geo_data=gdf_all_ownership,
    data=gdf_all_ownership,
    columns=["GEOID", "car_ownership_density"],
    key_on="feature.properties.GEOID",
    fill_color="YlGnBu",
    fill_opacity=0.5,
    line_opacity=0.2,
    nan_fill_color="white",
    legend_name="Car Ownership Density (Households/km²)",
    name="Car Ownership Density"
).add_to(m)

# 2. Hail Risk Score (non-zero only)
folium.Choropleth(
    geo_data=gdf_all_risk,
    data=gdf_all_risk,
    columns=["GEOID", "hail_risk_score"],
    key_on="feature.properties.GEOID",
    fill_color="OrRd",
    fill_opacity=0.4,
    line_opacity=0.1,
    nan_fill_color="transparent",
    legend_name="Hail Risk Score",
    name="Hail Risk Score"
).add_to(m)

# 3. Hail Reports Markers
marker_cluster = MarkerCluster(name="Hail Reports").add_to(m)
for _, row in hail_gdf.iterrows():
    folium.CircleMarker(
        location=[row["Lat"], row["Lon"]],
        radius=4,
        color="blue",
        fill=True,
        fill_opacity=0.6,
        popup=row.get("Comments", "No comment"),
    ).add_to(marker_cluster)

# 4. Layer Control
folium.LayerControl().add_to(m)

# --- Display map ---
st_folium(m, width=1400, height=700)
