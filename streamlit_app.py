'''import streamlit as st
import folium
import pandas as pd
from streamlit_folium import st_folium
from folium.plugins import MarkerCluster
from hail_pipeline import run_hail_risk_pipeline  # This must contain ONLY data logic, no Streamlit calls

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


# -- Create folium map
m = folium.Map(location=[39.5, -96.5], zoom_start=6, tiles="cartodbpositron")

# --- Vehicle Ownership Density Layer ---
gdf_all_ownership = gdf_all[~gdf_all["car_ownership_density"].isna()]
folium.Choropleth(
    geo_data=gdf_all_ownership,
    data=gdf_all_ownership,
    columns=["GEOID", "car_ownership_density"],
    key_on="feature.properties.GEOID",
    fill_color="BuGn",
    fill_opacity=0.6,
    line_opacity=0.2,
    legend_name="Car Ownership Density (households/km²)",
    highlight=True
).add_to(m)



# --- Hail Risk Score Layer ---
gdf_all_risk = gdf_all[gdf_all["hail_risk_score"] > 0]
folium.Choropleth(
    geo_data=gdf_all_risk,
    data=gdf_all_risk,
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

for _, row in hail_gdf.iterrows():
    lat, lon = row.get("Lat"), row.get("Lon")

    # Only add valid points
    if pd.notna(lat) and pd.notna(lon):
        folium.CircleMarker(
            location=[lat, lon],
            radius=4,
            color="blue",
            fill=True,
            fill_opacity=0.6,
            popup=str(row.get("Comments", "No comment"))
        ).add_to(marker_cluster)

# 4. Layer Control
folium.LayerControl().add_to(m)

# --- Render map ---
folium.LayerControl().add_to(m)
st_folium(m, width=1300, height=700)


# --- Button to reload pipeline if needed ---
if st.button("🔁 Reload Data"):
    with st.spinner("Reloading data..."):
        gdf_all, hail_gdf = run_hail_risk_pipeline()
        st.session_state["gdf_all"] = gdf_all
        st.session_state["hail_gdf"] = hail_gdf
        st.experimental_rerun()

import streamlit as st
import geopandas as gpd
import pandas as pd
import pydeck as pdk
from hail_pipeline import load_cached_pipeline

st.set_page_config(layout="wide")

st.title("Hail Risk and Vehicle Ownership Dashboard")

# --- Load data ---
with st.spinner("Loading processed data..."):
    gdf_all, hail_gdf = load_cached_pipeline()

# --- Ensure lat/lon are present ---
gdf_all = gdf_all.to_crs(epsg=4326).copy()
gdf_all["lon"] = gdf_all.geometry.centroid.x
gdf_all["lat"] = gdf_all.geometry.centroid.y

hail_gdf = hail_gdf.copy()
hail_gdf["lon"] = hail_gdf.geometry.x
hail_gdf["lat"] = hail_gdf.geometry.y

# --- Layer toggles ---
show_ownership = st.checkbox("Show Vehicle Ownership Density", value=True)
show_risk = st.checkbox("Show Hail Risk Score", value=True)
show_hail = st.checkbox("Show Hail Reports", value=True)

layers = []

if show_ownership:
    layers.append(
        pdk.Layer(
            "ScatterplotLayer",
            data=gdf_all,
            get_position='[lon, lat]',
            get_fill_color='[255, (1 - car_ownership_density / 1000) * 255, 0, 180]',
            get_radius=2000,
            pickable=True,
            auto_highlight=True,
        )
    )

if show_risk:
    layers.append(
        pdk.Layer(
            "ScatterplotLayer",
            data=gdf_all[gdf_all["hail_risk_score"] > 0],
            get_position='[lon, lat]',
            get_fill_color='[255, 0, (1 - hail_risk_score / 1000) * 255, 160]',
            get_radius=2500,
            pickable=True,
            auto_highlight=True,
        )
    )

if show_hail:
    layers.append(
        pdk.Layer(
            "ScatterplotLayer",
            data=hail_gdf,
            get_position='[lon, lat]',
            get_fill_color='[0, 0, 255, 160]',
            get_radius=300,
            pickable=True,
            auto_highlight=True,
        )
    )

# --- View settings ---
midpoint = [gdf_all["lat"].mean(), gdf_all["lon"].mean()]
view_state = pdk.ViewState(latitude=midpoint[0], longitude=midpoint[1], zoom=6, pitch=0)

st.pydeck_chart(pdk.Deck(
    layers=layers,
    initial_view_state=view_state,
    tooltip={"text": "Lat: {lat}\nLon: {lon}"}
))

'''
import streamlit as st
import geopandas as gpd
import pydeck as pdk
import os

# --- Constants ---
PROCESSED_FOLDER = "census_data"
STATE_OPTIONS = ["MO", "KS", "IA", "NE"]

# --- UI Controls ---
st.title("Vehicle Ownership Density by State")
selected_state = st.selectbox("Choose a state to display vehicle density:", STATE_OPTIONS)

# --- Load GeoData ---
geojson_path = f"{PROCESSED_FOLDER}/gdf_{selected_state}_with_hail_risk.geojson"
if not os.path.exists(geojson_path):
    st.warning(f"GeoJSON for {selected_state} not found. Please run the data generation script.")
    st.stop()

# --- Load GeoDataFrame ---
gdf = gpd.read_file(geojson_path)
gdf = gdf.to_crs(epsg=4326)

# --- Add color scale ---
gdf["fill_color"] = gdf["car_ownership_density"].fillna(0).apply(
    lambda x: [255, max(0, 255 - int(x * 2)), 0, 100] if x < 100 else [255, 0, 0, 150]
)
# Tooltip load
gdf["tooltip_text"] = gdf["car_ownership_density"].apply(lambda x: f"Density: {x:.2f}")
# --- Convert polygons to JSON-like format ---
def polygon_to_coords(geom):
    if geom.geom_type == "Polygon":
        return [list(geom.exterior.coords)]
    elif geom.geom_type == "MultiPolygon":
        return [list(p.exterior.coords) for p in geom.geoms]
    else:
        return []

gdf["coordinates"] = gdf.geometry.apply(polygon_to_coords)

# --- Pydeck Polygon Layer ---
polygon_layer = pdk.Layer(
    "PolygonLayer",
    data=gdf,
    get_polygon="coordinates",
    get_fill_color="fill_color",
    pickable=True,
    auto_highlight=True,
)

# --- View ---
midpoint = gdf.geometry.centroid.unary_union.centroid
view_state = pdk.ViewState(
    latitude=midpoint.y,
    longitude=midpoint.x,
    zoom=6,
    pitch=0
)

# --- Deck ---
r = pdk.Deck(
    layers=[polygon_layer],
    initial_view_state=view_state,
    tooltip={"text": "{tooltip_text}"}
)

st.pydeck_chart(r, use_container_width=True, height=800)
