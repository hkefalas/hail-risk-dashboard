

import streamlit as st
import json
import pydeck as pdk
import os

# --- Constants ---
PROCESSED_FOLDER = "census_data"
STATE_OPTIONS = ["MO", "KS", "IA", "NE"]
LAYER_OPTIONS = {
    "Vehicle Ownership Density": "car_ownership_density",
    "Population Density": "population_density",
    "Median Income": "median_income",
    "Per Capita Income": "per_capita_income"
}

COLOR_FUNCTIONS = {
    "car_ownership_density": lambda x: [255, max(0, 255 - int(x * 2)), 0, 100] if x < 100 else [255, 0, 0, 150],
    "population_density": lambda x: [128 + min(127, int(x / 2)), 0, 128 + min(127, int(x / 2)), 120],
    "median_income": lambda x: [min(255, int(x / 200)), min(255, int(x / 400)), 255, 120],
    "per_capita_income": lambda x: [0, min(255, int(x / 300)), 0, 120],
}

# --- UI Controls ---
st.title("Hail Risk Dashboard")
selected_state = st.selectbox("Choose a state:", STATE_OPTIONS, index=0)
selected_layer = st.selectbox("Select layer to visualize:", list(LAYER_OPTIONS.keys()), index=0)

# --- Load GeoJSON Data ---
geojson_path = f"{PROCESSED_FOLDER}/gdf_{selected_state}_with_hail_risk.geojson"
if not os.path.exists(geojson_path):
    st.warning(f"GeoJSON for {selected_state} not found. Please run the data generation script.")
    st.stop()

with open(geojson_path, "r") as f:
    data = json.load(f)

field = LAYER_OPTIONS[selected_layer]
color_fn = COLOR_FUNCTIONS[field]

polygon_data = []
for feature in data["features"]:
    props = feature["properties"]
    geometry = feature["geometry"]
    value = props.get(field, 0)
    coords = geometry["coordinates"]

    polygon_data.append({
        "coordinates": coords,
        "fill_color": color_fn(value),
        "tooltip_text": f"{selected_layer}: {value:,.2f}"
    })

# --- Pydeck Polygon Layer ---
polygon_layer = pdk.Layer(
    "PolygonLayer",
    data=polygon_data,
    get_polygon="coordinates",
    get_fill_color="fill_color",
    pickable=True,
    auto_highlight=True,
)

# --- View Setup (Missouri Default) ---
state_centers = {
    "MO": (38.5, -92.5),
    "KS": (38.5, -98.0),
    "IA": (42.0, -93.0),
    "NE": (41.5, -99.5)
}
lat, lon = state_centers[selected_state]
view_state = pdk.ViewState(latitude=lat, longitude=lon, zoom=6, pitch=0)

# --- Render ---
r = pdk.Deck(
    layers=[polygon_layer],
    initial_view_state=view_state,
    tooltip={"text": "{tooltip_text}"}
)

st.pydeck_chart(r, use_container_width=True, height=800)

