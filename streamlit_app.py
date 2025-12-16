import streamlit as st
import pandas as pd
import pydeck as pdk
import requests
import os
import json
from config import STATES, LAYER_OPTIONS, PROCESSED_DATA_DIR
from utils import setup_logging, load_geojson

# Setup logger
logger = setup_logging()

# --- UI Controls ---
st.title("Hail Risk Dashboard")

# Use keys from the STATES dictionary for the dropdown
state_options = list(STATES.keys())
selected_state = st.selectbox("Choose a state:", state_options, index=0)

# Use keys from LAYER_OPTIONS for the layer selection
selected_layer = st.selectbox("Select layer to visualize:", list(LAYER_OPTIONS.keys()), index=0)

# --- Load GeoJSON Data ---
geojson_filename = f"gdf_{selected_state}_with_hail_risk.geojson"
geojson_path = os.path.join(PROCESSED_DATA_DIR, geojson_filename)

if not os.path.exists(geojson_path):
    st.warning(f"Processed data for {selected_state} not found at {geojson_path}.")
    st.warning("Please run the data pipeline first by executing 'python pipeline/main.py' in your terminal.")
    st.stop()

try:
    # Use the utility function to load the GeoJSON
    gdf = load_geojson(geojson_path, logger)

    # Pydeck requires a plain dictionary for its data source, so we convert gdf to json
    # This is a common pattern when using pydeck with geopandas
    data = json.loads(gdf.to_json())

except Exception as e:
    st.error(f"An error occurred while loading the data for {selected_state}: {e}")
    logger.error(f"Failed to load or process GeoJSON {geojson_path}: {e}")
    st.stop()


# --- Data Transformation for Pydeck ---
field_to_visualize = LAYER_OPTIONS[selected_layer]

# Define color functions for visualization
def get_color(value, layer):
    # Default color
    color = [200, 200, 200, 100]
    if pd.isna(value):
        return color

    if layer == "car_ownership_density":
        # Green to Red scale
        intensity = min(1, value / 150) # Normalize to 0-1 range, capped at 150
        red = int(255 * intensity)
        green = int(255 * (1 - intensity))
        color = [red, green, 0, 150]
    elif layer == "population_density":
        # Blue scale
        intensity = min(1, value / 1000) # Normalize to 0-1 range, capped at 1000
        blue = int(100 + 155 * intensity)
        color = [0, 0, blue, 150]
    elif layer in ["median_income", "per_capita_income"]:
        # Purple scale
        cap = 100000 if layer == "median_income" else 75000
        intensity = min(1, value / cap)
        purple = int(100 + 155 * intensity)
        color = [purple, 0, purple, 150]
    elif layer == "hail_risk_score":
        # Yellow to Orange/Red scale
        intensity = min(1, value / 500) # Normalize, cap at 500
        red = 255
        green = int(255 * (1 - intensity))
        color = [red, green, 0, 160]

    return color

# Add color and tooltip to each feature
for feature in data["features"]:
    props = feature["properties"]
    value = props.get(field_to_visualize, 0)
    props["fill_color"] = get_color(value, field_to_visualize)
    props["tooltip_text"] = f"{selected_layer}: {value:,.2f}<br>Tract: {props.get('GEOID', 'N/A')}"

    # Handle None/NaN values for tooltip
    if pd.isna(value):
        formatted_value = "N/A"
    else:
        formatted_value = f"{value:,.2f}"
    
    props["tooltip_text"] = f"{selected_layer}: {formatted_value}<br>Tract: {props.get('GEOID', 'N/A')}"

# --- Pydeck Layer ---
polygon_layer = pdk.Layer(
    "GeoJsonLayer",
     data=data,
    get_fill_color="properties.fill_color",
    pickable=True,
    auto_highlight=True,
)

# --- View Setup ---
# Get the center from the config
lat, lon = STATES[selected_state]["center"]
view_state = pdk.ViewState(latitude=lat, longitude=lon, zoom=6, pitch=30)

# --- Render Map ---
r = pdk.Deck(
    layers=[polygon_layer],
    initial_view_state=view_state,
    tooltip={"html": "{tooltip_text}", "style": {"color": "white"}}

)

st.pydeck_chart(r, use_container_width=True)
