
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
