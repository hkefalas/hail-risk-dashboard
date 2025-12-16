# Hail Risk Dashboard

This project provides a data processing pipeline and an interactive web dashboard to analyze and visualize hail storm risk in relation to demographic and economic data across several US states.

## Features

- **Automated Data Pipeline**: Downloads the latest hail reports from the NOAA, processes them, and merges them with census data for vehicle ownership, income, and population.
- **Interactive Dashboard**: A Streamlit application that visualizes the processed data on an interactive map, allowing users to explore various metrics like vehicle density, population density, and a calculated hail risk score.
- **Modular and Configurable**: The project is built with a modular structure that separates concerns (data downloading, loading, processing) and uses a central configuration file (`config.py`) for easy management of paths, states, and other parameters.

## Project Structure

The repository is organized into the following key directories and files:

```
.
├── pipeline/
│   ├── download_data.py    # Downloads hail reports
│   ├── load_data.py        # Loads all source data
│   ├── process_data.py     # Core data processing and analysis
│   └── main.py             # Main entry point to run the pipeline
├── census_data/
│   ├── tracts/             # Census tract shapefiles
│   └── vehicle_ownership/  # Vehicle ownership data
├── config.py               # Central configuration file
├── utils.py                # Shared utility functions (logging, file I/O)
├── streamlit_app.py        # The Streamlit web application
├── requirements.txt        # Python dependencies
└── README.md               # This file
```

## Setup and Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd hail-risk-dashboard
    ```

2.  **Create and activate a virtual environment (recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

3.  **Install the required dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## How to Run

There are two main steps to use this project: running the data pipeline and launching the web app.

### 1. Run the Data Pipeline

First, you need to run the pipeline to download the latest hail data and process it. This will generate the GeoJSON files required by the dashboard.

Execute the following command from the root of the project directory:

```bash
python pipeline/main.py
```

This will create the processed data files in the `census_data/processed/` directory.

### 2. Launch the Streamlit Dashboard

Once the pipeline has finished successfully, you can launch the interactive dashboard.

Run the following command:

```bash
streamlit run streamlit_app.py
```

This will open the dashboard in your web browser, where you can select a state and a data layer to visualize.

## Configuration

The project can be configured by modifying the `config.py` file. Here you can change:
- The list of states to process (`STATES` dictionary).
- File paths and directories.
- The URL for the hail data.
- Map visualization settings.
