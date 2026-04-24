# MSBA 305 Health Sensor Data Processing Pipeline

A full batch data processing pipeline combining wearable sensor data, weather, and activity intensity references into a query-ready analytical layer, plus a Streamlit app for calorie estimation and activity recommendations.

**Team:** Catalina Sara, Charbel El-Fakhry, Rayya Sarieddine, Talia Taha, Yasmin El-Souki  
**Course:** MSBA 305 - Data Processing Framework, Spring 2025/2026  
**Instructor:** Dr. Ahmad El-Hajj

## What this project does

The pipeline ingests four data sources (HARTH and HAR70+ wearable sensor datasets, Open-Meteo historical weather, and the 2024 Compendium of Physical Activities), cleans and standardises them, segments the raw 50Hz sensor stream into 2-second activity windows, enriches each window with weather context and intensity-calibrated MET values, and loads the result into a SQLite star schema. The Streamlit app reads from that database to estimate calories and recommend activities.
## Database Output

The final output of the pipeline is `pipeline.db`, a SQLite database structured as a star schema.

Main tables include:
- `fact_window` - activity windows with MET and weather context  
- `dim_activity` - standardized activity labels  
- `dim_subject` - participant metadata  
- `dim_weather_hour` - hourly environmental conditions  

This database supports all analytical queries used in the report and Streamlit dashboard.
## Repository structure

```
Data-Processing-Project/
├── README.md
├── requirements.txt
├── notebook/
│   └── data_processing_pipeline.ipynb
├── app/
│   ├── app.py
│   └── config.toml
├── data/
│   ├── met_lookup.csv
│   ├── weather_raw.json
│   └── sample/
│       ├── harth_S006_sample.csv
│       └── har70plus_501_sample.csv
└── .devcontainer/
```
## Streamlit Application Purpose - BurnWise

Burnwise demonstrates how processed data can be used in an interactive decision-support tool. Users can:

- Input age, weight, and calorie goals  
- Select an activity  
- Estimate calories burned  
- Explore activity recommendations
- Explore visulization dashboard and insights 

## Running the Streamlit app

**Requirements:** Python 3.10 or higher.
1. Clone the repository:
```
   git clone https://github.com/catalinasara/health-sensor-pipeline.git
   cd health-sensor-pipeline
```

2. Install dependencies:
```
   pip install -r requirements.txt
```

3. Run the app:
```
   streamlit run app/app.py
```
The app opens at http://localhost:8501. On first run it downloads `pipeline.db` from the v1.1.0 release (around 63 MB, one-time only).

## Running the pipeline notebook

The notebook in `notebook/data_processing_pipeline.ipynb` runs end-to-end against the full raw datasets.

**Quick inspection (no raw data download needed):** open the notebook. All cells already contain their outputs from the last full run, including data quality reports, intermediate statistics, and final query results. This lets any reader verify the pipeline worked without re-running anything.

**Full reproduction:**

1. Download the raw datasets from UCI Machine Learning Repository:
   - HARTH: https://doi.org/10.24432/C5NC90
   - HAR70+: https://doi.org/10.24432/C5CW3D
2. Extract both ZIP archives.
3. Place the CSVs into `data/harth/` and `data/har70plus/` respectively.
4. Open the notebook in Jupyter or Google Colab.
5. Run sections in order. Each section produces artifacts consumed by later sections.

A small sample of raw data (one subject from each cohort, first 10,000 rows each) is included in `data/sample/` for quick structural inspection without the full download.

**Outputs produced by a full run:** cleaned CSVs, `weather_raw.json` (Open-Meteo archive response), `met_lookup.csv`, and `pipeline.db` (the final SQLite database consumed by the app).

## Dependencies

Streamlit app:
- streamlit, pandas, plotly

Pipeline notebook additionally uses:
- numpy, requests, beautifulsoup4 (for the MET Compendium scrape), matplotlib (EDA plots)

All dependencies required to run both the pipeline and the Streamlit app are listed in `requirements.txt`. Installing this file recreates the full working environment.

## External data sources

- **HARTH and HAR70+:** UCI Machine Learning Repository, CC BY 4.0
- **Open-Meteo:** Free historical and forecast weather API, no API key required
- **2024 Compendium of Physical Activities:** Ainsworth et al., https://pacompendium.com

## AI usage

This project was developed with AI assistance, documented in full in the Architecture Report (Section 5, "AI Usage"). Every AI interaction was verified against the actual code and data before inclusion.

## License

Academic use only. The HARTH and HAR70+ datasets are CC BY 4.0 and require attribution to NTNU.
