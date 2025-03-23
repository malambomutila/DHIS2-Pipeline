# DHIS2 Analytics Pipeline

## Overview

The **DHIS2 Analytics Pipeline** automates the extraction, transformation, and analysis of health data from a DHIS2 instance. This project focuses on retrieving facility-level weekly data, enriching it with organisational unit details, and exporting the processed data in a structured format for further reporting and visualisation.

## Features

- **Automated Data Extraction:** Queries DHIS2 API to fetch organisational units at facility level.
- **Facility-Level Aggregation:** Aggregates health data weekly.
- **CSV and Postgres Export:** Outputs processed data to CSV files and writes to Postgres for easy access and analysis.

## Installation

1. Clone the repository:
   ```sh
   git clone https://github.com/malambomutila/DHIS2-Pipeline.git
   ```
2. Navigate to the project directory:
   ```sh
   cd DHIS2-Pipeline
   ```
3. Install required dependencies but note that the requirements file contains other additional packages that were used to run Airflow so you might not need them and they might conflict with your Airflow setup. Install only the libraries used in the script.
   ```sh
   pip install -r requirements.txt
   ```

## Usage

1. Add the script to your Airflow DAGS folder and run it from therequerydata.
2. Processed data will be saved your specificied irectory as CSV files and also written to your Postgres database.

## Output Files

The following CSV files are generated by the pipeline:

- data_v1.csv: Weekly facility-level data for period defined.

## Contributions

Contributions are welcome! Feel free to submit issues, feature requests, or pull requests to improve the project.
