# ------ Import Packages and Libraries -----------------------------------------------------------
import pandas as pd
import requests
import json
from pathlib import Path
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
import logging
import ast
import math

# ------ Directories, Credentials and Data Fetching and Transformation ---------------------------
class DHIS2DataExtractor:
    def __init__(self, base_dir=None):
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Setup paths
        self.base_dir = Path.cwd().parent if base_dir is None else Path(base_dir)
        self.credentials_path = self.base_dir / '00_Local' / '01_Configs' / 'credentials.txt'
        self.data_dir = self.base_dir / '00_Local' / '02_Data'
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # DHIS2 configurations
        self.DHIS2_URL = None
        self.PAT = None
        self.HEADERS = None
        self.API_ENDPOINT_ANALYTICS = "/api/40/analytics.json"
        self.API_ENDPOINT_DATA_ELEMENTS = "/api/40/dataElements.json"
        self.BATCH_SIZE = 200
        
        # Initialize configurations
        self._load_credentials()
        
    def _load_credentials(self):
        # Load DHIS2 credentials from text file.
        try:
            credentials = {}
            with self.credentials_path.open('r') as file:
                for line in file:
                    if '=' in line:
                        key, value = line.strip().split('=', 1)
                        credentials[key.strip()] = value.strip()
            
            self.DHIS2_URL = credentials.get('DHIS2_URL')
            self.PAT = credentials.get('PAT')
            self.HEADERS = {"Authorization": f"ApiToken {self.PAT}"}
            self.logger.info(f"Credentials loaded successfully. DHIS2 instance: {self.DHIS2_URL}")
            
        except Exception as e:
            self.logger.error(f"Error loading credentials: {str(e)}")
            raise
    
    def get_week_range(self, year=None, week=None):
        # Gets the start and end dates for a specific week.
        # Returns current week if no parameters are provided.
        
        if year is None or week is None:
            today = datetime.today()
            year = today.year
            week = today.isocalendar()[1]
        
        # Formats to DHIS2 Standard
        start_date = datetime.strptime(f"{year}-W{week}-1", "%Y-W%W-%w")
        end_date = start_date + timedelta(days=6)
        return start_date, end_date, f"{year}W{week}"  # DHIS2 period format
    
    def fetch_aggregatable_data_elements(self):
        # Fetches all numeric data elements that allow aggregation.
        response = requests.get(
            f"{self.DHIS2_URL}{self.API_ENDPOINT_DATA_ELEMENTS}?fields=id,valueType,aggregationType&paging=false",
            headers=self.HEADERS
        )
        
        if response.status_code == 200:
            data = response.json()
            valid_data_elements = [
                item["id"]
                for item in data.get("dataElements", [])
                if item.get("valueType") in ["INTEGER", "NUMBER", "PERCENTAGE", "UNIT_INTERVAL"]
                and item.get("aggregationType") in ["SUM", "AVERAGE", "COUNT"]
            ]
            return valid_data_elements
        else:
            self.logger.error(f"Error fetching data elements: {response.status_code}")
            return []

    def fetch_weekly_data(self, year=None, week=None):
        # Fetches data for a specific week or current week if none specified.
        # Returns a DataFrame with the weekly data.
        # Get week period in DHIS2 format
        start_date, end_date, period = self.get_week_range(year, week)
        
        self.logger.info(f"Fetching data for period: {period}")
        
        # Get data elements
        data_element_uids = self.fetch_aggregatable_data_elements()
        if not data_element_uids:
            raise ValueError("No valid data elements found")
        
        # Process in batches
        all_rows = []
        num_batches = math.ceil(len(data_element_uids) / self.BATCH_SIZE)
        
        for batch in range(num_batches):
            batch_uids = data_element_uids[batch * self.BATCH_SIZE : (batch + 1) * self.BATCH_SIZE]
            dx_param = f"dx:{';'.join(batch_uids)}"
            
            params = {
                "dimension": [
                    dx_param,
                    "ou:LEVEL-4",
                    f"pe:{period}"
                ],
                "displayProperty": "NAME",
                "outputIdScheme": "UID",
                "includeMetadata": "true",
                "includeNames": "true",
                "limit": 10000,
                "paging": "true",
                "page": 1
            }
            
            response = requests.get(
                f"{self.DHIS2_URL}{self.API_ENDPOINT_ANALYTICS}",
                params=params,
                headers=self.HEADERS
            )
            
            if response.status_code == 200:
                data = response.json()
                metadata = data.get("metaData", {})
                data_elements = metadata.get("items", {})
                rows = data.get("rows", [])
                
                for row in rows:
                    data_element_id = row[0]
                    org_id = row[1]
                    period = row[2]
                    value = row[3]
                    data_element_name = data_elements.get(data_element_id, {}).get("name", data_element_id)
                    all_rows.append([period, org_id, data_element_name, value])
            
            else:
                self.logger.error(f"Error in batch {batch + 1}: {response.status_code}")
        
        return self._process_data(all_rows, period)
    
    def _process_data(self, rows, period):
        # Process the raw data into a pivoted DataFrame.
        if not rows:
            return pd.DataFrame()
        
        df = pd.DataFrame(rows, columns=["period", "org_id", "data_element", "value"])
        df.drop_duplicates(inplace=True)
        
        # Convert period to date using correct format
        df["date"] = df["period"].apply(lambda x: 
            datetime.strptime(x[:4] + x[5:] + '1', "%G%V%w").strftime("%Y-%m-%dT00:00:00"))
        
        # Pivot and process
        df_pivoted = df.pivot_table(
            index=["period", "org_id", "date"],
            columns="data_element",
            values="value",
            aggfunc="sum"
        ).reset_index()
        
        df_pivoted["date"] = pd.to_datetime(df_pivoted['date'])
        df_pivoted.fillna(0, inplace=True)
        df_pivoted = df_pivoted.astype({col: 'int64' for col in df_pivoted.select_dtypes('float64').columns})
        
        return df_pivoted
    
    def fetch_and_save_weekly_data(self, start_year=None, start_week=None, end_year=None, end_week=None, combine=None):
        # Fetch and save data for a range of weeks.
        # If no parameters provided, fetches current week only.
        
        all_weeks_data = []  # List to store DataFrames if combining

        if start_year is None:
            # Fetch only current week
            df = self.fetch_weekly_data()
            if not df.empty:
                period = df['period'].iloc[0]
                filename = f"nd2_{period}.csv"
                df.to_csv(self.data_dir / filename, index=False)
                self.logger.info(f"Saved data to {filename}")
                return df if combine else None
            return None

        # Handle the week range directly
        if end_year is None or end_week is None:
            end_year = start_year
            end_week = start_week

        # Create list of weeks to process
        weeks_to_process = []
        if start_year == end_year:
            weeks_to_process = [(start_year, week) for week in range(start_week, end_week + 1)]
        else:
            # Handle multi-year ranges if needed
            current_year = start_year
            current_week = start_week
            
            while current_year < end_year or (current_year == end_year and current_week <= end_week):
                weeks_to_process.append((current_year, current_week))
                current_week += 1
                
                # Handle year transition
                if current_year < end_year and current_week > 52:
                    current_year += 1
                    current_week = 1

        # Process each week
        for year, week in weeks_to_process:
            df = self.fetch_weekly_data(year, week)
            if not df.empty:
                filename = f"nd2_{year}W{week}.csv"
                df.to_csv(self.data_dir / filename, index=False)
                self.logger.info(f"Saved data to {filename}")
                
                if combine:
                    all_weeks_data.append(df)
        
        if combine and all_weeks_data:
            combined_df = pd.concat(all_weeks_data, ignore_index=True)
            combined_df.sort_values('period', ascending=False, inplace=True)
            return combined_df
        
        return None

        def update_and_save_dataframes(self, dataframes, df_combined):
            # Directory to save the updated CSV files
            output_dir = self.data_dir

            # Get the list of all columns in the combined DataFrame
            combined_columns = df_combined.columns.tolist()

            # Loop through each DataFrame in the dictionary
            for name, df in dataframes.items():
                # Identify columns missing in the current DataFrame
                missing_columns = set(combined_columns) - set(df.columns)
                
                # Add the missing columns with zeros
                for col in missing_columns:
                    df[col] = 0  # Add the column and fill with 0
                
                # Reorder columns to match the combined DataFrame's column order
                dataframes[name] = df[combined_columns]
                
                # Save the updated DataFrame to a new CSV file
                output_path = output_dir / f"{name}_mcols.csv"
                df.to_csv(output_path, index=False)

                self.logger.info(f"Saved updated DataFrame '{name}' to {output_path}")

            self.logger.info("All updated DataFrames saved.") 
        

# RUN
if __name__ == "__main__":
    extractor = DHIS2DataExtractor()
    
    # Fetch current week only
    # extractor.fetch_and_save_weekly_data()
    
    # Fetch specific week
    # extractor.fetch_and_save_weekly_data(2024, 1)
    # extractor.fetch_and_save_weekly_data(2025, 1)
    
    # Fetch range of weeks
    # extractor.fetch_and_save_weekly_data(2024, 1, 2024, 52)
    # extractor.fetch_and_save_weekly_data(2025, 1, 2025, 6)

    # Fetch and get combined DataFrame and add missing columns
    combined_df = extractor.fetch_and_save_weekly_data(2025, 1, 2025, 6, combine=True)    
    if combined_df is not None:
        # Save combined data if needed
        combined_df.to_csv('nd2_combined.csv', index=False)
        print("Periods in combined data:", sorted(combined_df['period'].unique()))

        # Dictionary of DataFrames (can be created or fetched separately)
        csv_files = list(extractor.data_dir.glob("nd2_2025W*.csv"))
        dataframes = {file.stem: pd.read_csv(file) for file in csv_files}

        # Update individual DataFrames with the combined DataFrame's schema
        extractor.update_and_save_dataframes(dataframes, combined_df)