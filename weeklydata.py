from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine, inspect
from sqlalchemy.types import SMALLINT
from airflow.utils import timezone
import pendulum
from datetime import datetime, timedelta
import pandas as pd
import requests
import os
import logging
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
import math

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
BATCH_SIZE = 200  # Data elements per API request
WEEK_BATCH_SIZE = 26  # Weeks per processing batch
DHIS2_PAT = Variable.get("DHIS2_PAT")
DHIS2_URL = Variable.get("DHIS2_BASE_URL")
HEADERS = {"Authorization": f"ApiToken {DHIS2_PAT}"}
API_ENDPOINT_ANALYTICS = "api/analytics.json"

# File paths
DATA_DIR = "<>"
EXPORTS_DIR = "<>"
INDICATORS_FILE = os.path.join(DATA_DIR, "data_elements.csv")
OUTPUT_FILE = os.path.join(EXPORTS_DIR, "nd2_data.csv")

# DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 20),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

logger.info("DAG configure successfully")

dag = DAG(
    'nd2_data_v1_postgres',
    default_args=default_args,
    description='ND2 data pipeline with EPI week handling and advanced backfilling',
    schedule_interval='0 3 * * 4',  # Run every Thursday at 3 AM
    catchup=False
)

def week_to_date(week_str):
    """Convert DHIS2 week format (YYYYWn) to Monday-based date"""
    year = int(week_str[:4])
    week = int(week_str[5:])  # Handles both single and double digit weeks
    
    # Find first Monday of the year
    jan1 = datetime(year, 1, 1)
    first_monday = jan1 + timedelta(days=(0 - jan1.weekday()) % 7)
    
    # Calculate week start date
    return first_monday + timedelta(weeks=week-1)

def date_to_epi_week(date_obj):
    """Convert datetime to DHIS2 week format (YYYYWn) without leading zeros"""
    # Find first Monday of the year
    jan1 = datetime(date_obj.year, 1, 1)
    first_monday = jan1 + timedelta(days=(0 - jan1.weekday()) % 7)
    
    # Calculate week number
    delta = (date_obj - first_monday).days
    week_number = (delta // 7) + 1
    
    # Handle year boundary cases
    if week_number < 1:
        prev_year_date = date_obj - timedelta(weeks=52)
        return f"{prev_year_date.year}W{52}"
    if week_number > 52:
        next_year_date = date_obj + timedelta(weeks=52)
        return f"{next_year_date.year}W1"
    
    return f"{date_obj.year}W{week_number}"


### Weekly data
def get_last_n_weeks(n=1):
    """Get last n EPI weeks in DHIS2 format (YYYYWn) based on current date"""
    weeks = []
    today = datetime.now()
    
    for i in range(n, 0, -1):
        # Calculate date for each historical week
        target_date = today - timedelta(weeks=i)
        
        # Convert to EPI week format using existing function
        week_str = date_to_epi_week(target_date)
        weeks.append(week_str)
    
    logger.info(f"Last {n} weeks: {weeks}")
    return weeks

# Multiple Weeks
def generate_weeks(start_year, start_week=None, end_year=None, end_week=None):
    """Generate EPI weeks in DHIS2 format for various input types"""
    def _to_date(year, week):
        return week_to_date(f"{year}W{week}")

    # Determine date range
    if start_week and end_year and end_week:
        # Specific week range
        start_date = _to_date(start_year, start_week)
        end_date = _to_date(end_year, end_week)
    elif not start_week and end_year and not end_week:
        # Year range
        start_date = _to_date(start_year, 1)
        end_date = _to_date(end_year, 52)
    else:
        # Single week
        start_date = end_date = _to_date(start_year, start_week)

    # Generate all Mondays in range
    current_date = start_date
    weeks = []
    while current_date <= end_date:
        week_str = date_to_epi_week(current_date)
        weeks.append(week_str)
        current_date += timedelta(weeks=1)

    # Remove duplicates and sort descending
    unique_weeks = list(dict.fromkeys(weeks))  # Preserve order while deduping
    return sorted(unique_weeks,
                key=lambda x: (int(x.split('W')[0]), int(x.split('W')[1])), 
                reverse=True)


def fetch_data_batch(data_elements, periods):
    """Fetch data in batches from DHIS2 API"""
    all_rows = []
    params_base = {
        "skipMeta": "true",
        "includeNumDen": "false",
        "dimension": ["ou:LEVEL-4"]
    }

    # Batch data elements
    num_batches = math.ceil(len(data_elements) / BATCH_SIZE)
    for batch_idx in range(num_batches):
        batch_start = batch_idx * BATCH_SIZE
        batch_elements = data_elements[batch_start:batch_start+BATCH_SIZE]
        
        # Batch periods
        num_week_batches = math.ceil(len(periods) / WEEK_BATCH_SIZE)
        for week_batch_idx in range(num_week_batches):
            week_start = week_batch_idx * WEEK_BATCH_SIZE
            batch_weeks = periods[week_start:week_start+WEEK_BATCH_SIZE]
            
            try:
                response = requests.get(
                    f"{DHIS2_URL}{API_ENDPOINT_ANALYTICS}",
                    params={
                        "dimension": [
                            f"dx:{';'.join(batch_elements)}",
                            f"pe:{';'.join(batch_weeks)}",
                            "ou:LEVEL-4"
                        ],
                        "skipMeta": "true",
                        "includeNumDen": "false"
                    },
                    headers=HEADERS,
                    timeout=300
                )
                response.raise_for_status()
                data = response.json()
                if 'rows' in data:
                    all_rows.extend(data['rows'])
                logger.info(f"Batch {batch_idx+1}-{week_batch_idx+1} fetched {len(data.get('rows', []))} rows")
            except Exception as e:
                logger.error(f"Batch {batch_idx+1}-{week_batch_idx+1} failed: {str(e)}")
    
    if not all_rows:
        return pd.DataFrame()
    
    columns = [h['name'] for h in data.get('headers', [])]
    return pd.DataFrame(all_rows, columns=columns)

def create_complete_dataset(raw_df, indicators_df, periods):
    """Create sanitized dataset with proper typing and sorting"""
    try:
        # Sort indicators alphabetically
        all_indicators = sorted(indicators_df['name'].unique())
        
        if raw_df.empty:
            return pd.DataFrame(columns=['period', 'org_id', 'date'] + all_indicators)

        # Merge and process data
        merged = raw_df.merge(
            indicators_df[['id', 'name']],
            left_on='dx',
            right_on='id',
            how='left'
        ).drop(columns=['id']).fillna(0)

        # Pivot with proper null handling
        pivoted = merged.pivot_table(
            index=['pe', 'ou'],
            columns='name',
            values='value',
            aggfunc='first',
            fill_value=0
        ).reset_index()

        # Generate complete facility-week matrix
        facilities = merged['ou'].unique()
        week_facility_combos = [(p, ou) for p in periods for ou in facilities]
        base_df = pd.DataFrame(week_facility_combos, columns=['period', 'org_id'])
        base_df['date'] = base_df['period'].apply(week_to_date)

        # Merge with pivoted data
        complete_df = base_df.merge(
            pivoted.rename(columns={'pe': 'period', 'ou': 'org_id'}),
            on=['period', 'org_id'],
            how='left'
        ).fillna(0)

        # Ensure all columns exist
        for indicator in all_indicators:
            if indicator not in complete_df:
                complete_df[indicator] = 0

        # Explicitly convert indicator columns to int16 by rounding values first
        for indicator in all_indicators:
            complete_df[indicator] = (
                pd.to_numeric(complete_df[indicator], errors='coerce')
                .fillna(0)
                .round(0)
                .astype('int16')
            )

        complete_df = complete_df[['period', 'org_id', 'date'] + all_indicators]\
                       .sort_values(['date', 'org_id'], ascending=[False, True])

        return complete_df

    except Exception as e:
        logger.error(f"Dataset creation failed: {str(e)}")
        return pd.DataFrame()


def update_database(df, engine, weeks):
    """Update PostgreSQL database with new data"""
    try:
        # Check if table exists first
        inspector = inspect(engine)
        table_exists = inspector.has_table('nd2_data')
        
        with engine.begin() as conn:
            # Create table if it doesn't exist
            if not table_exists:
                logger.info("Creating nd2_data table as it doesn't exist")
                # Sort DataFrame by date and org_id
                df.sort_values(['date', 'org_id'], ascending=[False, True], inplace=True)
                # Create table using DataFrame structure
                df.to_sql(
                    'nd2_data',
                    con=conn,
                    if_exists='replace',  # Creates new table
                    index=False,
                    dtype={col: SMALLINT() for col in df.columns if col not in ['period', 'org_id', 'date']}
                )
                logger.info("nd2_data table created successfully")
            else:
                # Clear existing data in range if weeks are provided
                if weeks:
                    min_week = min(weeks, key=lambda x: (int(x.split('W')[0]), int(x.split('W')[1])))
                    max_week = max(weeks, key=lambda x: (int(x.split('W')[0]), int(x.split('W')[1])))
                    logger.info(f"Clearing data for period range: {min_week} to {max_week}")
                    conn.execute(f"""
                        DELETE FROM nd2_data 
                        WHERE period BETWEEN %s AND %s
                    """, (min_week, max_week))
            
                # Insert new data if not empty and table already existed
                if not df.empty:
                    df.to_sql(
                        'nd2_data',
                        con=conn,
                        if_exists='append',
                        index=False,
                        method='multi',
                        chunksize=1000,
                        dtype={col: SMALLINT() for col in df.columns if col not in ['period', 'org_id', 'date']}
                    )
                    logger.info(f"Added {len(df)} records to PostgreSQL")
            
    except Exception as e:
        logger.error(f"Database update failed: {str(e)}")
        raise

def update_csv(df, target_file):
    """Update CSV file with new data, handling duplicates"""
    try:
        if os.path.exists(target_file):
            # Read existing data
            existing_df = pd.read_csv(target_file)
            
            # Combine with new data
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            
            # Remove duplicates, keeping newest version
            combined_df = combined_df.drop_duplicates(['period', 'org_id'], keep='last')
            
            # Sort and save
            combined_df.sort_values(['date', 'org_id'], ascending=[False, True], inplace=True)
            combined_df.to_csv(target_file, index=False)
            logger.info(f"Updated CSV with {len(df)} new records, total: {len(combined_df)}")
        else:
            # Create new file
            df.to_csv(target_file, index=False)
            logger.info(f"Created new CSV with {len(df)} records")

    except Exception as e:
        logger.error(f"CSV update failed: {str(e)}")
        raise

def backfill_historical_data(**kwargs):
    """Flexible backfill with EPI week handling"""
    try:
        # Load indicators
        indicators_df = pd.read_csv(INDICATORS_FILE)
        data_elements = indicators_df['id'].tolist()
        
        # Generate weeks based on input parameters
        weeks = generate_weeks(
            start_year=kwargs.get('start_year'),
            start_week=kwargs.get('start_week'),
            end_year=kwargs.get('end_year'),
            end_week=kwargs.get('end_week')
        )
        logger.info(f"Processing {len(weeks)} weeks from {weeks[0]} to {weeks[-1]}")

        # Process in weekly batches
        all_data = []
        for i in range(0, len(weeks), WEEK_BATCH_SIZE):
            batch_weeks = weeks[i:i+WEEK_BATCH_SIZE]
            logger.info(f"Processing batch {i//WEEK_BATCH_SIZE+1}: {batch_weeks[0]} to {batch_weeks[-1]}")
            
            batch_df = fetch_data_batch(data_elements, batch_weeks)
            if not batch_df.empty:
                processed = create_complete_dataset(batch_df, indicators_df, batch_weeks)
                all_data.append(processed)

        if not all_data:
            logger.error("No data collected during backfill")
            return

        final_df = pd.concat(all_data, ignore_index=True)
        
        # Save to CSV
        update_csv(final_df, OUTPUT_FILE)
        
        # Update PostgreSQL
        hook = PostgresHook(postgres_conn_id="superset_db")
        engine = hook.get_sqlalchemy_engine()
        update_database(final_df, engine, weeks)
        
        logger.info(f"Backfill completed for {len(final_df)} records")

    except Exception as e:
        logger.error(f"Backfill failed: {str(e)}")
        raise

def process_and_append_data(**context):
    """Weekly update process with null handling"""
    try:
        # Fetch new data
        logger.info("Fetching latest data...")
        indicators_df = pd.read_csv(INDICATORS_FILE)
        data_elements = indicators_df['id'].tolist()
        weeks = get_last_n_weeks(1)  # Get last week's data
        df = fetch_data_batch(data_elements, weeks)
        
        if df.empty:
            logger.error("No data fetched")
            return

        # Process data
        processed_df = create_complete_dataset(df, indicators_df, weeks)
        if processed_df.empty:
            logger.error("Data processing failed")
            return

        # Update CSV
        update_csv(processed_df, OUTPUT_FILE)
        logger.info(f"Saved CSV to {OUTPUT_FILE}")
        
        # Update PostgreSQL
        hook = PostgresHook(postgres_conn_id="superset_db")
        engine = hook.get_sqlalchemy_engine()
        update_database(processed_df, engine, weeks)
        logger.info(f"Saved to Postgres DB")
        
        logger.info(f"Weekly update completed for {len(processed_df)} records")

    except Exception as e:
        logger.error(f"Update failed: {str(e)}")
        raise

# Define tasks
start_task = EmptyOperator(task_id='start', dag=dag)

fetch_weekly_task = PythonOperator(
    task_id='fetch_weekly_data',
    python_callable=process_and_append_data,
    dag=dag,
)

backfill_task = PythonOperator(
    task_id='backfill_data',
    python_callable=backfill_historical_data,
    op_kwargs={
        'start_year': 2025,
        'start_week': 1,
        'end_year': 2025,
        'end_week': 10
    },
    dag=dag,
    trigger_rule='none_failed',
)


# Other backfills:

# # Single week
# PythonOperator(
#     task_id='backfill_single_week',
#     python_callable=backfill_historical_data,
#     op_kwargs={'start_year': 2024, 'start_week': 1},
#     dag=dag
# )

# # Week range
# PythonOperator(
#     task_id='backfill_week_range',
#     python_callable=backfill_historical_data,
#     op_kwargs={'start_year': 2024, 'start_week': 1, 'end_year': 2025, 'end_week': 11},
#     dag=dag
# )

# # Year range
# PythonOperator(
#     task_id='backfill_year_range',
#     python_callable=backfill_historical_data,
#     op_kwargs={'start_year': 2020, 'end_year': 2025},
#     dag=dag
# )



end_task = EmptyOperator(task_id='end', dag=dag)

# Set dependencies
start_task >> [fetch_weekly_task, backfill_task] >> end_task