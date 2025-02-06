from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import os
import logging
from sqlalchemy import create_engine, Column, String, Date, Integer, func, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine import reflection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

username = "DHIS2_USERNAME"
password = "DHIS2_PASSWORD"
base_url = "https://eidsr.znphi.co.zm/"

db_name = ""
db_user = ""
db_pass = ""

db_url = f"postgresql+psycopg2://{db_name}:{db_pass}@pgdb2:5432/{db_name}"


# engine = create_engine(db_url)
# Session = sessionmaker(bind=engine)
# Base = declarative_base()
# inspector = inspect(engine)

# class Alert(Base):
#     __tablename__ = 'alerts'
#     trackedEntityInstance = Column(String, primary_key=True)
#     disease_name = Column(String)
#     alert_disease = Column(String)
#     alert_id = Column(String)
#     notificationDate = Column(Date)
#     org_unit_name = Column(String)
#     org_unit_id = Column(String)
#     week = Column(String)
#     status = Column(String)

# def save_alert_to_db(trackedEntityInstance, disease_name, alert_disease, alert_id, enrollmentDate, org_unit_name, org_unit_id, week):
#     with Session() as session:
#         alert = session.query(Alert).filter_by(trackedEntityInstance=trackedEntityInstance).first()

#         # If alert exists, update it; otherwise, create a new one
#         if alert:
#             alert.disease_name = disease_name
#             alert.alert_disease = alert_disease
#             alert.alert_id = alert_id
#             alert.notificationDate = enrollmentDate
#             alert.org_unit_name = org_unit_name
#             alert.org_unit_id = org_unit_id
#             alert.week = week
#         else:
#             alert = Alert(
#                 trackedEntityInstance=trackedEntityInstance,
#                 disease_name=disease_name,
#                 alert_disease=alert_disease,
#                 alert_id=alert_id,
#                 notificationDate=enrollmentDate,
#                 org_unit_name=org_unit_name,
#                 org_unit_id=org_unit_id,
#                 week=week,
#                 status="VERIFICATION_STATUS_PENDING"
#             )
#             session.add(alert)        
#         session.commit()

def week_to_date(week_str):
    year, week = int(week_str[:4]), int(week_str[5:])
    first_day_of_year = datetime(year, 1, 1)
    first_week_start = first_day_of_year - timedelta(days=first_day_of_year.weekday())
    week_start_date = first_week_start + timedelta(weeks=week - 1)
    return week_start_date


def fetch_and_transform_data():
    logger.info(f'Reporting LIVE FROM The SKY')
    # logger.info(os.listdir())
    ind_file_path = '/data/nd2_des.csv'  # Adjust this path as needed

    url = f'{base_url}/analytics.json'

    try:
        de_df = pd.read_csv(ind_file_path)
        logger.info("CSV file loaded successfully.")
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        return None
    except Exception as e:
        logger.error(f"Error loading CSV file: {e}")
        return None

    dx_ids = ';'.join(de_df['id'].tolist())
    ou_level = "LEVEL-MQLiogB9XBV"  #Dz7Sm3imvLU
    params = {
        "dimension": f"dx:{dx_ids},pe:LAST_5_YEARS,ou:{ou_level}"
    }

    try:
        response = requests.get(url, params=params, auth=HTTPBasicAuth(username, password))
        response.raise_for_status()
        logger.info("Data fetched from DHIS2 successfully.")
        data = response.json()
    except requests.exceptions.HTTPError as err:
        logger.error(f"HTTP error occurred: {err}")
        return None
    except requests.exceptions.RequestException as err:
        logger.error(f"Request error occurred: {err}")
        return None

    try:
        headers = data.get('headers', [])
        rows = data.get('rows', [])
        columns = [header['name'] for header in headers]
        df = pd.DataFrame(rows, columns=columns)

        df['date'] = df['pe'].apply(week_to_date)
        df.sort_values('date', ascending=False, inplace=True)

        df['value'] = pd.to_numeric(df['value'], downcast='integer', errors='coerce')
        df = df.merge(de_df, left_on='dx', right_on='id', how='left')
        df['dx'] = df['name']
        df.drop(['name', 'id'], axis=1, inplace=True)

        pivoted_df = df.pivot_table(index=['pe', 'ou', 'date'], columns='dx', values='value', aggfunc='first')
        pivoted_df.reset_index(inplace=True)
        pivoted_df.fillna(0, inplace=True)
        pivoted_df.rename(columns={'pe': 'period', 'ou': 'org_id'}, inplace=True)

        # Ensure specific columns are numeric, get column named from df.
        excluded_columns = ['dx', 'period', 'org_id', 'date']
        columns_to_convert = [col for col in pivoted_df.columns if col not in excluded_columns]

        for column in columns_to_convert:
            pivoted_df[column] = pd.to_numeric(pivoted_df[column], downcast='integer', errors='coerce')

        return pivoted_df

    except Exception as e:
        logger.error(f"Error processing data: {e}")
        return None


def process_data():
    logger.info("Preparing data...")
    df = fetch_and_transform_data()
    if df is None:
        logger.error("Data processing failed. Skipping saving steps.")
        return

    output_file = 'nd2_data_pivoted.csv'
    df.to_csv(output_file, index=False)
    logger.info("Data saved to CSV successfully.")

    # try:
    #     logger.info("Saving data to PostgreSQL...")
    #     # df.to_sql(name='nd2_data', con=connection, if_exists='append', index=False)
    #     # df.csv('final_data.csv', index=False)
    #     logger.info("Data saved to PostgreSQL successfully.")
    # except Exception as e:
    #     logger.error(f"Error saving data to PostgreSQL: {e}")


