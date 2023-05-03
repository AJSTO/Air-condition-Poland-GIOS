import logging
import time
from datetime import datetime
from typing import List, Optional

import pandas as pd
import requests
import yaml

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.oauth2 import service_account

from pydantic import BaseModel

with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)
PROJECT_ID = config['PROJECT_ID']
DATASET_NAME = config['DATASET_NAME']
TABLE_STATIONS = config['TABLE_STATIONS']
TABLE_MEASUREMENTS = config['TABLE_MEASUREMENTS']
JSON_KEY_BQ = config['JSON_KEY_BQ']
KEY_PATH = f"{JSON_KEY_BQ}"
CREDENTIALS = service_account.Credentials.from_service_account_file(
    KEY_PATH,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
CLIENT = bigquery.Client(credentials=CREDENTIALS, project=CREDENTIALS.project_id,)
TABLE_STATIONS_SCHEMA = [
    bigquery.SchemaField("id", "INTEGER"),
    bigquery.SchemaField("stationName", "STRING"),
    bigquery.SchemaField("gegrLat", "FLOAT"),
    bigquery.SchemaField("gegrLon", "FLOAT"),
    bigquery.SchemaField("city", "STRING"),
    bigquery.SchemaField("addressStreet", "STRING"),
    bigquery.SchemaField("district_name", "STRING"),
    bigquery.SchemaField("province", "STRING"),
]
TABLE_MEASUREMENTS_SCHEMA = [
    bigquery.SchemaField("station_id", "INTEGER"),
    bigquery.SchemaField("sensor_id", "INTEGER"),
    bigquery.SchemaField("param_code", "STRING"),
    bigquery.SchemaField("datetime", "DATETIME"),
    bigquery.SchemaField("value", "FLOAT"),
]

logging.basicConfig(
    filename='app.log', level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)


def create_dataset(client, dataset_name):
    """
    Creates a BigQuery dataset with the specified name in the specified client's project.

    Args:
        client: A `bigquery.Client` object representing the BigQuery client to use.
        dataset_name: A string representing the name of the dataset to create.

    Returns:
        None

    Raises:
        google.api_core.exceptions.NotFound: If the dataset is not found.
    """
    # Construct the dataset reference object.
    dataset_ref = client.dataset(dataset_name)

    # Check if the dataset exists, and create it if it doesn't.
    try:
        client.get_dataset(dataset_ref)
        logging.info("Dataset exists: {}.{}".format(client.project, dataset_ref.dataset_id))
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "EU"  # Set the location to your preferred location.
        client.create_dataset(dataset)
        logging.info("Created dataset: {}.{}".format(client.project, dataset_ref.dataset_id))


def create_table(client, dataset_name, table_name, table_schema):
    """
    Creates a new table in a BigQuery dataset if it doesn't already exist.

    Args:
        client: A BigQuery client instance.
        dataset_name: Name of the dataset to create the table in.
        table_name: Name of the table to create.
        table_schema: Schema of the table to create.

    Raises:
        google.api_core.exceptions.NotFound: If the dataset doesn't exist.

    Returns:
        None.
    """
    # Construct the dataset reference object.
    dataset_ref = client.dataset(dataset_name)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "EU"

    # Construct the table measurement reference object.
    table_ref = dataset_ref.table(table_name)

    # Check if the table exists, and create it if it doesn't.
    try:
        client.get_table(table_ref)
        logging.info("Table exists: {}.{}.{}".format(client.project, dataset.dataset_id, table_name))
    except NotFound:
        table = bigquery.Table(table_ref, schema=table_schema)
        table = client.create_table(table)
        logging.info("Created table: {}.{}.{}".format(client.project, dataset.dataset_id, table.table_id))


def upload_dataframe_to_bq(client, dataset_name, table_name, df):
    """
    Uploads a Pandas DataFrame to a BigQuery table.

    Args:
        client: A BigQuery client object.
        dataset_name: The name of the dataset in which to create the table.
        table_name: The name of the table to create.
        df: The Pandas DataFrame to upload.

    Returns:
        None.
    """
    # Construct the dataset reference object.
    dataset_ref = client.dataset(dataset_name)

    # Construct the table reference object.
    table_ref = dataset_ref.table(table_name)

    # Upload the DataFrame to the table.
    job_config = bigquery.LoadJobConfig()
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()


def get_station_info() -> pd.DataFrame:
    """
    Retrieves information about air quality monitoring stations in Poland from the GIOŚ API.

    Returns:
        pandas.DataFrame: A dataframe containing information about each monitoring station, including its
        ID, name, geographical coordinates, address, district, province, and city.
    """
    all_stations_url = 'https://api.gios.gov.pl/pjp-api/rest/station/findAll'
    response = requests.get(all_stations_url)

    if response.status_code == 200:
        data = response.json()
        logging.info(f"Data captured. "
                     f"URL: {all_stations_url}."
                     f"Status code: {response.status_code}.")
    else:
        logging.error(f"Error to getting stations information from: {all_stations_url}."
                      f"Status code: {response.status_code}")

    station_info_df = pd.DataFrame(data)
    station_info_df['district_name'] = station_info_df['city'].apply(lambda x: x['commune']['districtName'])
    station_info_df['province'] = station_info_df['city'].apply(lambda x: x['commune']['provinceName'].capitalize())
    station_info_df['city'] = station_info_df['city'].apply(lambda x: x['commune']['communeName'].capitalize())
    # Convert string to float.
    station_info_df['gegrLat'] = station_info_df['gegrLat'].apply(lambda x: float(x))
    station_info_df['gegrLon'] = station_info_df['gegrLon'].apply(lambda x: float(x))

    return station_info_df


def get_measurement_data(station_ids: List[int]) -> pd.DataFrame:
    """
    Retrieves air quality measurement data from the API and returns a Pandas DataFrame.

    Args:
        station_ids: List of stations ids.

    Returns:
    - pd.DataFrame: A DataFrame containing the air quality measurement data.
    """
    measurement_df = pd.DataFrame(
        columns=[
            'station_id', 'sensor_id', 'param_code', 'datetime', 'value'
        ]
    )

    class Measurement(BaseModel):
        station_id: Optional[int]
        sensor_id: Optional[int]
        param_code: Optional[str]
        datetime: Optional[datetime]
        value: Optional[float]

    for station_num in station_ids:
        # Get all used sensors in each station.
        try:
            sensors_info = requests.get(f'https://api.gios.gov.pl/pjp-api/rest/station/sensors/{station_num}').json()
        except Exception as e:
            logging.error(f"Get error when trying to get informations for station id: {station_num}: {e}")
        for sensor in sensors_info:
            param_code = sensor['param']['paramCode']
            sensor_id = sensor['id']
            try:
                sensor_measurement = requests.get(
                    f'https://api.gios.gov.pl/pjp-api/rest/data/getData/{sensor_id}'
                ).json()
            except Exception as e:
                logging.error(f"Get error when trying to get measurements for sensor id: {sensor_id}, "
                              f"localised on station id: {station_num}: {e}")
            # Get single measurement.
            values = sensor_measurement['values']
            if values:
                iter_value = iter(values)
                # Checking if there is not None value.
                while True:
                    try:
                        measure_value = next(iter_value)
                        if measure_value['value']:
                            break
                    except StopIteration:
                        break
                single_measure = Measurement(
                    **{
                        'station_id': station_num,
                        'sensor_id': sensor_id,
                        'param_code': param_code,
                        'datetime': datetime.strptime(measure_value['date'], '%Y-%m-%d %H:%M:%S'),
                        'value': measure_value['value'],
                    }
                )
            else:
                logging.info(
                    f"There is no any measure for "
                    f"station_id: {station_num}, "
                    f"sensor_id: {sensor_id}, "
                    f"param: {param_code}"
                )

            new_row = pd.DataFrame(single_measure.dict(), index=[0])
            measurement_df = measurement_df._append(new_row, ignore_index=True)

    return measurement_df


if __name__ == '__main__':
    # Trying to create dataset and tables.
    create_dataset(CLIENT, DATASET_NAME)
    create_table(CLIENT, DATASET_NAME, TABLE_STATIONS, TABLE_STATIONS_SCHEMA)
    create_table(CLIENT, DATASET_NAME, TABLE_MEASUREMENTS, TABLE_MEASUREMENTS_SCHEMA)
    # Uploading stations info not exists
    table_ref = CLIENT.dataset(DATASET_NAME).table(TABLE_STATIONS)
    table = CLIENT.get_table(table_ref)
    # Check if the table of stations info is empty.
    if not table.num_rows:
        upload_dataframe_to_bq(CLIENT, DATASET_NAME, TABLE_STATIONS, get_station_info())
    # Uploading measurements every hour.
    while True:
        upload_dataframe_to_bq(CLIENT, DATASET_NAME, TABLE_MEASUREMENTS, get_measurement_data(get_station_info()['id']))
        time.sleep(3600)
