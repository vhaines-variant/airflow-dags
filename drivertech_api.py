import json
import logging
from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.helpers import chain
from airflow.operators.dummy_operator import DummyOperator
import airflow.hooks.S3_hook

from requests import Session
from requests.auth import HTTPBasicAuth
from fastapi.encoders import jsonable_encoder

from zeep import CachingClient
from zeep.transports import Transport
from zeep.helpers import serialize_object

import os
from pathlib import Path


def get_drivertech_secret(secret_name):
    secrets_dir = Path('/var/airflow/secrets/drivertech')
    secret_path = secrets_dir / secret_name
    assert secret_path.exists(), f'could not find {secret_name} at {secret_path}'
    secret_data = secret_path.read_text().strip()
    return secret_data


def get_all_status_from_drivertech(**kwargs):
    GETTING = "driver_status"
    AWS_CONNECTION = 'drivertech_S3_conn'

    # SOAP config
    URL = get_drivertech_secret('dt_url')
    USERNAME = get_drivertech_secret('dt_username')
    PASSWORD = get_drivertech_secret('dt_password')

    LOGGER = logging.getLogger(f"airflow.get_{GETTING}")
    try:
        # SOAP client setup
        session = Session()
        session.auth = HTTPBasicAuth(USERNAME, PASSWORD)
        client = CachingClient(URL, transport=Transport(session=session))
        interval_start = (datetime.now() - timedelta(minutes=10)).timestamp()
        res = client.service.VehicleStatus_GetAll(datetime.utcfromtimestamp(interval_start))
        json_obj = jsonable_encoder(serialize_object(res))
        hook = airflow.hooks.S3_hook.S3Hook(AWS_CONNECTION)
        hook.load_string(json.dumps(json_obj), os.path.join(GETTING, f'{interval_start}__{GETTING}.json'),
                         'drivertech-test')
    except Exception as e:
        LOGGER.error(e)
        raise e


default_args = {
    'owner': 'Vincent',
    'start_date': datetime.now(),

}
# 'retry_delay': timedelta(minutes=5)

with DAG('drivertech_api', default_args=default_args, schedule_interval=timedelta(minutes=10)) as dag:
    start_task = DummyOperator(
        task_id='start_drivertech_pull'
    )

    get_all_driver_status = PythonOperator(
        task_id='get_all_driver_status',
        python_callable=get_all_status_from_drivertech
    )

    # Use arrows to set dependencies between tasks
    chain(start_task, [get_all_driver_status])
