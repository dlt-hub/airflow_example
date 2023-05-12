import requests
import json

from airflow.hooks.base_hook import BaseHook

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import dlt


def generate_data(num_rows=10000000):
    for i in range(num_rows):
        row = {"id": i, 'properties': {'createdate': '2023-02-24T03:00:13.789Z', 'email': 'bh@hubspot.com',
                                                  'firstname': 'Brian', 'hs_object_id': '51',
                                                  'lastmodifieddate': '2023-02-24T03:00:25.112Z',
                                                  'lastname': 'Halligan (Sample Contact)',
                                                    'more_data':{'createdate': '2023-02-24T03:00:13.789Z', 'email': 'bh@hubspot.com',
                                                  'firstname': 'Brian', 'hs_object_id': '51',
                                                  'lastmodifieddate': '2023-02-24T03:00:25.112Z',
                                                  'lastname': 'Halligan (Sample Contact)'},},
                                   'createdAt': '2023-02-24T03:00:13.789Z', 'updatedAt': '2023-02-24T03:00:25.112Z', 'archived': False}
        yield row

def dummy_row_pipeline():
    pipeline = dlt.pipeline(pipeline_name=f'1m', destination='bigquery', dataset_name='tests')
    load_info = pipeline.run(generate_data())
    print(load_info)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': 'adrian@dlthub.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2023, 2, 25),
    'max_active_runs': 1
}

dag = DAG(dag_id='test_data',
          default_args=default_args,
          schedule_interval='00 08 * * 1-5',
          max_active_runs=1,
          catchup=False)





a = PythonOperator(
        task_id=f"make_test_data",
        python_callable=dummy_row_pipeline,
        trigger_rule="all_done",
        retries=1,
        dag=dag)


a