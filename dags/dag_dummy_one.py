from datetime import timedelta

from airflow import DAG

from source import dummy_source
from helpers.airflow.pipeline import DltAirflowPipeline
from helpers.airflow.source import DltAirflowSource

PIPELINE_NAME = 'dummy_pipeline_one'
DESTINATION_NAME = 'bigquery'

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': '2023-03-08'
}

dag_args = {'max_active_runs': 1,
            'catchup': False
}


with DAG(f'dag_{PIPELINE_NAME}', default_args=default_args, **dag_args):
    with DltAirflowPipeline(
            name=PIPELINE_NAME,
            destination='dummy',
            dataset_name='dummy_dataset',
            full_refresh=False
    ) as _pipeline:
        DltAirflowSource(
            name='dummy_1',
            source=dummy_source(prefix='prefix_1')
        )

        DltAirflowSource(
            name='dummy_2',
            source=dummy_source(prefix='prefix_2')
        )
