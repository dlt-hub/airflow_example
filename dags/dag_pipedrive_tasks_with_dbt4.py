
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from pipedrive import pipedrive_source
import dlt
import os


def pipedrive_resource(resource_list):
    pipeline = dlt.pipeline(pipeline_name=f'pipedrive_tasks', destination='bigquery', dataset_name='pipedrive_raw')
    load_info = pipeline.run(pipedrive_source().with_resources(*resource_list))
    print(load_info)

def pipedrive_dbt():
    pipeline = dlt.pipeline(pipeline_name='pipedrive_tasks', destination='bigquery', dataset_name='pipedrive_dbt')
    # now that data is loaded, let's transform it
    # make or restore venv for dbt, uses latest dbt version
    venv = dlt.dbt.get_venv(pipeline)
    # get runner, optionally pass the venv
    here = os.path.dirname(os.path.realpath(__file__))
    dbt = dlt.dbt.package(pipeline,
        os.path.join(here,"pipedrive/dbt_pipedrive/pipedrive"),
        venv=venv)
    models = dbt.run_all()
    for m in models:
        print(f"Model {m.model_name} materialized in {m.time} with status {m.status} and message {m.message}")


resource_groups = [['organizations'],
                   ['pipelines'],
                   ['persons'],
                   ['products'],
                   ['stages'],
                   ['users'],
                   ['activities'],
                   ['deals', 'deals_flow', 'deals_participants'],
                   ['custom_fields_mapping']
]
#'custom_fields_mapping', 'activityFields', 'personFields', 'pipelines', 'organizations', 'products', 'persons', 'deals_flow', 'stages', 'dealFields', 'organizationFields', 'activities', 'deals_participants', 'users', 'productFields', 'deals'
#['activityFields', 'dealFields', 'organizationFields', 'personFields', 'productFields']




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': 'adrian@dlthub.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2023, 2, 21),
    'max_active_runs': 1
}

dag = DAG(dag_id='pipedrive_with_tasks_with_dbt4',
          default_args=default_args,
          schedule_interval='00 2 * * *',
          max_active_runs=1,
          catchup=False)


def make_loading_task(resource_list):
    name = '_'.join(resource_list)
    # load the values if needed in the command you plan to execute
    return PythonOperator(
        task_id=f"load_pipedrive_{name}",
        op_args=[resource_list],
        python_callable=pipedrive_resource,
        trigger_rule="all_done",
        retries=0,
        dag=dag)


field_names_task = PythonOperator(
        task_id=f"load_pipedrive_field_names",
        op_args=[['activityFields', 'dealFields', 'organizationFields', 'personFields', 'productFields']],
        python_callable=pipedrive_resource,
        trigger_rule="all_done",
        retries=3,
        dag=dag)

dbt_pipedrive_task = PythonOperator(
        task_id=f"dbt_pipedrive_task",
        python_callable=pipedrive_dbt,
        trigger_rule="all_done",
        retries=3,
        dag=dag)

prv_task = field_names_task
for resource_list in resource_groups:
        task = make_loading_task(resource_list)
        prv_task >> task
        prv_task = task

prv_task >> dbt_pipedrive_task