
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from active_campaign import active_campaign
import dlt
import os


def active_campaign_resource():
    pipeline = dlt.pipeline(pipeline_name=f'active_campaign_dbt', destination='bigquery', dataset_name='active_campaign_dbt_raw')
    load_info = pipeline.run(active_campaign().with_resources("accounts","accountContacts","contacts"))
    print(load_info)

def active_campaign_dbt():
    pipeline = dlt.pipeline(pipeline_name='active_campaign_dbt', destination='bigquery', dataset_name='active_campaign_dbt')
    # now that data is loaded, let's transform it
    # make or restore venv for dbt, uses latest dbt version
    venv = dlt.dbt.get_venv(pipeline)
    # get runner, optionally pass the venv
    here = os.path.dirname(os.path.realpath(__file__))
    dbt = dlt.dbt.package(pipeline,
        os.path.join(here,"active_campaign/dbt_active_campaign/active_campaign" ),
        venv=venv)
    models = dbt.run_all()
    for m in models:
        print(f"Model {m.model_name} materialized in {m.time} with status {m.status} and message {m.message}")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': 'grunwaldnguptanalytics@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2023, 2, 21),
    'max_active_runs': 1
}

dag = DAG(dag_id='active_campaign_with_dbt',
          default_args=default_args,
          schedule_interval='00 2 * * *',
          max_active_runs=1,
          catchup=False)


load_task = PythonOperator(
        task_id="load_active_campaign_with_dbt",
        python_callable=active_campaign_resource,
        trigger_rule="all_done",
        retries=0,
        provide_context=True,
        #on_failure_callback=,
        dag=dag)


dbt_active_campaign_task = PythonOperator(
        task_id=f"dbt_active_campaign_task",
        python_callable=active_campaign_dbt,
        trigger_rule="all_done",
        retries=3,
        dag=dag)


load_task >> dbt_active_campaign_task