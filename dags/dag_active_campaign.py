from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from active_campaign import active_campaign
import dlt


def active_campaign_pipeline():

    pipeline = dlt.pipeline(pipeline_name='active_pipeline', destination='bigquery', dataset_name='active_campaign_raw')
    load_info = pipeline.run(active_campaign())
    print(load_info)


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

dag = DAG(dag_id='active_campaign',
          default_args=default_args,
          schedule_interval='00 2 * * *',
          max_active_runs=1,
          catchup=False)


load_task = PythonOperator(
        task_id="load_active_campaign",
        python_callable=active_campaign_pipeline,
        trigger_rule="all_done",
        retries=0,
        provide_context=True,
        #on_failure_callback=,
        dag=dag)

load_task