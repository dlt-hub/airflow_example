from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from zendesk_pipeline import incremental_load_all_default



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': 'adrian@dlthub.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2023, 2, 21)
}

dag = DAG(dag_id='Zendesk_Pipeline',
          default_args=default_args,
          schedule_interval='00 2 * * *',
          max_active_runs=1,
          catchup=False)


load_task = PythonOperator(
        task_id="load_Zendesk",
        python_callable=incremental_load_all_default,
        trigger_rule="all_done",
        retries=0,
        dag=dag)

load_task

