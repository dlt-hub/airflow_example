"""
This is a dag that reads the source from the zendesk folder, without using the pipeline file.
The advantage is that you do not need the pipeline file anymore, or can use it separately, and you can use it with multiple sources, creating a task and pipeline for each.
The disadvantage is that it's less easy to customise than doing your own functions and tasks
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from zendesk import zendesk_chat, zendesk_talk, zendesk_support


sources = {'zendesk_support':zendesk_support}#, 'zendesk_talk':zendesk_talk, 'zendesk_chat':zendesk_chat}


def resource_task(source, resource):
    source_name, source_function = source
    pipeline = dlt.pipeline(pipeline_name=f'{source_name}', destination='bigquery', dataset_name=f'{source_name}')
    load_info = pipeline.run(source_function().with_resources(resource))
    print(load_info)


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

dag = DAG(dag_id='Zendesk_serialised',
          default_args=default_args,
          schedule_interval='00 2 * * *',
          max_active_runs=1,
          catchup=False)


def make_loading_task(source, resource_name):
    # load the values if needed in the command you plan to execute
    return PythonOperator(
        task_id=f"load_pipedrive_{resource_name}",
        op_args=[source, resource_name],
        python_callable=resource_task,
        trigger_rule="all_done",
        retries=0,
        dag=dag)




prv_task = None
for source in sources.items():
    source_name, source_function = source
    for resource_name in source_function().resources.keys():
        task = make_loading_task(source, resource_name)
        if not prv_task:
            task
        else:
            prv_task >> task
            prv_task = task


