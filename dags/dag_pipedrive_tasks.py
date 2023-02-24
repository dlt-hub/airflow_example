
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from pipedrive import pipedrive_source
import dlt



def pipedrive_resource(resource_list):
    pipeline = dlt.pipeline(pipeline_name=f'pipedrive_tasks', destination='bigquery', dataset_name='pipedrive_raw_tasks')
    load_info = pipeline.run(pipedrive_source().with_resources(*resource_list))
    print(load_info)


resource_groups = [['organizations'],
                   ['pipelines'],
                   ['persons'],
                   ['products'],
                   ['stages'],
                   ['users'],
                   ['activities'],
['deals', 'deals_flow', 'deals_participants']
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

dag = DAG(dag_id='pipedrive_with_tasks',
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
        retries=0,
        dag=dag)



for resource_list in resource_groups:
        task = make_loading_task(resource_list)
        field_names_task >> task