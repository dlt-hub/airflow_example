
from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.python import PythonOperator
from pipedrive import pipedrive_source
import dlt

from sql_source.sql_database import sql_database

from airflow.models import Variable
from datetime import datetime, timedelta


def get_resource_names_cached(name, cache_expiry_hours=6):
    v = Variable.get(name, default_var={'created_at': datetime(2020, 1, 1)}, deserialize_json=True)
    now = datetime.now()
    if v.get('created_at') > now - timedelta(hours=cache_expiry_hours):
        return v.get['value']
    else:
        value = sql_database().resources.keys()
        Variable.set(name, {'created_at': datetime.now(), 'value': value}, serialize_json=True)
        return value




def resource_pipeline(resource):
    pipeline = dlt.pipeline(
        destination='bigquery',
        pipeline_name=f'sql_prod_{resource}',
        dataset_name='prod',
        full_refresh=True
    )
    load_info = pipeline.run(pipedrive_source().with_resources(resource))
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

dag = DAG(dag_id='pipedrive_with_tasks',
          default_args=default_args,
          schedule_interval='00 2 * * *',
          max_active_runs=1,
          catchup=False)


def make_loading_task(resource):
    return PythonOperator(
        task_id=f"load_pipedrive_{resource}",
        op_args=[resource],
        python_callable=resource_pipeline,
        trigger_rule="all_done",
        retries=0,
        dag=dag)


resource_list = get_resource_names_cached('prod_sql_resource_list')
prv_task = None
for r in resource_list:
        task = make_loading_task(r)
        if not prv_task:
            task
        else:
            prv_task >> task
        prv_task = task