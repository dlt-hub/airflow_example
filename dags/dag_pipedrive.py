from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from pipedrive.pipedrive import pipedrive_source

def pipedrive_pipeline():
    import subprocess
    libs = subprocess.check_output("pip freeze", shell=True);
    print(libs)

    pipeline = dlt.pipeline(pipeline_name='pipedrive_renamed', destination='bigquery', dataset_name='pipedrive_raw')
    load_info = pipeline.run(pipedrive_source(fix_custom_fields=True))
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

dag = DAG(dag_id='pipedrive',
          default_args=default_args,
          schedule_interval='00 2 * * *',
          max_active_runs=1,
          catchup=False)


libs_check = BashOperator(
    task_id="run_after_loop",
    bash_command="pip freeze",
)

load_task = PythonOperator(
        task_id="load_pipedrive",
        python_callable=pipedrive_pipeline,
        trigger_rule="all_done",
        retries=0,
        provide_context=True,
        #on_failure_callback=,
        dag=dag)

libs_check >> load_task