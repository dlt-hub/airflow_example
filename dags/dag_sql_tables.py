"""
This is a dag that reads the source from the zendesk folder, without using the pipeline file.
The advantage is that you do not need the pipeline file anymore, or can use it separately, and you can use it with multiple sources, creating a task and pipeline for each.
The disadvantage is that it's less easy to customise than doing your own functions and tasks
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


import dlt

from sql_database import sql_database, sql_table

from airflow.hooks.base_hook import BaseHook

source_airflow_conn = "bridge-mysql-production"
destination_airlfow_conn = "postgres_default"
source_connection = BaseHook.get_connection(source_airflow_conn)
destination_connection = BaseHook.get_connection(destination_airlfow_conn)



source_credential_string = f"mysql+pymysql://{source_connection.login()}:{source_connection.password()}@{source_connection.host()}:{source_connection.port()}/{source_connection.schema()}"
destination_credential_string =f"postgres://{destination_connection.login()}:{destination_connection.password()}@{destination_connection.host()}:{destination_connection.port()}/{destination_connection.schema()}"

from dlt.sources.credentials import ConnectionStringCredentials

source_credential = ConnectionStringCredentials("source_credential_string")
destination_credential = ConnectionStringCredentials("destination_credential_string")


tables_to_load = [{'write_disposition': 'replace', 'tables':[{'name':'billing_items'},
                                                             {'name':'accounts'},
                                                            {'name':'packages'},
                                                            {'name':'presentation_templates'},
                                                            {'name':'presentation_sessions'},

                                                            ]},
                  {'write_disposition': 'append','tables':[{'name':'signature_audit_log', 'column':'id'},
                                                            {'name':'user_audit_log', 'column':'id'},
                                                            {'name':'consultation_log', 'column':'session_id'}
                                                            ]},
                  {'write_disposition': 'merge', 'tables':[{'name':'users', 'column':'updated_at'}, {'name':'accounts'}
                                                            ]},

                  ]

def load_tables(tables_to_load, source_credential, destination_credential):
    pipeline = dlt.pipeline(pipeline_name='rfam', destination='postgres', dataset_name='rfam_data', credentials=destination_credential)
    for job in tables_to_load:
        write_disposition = job['write_disposition']
        tables = job['tables']
        source = sql_database(source_credential).with_resources(*[t['name'] for t in tables])
        for table in tables:
            if write_disposition == 'replace':
                # no hints are needed
                pass
            else:
                #apply loading hints
                source.table['name'].apply_hints(incremental=dlt.sources.incremental(table['column']))
        info = pipeline.run(source,
                            write_disposition=write_disposition)
        print(info)



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

dag = DAG(dag_id='test_sql_dag',
          default_args=default_args,
          schedule_interval='00 08 * * 1-5',
          max_active_runs=1,
          catchup=False)



a = PythonOperator(
        task_id=f"make_test_data",
        python_callable=load_tables,
        op_args=[tables_to_load, source_credential, destination_credential]
        trigger_rule="all_done",
        retries=1,
        dag=dag)


a