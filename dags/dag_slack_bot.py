import requests
import json

from airflow.hooks.base_hook import BaseHook

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

connection = BaseHook.get_connection("slack_standup")
webhook_url = connection.password


def send_slack_message(webhook_url, message):
    data = {'text': message}
    headers = {'Content-Type': 'application/json'}
    response = requests.post(webhook_url, data=json.dumps(data), headers=headers)
    if response.status_code != 200:
        raise ValueError(f'Request to Slack returned an error {response.status_code}, the response is:\n{response.text}')



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

dag = DAG(dag_id='send_slack_message',
          default_args=default_args,
          schedule_interval='00 12 * * 1-5',
          max_active_runs=1,
          catchup=False)



message = """@channel ** Standup bot: **
:hourglass: What did you do yesterday?
:hourglass_flowing_sand: What will you do today?
:construction: Any blockers?
"""



a = PythonOperator(
        task_id=f"standup_notification",
        op_args=[webhook_url, message],
        python_callable=send_slack_message,
        trigger_rule="all_done",
        retries=1,
        dag=dag)


a