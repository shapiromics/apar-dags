from airflow import DAG
from datetime import datetime, timedelta
import os
import sys

sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from utils.callbacks import callback_factory


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.utcnow(),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

dag = DAG(
    "bacgwasim",
    default_args=default_args,
    schedule_interval=None,
)

# Callbacks
start_callback = callback_factory(dag, "start_callback", "RUNNING")
completed_callback = callback_factory(dag, "completed_callback", "COMPLETED")
failed_callback = callback_factory(dag, "failed_callback", "FAILED")

passing = KubernetesPodOperator(
    namespace="airflow",
    image="python:3.6",
    cmds=["python","-c"],
    arguments=["print('hello world')"],
    labels={"foo": "bar"},
    name="passing-test",
    task_id="passing-task",
    get_logs=True,
    dag=dag
)

with dag:
    start_callback >> passing >> [completed_callback, failed_callback]