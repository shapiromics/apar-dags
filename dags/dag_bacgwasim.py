from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
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
failed_callback = callback_factory(
    dag, "failed_callback", "FAILED", trigger_rule="all_failed"
)

bacgwasim_help = KubernetesPodOperator(
    namespace="airflow",
    image="quay.io/biocontainers/bacgwasim:2.0.0--py_1",
    cmds=["BacGWASim","--help"],
    name="bacgwasim-help",
    task_id="bacgwasim-help",
    get_logs=True,
    dag=dag
)

with dag:
    start_callback >> bacgwasim_help >> [completed_callback, failed_callback]