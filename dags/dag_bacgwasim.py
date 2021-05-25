from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta
import ast
import os
import sys

sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from kubernetes.client import models as k8s
from operators.FileOperators import ZipOperator
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

user_filters = {
    "to_list": lambda _list: _list.split(' ')
}

dag = DAG(
    "bacgwasim",
    default_args=default_args,
    schedule_interval=None,
    user_defined_filters=user_filters,
)

# Callbacks
start_callback = callback_factory(dag, "start_callback", "RUNNING")
completed_callback = callback_factory(dag, "completed_callback", "COMPLETED")
failed_callback = callback_factory(
    dag, "failed_callback", "FAILED", trigger_rule="all_failed"
)


volume = k8s.V1Volume(
    name="apar-pv",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="rook-nfs-pvc"),
)

volume_mount = k8s.V1VolumeMount(
    name="apar-pv", mount_path="/data", sub_path=None, read_only=False
)

bacgwasim = KubernetesPodOperator(
    namespace="apar",
    image="quay.io/biocontainers/bacgwasim:2.1.0--pyhdfd78af_0",
    cmds=[
        "BacGWASim", 
        "--output-dir", "/data/{{ dag_run.conf['files_id'] }}",
    ],
    arguments=ast.literal_eval("{{ dag_run.conf['parameters'] | to_list }}"),
    name="bacgwasim",
    task_id="bacgwasim",
    get_logs=True,
    dag=dag,
    volumes=[volume],
    volume_mounts=[volume_mount],
)

zip_results = ZipOperator(
    task_id="zip_bacgwasim_results",
    path_to_zip="/data/{{ dag_run.conf['files_id'] }}",
    path_to_save="/data/{{ dag_run.conf['files_id'] }}.zip",
    dag=dag,
)

with dag:
    start_callback >> bacgwasim >> zip_results >> [completed_callback, failed_callback]