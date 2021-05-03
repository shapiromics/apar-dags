from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime, timedelta
import os
import sys

sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from kubernetes.client import models as k8s
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


volume = k8s.V1Volume(
    name="apar-pv",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="rook-nfs-pvc"),
)

volume_mount = k8s.V1VolumeMount(
    name="apar-pv", mount_path="/data", sub_path=None, read_only=False
)

bacgwasim = KubernetesPodOperator(
    namespace="apar",
    image="quay.io/biocontainers/bacgwasim:2.0.0--py_1",
    cmds=["BacGWASim", "--output-dir", "/data/{{ dag_run.conf['files_id'] }}"],
    name="bacgwasim",
    task_id="bacgwasim",
    get_logs=True,
    dag=dag,
    volumes=[volume],
    volume_mounts=[volume_mount],
)

with dag:
    start_callback >> bacgwasim >> [completed_callback, failed_callback]