from functools import partial
import json 

from operators.ExtendedHttpOperator import ExtendedHttpOperator

queries = dict()

queries["jobUpdateStatus"] = """
mutation jobUpdateStatus($jobId: ID!, $status: String!) {
    jobUpdateStatus(input: { jobId: $jobId, status: $status }) {
        job {
            id
            status
        }
    }
}
"""


def get_job_status_update(status, **context):
    job_id = context["dag_run"].conf.get("job_id")

    return json.dumps({
        "query": queries["jobUpdateStatus"],
        "variables": {
            "jobId": job_id,
            "status": status
        }
    })


def callback_factory(dag, task_id, status, trigger_rule="all_success"):
    return ExtendedHttpOperator(
        http_conn_id="apar_graphql",
        endpoint="graphql/",
        method="POST",
        headers={"Content-Type": "application/json"},
        data_fn=partial(get_job_status_update, status),
        task_id=task_id,
        trigger_rule=trigger_rule,
        dag=dag
    )