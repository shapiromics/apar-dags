from typing import Optional

from airflow.exceptions import AirflowException
from airflow.kubernetes import kube_client
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.utils import pod_launcher
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State


class ContextualizedKubernetesPodOperator(KubernetesPodOperator):
    
    @apply_defaults
    def __init__(self,
                 *args, **kwargs):
        super(ContextualizedKubernetesPodOperator, self).__init__(*args, **kwargs)


    def execute(self, context) -> Optional[str]:
        try:
            if self.in_cluster is not None:
                client = kube_client.get_kube_client(
                    in_cluster=self.in_cluster,
                    cluster_context=self.cluster_context,
                    config_file=self.config_file,
                )
            else:
                client = kube_client.get_kube_client(
                    cluster_context=self.cluster_context, config_file=self.config_file
                )

            self.arguments = eval(self.arguments)
            self.pod = self.create_pod_request_obj()
            self.namespace = self.pod.metadata.namespace

            self.client = client

            # Add combination of labels to uniquely identify a running pod
            labels = self.create_labels_for_pod(context)

            label_selector = self._get_pod_identifying_label_string(labels)

            pod_list = client.list_namespaced_pod(self.namespace, label_selector=label_selector)

            if len(pod_list.items) > 1 and self.reattach_on_restart:
                raise AirflowException(
                    f'More than one pod running with labels: {label_selector}'
                )

            launcher = pod_launcher.PodLauncher(kube_client=client, extract_xcom=self.do_xcom_push)

            if len(pod_list.items) == 1:
                try_numbers_match = self._try_numbers_match(context, pod_list.items[0])
                final_state, result = self.handle_pod_overlap(
                    labels, try_numbers_match, launcher, pod_list.items[0]
                )
            else:
                self.log.info("creating pod with labels %s and launcher %s", labels, launcher)
                final_state, _, result = self.create_new_pod_for_operator(labels, launcher)
            if final_state != State.SUCCESS:
                status = self.client.read_namespaced_pod(self.pod.metadata.name, self.namespace)
                raise AirflowException(f'Pod {self.pod.metadata.name} returned a failure: {status}')
            context['task_instance'].xcom_push(key='pod_name', value=self.pod.metadata.name)
            context['task_instance'].xcom_push(key='pod_namespace', value=self.namespace)
            return result
        except AirflowException as ex:
            raise AirflowException(f'Pod Launching failed: {ex}')
