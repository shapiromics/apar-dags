from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.decorators import apply_defaults

class ContextualizedKubernetesPodOperator(KubernetesPodOperator):
    
    @apply_defaults
    def __init__(self,
                 *args, **kwargs):
        super(ContextualizedKubernetesPodOperator, self).__init__(*args, **kwargs)
