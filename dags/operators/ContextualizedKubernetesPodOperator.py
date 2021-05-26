from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.decorators import apply_defaults

class ContextualizedKubernetesPodOperator(KubernetesPodOperator):
    
    @apply_defaults
    def __init__(self,
                 *args, **kwargs):
        super(ContextualizedKubernetesPodOperator, self).__init__(*args, **kwargs)


    def _render_nested_template_fields(
        self,
        content: Any,
        context: Dict,
        jinja_env: "jinja2.NativeEnvironment",
        seen_oids: set,
    ) -> None:
        if id(content) not in seen_oids and isinstance(content, k8s.V1EnvVar):
            seen_oids.add(id(content))
            self._do_render_template_fields(content, ('value', 'name'), context, jinja_env, seen_oids)
            return

        super()._render_nested_template_fields(
            content,
            context,
            jinja_env,
            seen_oids
        )