from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook


class ExtendedHttpOperator(SimpleHttpOperator):
    """
    Extend Simple Http Operator with a callable function to formulate data. 
    This data function will be able to access the context to retrieve data such 
    as task instance. This allow us to write cleaner code rather than writing 
    one long template line to formulate the json data.

    From : https://gist.github.com/nndd91/56c2337b89e2b8705a90da088020b609
    """
    @apply_defaults
    def __init__(self, 
                data_fn,
                http_conn_id,
                *args, **kwargs):
        super(ExtendedHttpOperator, self).__init__(*args, **kwargs)
        if not callable(data_fn):
            raise AirflowException('`data_fn` param must be callable')
        self.data_fn = data_fn
        self.http_conn_id = http_conn_id
        self.context = None

    def execute(self, context):
        self.context = context          
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)

        data_result = self.execute_callable(context)
        
        self.log.info("Calling HTTP method")
        self.log.info("Post Data: {}".format(data_result))
        response = http.run(self.endpoint,
                            data_result,
                            self.headers,
                            self.extra_options)
        return response.text

    def execute_callable(self, context):
        return self.data_fn(**context)