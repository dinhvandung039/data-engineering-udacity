from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=[],  
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks 

    def execute(self, context):
        expected_count = 1

        redshift_hook = PostgresHook(self.redshift_conn_id)

        for check in self.dq_checks: 
            check_sql = check['check_sql']
            expected_result = check['expected_result']
            self.log.info(f"Checking Data Quality with SQL: {check_sql}")
            records = redshift_hook.get_records(check_sql)
            if len(records) < 1 or records[0][0] != expected_result:  
                raise ValueError(f"Data quality check failed: SQL {check_sql} returned {records[0][0]}, expected {expected_result}")