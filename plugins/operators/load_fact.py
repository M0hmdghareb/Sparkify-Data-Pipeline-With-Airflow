from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    insert_sql = """
        INSERT INTO {}
        {};
    """

    @apply_defaults
    def __init__(self, table="", redshift_conn_id="", sql="", *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        formatted_sql = LoadFactOperator.insert_sql.format(self.table, self.sql)

        self.log.info(f"Loading fact table '{self.table}' into Redshift")
        redshift_hook.run(formatted_sql)
