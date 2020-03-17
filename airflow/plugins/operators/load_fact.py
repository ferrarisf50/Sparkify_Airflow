from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 table,
                 redshift_conn_id='redshift',
                 sql_query='',
                 delete_existing_data = False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.delete_existing_data = delete_existing_data

    def execute(self, context):

        
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Inserting {self.table} at Redshift")
        
        if self.delete_existing_data == False:
            self.log.info(f"Delete data from {self.table}")
            redshift_hook.run("DELETE FROM {}".format(self.table))    
            
        insert_sql = """
            INSERT INTO {table}
            {sql_query};
        """.format(table=self.table, sql_query=self.sql_query)
        
        redshift_hook.run(insert_sql)
        self.log.info(f"Loading {self.table} complete.")
        
        
