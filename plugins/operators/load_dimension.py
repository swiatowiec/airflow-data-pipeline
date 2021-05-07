from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.delete_flag:
            self.log.info('LoadDimensionOperator: runninh delete function')
            delete_statement = f'DELETE FROM {self.table_name}'
            redshift_hook.run(delete_statement)
            
        self.log.info('LoadDimensionOperator: execute SQL query')
        insert_statement = f'INSERT INTO {self.table_name} {self.sql_query}'
        redshift_hook.run(insert_statement)
                
        create_table_stmt = getattr(SqlQueries, f'{self.table}_table_create')
        redshift.run(create_table_stmt, autocommit=True)
        
        insert_table_stmt = getattr(SqlQueries, f'{self.table}_table_insert')
        redshift.run(insert_table_stmt, autocommit=True)
        
        self.log.info(f'LoadDimensionOperator for [{self.table}] has been done')