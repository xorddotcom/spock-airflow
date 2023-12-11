from include.common.constants.index import GCP_CONN_ID
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryExecuteQueryOperator, BigQueryCreateEmptyTableOperator

def create_dataset(task_id, dataset_id, **kwargs):
    return BigQueryCreateEmptyDatasetOperator(
        task_id = task_id,
        dataset_id = dataset_id,
        gcp_conn_id = GCP_CONN_ID,
        if_exists = 'skip',
        **kwargs
    )

def execute_query(task_id, sql, **kwargs):
    return BigQueryExecuteQueryOperator(
        task_id = task_id,
        sql = sql,
        use_legacy_sql = False,
        gcp_conn_id = GCP_CONN_ID,
        **kwargs
    )

def create_table(task_id, dataset_id, table_id, table_schema, **kwargs):
    return BigQueryCreateEmptyTableOperator(
        task_id = task_id,
        dataset_id = dataset_id,
        table_id= table_id,
        schema_fields=table_schema,
        gcp_conn_id = GCP_CONN_ID,
        exists_ok= True,
        **kwargs
    )
    

 


