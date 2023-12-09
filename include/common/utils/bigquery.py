from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryExecuteQueryOperator, BigQueryCreateEmptyTableOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

from include.common.constants.index import GCP_CONN_ID

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
    
# TODO: implementation
def load_protocol_metadata(dataset_id, force):
    return

# TODO: implementation
def load_metadata(dataset_id, force=False):
    return PythonOperator(
        task_id=f'load_metadata_{dataset_id}',
        python_callable=load_protocol_metadata,
        provide_context=True,
        op_kwargs={'dataset_id': dataset_id, 'force': force}
    )  
    
# TODO: implementation   
def update_protocol_metadata(dataset_id):
    return

# TODO: implementation    
def update_metadata(dataset_id):
    return PythonOperator(
        task_id='update_metadata',
        python_callable=update_protocol_metadata,
        provide_context=True,
        op_kwargs={'dataset_id': dataset_id}
    )  

# TODO: implementation

def check_protocol_backlog(dataset_id):
    has_backlog = False  
    
    if has_backlog:
        return dataset_id
    else:
        return 'finish'
    
# TODO: implementation
def check_historical_backlog(dataset_id):
    return BranchPythonOperator(
        task_id=f'check_historical_backlog_{dataset_id}',
        python_callable=check_protocol_backlog,
        provide_context=True,
        op_kwargs={'dataset_id': dataset_id}
    ) 
   
     
# TODO: implementation
def check_protocol_syncing_state(dataset_id):
    has_backlog = False  
    
    if has_backlog:
        return f"run_{dataset_id}"
    else:
        return 'finish'
    
# TODO: implementation
def check_syncing_state(dataset_id):
    return BranchPythonOperator(
        task_id=f'check_syncing_state_{dataset_id}',
        python_callable=check_protocol_syncing_state,
        provide_context=True,
        op_kwargs={'dataset_id': dataset_id}
    ) 