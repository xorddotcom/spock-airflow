from include.common.constants.index import PROJECT_ID, COMMON_DATASET
from include.common.utils.bigquery import execute_raw_query, create_dataset, execute_query
from include.common.utils.xcom import push_to_xcom
from include.common.utils.parse_table_definition import generate_parsers_udf_sql

from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import BranchPythonOperator

# TODO read all json files create hash and find the earliest data
def compute_hash_and_date(protocol_id):
    return {"computed_hash":"my_hash", "start_date":"2023-01-01 00:00:00 UTC"}


def fetch(protocol_id, **kwargs):
    result = execute_raw_query(
        f"""
            SELECT * FROM `{PROJECT_ID}.{COMMON_DATASET}.protocol_metadata`
            WHERE id = '{protocol_id}'
        """
    )

    # get the first item from record
    protocol_record = next(result, None)
    computed_items = compute_hash_and_date(protocol_id)

    info = {"id":protocol_record["id"],"last_synced":protocol_record["last_synced"]} if protocol_record else None
    data={"info":info,
          "computed_hash":computed_items["computed_hash"],
          "start_date":computed_items["start_date"]
          }

    #store metadata and computed info in xcom
    push_to_xcom(key=f"{protocol_id}.metadata", data=data, **kwargs)

    
    if protocol_record:
        #if the computed and stored hash is different add udfs again in dataset
        if computed_items["computed_hash"] == protocol_record["utility_hash"]:
            return f'{protocol_id}.{protocol_id}_load_metadata.finish'
        else:
            return f'{protocol_id}.{protocol_id}_load_metadata.add_udfs'
    else:
        return f'{protocol_id}.{protocol_id}_load_metadata.create.create_dataset'

def create(protocol_id):
    with TaskGroup(group_id='create') as task_group:
        #create a dataset for protocol
        _create_dataset = create_dataset(
            task_id="create_dataset",
            dataset_id=f"p_{protocol_id}",
        )

        def jinja_metadata(key):
            return f"'{{{{ ti.xcom_pull(task_ids='{protocol_id}.{protocol_id}_load_metadata.fetch', key=\'{protocol_id}.metadata\')['{key}'] }}}}'"
        
    
        #insert protocol_metadata with using date and hash from xcom
        _insert_metadata = execute_query(
            task_id="insert_metadata",
            sql=f"""
                INSERT INTO `{PROJECT_ID}.{COMMON_DATASET}.protocol_metadata`
                VALUES
                    ('{protocol_id}', 
                    TIMESTAMP {jinja_metadata('start_date')},
                    {jinja_metadata('computed_hash')}
                    )
            """,
        )

        _create_dataset >> _insert_metadata

    return task_group

# Function to create a task group for metadata loading
def load_metadata(protocol_id, **kwargs):
    
    # Create a TaskGroup named 'load_metadata'
    with TaskGroup(group_id=f'{protocol_id}_load_metadata') as task_group:
       
        # Fetch metadata for the protocol
        _fetch = BranchPythonOperator(
            task_id='fetch',
            python_callable=fetch,
            provide_context=True,
            op_kwargs={'protocol_id': protocol_id},
            **kwargs
        )

        #create dataset and insert metadata if protocol not found
        _create = create(protocol_id)

        # add udfs in protocol dataset
        _add_udfs = execute_query(
            task_id="add_udfs",
            sql=generate_parsers_udf_sql(protocol_id),
        )

        # Dummy operator to indicate finishing the task group
        _finish = EmptyOperator(
            task_id='finish',
            trigger_rule="none_failed",
            **kwargs
        )

        _fetch >> [_create, _add_udfs, _finish]
        _create >> _add_udfs >> _finish

    return task_group
