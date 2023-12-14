import os

from include.common.constants.index import PROJECT_ID, COMMON_DATASET, PROTOCOL_POSITIONS_PATH
from include.common.utils.bigquery import execute_raw_query, create_dataset, execute_query
from include.common.utils.xcom import push_to_xcom, pull_from_xcom
from include.common.utils.parse_table_definition import generate_udfs_sql
from include.common.utils.hashing import calculate_sha256_hash
from include.common.utils.file_helpers import load_json_file
from include.common.utils.time import get_current_utc_timestamp

from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime

#calculate hash for based on protocol's parser and sql content, to identify change
def calculate_utility_hash(protocol_id):
    def load_files_content(dir_name):
        dir_path = os.path.join(PROTOCOL_POSITIONS_PATH, protocol_id, dir_name)
        content = ''
        if os.path.exists(dir_path) and os.path.isdir(dir_path):
            for filename in os.listdir(dir_path):
                file_path = os.path.join(dir_path, filename)
                with open(file_path, 'r') as loaded_file:
                    content += loaded_file.read()
        
        return content
    
    parser_content = load_files_content('parser')
    sql_content = load_files_content('sql')
    combined_content = parser_content + sql_content

    computed_hash = calculate_sha256_hash(combined_content)
    return computed_hash

#get the earliest time from protocol's all parsers
def find_earliest_start_date(protocol_id):
    earliest_date = None
    parser_dir = os.path.join(PROTOCOL_POSITIONS_PATH, protocol_id, 'parser')

    for filename in os.listdir(parser_dir):
        data = load_json_file(os.path.join(parser_dir, filename))
        start_date_str = data["parser"]["start_date"]
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d %H:%M:%S %Z')
        if earliest_date is None or start_date < earliest_date:
            earliest_date = start_date

    return earliest_date

def jinja_metadata(protocol_id, key):
        return f"'{{{{ ti.xcom_pull(task_ids='{protocol_id}.load_metadata.check', key='protocol_metadata')['{key}'] }}}}'"

#fetch metadata from bigquery and store that in xcom
def fetch_metadata(protocol_id, **kwargs):
    result = execute_raw_query(
        f"""
            SELECT * FROM `{PROJECT_ID}.{COMMON_DATASET}.protocol_metadata`
            WHERE id = '{protocol_id}'
        """
    )

    # get the first item from record
    protocol_record = next(result, None)
    data = {"last_block_timestamp":protocol_record["last_block_timestamp"], "utility_hash": protocol_record["utility_hash"]} if protocol_record else None
    push_to_xcom(key='fetched_metadata', data=data, **kwargs)

#check conditions on metadata
def check_metadata(protocol_id, **kwargs):
    fetched_metadata = pull_from_xcom(key='fetched_metadata',
                        task_ids=f'{protocol_id}.load_metadata.fetch',
                        **kwargs)

    computed_hash = calculate_utility_hash(protocol_id)
    last_block_timestamp = fetched_metadata["last_block_timestamp"] if fetched_metadata else find_earliest_start_date(protocol_id)

    data = {"computed_hash":computed_hash, "last_block_timestamp":last_block_timestamp}
    push_to_xcom(key="protocol_metadata", data=data, **kwargs)

    if fetched_metadata:
        if computed_hash == fetched_metadata["utility_hash"]:
            return f'{protocol_id}.load_metadata.finish'
        else:
            return f'{protocol_id}.load_metadata.update.add_udfs'
    else:
        return f'{protocol_id}.load_metadata.create.create_dataset'

#create metadata if not found    
def create_metadata(protocol_id):
    with TaskGroup(group_id='create') as task_group:

        _create_dataset = create_dataset(
                task_id="create_dataset",
                dataset_id=f"p_{protocol_id}",
            )
            
        #insert protocol_metadata with using date and hash from xcom
        _insert_metadata = execute_query(
            task_id="insert_metadata",
            sql=f"""
                    INSERT INTO `{PROJECT_ID}.{COMMON_DATASET}.protocol_metadata`
                    VALUES
                        ('{protocol_id}', 
                        TIMESTAMP {jinja_metadata(protocol_id, 'last_block_timestamp')},
                        {jinja_metadata(protocol_id, 'computed_hash')},
                        FALSE,
                        '{get_current_utc_timestamp()}'
                        )
                """,
            #in-case we delete the protocol record from metadata and it's dataset exists
            # trigger_rule=TriggerRule.ALL_DONE  
        )

        # add udfs in protocol dataset
        _add_udfs = execute_query(
            task_id="add_udfs",
            sql=generate_udfs_sql(protocol_id),
        )

        _create_dataset >> _insert_metadata >> _add_udfs
    
    return task_group

#update udfs and metadata if found
def update_metadata(protocol_id):
    with TaskGroup(group_id='update') as task_group:

        _add_udfs = execute_query(
            task_id="add_udfs",
            sql=generate_udfs_sql(protocol_id),
        )

        _update_metadata = execute_query(
            task_id="change_utility_hash",
            sql=f"""
                UPDATE `{PROJECT_ID}.{COMMON_DATASET}.protocol_metadata`
                SET utility_hash = {jinja_metadata(protocol_id, 'computed_hash')}
                WHERE id = '{protocol_id}'
            """
        )

        _add_udfs >> _update_metadata
    
    return task_group

#load protocol metadata and apply conditions accordingly
def load_metadata(protocol_id, **kwargs):
    
    with TaskGroup(group_id='load_metadata') as task_group:
       
        _fetch = PythonOperator(
            task_id='fetch',
            python_callable=fetch_metadata,
            provide_context=True,
            op_kwargs={'protocol_id': protocol_id},
            **kwargs
        )

        _check = BranchPythonOperator(
            task_id='check',
            python_callable=check_metadata,
            provide_context=True,
            op_kwargs={'protocol_id': protocol_id},
            **kwargs
        )

        _create = create_metadata(protocol_id)

        _update = update_metadata(protocol_id)

        _finish = EmptyOperator(
            task_id='finish',
            trigger_rule="none_failed",
            **kwargs
        )

        _fetch >> _check
        _check >> [_create, _update, _finish]
        _create >> _finish
        _update >> _finish

    return task_group
