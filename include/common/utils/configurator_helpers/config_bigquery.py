from include.common.constants.index import COMMON_DATASET, PROJECT_ID
from include.common.utils.bigquery import create_dataset, create_table, execute_query

from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

PROTOCOL_METADATA_SCHEMA = [
    {'name': 'id', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'last_block_timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    {'name': 'utility_hash', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'syncing_status', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
    {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
]

TOKEN_METADATA_SCHEMA = [
    {'name': 'chain', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    {'name': 'last_block_timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
]

raw_data_view_sql = f"""
    CREATE OR REPLACE VIEW `{PROJECT_ID}.{COMMON_DATASET}.ethereum_logs` AS
        SELECT * FROM `bigquery-public-data.crypto_ethereum.logs`;

    CREATE OR REPLACE VIEW `{PROJECT_ID}.{COMMON_DATASET}.ethereum_token_transfers` AS
        SELECT * FROM `bigquery-public-data.crypto_ethereum.token_transfers`; 
"""

def config_bigquery():
    with TaskGroup(group_id="configure_bigquery") as bigquery_config:
        _start = EmptyOperator(task_id="start")

        _finish = EmptyOperator(task_id="finish", trigger_rule="none_failed")

        with TaskGroup(group_id="common-ds") as common_dataset:
            _create_common_ds = create_dataset(
                task_id="create_common_ds",
                dataset_id=COMMON_DATASET
            )

            _create_common_metadata_table = create_table(
                task_id="create_common_metadata_table",
                dataset_id=COMMON_DATASET,
                table_id="protocol_metadata",
                table_schema=PROTOCOL_METADATA_SCHEMA,
                trigger_rule=TriggerRule.ALL_DONE
            )

            _add_raw_data_views = execute_query(
                task_id="add_raw_data_views",
                sql=raw_data_view_sql,
                trigger_rule=TriggerRule.ALL_DONE
            )

            _create_common_ds >> _create_common_metadata_table  >> _add_raw_data_views

        with TaskGroup(group_id="token-ds") as token_dataset:
            _create_token_ds = create_dataset(
                task_id="create_token_ds",
                dataset_id='token'
            )

            _create_token_metadata_table = create_table(
                task_id="create_token_metadata_table",
                dataset_id='token',
                table_id="token_metadata",
                table_schema=TOKEN_METADATA_SCHEMA,
                trigger_rule=TriggerRule.ALL_DONE
            )

            _create_token_ds >> _create_token_metadata_table

        _start >> [common_dataset, token_dataset] >> _finish

    return bigquery_config