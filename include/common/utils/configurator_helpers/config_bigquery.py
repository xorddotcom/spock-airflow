from include.common.constants.index import COMMON_DATASET, PROJECT_ID
from include.common.utils.bigquery import create_dataset, create_table, execute_query

from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

PROTOCOL_METADATA_SCHEMA = [
    {'name': 'id', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'last_synced', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    {'name': 'utility_hash', 'type': 'STRING', 'mode': 'NULLABLE'}
]

raw_data_view_sql = f"""
    CREATE OR REPLACE VIEW `{PROJECT_ID}.{COMMON_DATASET}.ethereum_logs` AS
        SELECT 
            *
        FROM 
            `bigquery-public-data.crypto_ethereum.logs`
"""

def config_bigquery():
    with TaskGroup(group_id="configure_bigquery") as bigquery_config:
        _create_common_ds = create_dataset(
            task_id="create_common_ds",
            dataset_id=COMMON_DATASET
        )

        _create_metadata_table = create_table(
            task_id="create_metadata_table",
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

        _create_common_ds >> _create_metadata_table  >> _add_raw_data_views

    return bigquery_config