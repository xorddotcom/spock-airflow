from include.common.constants.index import PROJECT_ID, COMMON_DATASET
from include.common.utils.bigquery import execute_query
from airflow.utils.trigger_rule import TriggerRule

   
def update_last_block_timestamp(protocol_id, last_block_timestamp, trigger_rule=TriggerRule.ALL_DONE):
    sql = f"""
        UPDATE `{PROJECT_ID}.{COMMON_DATASET}.protocol_metadata`
        SET last_block_timestamp = TIMESTAMP('{last_block_timestamp}')
        WHERE id = '{protocol_id}'
    """
    return execute_query(
        task_id ='update_last_block_timestamp',
        sql=sql,
        trigger_rule=trigger_rule
    )
    
   
    
def update_syncing_status(protocol_id, syncing_status, trigger_rule=TriggerRule.ALL_DONE):
    sql = f"""
        UPDATE `{PROJECT_ID}.{COMMON_DATASET}.protocol_metadata`
        SET syncing_status = {syncing_status}
        WHERE id = '{protocol_id}'
    """
    return execute_query(
        task_id ='update_syncing_status',
        sql=sql,
        trigger_rule=trigger_rule
    )


