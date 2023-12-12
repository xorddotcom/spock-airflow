from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from include.common.constants.index import PROJECT_ID
   
def update_last_block_timestamp(protocol_id, last_block_timestamp, trigger_rule='all_done', **kwargs):
    sql = f"""
        UPDATE `{PROJECT_ID}.spock.protocol_metadata`
        SET last_block_timestamp = TIMESTAMP('{last_block_timestamp}')
        WHERE id = '{protocol_id}'
    """
    
    return BigQueryExecuteQueryOperator(
        task_id ='update_last_block_timestamp',
        sql = sql,
        use_legacy_sql = False,
        gcp_conn_id = 'gcp',
        trigger_rule=trigger_rule,  
        **kwargs
    )
    
def update_syncing_status(protocol_id, syncing_status, trigger_rule='all_done', **kwargs):
    sql = f"""
        UPDATE `{PROJECT_ID}.spock.protocol_metadata`
        SET syncing_state = {syncing_status}
        WHERE id = '{protocol_id}'
    """
    
    return BigQueryExecuteQueryOperator(
        task_id ='update_syncing_status',
        sql = sql,
        use_legacy_sql = False,
        gcp_conn_id = 'gcp',
        trigger_rule=trigger_rule,  
        **kwargs
    )


