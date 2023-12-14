import os, json
from datetime import datetime, timedelta

from include.common.utils.builder_helpers.update_metadata import update_last_block_timestamp, update_syncing_status
from include.common.utils.builder_helpers.check_historical_backlog import check_historical_backlog
from include.common.utils.builder_helpers.slack_notifications import notify_success, notify_failure
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from include.common.constants.index import PROTOCOLS

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.config import RenderConfig, ExecutionConfig
from cosmos.constants import LoadMode


def process_timestamps(**kwargs):
    last_block_timestamp_str = kwargs['dag_run'].conf.get('last_block_timestamp')
    
    last_block_timestamp = datetime.strptime(last_block_timestamp_str.strip("'"), '%Y-%m-%d %H:%M:%S %Z')
    next_block_timestamp = last_block_timestamp.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    
    print("last_block_timestamp: ", last_block_timestamp)
    print("next_block_timestamp: ", next_block_timestamp)
    
    kwargs['ti'].xcom_push(key='last_block_timestamp', value=last_block_timestamp.strftime('%Y-%m-%d %H:%M:%S %Z'))
    kwargs['ti'].xcom_push(key='next_block_timestamp', value=next_block_timestamp.strftime('%Y-%m-%d %H:%M:%S %Z'))
    

def builder(protocol_id):
    @dag(
        dag_id = protocol_id,
        schedule = None,
        start_date = datetime(2023,1,1),
        catchup = False,
        on_failure_callback=notify_failure
    )
    def protocol_dag():
        
        _start = EmptyOperator(
            task_id="start"
        )

        _finish = EmptyOperator(
            task_id="finish",
            trigger_rule="none_failed",
            on_success_callback=notify_success
        )
        
        _process_timestamps = PythonOperator(
            task_id='process_timestamps',
            python_callable=process_timestamps,
            provide_context=True
        )

        last_block_timestamp = "{{ ti.xcom_pull(task_ids='process_timestamps', key='last_block_timestamp') }}"
        next_block_timestamp = "{{ ti.xcom_pull(task_ids='process_timestamps', key='next_block_timestamp') }}"

        _transform = DbtTaskGroup(
            group_id='transform',
            project_config=DBT_PROJECT_CONFIG,
            profile_config=DBT_CONFIG,
            render_config=RenderConfig(
                load_method=LoadMode.DBT_LS,
                select=[f'path:models/protocol_positions/{protocol_id}/transform']
            ),
            execution_config=ExecutionConfig(
                dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
            ),
            operator_args={
                "install_deps": True,  
                "full_refresh": True,
                "vars": json.dumps(
                    {
                        "last_block_timestamp": f"'{last_block_timestamp}'",
                        "next_block_timestamp": f"'{next_block_timestamp}'"
                    }
                )
            },
        )
        
        _update_last_block_timestamp = update_last_block_timestamp(
            protocol_id=protocol_id,
            last_block_timestamp=next_block_timestamp,
            trigger_rule="none_failed"
        )

        _check_historical_backlog = check_historical_backlog(
            last_block_timestamp=next_block_timestamp
        )
        
        _run_again = TriggerDagRunOperator(
            task_id='run_again',
            trigger_dag_id=protocol_id,
            conf={
                "last_block_timestamp": next_block_timestamp
            }
        )
        
        _update_syncing_status = update_syncing_status(
            protocol_id=protocol_id,
            syncing_status=True,
            trigger_rule="none_failed"
        )
    
        _start >> _process_timestamps >> _transform >> _update_last_block_timestamp >> _check_historical_backlog
        _check_historical_backlog >> [_run_again, _update_syncing_status]
        _update_syncing_status >> _finish
            
    _protocol_dag = protocol_dag()

    return _protocol_dag


for protocol_id in PROTOCOLS:
    globals()[protocol_id] = builder(protocol_id)