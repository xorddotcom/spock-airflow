import os, json
from datetime import datetime, timedelta

from include.common.utils.operator_helpers.update_metadata import update_last_block_timestamp, update_syncing_status
from include.common.utils.operator_helpers.check_historical_backlog import check_historical_backlog
from include.common.utils.slack_notifications import notify_success, notify_failure
from include.soda.check import check_transform
from include.common.utils.xcom import push_to_xcom
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG

from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.config import RenderConfig, ExecutionConfig
from cosmos.constants import LoadMode


def load_config(**kwargs):
    last_block_timestamp_str = kwargs['dag_run'].conf.get('last_block_timestamp')
    run_once = kwargs['dag_run'].conf.get('run_once')
    
    last_block_timestamp = datetime.strptime(last_block_timestamp_str.strip("'"), '%Y-%m-%d %H:%M:%S %Z')
    next_block_timestamp = last_block_timestamp.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    run_once = True if run_once else False
    
    print(
        f"""
        {{
            last_block_timestamp: {last_block_timestamp},
            next_block_timestamp: {next_block_timestamp},
            run_once: {run_once},
        }}
        """                                                                                                                                                                  
    )
    
    push_to_xcom(
        key='config',
        data={
            "last_block_timestamp": last_block_timestamp.strftime('%Y-%m-%d %H:%M:%S %Z'),
            "next_block_timestamp": next_block_timestamp.strftime('%Y-%m-%d %H:%M:%S %Z'),
            "run_once": run_once
        },
        **kwargs
    )

PROTOCOL_ID = 'template_position_dag'
    
@dag(
    dag_id = PROTOCOL_ID,
    schedule = None,
    catchup = False,
    start_date = datetime(2023,1,1),
    on_failure_callback=notify_failure
)
def protocol_dag():
    
    _start = EmptyOperator(
        task_id="start"
    )

    _finish = EmptyOperator(
        task_id="finish",
        trigger_rule=TriggerRule.NONE_FAILED,
        on_success_callback=notify_success
    )
    
    _load_config = PythonOperator(
        task_id='load_config',
        python_callable=load_config,
        provide_context=True
    )
    
    last_block_timestamp = "{{ ti.xcom_pull(task_ids='load_config', key='config')['last_block_timestamp'] }}"
    next_block_timestamp = "{{ ti.xcom_pull(task_ids='load_config', key='config')['next_block_timestamp'] }}"
    run_once = "{{ ti.xcom_pull(task_ids='load_config', key='config')['run_once'] }}"

    _transform = DbtTaskGroup(
        group_id='transform',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=[f'path:models/protocol_positions/{PROTOCOL_ID}/transform']
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
    
    with TaskGroup(group_id='parallel_task_group_1') as parallel_task_group_1:
        update_last_block_timestamp(
            protocol_id=PROTOCOL_ID,
            last_block_timestamp=next_block_timestamp,
            trigger_rule=TriggerRule.NONE_FAILED
        )
        
        _check_historical_backlog = check_historical_backlog(
            last_block_timestamp=next_block_timestamp,
            run_once=run_once,
            options=["run_again", "parallel_task_group_2"]
        )
        

    with TaskGroup(group_id='parallel_task_group_2') as parallel_task_group_2:
        check_transform(
            scan_name='check_transform',
            protocol_id=PROTOCOL_ID
        )
        
        update_syncing_status(
            protocol_id=PROTOCOL_ID,
            syncing_status=False,
            trigger_rule=TriggerRule.NONE_FAILED
        )
        
    _run_again = TriggerDagRunOperator(
        task_id='run_again',
        trigger_dag_id=PROTOCOL_ID,
        conf={
            "last_block_timestamp": next_block_timestamp
        }
    )

    _start >> _load_config >> _transform 
    _transform >> parallel_task_group_1

    _check_historical_backlog >> [_run_again, parallel_task_group_2]
    parallel_task_group_2 >> _finish
        
protocol_dag()