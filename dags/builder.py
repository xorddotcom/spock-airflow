import os
from datetime import datetime

from include.common.utils.builder_helpers.update_metadata import update_metadata
from include.common.utils.builder_helpers.check_historical_backlog import check_historical_backlog
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from include.common.constants.index import PROTOCOLS

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.config import RenderConfig, ExecutionConfig
from cosmos.constants import LoadMode


def builder(protocol_id):
    @dag(
        dag_id = protocol_id,
        schedule = None,
        start_date = datetime(2023,1,1),
        catchup = False
    )
    def build():

        _start = EmptyOperator(task_id="start")

        _finish = EmptyOperator(task_id="finish", trigger_rule="none_failed")
                
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
            "vars": '{"UNI_TIME": "hello" }'
            },
        )
        
        _update_metadata = update_metadata(protocol_id)

        _check_historical_backlog = check_historical_backlog(protocol_id)

        _run_again = TriggerDagRunOperator(
            task_id='run_again',
            trigger_dag_id=protocol_id,
        )
    
        _start >> _transform >> _update_metadata >> _check_historical_backlog
        _check_historical_backlog >> [_run_again, _finish]
    
    new_dag = build()

    return new_dag


for protocol_id in PROTOCOLS:
    globals()[protocol_id] = builder(protocol_id)