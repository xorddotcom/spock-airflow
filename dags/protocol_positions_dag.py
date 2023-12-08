import os

from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig, ExecutionConfig

from include.common.utils.bigquery import create_dataset, execute_query, create_table
from include.common.utils.parse_table_definition import generate_parsers_udf_sql
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from include.common.constants.index import PROTOCOLS, PROJECT_ID


# EVENTS_METADATA_SCHEMA = [
#     {'name': 'topic', 'type': 'STRING', 'mode': 'NULLABLE'},
#     {'name': 'last_synced', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'}
# ]

# def processEventsMetadata(**kwargs):
#     task_instance = kwargs['ti']
#     query_result = task_instance.xcom_pull(task_ids='fetch_events_metadata')

#     print("events_data",query_result)


def build_protocol_position_dag(project):
    @dag(dag_id=project,
         schedule=None,
         start_date=datetime(2023,11,1),
         catchup=False)
    def new_dag():

        dataset_id = f"p_{project}"

        _start = EmptyOperator(task_id="start")

        _finish = EmptyOperator(task_id="finish", trigger_rule="none_failed")

        # _create_ds = create_dataset(task_id='create_dataset', dataset_id=dataset_id)

        # _add_udfs = execute_query(
        #      task_id='add_udfs',
        #      sql=generate_parsers_udf_sql(project)
        #     )

        # _create_event_metadata_table = create_table(
        #     task_id="create_event_metadata_table",
        #     dataset_id=dataset_id,
        #     table_id="events_metadata",
        #     table_schema=EVENTS_METADATA_SCHEMA
        # )

        # _events_metadata = execute_query(
        #     task_id='fetch_events_metadata',
        #     sql=f"SELECT * FROM {PROJECT_ID}.{dataset_id}.events_metadata"
        #     # xcom_push=True
        # )

        # _process_events_metadata = PythonOperator(
        #     task_id="process_events_metadata",
        #     python_callable=processEventsMetadata,
        #     provide_context=True,
        # )

        _transform = DbtTaskGroup(
            group_id='transform',
            project_config=DBT_PROJECT_CONFIG,
            profile_config=DBT_CONFIG,
            render_config=RenderConfig(
                load_method=LoadMode.DBT_LS,
                select=[f'path:models/protocol_positions/{project}/transform']
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
    
        chain(_start, _transform, _finish)
    
    generated_dag = new_dag()

    return generated_dag


#dynamically create dags form all protocols
for dag_id in PROTOCOLS:
    globals()[dag_id] = build_protocol_position_dag(dag_id)