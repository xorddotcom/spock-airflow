from include.common.constants.index import GCP_CONN_ID, PROJECT_ID, COMMON_DATASET
from include.common.utils.bigquery import execute_query, execute_raw_query
from include.common.utils.xcom import push_to_xcom, pull_from_xcom_jinja
from include.common.utils.token_helpers.resolve_tokens import reolsve_erc20s
from include.common.utils.time import get_current_utc_timestamp
from include.common.utils.web3.provider import Network, Web3Node

from datetime import datetime
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.task_group import TaskGroup

DEFAULT_TOKENS_DATE = '2023-12-26 23:59:59 UTC'

def check_config(**kwargs):
    load_csv = kwargs['dag_run'].conf.get('load_csv')
    if load_csv == True:
        last_block_timestamp = kwargs['dag_run'].conf.get('last_block_timestamp')
        last_block_timestamp = DEFAULT_TOKENS_DATE if last_block_timestamp == None else last_block_timestamp
        push_to_xcom(
            key='token_config',
            data={
                "last_block_timestamp": last_block_timestamp,
            },
            **kwargs
        )
        return 'load_csv.upload_tokens_csv_to_gcs'
    else:
      return 'regular_syncing.fetch_token_metadata'

def fetch_token_metadata(network:Web3Node, **kwargs):
    result = execute_raw_query(
        f"""
            SELECT last_block_timestamp FROM `{PROJECT_ID}.token.token_metadata`
            WHERE chain = {network.chain_id}
            LIMIT 1
        """
    )
    last_block_timestamp = next(result, None)['last_block_timestamp']
    push_to_xcom(
            key='token_metadata',
            data={
                "last_block_timestamp": last_block_timestamp,
            },
            **kwargs
    )

def fetch_resolve_load_new_tokens(network:Web3Node, last_block_timestamp, **kwargs):
   chain_name = network.name.lower()

   result = execute_raw_query(
        f"""
            SELECT DISTINCT token_address FROM `{PROJECT_ID}.{COMMON_DATASET}.{chain_name}_token_transfers`
            WHERE block_timestamp > TIMESTAMP('{last_block_timestamp}')

            EXCEPT DISTINCT

            SELECT DISTINCT address AS token_address FROM `{PROJECT_ID}.token.{chain_name}_tokens`
        """
    )
   token_addresses = [row['token_address'] for row in result]
   print(f"fetched[{len(token_addresses)}] = ", token_addresses)

   #push current time so after resolving token it will se replaced in metadata
   new_last_block_timestamp = get_current_utc_timestamp()
   push_to_xcom(
        key='token_new_metadata',
        data={
            "new_last_block_timestamp": new_last_block_timestamp,
        },
        **kwargs
    )

   tokens = reolsve_erc20s(addresses=token_addresses, network=network, batch_size=20)
   print(f"recolved[{len(tokens)}] = ", tokens)
   
   insert_values = []
   for token in tokens:
       address, name, symbol, decimals, _type = token['address'], token['name'], token['symbol'], token['decimals'], token['type']
       name = name.replace("'", "\\'")
       symbol = symbol.replace("'", "\\'")
       insert_values.append(f"('{address}','{name}','{symbol}',{decimals},'{_type}')")

   values_section = ', '.join(insert_values)

   execute_raw_query(
        f"""
            INSERT INTO `{PROJECT_ID}.token.{chain_name}_tokens`
            VALUES {values_section}
        """
    )
   print('tokens loaded')

def create_token_dag(network:Web3Node):
    @dag(
        dag_id='tokens_dag',
        schedule_interval="@daily",
        catchup=True,
        start_date=datetime(2023, 12, 23),
    )
    def tokens_dag():
        _start = EmptyOperator(task_id="start")
        _finish = EmptyOperator(task_id="finish", trigger_rule="none_failed")

        chain_name = network.name.lower()

        _check_config = BranchPythonOperator(
            task_id='check_config',
            python_callable=check_config,
            provide_context=True
        )

        with TaskGroup(group_id='load_csv') as load_csv: 
            _upload_tokens_csv_to_gcs = LocalFilesystemToGCSOperator(
                task_id='upload_tokens_csv_to_gcs',
                src=f"include/dataset/{chain_name}_tokens.csv",
                dst=f"tokens/{chain_name}_tokens.csv",
                bucket='spock_common',
                gcp_conn_id=GCP_CONN_ID,
                mime_type='text/csv'
            )

            _load_tokens_gcs_csv = aql.load_file(
                task_id='load_tokens_gcs_csv',
                input_file=File(
                    f"gs://spock_common/tokens/{chain_name}_tokens.csv",
                    conn_id=GCP_CONN_ID,
                    filetype=FileType.CSV,
                ),
                output_table=Table(
                    name=f"{chain_name}_tokens",
                    conn_id=GCP_CONN_ID,
                    metadata=Metadata(schema='token'),
                ),
                use_native_support=False,
            )

            last_block_timestamp = pull_from_xcom_jinja(
                task_ids='check_config',
                key='token_config', 
                dict_key='last_block_timestamp'
            )

            _add_metadata = execute_query(
                task_id='add_tokens_metadata',
                sql=f"""
                        INSERT INTO `{PROJECT_ID}.token.token_metadata`
                        VALUES ({network.chain_id}, TIMESTAMP '{last_block_timestamp}')
                """
            )

            _upload_tokens_csv_to_gcs >> _load_tokens_gcs_csv >> _add_metadata

        with TaskGroup(group_id='regular_syncing') as regular_syncing:
            _fetch_token_metadata = PythonOperator(
                task_id='fetch_token_metadata',
                python_callable=fetch_token_metadata,
                op_kwargs={"network":network},
                provide_context=True
            )

            last_block_timestamp = pull_from_xcom_jinja(
                    task_ids='regular_syncing.fetch_token_metadata',
                    key='token_metadata', 
                    dict_key='last_block_timestamp'
                )

            _fetch_resolve_load_new_tokens = PythonOperator(
                task_id='fetch_resolve_load_new_tokens',
                python_callable=fetch_resolve_load_new_tokens,
                op_kwargs={"network":network, "last_block_timestamp":last_block_timestamp},
                provide_context=True
                
            )

            new_last_block_timestamp = pull_from_xcom_jinja(
                    task_ids='regular_syncing.fetch_resolve_load_new_tokens',
                    key='token_new_metadata', 
                    dict_key='new_last_block_timestamp'
                )
            
            _update_token_metadata = execute_query(
                task_id='update_token_metadata',
                sql=f"""
                        UPDATE `{PROJECT_ID}.token.token_metadata`
                        SET last_block_timestamp = TIMESTAMP('{new_last_block_timestamp}')
                        WHERE chain = {network.chain_id}
                """
                )

            _fetch_token_metadata >> _fetch_resolve_load_new_tokens >> _update_token_metadata

        _start >> _check_config >> [load_csv, regular_syncing]
        load_csv >> _finish
        regular_syncing >> _finish

    tokens_dag()

create_token_dag(Network.Ethereum)