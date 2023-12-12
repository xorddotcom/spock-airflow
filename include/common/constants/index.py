from glob import glob

GCP_CONN_ID = 'gcp'
SLACK_CONN_ID = 'slack'
PROJECT_ID = 'spock-main'
PROTOCOL_POSITIONS_PATH = '/usr/local/airflow/include/dbt/models/protocol_positions'

PROTOCOLS = [protocol.split('/')[-1] for protocol in glob(f"{PROTOCOL_POSITIONS_PATH}/*")]

COMMON_DATASET = "common"