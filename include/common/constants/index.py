from glob import glob

GCP_CONN_ID = 'gcp'
SLACK_CONN_ID = 'slack'
PROJECT_ID = 'spock-main'
PROTOCOLS_PATH = '/usr/local/airflow/include/dbt/models'

PROTOCOLS = [protocol.split('/')[-1] for protocol in glob(f"{PROTOCOLS_PATH}/*")]

COMMON_DATASET = "common"