import os, json

from airflow import settings
from airflow.models import Connection

from include.common.constants.index import PROJECT_ID, GCP_CONN_ID, SLACK_CONN_ID

def add_gcp_connection():
    new_conn = Connection(
        conn_id=GCP_CONN_ID,
        conn_type='google_cloud_platform',
    )
    extra_field = {
        "extra__google_cloud_platform__project": PROJECT_ID,
        "extra__google_cloud_platform__key_path": '/usr/local/airflow/include/gcp/service_account.json'
    }

    session = settings.Session()

    #checking if connection exist
    if session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).first():
        my_connection = session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).one()
        my_connection.set_extra(json.dumps(extra_field))
        session.add(my_connection)
        session.commit()
    else: #if it doesn't exit create one
        new_conn.set_extra(json.dumps(extra_field))
        session.add(new_conn)
        session.commit()
        
        
def add_slack_connection():
    new_conn = Connection(
        conn_id=SLACK_CONN_ID,
        conn_type='slack',
        password=os.environ.get('SLACK_API_KEY'),
    )

    session = settings.Session()

    #checking if connection exist
    if session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).first():
        my_connection = session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).one()
        session.add(my_connection)
        session.commit()
    else: #if it doesn't exit create one
        session.add(new_conn)
        session.commit()