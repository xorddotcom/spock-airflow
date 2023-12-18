from airflow.decorators import task
  
@task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
def check_transform(scan_name, protocol_id):
  import os
  from soda.scan import Scan
  
  config_file = f'include/soda/configurations/{protocol_id}_configuration.yml'

  if not os.path.exists(config_file):
      yaml_config = f"""
data_source {protocol_id}:
  type: bigquery
  connection:
    account_info_json_path: /usr/local/airflow/include/gcp/service_account.json
    auth_scopes:
      - https://www.googleapis.com/auth/bigquery
      - https://www.googleapis.com/auth/cloud-platform
      - https://www.googleapis.com/auth/drive
    project_id: "spock-main"
    dataset: p_{protocol_id}

soda_cloud:
  host: cloud.us.soda.io
  api_key_id: ${{SODA_API_KEY_ID}}
  api_key_secret: ${{SODA_API_KEY_SECRET}}
      """

      with open(config_file, 'w+') as file:
          file.write(yaml_config)

  scan = Scan()
  scan.set_verbose()
  scan.add_configuration_yaml_file(config_file)
  scan.set_data_source_name(protocol_id)
  scan.add_sodacl_yaml_files(f'include/dbt/models/protocol_positions/{protocol_id}/check')
  scan.set_scan_definition_name(scan_name)

  result = scan.execute()
  print(scan.get_logs_text())

  if result != 0:
      raise ValueError('Soda Scan failed')

  return result