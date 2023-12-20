{{ config(
  materialized='view',
  scheme={{ var('scheme') }}
)}}

WITH decoded_log AS (
  SELECT
    `{{ var("project_id") }}.{{ var("dataset_id") }}.decode_vaultcreated_log`(data, topics) AS decoded_data
  FROM 
    {{ ref('a51_finance_ethereum_filtered_logs') }}
  WHERE 
    -- VAULT_CREATED
    topics[0] = '0xaf0c778584d4b20d9768e68ed24ede73b034bc5747a377a69f1db53ff3933def'
)

SELECT
    LOWER(decoded_data._tokenA) as `_tokenA`,
    LOWER(decoded_data._tokenB) as `_tokenB`,
    decoded_data._fee as `_fee`,
    LOWER(decoded_data._vault) as `_vault`,
FROM
    decoded_log