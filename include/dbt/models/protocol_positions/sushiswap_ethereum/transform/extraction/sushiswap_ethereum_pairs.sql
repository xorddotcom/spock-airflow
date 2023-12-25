{{ config(
  materialized='incremental',
  unique_key='pair',
  schema='sushiswap_ethereum',
  alias='pairs'
) }}

WITH decoded_log AS (
  SELECT
    `spock-main.sushiswap_ethereum.decode_poolcreated_log`(data, topics) AS decoded_data
  FROM 
    {{ ref('sushiswap_ethereum_filtered_logs') }}
  WHERE
    -- PAIR_CREATED
    topics[0] = '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9'
)

SELECT 
    LOWER(decoded_data.pair) as pool,
    LOWER(decoded_data.token0) as token_0,
    LOWER(decoded_data.token1) as token_1,
    tokens_0.decimals as decimals_0,
    tokens_1.decimals as decimals_1,
FROM 
    decoded_log
LEFT JOIN 
    {{ source('common', 'tokens') }} AS tokens_0 ON LOWER(decoded_log.decoded_data.token0) = LOWER(tokens_0.address)
LEFT JOIN 
    {{ source('common', 'tokens') }} AS tokens_1 ON LOWER(decoded_log.decoded_data.token1) = LOWER(tokens_1.address)
