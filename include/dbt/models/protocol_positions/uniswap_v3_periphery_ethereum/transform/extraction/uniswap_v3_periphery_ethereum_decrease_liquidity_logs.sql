{{ config(
    materialized='table',
    schema='uniswap_v3_periphery_ethereum',
    alias='decrease_liquidity_logs'
)}}

WITH decoded_log AS (
  SELECT
    `spock-main.uniswap_v3_periphery_ethereum.decode_decrease_liquidity_log`(data, topics) AS decoded_data
  FROM 
    {{ ref('uniswap_v3_periphery_ethereum_filtered_logs') }}
  WHERE 
    -- DECREASE_LIQUIDITY
    topics[0] = '0x26f6a048ee9138f2c0ce266f322cb99228e8d619ae2bff30c67f8dcf9d2377b4'
)

SELECT 
    decoded_data.tokenId AS `token_id`,
    decoded_data.liquidity AS `liquidity`,
FROM 
    decoded_log
