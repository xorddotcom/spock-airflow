{{ config(
    materialized='table',
    schema='uniswap_v3_core_ethereum',
    alias='burn_logs'
)}}

WITH decoded_log AS (
  SELECT
    `spock-main.historical_test.decode_burn_log`(data, topics) AS decoded_data,
    LOWER(address) AS pool,
    block_number
  FROM 
    {{ ref('uniswap_v3_core_ethereum_filtered_logs') }}
  WHERE 
    -- BURN
    topics[0] = '0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c'
)

SELECT 
    LOWER(decoded_data.sender) as `sender`,
    LOWER(decoded_data.owner) as `owner`,
    decoded_data.tickLower as `tick_lower`,
    decoded_data.tickUpper as `tick_upper`,
    decoded_data.amount as `amount`,
    decoded_data.amount0 as `amount_0`,
    decoded_data.amount1 as `amount_1`,
    `pool`
FROM 
    decoded_log
