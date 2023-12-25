{{ config(
    materialized='incremental',
    unique_key='pair',
    schema='sushiswap_ethereum',
    alias='pairs_details'
)}}

WITH decoded_log AS (
  SELECT
    `spock-main.sushiswap_ethereum.decode_sync_log`(data, topics) AS decoded_data,
    LOWER(address) AS pair,
    block_number,
    log_index
  FROM 
    {{ ref('sushiswap_ethereum_filtered_logs') }}
  WHERE 
    -- SYNC
    topics[OFFSET(0)] = '0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1'
)

SELECT 
  p.pair AS pair,
  p.token_0 AS token_0,
  p.token_1 AS token_1,
  p.decimals_0 AS decimals_0,
  p.decimals_1 AS decimals_1,
  ls.reserve0 / POWER(10, p.decimals_0) AS reserve_0,
  ls.reserve1 / POWER(10, p.decimals_1) AS reserve_1
FROM 
  {{ ref('sushiswap_ethereum_pairs') }} p
JOIN (
  SELECT 
    decoded_data,
    pool,
    ROW_NUMBER() OVER (PARTITION BY pool ORDER BY block_number DESC, log_index DESC) AS rn
  FROM decoded_log
) ls 
ON
  ls.pool = pools.pool
WHERE ls.rn = 1


-- p : pools
-- ls : last sync
