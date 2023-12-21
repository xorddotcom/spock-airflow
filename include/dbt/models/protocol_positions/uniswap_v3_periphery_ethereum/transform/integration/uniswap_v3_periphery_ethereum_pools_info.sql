{{ config(
    materialized='table',
    schema='uniswap_v3_periphery_ethereum',
    alias='pools_info'
)}}

WITH decoded_log AS (
  SELECT
    `spock-main.uniswap_v3_periphery_ethereum.decode_swap_log`(data, topics) AS decoded_data,
    LOWER(address) AS pool,
    block_number,
    log_index
  FROM 
    {{ ref('uniswap_v3_periphery_ethereum_filtered_logs') }}
  WHERE 
    -- SWAP
    topics[OFFSET(0)] = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67'
)

SELECT 
  p.pool AS pool,
  p.token_0 AS token_0,
  p.token_1 AS token_1,
  p.decimals_0 AS decimals_0,
  p.decimals_1 AS decimals_1,
  p.fee AS fee,
  p.liquidity AS liquidity,
  p.sqrtPriceX96 AS sqrt_price_x96,
  p.tick AS tick
FROM 
  {{ ref('uniswap_v3_periphery_ethereum_pools') }} p
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
-- ls : last swap
