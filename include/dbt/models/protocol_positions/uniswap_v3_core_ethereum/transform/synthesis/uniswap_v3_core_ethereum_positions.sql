{{ config(
    materialized='incremental',
    schema='uniswap_v3_core_ethereum',
    alias='positions'
) }}

SELECT 
    pl.owner,
    pl.pool,
    cd.token_0,
    cd.token_1,
    pl.tick_lower,
    pl.tick_upper,
    cd.tick,
    cd.fee,
    `spock-main.uniswap_v3_core_ethereum.uniswap_v3_core_ethereum_decode_position`(
        1,
        cd.token_0,
        cd.token_1,
        cd.decimals_0,
        cd.decimals_1,
        cd.fee,
        cd.sqrt_price_x96,
        cd.liquidity,
        cd.tick,
        pl.liquidity,
        pl.tick_lower,
        pl.tick_upper
    ) AS amounts
FROM 
    {{ ref('uniswap_v3_core_ethereum_liquidities') }} AS pl
INNER JOIN 
    {{ ref('uniswap_v3_core_ethereum_pools_info') }} AS cd
ON
    pl.pool = cd.pool
WHERE
    cd.decimals_0 IS NOT NULL
    AND CAST(cd.decimals_0 AS INT) > 0
    AND cd.decimals_1 IS NOT NULL
    AND CAST(cd.decimals_1 AS INT) > 0
    AND pl.liquidity > 0


-- pl : positions liquidities
-- cd : positions calculation data