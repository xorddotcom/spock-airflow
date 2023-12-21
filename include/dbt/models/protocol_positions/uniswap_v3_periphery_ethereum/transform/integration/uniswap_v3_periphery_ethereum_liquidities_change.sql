{{ config(
    materialized='table',
    schema='uniswap_v3_periphery_ethereum',
    alias='liquidities_change'
)}}

WITH increased_liquidity AS (
    SELECT
        token_id,
        SUM(liquidity) AS increased_liquidity
    FROM
        {{ref('uniswap_v3_periphery_ethereum_increase_liquidity_logs')}}
    GROUP BY
        token_id
),

decreased_liquidity AS (
    SELECT
        token_id,
        SUM(liquidity) AS decreased_liquidity
    FROM
        {{ref('uniswap_v3_periphery_ethereum_decrease_liquidity_logs')}}
    GROUP BY
        token_id
)

SELECT
    COALESCE(inc.token_id, dec.token_id) AS token_id,
    COALESCE(increased_liquidity, 0) - COALESCE(decreased_liquidity, 0) AS liquidity,
    nft.pool AS pool,
    nft.owner AS owner,
    nft.tick_lower AS tick_lower,
    nft.tick_upper AS tick_upper,
FROM
    increased_liquidity inc
FULL OUTER JOIN
    decreased_liquidity dec
ON
    inc.token_id = dec.token_id
JOIN
    {{ref('uniswap_v3_periphery_ethereum_nfts')}} nft
ON
    inc.token_id = nft.token_id
