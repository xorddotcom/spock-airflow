{{ config(
    materialized='table',
    schema='uniswap_v3_core_ethereum',
    alias='liquidities_change'
)}}

WITH mint_amounts AS (
    SELECT
        owner,
        pool,
        tick_lower,
        tick_upper,
        SUM(amount) AS mint_total_amount,
    FROM {{ ref('uniswap_v3_core_ethereum_mint_logs') }}
    GROUP BY owner, pool, tick_lower, tick_upper
),
burn_amounts AS (
    SELECT
        owner,
        pool,
        tick_lower,
        tick_upper,
        SUM(amount) AS burn_total_amount,
    FROM {{ ref('uniswap_v3_core_ethereum_burn_logs') }}
    GROUP BY owner, pool, tick_lower, tick_upper
)

SELECT
    COALESCE(ma.owner, b.owner) as owner,
    COALESCE(ma.pool, b.pool) as pool,
    COALESCE(ma.tick_lower, b.tick_lower) as tick_lower,
    COALESCE(ma.tick_upper, b.tick_upper) as tick_upper,
    COALESCE(ma.mint_total_amount, 0) - COALESCE(b.burn_total_amount, 0) AS liquidity,
FROM mint_amounts ma
FULL OUTER JOIN burn_amounts ba
    ON ma.owner = ba.owner
    AND ma.pool = ba.pool
    AND ma.tick_lower = ba.tick_lower
    AND ma.tick_upper = ba.tick_upper