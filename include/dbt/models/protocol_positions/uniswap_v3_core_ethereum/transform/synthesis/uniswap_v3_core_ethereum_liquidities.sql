{{ config(
    materialized='incremental',
    unique_key='owner, pool, tick_lower, tick_upper',
    schema='uniswap_v3_core_ethereum',
    alias='liquidities'
) }}

{% if not is_incremental() %}

    WITH initial_state AS (
        SELECT * FROM {{ ref('uniswap_v3_core_ethereum_liquidities_change') }}
    )

    SELECT * FROM initial_state

{% else %}

    WITH previous_state AS (
        SELECT
            *
        FROM 
            {{ source('uniswap_v3_core_ethereum', 'uniswap_v3_core_ethereum_liquidities') }}
    )

    SELECT
        COALESCE(ps.owner, cs.owner) as owner,
        COALESCE(ps.pool, cs.pool) as pool,
        COALESCE(ps.tick_lower, cs.tick_lower) as tick_lower,
        COALESCE(ps.tick_upper, cs.tick_upper) as tick_upper,
        COALESCE(ps.liquidity, 0) + COALESCE(cs.liquidity, 0) AS liquidity
    FROM 
        previous_state ps
    FULL OUTER JOIN 
        {{ ref('uniswap_v3_core_ethereum_liquidities_change') }} cs
        ON ps.owner = cs.owner
        AND ps.pool = cs.pool
        AND ps.tick_lower = cs.tick_lower
        AND ps.tick_upper = cs.tick_upper

{% endif %}
