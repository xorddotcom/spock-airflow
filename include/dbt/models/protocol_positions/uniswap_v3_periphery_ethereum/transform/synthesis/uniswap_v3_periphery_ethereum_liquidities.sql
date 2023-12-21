{{ config(
    materialized='incremental',
    unique_key='token_id, owner, pool, tick_lower, tick_upper',
    schema='uniswap_v3_periphery_ethereum',
    alias='liquidities'
) }}

{% set roll_back = var('roll_back') == 'True' if var('roll_back') %}

{% if not is_incremental() or roll_back %}

    WITH initial_state AS (
        SELECT * FROM {{ ref('uniswap_v3_periphery_ethereum_liquidities_change') }}
    )

    SELECT * FROM initial_state

{% else %}

    WITH previous_state AS (
        SELECT
            *
        FROM 
            {{ source('uniswap_v3_periphery_ethereum', 'uniswap_v3_periphery_ethereum_liquidities') }}
    )

    SELECT
    COALESCE(ps.token_id, cs.token_id) AS token_id,
    COALESCE(ps.pool, cs.pool) as pool,
    COALESCE(ps.owner, cs.owner) as owner,
    COALESCE(ps.liquidity, 0) + COALESCE(cs.liquidity, 0) AS liquidity,
    COALESCE(ps.tick_lower, cs.tick_lower) as tick_lower,
    COALESCE(ps.tick_upper, cs.tick_upper) as tick_upper,
    FROM
        previous_state ps
    FULL OUTER JOIN
        {{ ref('uniswap_v3_periphery_ethereum_liquidities_change') }} cs
        ON ps.token_id = cs.token_id


{% endif %}
