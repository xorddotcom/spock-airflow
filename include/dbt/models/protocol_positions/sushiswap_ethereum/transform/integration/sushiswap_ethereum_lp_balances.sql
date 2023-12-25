{{ config(
    materialized='incremental',
    unique_key='pair, wallet',
    schema='sushiswap_ethereum',
    alias='lp_balances'
) }}

{% set roll_back = var('roll_back') == 'True' if var('roll_back') %}

WITH balances_change AS (
    SELECT pair, wallet, SUM(lp_amount) / POWER(10, 18) AS balance
    FROM (
        SELECT from_address AS wallet, pair, -value AS lp_amount
        FROM {{ ref('sushiswap_ethereum_lp_transfers') }}
        UNION ALL
        SELECT to_address AS wallet, pair, value AS lp_amount
        FROM {{ ref('sushiswap_ethereum_lp_transfers') }}
    )
    GROUP BY pair, wallet
)

{% if not is_incremental() or roll_back %}

    SELECT * FROM balances_change

{% else %}

    WITH previous_state AS (
        SELECT
            *
        FROM 
            {{ source('sushiswap_ethereum', 'sushiswap_ethereum_lp_balances') }}
    )

    SELECT
        COALESCE(ps.pair, cs.pair) as pair,
        COALESCE(ps.wallet, cs.wallet) as wallet,
        COALESCE(ps.balance, 0) + COALESCE(cs.balance, 0) AS balance
    FROM 
        previous_state ps
    FULL OUTER JOIN (
        SELECT 
            *
        FROM balances_change
    ) cs
    ON
        ps.pair = cs.pair
        AND 
        ps.wallet = cs.wallet

{% endif %}