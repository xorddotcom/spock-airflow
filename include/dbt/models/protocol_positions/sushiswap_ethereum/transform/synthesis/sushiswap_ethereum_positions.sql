{{ config(
    materialized='incremental',
    schema='sushiswap_ethereum',
    alias='positions'
) }}

SELECT
    lpb.pair,
    lpb.wallet,
    pd.token_0,
    pd.token_1,
    (lpb.balance * pd.reserve_0 / (pd.reserve_0 + pd.reserve_1)) AS amount_0,
    (lpb.balance * pd.reserve_1 / (pd.reserve_0 + pd.reserve_1)) AS amount_1
FROM
    {{ ref('sushiswap_ethereum_lp_balances') }} lpb
JOIN
    {{ ref('sushiswap_ethereum_pairs_details') }} pd
ON 
    lpb.pair = pd.pair


-- lpb : lp balances
-- pd : pool details