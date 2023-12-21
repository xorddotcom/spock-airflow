{{ config(
    materialized='table',
    schema='uniswap_v3_periphery_ethereum',
    alias='filtered_logs'
)}}

{% set last_block_timestamp = 'TIMESTAMP(' + var('last_block_timestamp') + ')' if var('last_block_timestamp') %}
{% set next_block_timestamp = 'TIMESTAMP(' + var('next_block_timestamp') + ')' if var('next_block_timestamp') %}
{% set roll_back = var('roll_back') == 'True' if var('roll_back') %}

SELECT 
    * 
FROM 
    {{ source('common', 'ethereum_logs') }}
WHERE 
    ARRAY_LENGTH(topics) > 0
    AND
    (
        -- SWAP
        topics[0] = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67'
        OR
        -- POOL_CREATED
        topics[0] = '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118'
        OR
        -- POSITION_MANAGER_LOGS
        (
            (
                -- INCREASE_LIQUIDITY
                topics[0] = '0x3067048beee31b25b2f1681f88dac838c8bba36af25bfb2b7cf7473a5847e35f'
                OR
                -- DECREASE_LIQUIDITY
                topics[0] = '0x26f6a048ee9138f2c0ce266f322cb99228e8d619ae2bff30c67f8dcf9d2377b4'
                OR
                -- TRANSFER
                topics[0] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
            )
            AND
            address = '0xc36442b4a4522e871399cd717abdd847ab11fe88'
        )
    )
    AND
    block_timestamp > {{ last_block_timestamp }}
    AND
    block_timestamp <= {{ next_block_timestamp }}