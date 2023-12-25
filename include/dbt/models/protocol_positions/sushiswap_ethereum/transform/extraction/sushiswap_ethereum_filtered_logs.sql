{{ config(
    materialized='table',
    schema='sushiswap_ethereum',
    alias='filtered_logs'
)}}

{% set last_block_timestamp = 'TIMESTAMP(' + var('last_block_timestamp') + ')' if var('last_block_timestamp') %}
{% set next_block_timestamp = 'TIMESTAMP(' + var('next_block_timestamp') + ')' if var('next_block_timestamp') %}

SELECT 
    * 
FROM 
    {{ source('common', 'ethereum_logs') }}
WHERE 
    ARRAY_LENGTH(topics) > 0
    AND
    (
        (
            -- POOL_CREATED
            topics[0] = '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9' 
            AND
            address = '0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac'
        )
        OR
        -- SYNC
        topics[0] = '0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1'
    )
    AND
    block_timestamp > {{ last_block_timestamp }}
    AND
    block_timestamp <= {{ next_block_timestamp }}