{{ config(
    materialized='table',
    schema='uniswap_v3_periphery'
)}}

SELECT 
    {{ var("last_block_timestamp") }} AS last_block_timestamp,
    {{ var("next_block_timestamp") }} AS next_block_timestamp


    