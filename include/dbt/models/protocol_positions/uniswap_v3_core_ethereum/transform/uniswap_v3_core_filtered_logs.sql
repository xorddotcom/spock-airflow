{{ config(
    materialized='table',
    schema='uniswap_v3_core_ethereum'
)}}

SELECT 
    TIMESTAMP({{ var("last_block_timestamp") }}) AS last_block_timestamp,
    TIMESTAMP({{ var("next_block_timestamp") }}) AS next_block_timestamp