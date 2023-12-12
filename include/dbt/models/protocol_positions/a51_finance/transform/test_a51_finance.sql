{{ config(materialized="table") }}

SELECT 
    {{ var("last_block_timestamp") }} AS last_block_timestamp,
    {{ var("next_block_timestamp") }} AS next_block_timestamp


    