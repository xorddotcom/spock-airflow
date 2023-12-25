{{ config(
    materialized='table',
    schema='sushiswap_ethereum',
    alias='lp_transfers'
)}}

{% set last_block_timestamp = 'TIMESTAMP(' + var('last_block_timestamp') + ')' if var('last_block_timestamp') %}
{% set next_block_timestamp = 'TIMESTAMP(' + var('next_block_timestamp') + ')' if var('next_block_timestamp') %}

SELECT 
    tt.token_address AS pair
    tt.from_address,
    tt.to_address,
    tt.value
FROM
    {{ ref('sushiswap_ethereum_pairs') }} p
JOIN (
  SELECT 
    *
  FROM 
    {{ source('common', 'ethereum_token_transfers') }}
  WHERE
    block_timestamp > {{ last_block_timestamp }}
    AND
    block_timestamp <= {{ next_block_timestamp }}
) tt 
ON
  p.pair = tt.token_address