{{ config(
    materialized='incremental',
    unique_key='token_id',
    schema='uniswap_v3_periphery_ethereum',
    alias='nfts'
) }}

WITH liquidity_mint_logs AS (
    SELECT
        *,
        `spock-main.uniswap_v3_periphery_ethereum.decode_mint_log`(data, topics) AS decoded_data
    FROM 
        {{ref('uniswap_v3_periphery_ethereum_filtered_logs')}}
    WHERE
        topics[0] = '0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde'
),

nft_mint_logs AS (
    SELECT
        *
    FROM 
        {{ref('uniswap_v3_periphery_ethereum_filtered_logs')}}
    WHERE
        topics[0] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        AND 
        topics[1] = '0x0000000000000000000000000000000000000000000000000000000000000000'
),

transfer_logs AS (
    SELECT
        *
    FROM 
        {{ref('uniswap_v3_periphery_ethereum_filtered_logs')}}
    WHERE
        topics[0] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
)

SELECT
    CAST(latest_transfer_logs.topics[3] AS INT64) AS token_id,
    LOWER(liquidity_mint_logs.address) AS pool,
    decoded_data.tickLower AS tick_lower,
    decoded_data.tickUpper AS tick_upper,
    CONCAT('0x', RIGHT(latest_transfer_logs.topics[2], 40)) AS owner
FROM 
    nft_mint_logs
JOIN
    liquidity_mint_logs
ON
    (
        liquidity_mint_logs.block_number = nft_mint_logs.block_number
        AND
        liquidity_mint_logs.log_index = nft_mint_logs.log_index - 1
    )
JOIN
    (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY topics[3] ORDER BY block_number DESC, log_index DESC) AS rn
        FROM transfer_logs
    ) latest_transfer_logs
ON
    latest_transfer_logs.rn = 1
    AND
    nft_mint_logs.topics[3] = latest_transfer_logs.topics[3]

