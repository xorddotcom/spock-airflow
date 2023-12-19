{{ config(
    materialized='table',
    schema='uniswap_v3_core_ethereum',
    alias='filtered_logs'
)}}

SELECT 
    * 
FROM 
    {{ source('common', 'ethereum_logs') }}
WHERE 
    ARRAY_LENGTH(topics) > 0
    AND
    (
        -- MINT
        topics[0] = '0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde' 
        OR
        -- BURN
        topics[0] = '0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c'
        OR
        -- SWAP
        topics[0] = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67'
        OR
        -- POOL_CREATED
        topics[0] = '0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118'
    )
    AND
    block_timestamp > TIMESTAMP({{ var("last_block_timestamp") }})
    AND
    block_timestamp <= TIMESTAMP({{ var("next_block_timestamp") }})