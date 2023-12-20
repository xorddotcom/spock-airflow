{{ config(
  materialized='view',
  scheme={{ var('scheme') }}
)}}

SELECT 
    * 
FROM 
    {{ source('common', 'ethereum_logs') }}
WHERE 
    ARRAY_LENGTH(topics) > 0
    AND (
        -- DEPOSIT
        topics[0] = '0x4e2ca0515ed1aef1395f66b5303bb5d6f1bf9d61a353fa53f73f8ac9973fa9f6' 
        OR
        -- WITHDRAW
        topics[0] = '0x02f25270a4d87bea75db541cdfe559334a275b4a233520ed6c0a2429667cca94'
        OR
        -- TRANSFER
        topics[0] = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        OR (
        -- VAULT_CREATED
        topics[0] = '0xaf0c778584d4b20d9768e68ed24ede73b034bc5747a377a69f1db53ff3933def'
            AND (
                -- ACTIVE FACTORY
                address = '0x4b8e58d252ba251e044ec63125e83172eca5118f'
                OR
                -- PASSIVE FACTORY
                address = '0x06c2ae330c57a6320b2de720971ebd09003c7a01'
            )
        )
    )
    AND block_timestamp > {{ var("last_block_timestamp")}} 
    AND block_timestamp <= {{ var("next_block_timestamp")}} 
