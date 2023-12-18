CREATE OR REPLACE FUNCTION 
    `{{project_id}}.{{dataset_id}}.decode_position`(
        chain INT64,
        token0 STRING,
        token1 STRING,
        decimal0 STRING,
        decimal1 STRING,
        feeTier NUMERIC,
        sqrtPrice FLOAT64,
        poolLiquidity FLOAT64,
        tick FLOAT64,
        walletLiquidity FLOAT64,
        tickLower NUMERIC,
        tickUpper NUMERIC
    ) RETURNS STRUCT<amount_0 FLOAT64, amount_1 FLOAT64> 
      LANGUAGE js AS """
        try {
            var {amount0, amount1} = v3Position.decodePosition(
                Number(chain),
                token0,
                token1,
                Number(decimal0),
                Number(decimal1),
                Number(feeTier),
                Number(sqrtPrice),
                Number(poolLiquidity),
                Number(tick),
                Number(walletLiquidity),
                Number(tickLower),
                Number(tickUpper)
            );

            return {
                amount_0: amount0,
                amount_1: amount1
            };
        } catch (e) {
            return null;
        }
    """
OPTIONS 
    (library="gs://libraries-bigquery/uniswap-v3-position.js")