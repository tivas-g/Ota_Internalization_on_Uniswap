WITH pool_info AS (
    SELECT
        f.pool,
        CAST(e.decimals AS DOUBLE) AS decimals1,
        CAST(r.decimals AS DOUBLE) AS decimals0,
        e.symbol AS symbol1,
        r.symbol AS symbol0,
        f.fee,
        f.token1,
        CASE
            WHEN e.symbol <= r.symbol THEN e.symbol || '-' || r.symbol
            ELSE r.symbol || '-' || e.symbol
        END as token_pair
    FROM uniswap_v3_ethereum.Factory_evt_PoolCreated AS f
    JOIN tokens.erc20 AS e ON e.contract_address = f.token1 
    JOIN tokens.erc20 AS r ON r.contract_address = f.token0
    WHERE f.pool = {{pool_address}}
),
price_info AS (
    SELECT
        pi.token1,
        p.evt_block_number,
        (1 / POWER(CAST(p.sqrtPriceX96 AS DOUBLE) / POWER(CAST(2 AS DOUBLE), 96), 2)) * 
        POWER(CAST(10 AS DOUBLE), pi.decimals1 - pi.decimals0) AS poolPrice,
        ABS(CAST(p.amount0 AS DOUBLE) / POWER(10, pi.decimals0)) / 
        ABS(CAST(p.amount1 AS DOUBLE) / POWER(10, pi.decimals1)) AS execution_price,
        p.amount1,
        p.evt_block_time,
        p.evt_index,
        pi.decimals1
    FROM uniswap_v3_ethereum.Pair_evt_Swap AS p
    JOIN pool_info pi ON p.contract_address = pi.pool
    WHERE p.evt_block_time >= CAST('2024-06-01' AS TIMESTAMP)
),
RankedData AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY evt_block_number ORDER BY evt_block_number, evt_index DESC) as rn
    FROM price_info
),
RankedData1 AS (
    SELECT *
    FROM RankedData
    WHERE rn = 1
),
database AS (
    SELECT 
        b.number,
        b.time,
        r.token1,
        r.poolPrice,
        r.amount1,
        r.evt_block_time,
        r.evt_index,
        r.decimals1,
        r.rn
    FROM ethereum.blocks b
    LEFT JOIN RankedData1 r ON b.number = r.evt_block_number
),
numbered_data AS (
    SELECT 
        *,
        MAX(CASE WHEN poolPrice IS NOT NULL THEN number END) 
            OVER (ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as last_price_block
    FROM database
),
price_lookup AS (
    SELECT DISTINCT
        time,
        number,
        poolPrice
    FROM database
    WHERE poolPrice IS NOT NULL
),
FilledData AS (
    SELECT
        d.time AS block_time,
        d.number,
        COALESCE(d.poolPrice, pl.poolPrice) as filled_poolPrice,
        LAG(COALESCE(d.poolPrice, pl.poolPrice),1) OVER (ORDER BY d.number) AS lagFilledpoolPrice,
        LEAD(COALESCE(d.poolPrice, pl.poolPrice), 25) OVER (ORDER BY d.number) AS fair_price_lag5m
    FROM numbered_data d
    LEFT JOIN price_lookup pl ON pl.number = d.last_price_block
    WHERE d.time >= CAST('2024-06-01' AS TIMESTAMP)
),
first_block AS (
    SELECT MIN(number) as start_block
    FROM FilledData
    WHERE block_time >= CAST('2024-06-01' AS TIMESTAMP)
),
block_intervals AS (
    SELECT 
        block_time,
        number,
        filled_poolPrice,
        fair_price_lag5m,
        (number - (SELECT start_block FROM first_block)) %  75 as block_mod
    FROM FilledData 
),
realized_return AS (
    SELECT 
        block_time,
        number as block_number,
        filled_poolPrice,
        LN(filled_poolPrice) - LN(LAG(filled_poolPrice, 1) OVER (ORDER BY number)) AS log_return_15m
    FROM block_intervals
    WHERE block_mod = 0
    AND block_time >= CAST('2024-08-01' AS TIMESTAMP)
    AND block_time <= CAST('2025-02-01' AS TIMESTAMP)
),
daily_variance AS (
  SELECT
    date_trunc('day', block_time) AS datetime,
    SUM(log_return_15m) AS daily_log_return,
    SUM(POWER(log_return_15m, 2)) AS daily_realized_variance
  FROM realized_return
  GROUP BY date_trunc('day', block_time)
)
SELECT
  datetime AS trade_day,
  (SELECT token_pair FROM pool_info) AS token_pair,
  daily_log_return * 100 AS daily_log_return,
  daily_realized_variance * 100 AS daily_realized_variance,
  SQRT(daily_realized_variance)  * 100 AS daily_realized_volatility
FROM daily_variance
ORDER BY datetime;