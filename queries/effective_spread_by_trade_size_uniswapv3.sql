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
        END as token_pair,
        {{pool_address}} AS pool_address
    FROM uniswap_v3_ethereum.Factory_evt_PoolCreated AS f
    JOIN tokens.erc20 AS e ON e.contract_address = f.token1 
    JOIN tokens.erc20 AS r ON r.contract_address = f.token0
    WHERE f.pool = {{pool_address}}
),
-- Detailed price and swap information
price_info AS (
    SELECT
        p.evt_index,
        p.evt_block_time, 
        p.evt_block_number,
        -- Calculate price from sqrtPriceX96
        (1 / POWER(CAST(p.sqrtPriceX96 AS DOUBLE) / POWER(CAST(2 AS DOUBLE), 96), 2)) * 
        POWER(10, pi.decimals1 - pi.decimals0) AS pool_price,
        CAST(p.sqrtPriceX96 AS DOUBLE) AS sqrtPriceX96,
        -- Calculate execution price
        ABS(CAST(p.amount0 AS DOUBLE) / POWER(10, pi.decimals0)) / 
        ABS(CAST(p.amount1 AS DOUBLE) / POWER(10, pi.decimals1)) AS execution_price,
        p.contract_address,
        -- Determine if it's a buy or sell
        CASE
            WHEN CAST(p.amount1 AS DOUBLE) >= 0 THEN -1  -- Sell
            WHEN CAST(p.amount1 AS DOUBLE) < 0 THEN 1    -- Buy
        END AS trader_buySell,
        
        -- Adjust amounts for fees (0.05% fee applied to negative amounts)
        CASE
            WHEN CAST(p.amount0 AS DOUBLE) >= 0 THEN CAST(p.amount0 AS DOUBLE) / POWER(10, pi.decimals0)
            ELSE CAST(p.amount0 AS DOUBLE) * 1 / (1 - pi.fee / POWER(10, 6)) / POWER(10, pi.decimals0)
        END AS adjusted_amount0,
        CASE
            WHEN CAST(p.amount1 AS DOUBLE) >= 0 THEN CAST(p.amount1 AS DOUBLE) / POWER(10, pi.decimals1)
            ELSE CAST(p.amount1 AS DOUBLE) * 1 / (1 - pi.fee / POWER(10, 6)) / POWER(10, pi.decimals1)
        END AS adjusted_amount1,
        
        CAST(p.amount0 AS DOUBLE) AS amount0,
        CAST(p.amount1 AS DOUBLE) AS amount1,
        pi.symbol0,
        pi.symbol1,
        abs(CAST(p.amount0 AS DOUBLE) / POWER(10, pi.decimals0)) AS trading_volume_usd,
        pi.token1,
        pi.decimals1,
        evt_tx_hash
    FROM uniswap_v3_ethereum.Pair_evt_Swap AS p
    JOIN pool_info pi ON p.contract_address = pi.pool_address
    WHERE evt_block_time >= CAST('2024-07-01' AS TIMESTAMP)
),
prices_usd AS (
    SELECT DISTINCT
        timestamp,
        price
    FROM prices.minute
    WHERE blockchain = 'ethereum'
    AND contract_address = ((SELECT token1 FROM pool_info))
    AND timestamp >= CAST('2024-07-01' AS TIMESTAMP)
),
dataset AS (

-- Main query for analysis
    SELECT 
        i.evt_block_time,
        i.evt_block_number,
        i.pool_price,
        i.execution_price,
        ABS(i.adjusted_amount0 / i.adjusted_amount1) AS adjusted_price,
        
        -- Calculate effective spread
        2 * trader_buySell * (i.execution_price - LAG(i.pool_price, 1) OVER (ORDER BY i.evt_block_number, i.evt_index)) /
        LAG(i.pool_price, 1) OVER (ORDER BY i.evt_block_number, i.evt_index) * 10000 AS effective_spread,
        
        -- Calculate fee spread
        trader_buySell * (i.execution_price - ABS(i.adjusted_amount0 / i.adjusted_amount1)) / ABS(i.adjusted_amount0 / i.adjusted_amount1) AS fee_spread,
        
        -- Calculate price impact
        trader_buySell * (ABS(i.adjusted_amount0 / i.adjusted_amount1) - LAG(i.pool_price, 1) OVER (ORDER BY i.evt_block_number, i.evt_index)) /
        LAG(i.pool_price, 1) OVER (ORDER BY i.evt_block_number, i.evt_index) AS price_impact,
        abs(CAST(amount1 AS DOUBLE) / POWER(10, decimals1)) AS share_amount,
        amount1,
        decimals1,
        evt_tx_hash
    FROM price_info AS i
    WHERE i.evt_block_time >= CAST('2024-04-01' AS TIMESTAMP)
    ),
trading_volume_usd AS (
    SELECT 
        *,
        abs(CAST(amount1 AS DOUBLE) / POWER(10, decimals1)) * u.price AS trading_volume_usd
    FROM dataset
    RIGHT JOIN query_4848299 AS lp ON evt_tx_hash = lp.hash
    LEFT JOIN prices_usd AS u ON u.timestamp = DATE_TRUNC('minute', evt_block_time)
    WHERE lp.token_pair = (SELECT token_pair FROM pool_info)
    AND (frontend = 'Uniswap Website & Wallet: Default' AND project_contract_address = (SELECT pool_address FROM pool_info)) OR (frontend = 'Uniswap Website: Uniswap X ON' AND project_contract_address = (SELECT pool_address FROM pool_info))
),
categorized_trades AS (
    SELECT 
        date_trunc('day', evt_block_time) AS trade_day,
        CASE 
            WHEN trading_volume_usd >= 1000 AND trading_volume_usd <= 1500 THEN '1,000-1,500 USDC'
            WHEN trading_volume_usd >= 5000 AND trading_volume_usd <= 7500 THEN '5,000-7,500 USDC'
            WHEN trading_volume_usd >= 10000 AND trading_volume_usd <= 15000 THEN '10,000-15,000 USDC'
            WHEN trading_volume_usd >= 50000 AND trading_volume_usd <= 75000 THEN '50,000-75,000 USDC'
            WHEN trading_volume_usd >= 100000 AND trading_volume_usd <= 150000 THEN '100,000-150,000 USDC'
        END AS volume_category,
        effective_spread,
        share_amount,
        trading_volume_usd
    FROM trading_volume_usd
    WHERE trading_volume_usd > 1
    AND project_contract_address = ((SELECT pool_address FROM pool_info))
    AND token_pair = (SELECT token_pair FROM pool_info)
),
all_trades AS (
    SELECT 
        trade_day,
        'All trading volume' AS volume_category,
        effective_spread,
        share_amount,
        trading_volume_usd
    FROM categorized_trades
),
combined_trades AS (
    SELECT * FROM categorized_trades
    UNION ALL
    SELECT * FROM all_trades
),
daily_averages AS (
    SELECT 
        trade_day,
        volume_category,
        sum(effective_spread * share_amount) / sum(share_amount) AS share_weighted_average_effective_spread,
        sum(share_amount) AS share_amount,
        AVG(effective_spread) AS average_effective_spread,
        approx_percentile(effective_spread, 0.5) AS median_effective_spread,
        avg(trading_volume_usd) AS average_trading_volume_usd
    FROM combined_trades
    WHERE trade_day >= CAST('2024-08-01' AS TIMESTAMP)
    AND trade_day <= CAST('2025-02-01' AS TIMESTAMP)
    GROUP BY trade_day, volume_category
)
SELECT
    trade_day,
    (SELECT fee FROM pool_info) AS fee,
    (SELECT token_pair FROM pool_info) AS token_pair,
    MAX(CASE WHEN volume_category = '1,000-1,500 USDC' THEN share_weighted_average_effective_spread END) AS "swaes_1,000-1,500 USDC",
    MAX(CASE WHEN volume_category = '5,000-7,500 USDC' THEN share_weighted_average_effective_spread END) AS "swaes_5,000-7,500 USDC",
    MAX(CASE WHEN volume_category = '10,000-15,000 USDC' THEN share_weighted_average_effective_spread END) AS "swaes_10,000-15,000 USDC",
    MAX(CASE WHEN volume_category = '50,000-75,000 USDC' THEN share_weighted_average_effective_spread END) AS "swaes_50,000-75,000 USDC"
FROM daily_averages
GROUP BY trade_day
ORDER BY trade_day;