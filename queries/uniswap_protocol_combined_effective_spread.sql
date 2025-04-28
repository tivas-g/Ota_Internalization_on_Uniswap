WITH pool_info AS (
    SELECT
        f.pool,
        CAST(e.decimals AS DOUBLE) AS decimals1,
        CAST(r.decimals AS DOUBLE) AS decimals0,
        e.symbol AS symbol1,
        r.symbol AS symbol0,
        f.fee,
        f.token1,
        CONCAT(r.symbol, '-', e.symbol) AS token_pair
    FROM uniswap_v3_ethereum.Factory_evt_PoolCreated AS f
    JOIN tokens.erc20 AS e ON e.contract_address = f.token1 
    JOIN tokens.erc20 AS r ON r.contract_address = f.token0
    WHERE f.pool = 0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640
),
gas_fees AS (
    SELECT
      tx.hash AS tx_hash,
      tx.gas_price / 1e18 * tx.gas_used AS eth_gas_fee
    FROM ethereum.transactions tx
    WHERE block_time >= CAST('2024-04-01' AS TIMESTAMP)
),
all_swaps AS (
  SELECT 
    evt_block_time AS block_time,
    evt_block_number AS block_number,
    evt_tx_hash,
    evt_index,
    contract_address AS protocol,
    (1 / POWER(CAST(sqrtPriceX96 AS DOUBLE) / POWER(CAST(2 AS DOUBLE), 96), 2)) * 
        POWER(CAST(10 AS DOUBLE), 12) AS poolPrice,
    
    ABS(CAST(amount0 AS DOUBLE) / POWER(10, 6)) / 
    ABS(CAST(amount1 AS DOUBLE) / POWER(10, 18)) AS execution_price,
    CASE
        WHEN CAST(amount1 AS DOUBLE) >= 0 THEN -1  -- Sell
        WHEN CAST(amount1 AS DOUBLE) < 0 THEN 1    -- Buy
    END AS trader_buySell,
    
    ABS(CAST(amount1 AS DOUBLE) / POWER(10, 18)) AS share_amount,
    NULL as token_pair,
    NULL as inventory_volume,
    NULL as received_token_symbol,
    NULL as sent_token_symbol,
    NULL sent_amount,
    NULL received_amount,
    NULL frontend,
    sender,
    recipient,
    ABS(CAST(amount0 AS DOUBLE) / POWER(10, 6)) AS trade_volume
  FROM uniswap_v3_ethereum.UniswapV3Pool_evt_Swap
  WHERE contract_address = 0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640
  AND evt_block_time >= CAST('2024-07-01' AS TIMESTAMP)
  
  UNION ALL
  
  SELECT
    lp.block_time,
    lp.block_number,
    lp.hash AS evt_tx_hash,
    up.tx_index AS evt_index,
    lp.hash AS protocol,
    NULL as poolPrice,
    NULL as execution_price,
    NULL trader_buySell,
    NULL share_amount,
    lp.token_pair,
    CASE
        WHEN pmm IS NOT NULL AND (metaaggregator = 'Uniswap X') THEN amount_usd
    END AS inventory_volume,
    received_token_symbol,
    sent_token_symbol,
    sent_amount,
    received_amount,
    frontend,
    NULL AS sender,
    NULL AS recipient,
    NULL AS trade_volume
  FROM query_4848299 AS lp
  JOIN dune.titania_research.result_uniswap_x_transactions_raw_amount_data AS up ON up.tx_hash = lp.hash
  WHERE sent_token_symbol IN ((SELECT symbol0 FROM pool_info), (SELECT symbol1 FROM pool_info))
  AND received_token_symbol IN ((SELECT symbol0 FROM pool_info), (SELECT symbol1 FROM pool_info))
  AND lp.token_pair = (SELECT token_pair FROM pool_info)
  AND lp.block_time >= CAST('2024-07-01' AS TIMESTAMP)
  AND frontend = 'Uniswap Website: Uniswap X ON' 
  AND pmm IS NOT NULL
),
inventory_trading AS (
  SELECT 
    *,
    LAST_VALUE(
      CASE 
        WHEN protocol = 0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640
        THEN poolPrice 
        ELSE NULL 
      END
    ) IGNORE NULLS 
    OVER (
      ORDER BY block_number ASC, evt_index ASC  
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as v3_price_in_block
  FROM all_swaps
),
erc20TokenTransfer AS (
    SELECT 
        tx_hash,
        amount AS interface_fee_amount0
    FROM tokens.transfers
    WHERE blockchain = 'ethereum'
    AND contract_address = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48
    AND to = 0x27213E28D7fDA5c57Fe9e5dD923818DBCcf71c47
    AND block_time >= CAST('2024-07-01' AS TIMESTAMP)
),
ethTransfer AS (
    SELECT 
        tx_hash,
        amount AS interface_fee_amount1
    FROM tokens_ethereum.transfers
    WHERE blockchain = 'ethereum'
    AND to IN (0x000000fee13a103a10d593b9ae06b3e05f2e7e1c, 0x37a8f295612602f2774d331e562be9e61B83a327)
    AND block_time >= CAST('2024-07-01' AS TIMESTAMP)
),
interface_fee AS (
    SELECT
        i.*,
        COALESCE(interface_fee_amount0, 0) as interface_fee_amount0,
        COALESCE(interface_fee_amount1, 0) as interface_fee_amount1
    FROM inventory_trading AS i
    LEFT JOIN erc20TokenTransfer AS e ON e.tx_hash = i.evt_tx_hash
    LEFT JOIN ethTransfer AS et ON et.tx_hash = i.evt_tx_hash
    WHERE protocol != 0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640
    ),
uniswap_trade_data AS (
    SELECT 
        i.block_time,
        2 * trader_buySell * (i.execution_price - LAG(i.poolPrice, 1) OVER (ORDER BY i.block_number, i.evt_index)) /
        LAG(i.poolPrice, 1) OVER (ORDER BY i.block_number, i.evt_index) * share_amount AS effective_spread,
        share_amount,
        protocol As app,
        evt_tx_hash
    FROM inventory_trading AS i
    WHERE protocol = 0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640
    
),
trading_data AS (
    SELECT
        block_time,
        block_number,
        evt_index,
        CASE
            WHEN i.sent_token_symbol = (SELECT symbol1 FROM pool_info) THEN -1
            WHEN i.received_token_symbol = (SELECT symbol1 FROM pool_info) THEN 1
        END AS trader_buySell,
        CASE
            WHEN i.sent_token_symbol = (SELECT symbol1 FROM pool_info) THEN (i.received_amount + i.interface_fee_amount0) / (i.sent_amount - eth_gas_fee)
            WHEN i.received_token_symbol = (SELECT symbol1 FROM pool_info) THEN i.sent_amount / (i.received_amount + eth_gas_fee + i.interface_fee_amount1)
        END AS execution_price,
        v3_price_in_block AS poolPrice,
        fair_price_lag5m,

        CASE
            WHEN i.sent_token_symbol = (SELECT symbol1 FROM pool_info) THEN i.sent_amount - eth_gas_fee
            WHEN i.received_token_symbol = (SELECT symbol1 FROM pool_info) THEN i.received_amount + eth_gas_fee + i.interface_fee_amount1
        END AS share_amount,
        interface_fee_amount0,
        interface_fee_amount1,
        sent_token_symbol,
        received_token_symbol,
        i.evt_tx_hash,  
        i.inventory_volume AS trading_volume_usd
    FROM interface_fee AS i
    JOIN gas_fees AS gf ON gf.tx_hash = i.evt_tx_hash
    LEFT JOIN dune.titania_research.result_uniswap_v_3_pool_price_info f ON f.time = block_time
    WHERE i.token_pair = (SELECT token_pair FROM pool_info)
    AND NOT (interface_fee_amount0 = 0 AND interface_fee_amount1 = 0)
    AND i.inventory_volume IS NOT NULL
    
    ),
inventory_daily_summary AS (
    SELECT 
        block_time,
        2 * trader_buySell * (execution_price - poolPrice) / poolPrice * share_amount AS effective_spread,
        share_amount,
        evt_tx_hash AS app,
        trading_volume_usd,
        evt_tx_hash
    FROM trading_data
    WHERE execution_price > 1000
    AND execution_price < 5000
    ),
prices_usd AS (
    SELECT DISTINCT
        timestamp,
        price
    FROM prices.minute
    WHERE blockchain = 'ethereum'
    AND contract_address = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2
    AND timestamp >= CAST('2024-07-01' AS TIMESTAMP)
),
unidata AS (
    SELECT 
        * 
    FROM query_4453144
    WHERE token_pair = 'USDC-WETH'
    AND (frontend = 'Uniswap Website & Wallet: Default' AND project_contract_address = 0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640) OR (frontend = 'Uniswap Website: Uniswap X ON' AND project_contract_address = 0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640)
),
aggregate_data AS (
    SELECT 
        i.block_time,
        i.effective_spread,
        i.share_amount,
        i.app,
        i.share_amount * price AS trading_volume_usd,
        i.evt_tx_hash
    FROM uniswap_trade_data AS i
    JOIN unidata AS lp ON i.evt_tx_hash = lp.hash
    JOIN prices_usd AS u ON u.timestamp = DATE_TRUNC('minute', i.block_time)
    AND lp.token_pair = 'USDC-WETH'
    
    UNION ALL
    SELECT * FROM inventory_daily_summary
),
categorized_trades AS (
    SELECT 
        date_trunc('day', block_time) AS trade_day,
        CASE 
            WHEN trading_volume_usd >= 1000 AND trading_volume_usd <= 1500 THEN '1,000-1,500 USDC'
            WHEN trading_volume_usd >= 5000 AND trading_volume_usd <= 7500 THEN '5,000-7,500 USDC'
            WHEN trading_volume_usd >= 10000 AND trading_volume_usd <= 15000 THEN '10,000-15,000 USDC'
            WHEN trading_volume_usd >= 50000 AND trading_volume_usd <= 75000 THEN '50,000-75,000 USDC'
            WHEN trading_volume_usd >= 100000 AND trading_volume_usd < 150000 THEN '100,000-150,000 USDC'
        END AS volume_category,
        effective_spread,
        share_amount,
        app,
        trading_volume_usd,
        evt_tx_hash
    FROM aggregate_data
    WHERE effective_spread >= -20000
    AND effective_spread <= 20000
    ),
daily_metrics AS (
    SELECT 
        trade_day,
        COUNT(*) AS trade_count,
        -- Overall weighted average
        SUM(effective_spread) / SUM(share_amount) * 10000 AS total_share_weighted_average_effective_spread,
        approx_percentile(trading_volume_usd, 0.5) median_trading_volume_usd,
        AVG(trading_volume_usd) AS average_trading_volume_usd,
        volume_category
    FROM categorized_trades
    WHERE trade_day >= CAST('2024-08-01' AS TIMESTAMP)
    AND trade_day <= CAST('2025-02-01' AS TIMESTAMP)
    GROUP BY trade_day, volume_category
)
SELECT 
    trade_day,
    MAX(CASE WHEN volume_category = '1,000-1,500 USDC' THEN total_share_weighted_average_effective_spread END) AS "swaes_1,000-1,500 USDC",
    MAX(CASE WHEN volume_category = '5,000-7,500 USDC' THEN total_share_weighted_average_effective_spread END) AS "swaes_5,000-7,500 USDC",
    MAX(CASE WHEN volume_category = '10,000-15,000 USDC' THEN total_share_weighted_average_effective_spread END) AS "swaes_10,000-15,000 USDC",
    MAX(CASE WHEN volume_category = '50,000-75,000 USDC' THEN total_share_weighted_average_effective_spread END) AS "swaes_50,000-75,000 USDC",
    MAX(CASE WHEN volume_category = '1,000-1,500 USDC' THEN average_trading_volume_usd END) AS "atvu_1,000-1,500 USDC",
    MAX(CASE WHEN volume_category = '5,000-7,500 USDC' THEN average_trading_volume_usd END) AS "atvu_5,000-7,500 USDC",
    MAX(CASE WHEN volume_category = '10,000-15,000 USDC' THEN average_trading_volume_usd END) AS "atvu_10,000-15,000 USDC",
    MAX(CASE WHEN volume_category = '50,000-75,000 USDC' THEN average_trading_volume_usd END) AS "atvu_50,000-75,000 USDC"
FROM daily_metrics
GROUP BY trade_day
ORDER BY trade_day;