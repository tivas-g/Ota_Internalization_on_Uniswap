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

X AS (
    SELECT
        block_time,
        token_pair,
        hash,
        CASE
            WHEN pmm IS NOT NULL AND (metaaggregator = 'Uniswap X') THEN amount_usd
            ELSE NULL
        END AS inventory_volume,
        amount_usd,
        frontend,
        project_contract_address
    FROM query_4848299
    WHERE token_pair = (SELECT token_pair FROM pool_info)
),
uniswap_trade_data AS (
    SELECT DISTINCT
        block_time,
        amount_usd,
        project_contract_address,
        frontend
    FROM X
    WHERE ((frontend = 'Uniswap Website & Wallet: Default') OR (frontend = 'Uniswap Website: Uniswap X ON' AND project_contract_address IS NOT NULL))
    
),
Y AS (
    SELECT *
    FROM X
    WHERE X.inventory_volume IS NOT NULL
    AND frontend = 'Uniswap Website: Uniswap X ON'
    AND project_contract_address IS NULL
    
),
inventory_daily_summary AS (
    SELECT 
        DATE_TRUNC('day', block_time) AS trade_day, 
        SUM(inventory_volume) AS internalization_volume
    FROM Y
    GROUP BY DATE_TRUNC('day', block_time)
),
uniswap_daily_summary AS (
    SELECT 
        DATE_TRUNC('day', block_time) AS trade_day, 
        SUM(amount_usd) AS dex_volume
    FROM uniswap_trade_data
    WHERE project_contract_address = (SELECT pool_address FROM pool_info)
    GROUP BY DATE_TRUNC('day', block_time)
)
SELECT 
    uws.trade_day,
    iws.internalization_volume,
    uws.dex_volume,
    iws.internalization_volume / (uws.dex_volume + iws.internalization_volume) AS internalization_rate
FROM inventory_daily_summary iws
JOIN uniswap_daily_summary uws ON uws.trade_day = iws.trade_day
WHERE uws.trade_day >= CAST('2024-08-01' AS TIMESTAMP)
AND uws.trade_day <= CAST('2025-02-01' AS TIMESTAMP)
ORDER BY uws.trade_day DESC