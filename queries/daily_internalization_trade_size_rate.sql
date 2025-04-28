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
            WHEN e.symbol <= r.symbol 
                 THEN e.symbol || '-' || r.symbol
            ELSE r.symbol || '-' || e.symbol
        END AS token_pair,
        {{pool_address}} AS pool_address
    FROM uniswap_v3_ethereum.Factory_evt_PoolCreated AS f
    JOIN tokens.erc20 AS e 
        ON e.contract_address = f.token1 
    JOIN tokens.erc20 AS r 
        ON r.contract_address = f.token0
    WHERE f.pool = {{pool_address}}
),

X AS (
    SELECT
        block_time,
        token_pair,
        hash,
        CASE
            WHEN pmm IS NOT NULL 
                 AND (metaaggregator = 'Uniswap X') 
            THEN amount_usd
            ELSE NULL
        END AS inventory_volume,
        amount_usd,
        frontend,
        project_contract_address
    FROM query_4848299
    WHERE token_pair = (SELECT token_pair FROM pool_info)
),

Y AS (
    SELECT
        *,
        CASE
            WHEN amount_usd >= 1000  
                 AND amount_usd <= 1500 
            THEN 'range_1000_1500'
            
            WHEN amount_usd >= 5000  
                 AND amount_usd <= 7500 
            THEN 'range_5000_7500'
            
            WHEN amount_usd >= 10000 
                 AND amount_usd <= 15000 
            THEN 'range_10000_15000'
            
            WHEN amount_usd >= 50000 
                 AND amount_usd <= 75000 
            THEN 'range_50000_75000'
            
            WHEN amount_usd >= 100000 
                 AND amount_usd <= 150000 
            THEN 'range_100000_150000'

            WHEN amount_usd >= 150000
            THEN 'over_150000'
        END AS trade_size
    FROM X
    WHERE inventory_volume IS NOT NULL
      AND frontend = 'Uniswap Website: Uniswap X ON'
),

inventory_daily_summary AS (
    SELECT
        DATE_TRUNC('day', block_time) AS trade_day,
        trade_size,
        SUM(inventory_volume) AS internalization_volume
    FROM Y
    GROUP BY 
        DATE_TRUNC('day', block_time), 
        trade_size
)

SELECT
    trade_day,
    CASE WHEN total_volume = 0 THEN 0 
         ELSE range_1000_1500 / total_volume * 100
    END AS ratio_range_1000_1500,
    CASE WHEN total_volume = 0 THEN 0 
         ELSE range_5000_7500 / total_volume * 100
    END AS ratio_range_5000_7500,
    CASE WHEN total_volume = 0 THEN 0 
         ELSE range_10000_15000 / total_volume * 100
    END AS ratio_range_10000_15000,
    CASE WHEN total_volume = 0 THEN 0 
         ELSE range_50000_75000 / total_volume * 100
    END AS ratio_range_50000_75000,
    CASE WHEN total_volume = 0 THEN 0 
         ELSE range_100000_150000 / total_volume * 100
    END AS ratio_range_100000_150000,
    CASE WHEN total_volume = 0 THEN 0 
         ELSE over_150000 / total_volume * 100
    END AS ratio_over_150000
FROM (
    SELECT
        trade_day,
        SUM(CASE WHEN trade_size = 'range_1000_1500' THEN internalization_volume ELSE 0 END) AS range_1000_1500,
        SUM(CASE WHEN trade_size = 'range_5000_7500' THEN internalization_volume ELSE 0 END) AS range_5000_7500,
        SUM(CASE WHEN trade_size = 'range_10000_15000' THEN internalization_volume ELSE 0 END) AS range_10000_15000,
        SUM(CASE WHEN trade_size = 'range_50000_75000' THEN internalization_volume ELSE 0 END) AS range_50000_75000,
        SUM(CASE WHEN trade_size = 'range_100000_150000' THEN internalization_volume ELSE 0 END) AS range_100000_150000,
        SUM(CASE WHEN trade_size = 'over_150000' THEN internalization_volume ELSE 0 END) AS over_150000,
        SUM(internalization_volume) AS total_volume
    FROM inventory_daily_summary
    GROUP BY trade_day
) t
WHERE trade_day >= CAST('2024-08-01' AS TIMESTAMP)
AND trade_day <= CAST('2025-02-01' AS TIMESTAMP)
ORDER BY trade_day DESC;