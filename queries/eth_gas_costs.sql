WITH price_usd AS (
    SELECT DISTINCT
        *
    FROM prices.minute
    WHERE timestamp >= CAST('2024-07-01' AS TIMESTAMP)
    AND blockchain = 'ethereum'
    AND contract_address = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2
),
gas_cost AS (
    SELECT
        block_time,
        block_number,
        index,
        gas_price,
        gas_price / 1e18 AS adjusted_gas_price_eth,
        gas_price / 1e18 * price AS adjusted_gas_price_usd,
        gas_price / 1e18 * gas_used AS gas_cost_eth,
        gas_price / 1e18 * gas_used * price AS gas_cost_usd,
        price,
        hash
    FROM ethereum.transactions AS et
    LEFT JOIN price_usd AS pu ON pu.timestamp = DATE_TRUNC('minute', et.block_time)
    WHERE block_time >= CAST('2024-08-01' AS TIMESTAMP)
    AND block_time <= CAST('2025-02-01' AS TIMESTAMP)
    AND success = true
)
SELECT
    date_trunc('day', block_time) AS trade_day,
    AVG(LN(gas_cost_usd)) AS log_average_gas_cost_usd,
    approx_percentile(LN(gas_cost_usd), 0.5) AS log_median_gas_cost_usd
FROM gas_cost
GROUP BY date_trunc('day', block_time) 
ORDER BY date_trunc('day', block_time) 

