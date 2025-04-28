WITH calendar AS (
  SELECT x AS end_block_time
  FROM UNNEST(
    sequence(
      CAST('2024-08-01 00:00:00' AS timestamp),
      CAST('2025-02-01 00:00:00' AS timestamp),
      INTERVAL '1' day
    )
  ) AS t(x)
),
constants AS (
  SELECT
    CAST(r.decimals AS INT) AS decimalToken0,
    CAST(e.decimals AS INT) AS decimalToken1,
    pool AS addressPool,
    f.token0,
    CASE
        WHEN e.symbol <= r.symbol THEN e.symbol || '-' || r.symbol
        ELSE r.symbol || '-' || e.symbol
    END as token_pair,
    f.fee
  FROM uniswap_v3_ethereum.UniswapV3Factory_evt_PoolCreated AS f
  JOIN tokens.erc20 AS r ON r.contract_address = f.token0
  JOIN tokens.erc20 AS e ON e.contract_address = f.token1
  WHERE pool = {{pool_address}}
),
ts AS (
  SELECT
    tickSpacing
  FROM uniswap_v3_ethereum.UniswapV3Factory_evt_PoolCreated, constants
  WHERE pool = addressPool
  LIMIT 1
),

swaps_with_calendar AS (
  SELECT
    c.end_block_time,
    s.tick,
    s.sqrtPriceX96,
    s.evt_block_time,
    ROW_NUMBER() OVER (PARTITION BY c.end_block_time ORDER BY s.evt_block_time DESC) AS rn
  FROM calendar c
  JOIN uniswap_v3_ethereum.Pair_evt_Swap s 
    ON s.evt_block_time < c.end_block_time
  CROSS JOIN constants
  WHERE s.contract_address = constants.addressPool
),
ct AS (
  SELECT
    end_block_time,
    tick AS currentTick,
    CAST(sqrtPriceX96 / POWER(2, 96) AS DOUBLE) AS sqrtPrice
  FROM swaps_with_calendar
  WHERE rn = 1
),
mb AS (
  SELECT
    c.end_block_time,
    tickLower AS lowerTick,
    tickUpper AS upperTick,
    CAST(amount AS DOUBLE) / SQRT(POWER(10, decimalToken0 + decimalToken1)) AS amount
  FROM uniswap_v3_ethereum.Pair_evt_Mint, constants, calendar c
  WHERE contract_address = addressPool
    AND evt_block_time < c.end_block_time
  UNION ALL
  SELECT
    c.end_block_time,
    tickLower AS lowerTick,
    tickUpper AS upperTick,
    -1 * CAST(amount AS DOUBLE) / SQRT(POWER(10, decimalToken0 + decimalToken1)) AS amount
  FROM uniswap_v3_ethereum.Pair_evt_Burn, constants, calendar c
  WHERE contract_address = addressPool
    AND evt_block_time < c.end_block_time
),
mint_burn AS (
  SELECT
    end_block_time,
    lowerTick,
    upperTick,
    SUM(amount) AS amount
  FROM mb
  GROUP BY end_block_time, lowerTick, upperTick
),
sequence_parts AS (
  SELECT
    end_block_time,
    CEIL((upperTick - lowerTick + 1) / (9999 * tickSpacing)) AS num_parts,
    lowerTick,
    upperTick,
    tickSpacing,
    amount AS amount_tick
  FROM mint_burn
  CROSS JOIN ts
  WHERE amount > 0
),
generated_sequences AS (
  SELECT
    end_block_time,
    seq_part,
    lowerTick + seq_part * 9999 * tickSpacing AS start_tick,
    LEAST(upperTick, lowerTick + (seq_part + 1) * 9999 * tickSpacing - tickSpacing) AS end_tick,
    tickSpacing,
    amount_tick
  FROM sequence_parts
  CROSS JOIN UNNEST(sequence(0, CAST(num_parts AS int), 1)) AS t(seq_part)
),
nest_ticks AS (
  SELECT
    end_block_time,
    sequence(start_tick, end_tick, CAST(tickSpacing AS int)) AS tick,
    amount_tick
  FROM generated_sequences
),
ticks AS (

  SELECT
    nt.end_block_time,
    t_val AS tick,
    nt.amount_tick,
    CASE
      WHEN ct_data.sqrtPrice <= SQRT(POWER(1.0001, t_val))
        THEN nt.amount_tick * (SQRT(POWER(1.0001, t_val + ts.tickSpacing)) - SQRT(POWER(1.0001, t_val))) /
             (SQRT(POWER(1.0001, t_val + ts.tickSpacing)) * SQRT(POWER(1.0001, t_val)))
      WHEN SQRT(POWER(1.0001, t_val)) < ct_data.sqrtPrice
           AND ct_data.sqrtPrice < SQRT(POWER(1.0001, t_val + ts.tickSpacing))
        THEN nt.amount_tick * (SQRT(POWER(1.0001, t_val + ts.tickSpacing)) - SQRT(POWER(1.0001, ct_data.currentTick))) /
             (SQRT(POWER(1.0001, t_val + ts.tickSpacing)) * SQRT(POWER(1.0001, ct_data.currentTick)))
      WHEN SQRT(POWER(1.0001, t_val + ts.tickSpacing)) <= ct_data.sqrtPrice
        THEN 0.0
    END AS amount_tick_token0,
    CASE
      WHEN ct_data.sqrtPrice <= SQRT(POWER(1.0001, t_val))
        THEN 0.0
      WHEN SQRT(POWER(1.0001, t_val)) < ct_data.sqrtPrice
           AND ct_data.sqrtPrice < SQRT(POWER(1.0001, t_val + ts.tickSpacing))
        THEN nt.amount_tick * (SQRT(POWER(1.0001, ct_data.currentTick)) - SQRT(POWER(1.0001, t_val)))
      WHEN SQRT(POWER(1.0001, t_val + ts.tickSpacing)) <= ct_data.sqrtPrice
        THEN nt.amount_tick * (SQRT(POWER(1.0001, t_val + ts.tickSpacing)) - SQRT(POWER(1.0001, t_val)))
    END AS amount_tick_token1
  FROM nest_ticks nt
  CROSS JOIN UNNEST(nt.tick) AS t(t_val)
  CROSS JOIN ts
  JOIN ct AS ct_data ON nt.end_block_time = ct_data.end_block_time
),
ld AS (
  SELECT
    t.end_block_time,
    POWER(1.0001, t.tick) * POWER(10, constants.decimalToken0 - constants.decimalToken1) AS price,
    SUM(t.amount_tick) AS total,
    SUM(t.amount_tick_token0 / SQRT(POWER(10, constants.decimalToken0 - constants.decimalToken1))) AS total_token0,
    SUM(t.amount_tick_token1 * SQRT(POWER(10, constants.decimalToken0 - constants.decimalToken1))) AS total_token1,
    'liq' AS series
  FROM ticks t
  CROSS JOIN constants
  CROSS JOIN ts
  JOIN ct AS ct_data ON t.end_block_time = ct_data.end_block_time
  WHERE
    CASE
      WHEN ({{range_min_token0_price_in_token1}}) != 0 
        THEN POWER(1.0001, t.tick) * POWER(10, constants.decimalToken0 - constants.decimalToken1) > {{range_min_token0_price_in_token1}}
      ELSE t.tick > ct_data.currentTick - 100 * ts.tickSpacing
    END
    AND
    CASE
      WHEN ({{range_max_token0_price_in_token1}}) != 0 
        THEN POWER(1.0001, t.tick) * POWER(10, constants.decimalToken0 - constants.decimalToken1) < {{range_max_token0_price_in_token1}}
      ELSE t.tick < ct_data.currentTick + 100 * ts.tickSpacing
    END
  GROUP BY t.end_block_time, POWER(1.0001, t.tick) * POWER(10, constants.decimalToken0 - constants.decimalToken1)
),
current_price AS (

  SELECT
    ct_data.end_block_time,
    POWER(1.0001, ct_data.currentTick) * POWER(10, constants.decimalToken0 - constants.decimalToken1) AS price,
    ld.total,
    ld.total_token0,
    ld.total_token1,
    'cp' AS series
  FROM ld
  CROSS JOIN ts
  JOIN ct AS ct_data ON ld.end_block_time = ct_data.end_block_time
  CROSS JOIN constants
  WHERE ld.price < POWER(1.0001, ct_data.currentTick) * POWER(10, constants.decimalToken0 - constants.decimalToken1)
    AND POWER(1.0001, ct_data.currentTick - ts.tickSpacing) * POWER(10, constants.decimalToken0 - constants.decimalToken1) <= ld.price
),
distribution_dataset AS (

  SELECT
    ld.end_block_time,
    ld.price,
    1 / ld.price AS inv_price,
    ld.total_token0,
    ld.total_token1,
    ld.series,
    ld.total_token0 + ld.total_token1 * (1 / ld.price) AS tick_liquidity_amount
  FROM ld
  UNION ALL
  SELECT
    cp.end_block_time,
    cp.price,
    1 / cp.price AS inv_price,
    cp.total_token0,
    cp.total_token1,
    cp.series,
    cp.total_token0 + cp.total_token1 * (1 / cp.price) AS tick_liquidity_amount
  FROM current_price cp
),
dataset AS (
  SELECT
    d.end_block_time,
    d.inv_price,
    d.tick_liquidity_amount,
    d.series,
    cp.current_price
  FROM distribution_dataset d
  JOIN (
    SELECT end_block_time, 1 / price AS current_price
    FROM current_price
  ) cp
    ON d.end_block_time = cp.end_block_time
),
prices_usd AS (
    SELECT DISTINCT
        timestamp,
        price
    FROM prices.day
    WHERE blockchain = 'ethereum'
    AND contract_address = ((SELECT token0 FROM constants))
),
market_depth AS (
  SELECT
    d.end_block_time,
    SUM(d.tick_liquidity_amount) AS market_depth_token
  FROM dataset d
  WHERE d.inv_price >= d.current_price * 0.98
    AND d.inv_price <= d.current_price * 1.02
  GROUP BY d.end_block_time
)
SELECT
  format_datetime(
    date_trunc('day', m.end_block_time), 
    'yyyy-MM-dd HH:mm:ss.SSS ''UTC'''
  ) AS trade_day,
  m.market_depth_token,
  LN(m.market_depth_token * p.price) AS log_market_depth_usd,
  (SELECT token_pair FROM constants) AS token_pair,
  (SELECT fee FROM constants) AS fee
FROM market_depth m
JOIN prices_usd p
  ON CAST(m.end_block_time AS DATE) = CAST(p.timestamp AS DATE)
ORDER BY m.end_block_time;