# Uniswap Protocol Internalization Analysis Dataset

A dataset analyzing the effects of order internalization on the Uniswap protocol, focusing on effective spreads, market depth, realized volatility, and transaction costs. The analysis examines how professional market makers' internalization practices impact market quality and transaction costs across different trade sizes.

The dataset is curated by [Koshi Ota](https://github.com/username-placeholder), and is part of the [TLDR 2025 fellowship program](https://www.thelatestindefi.org/fellowships).

## About the dataset
*For the entire-range dataset and more details of our work, stay in tune for this repo and the TLDR Conference 2025.*

**date_range:** Dataset spans transaction activity on Uniswap

**structure:** [153 rows, 19 columns]

**source:** Dune Analytics

**blockchain:** Ethereum

**variables:**

| Variable | Type | Description |
| ----------------------- | -------- | ------------------------------------------------------------ |
| `trade_day` | STRING | Date of the trading day |
| `UniswapV3: WETH/USDC (5bp ES, $1K–1.5K)` | FLOAT | Effective spread for $1K-1.5K trades on UniswapV3 5bp pool |
| `UniswapV3: WETH/USDC (5bp ES, $5K–7.5K)` | FLOAT | Effective spread for $5K-7.5K trades on UniswapV3 5bp pool |
| `UniswapV3: WETH/USDC (5bp ES, $10K–15K)` | FLOAT | Effective spread for $10K-15K trades on UniswapV3 5bp pool |
| `UniswapV3: WETH/USDC (5bp ES, $50K–75K)` | FLOAT | Effective spread for $50K-75K trades on UniswapV3 5bp pool |
| `Uniswap: WETH/USDC (ES, $1K–1.5K)` | FLOAT | Effective spread for $1K-1.5K trades across Uniswap pools |
| `Uniswap: WETH/USDC (ES, $5K–7.5K)` | FLOAT | Effective spread for $5K-7.5K trades across Uniswap pools |
| `Uniswap: WETH/USDC (ES, $10K–15K)` | FLOAT | Effective spread for $10K-15K trades across Uniswap pools |
| `Uniswap: WETH/USDC (ES, $50K–75K)` | FLOAT | Effective spread for $50K-75K trades across Uniswap pools |
| `internalization_rate` | FLOAT | Rate of trades internalized by professional market makers |
| `Trade Size Specific PMM Ratio ($1K–$1.5K)` | FLOAT | Professional Market Maker ratio for $1K-1.5K trades |
| `Trade Size Specific PMM Ratio ($5K–$7.5K)` | FLOAT | Professional Market Maker ratio for $5K-7.5K trades |
| `Trade Size Specific PMM Ratio ($10K–$15K)` | FLOAT | Professional Market Maker ratio for $10K-15K trades |
| `Trade Size Specific PMM Ratio ($50K–$75K)` | FLOAT | Professional Market Maker ratio for $50K-75K trades |
| `daily_log_return` | FLOAT | Daily logarithmic return for WETH/USDC |
| `daily_realized_volatility` | FLOAT | Realized volatility measure for the day |
| `market_depth` | FLOAT | Measure of liquidity depth in Uniswap markets |
| `log_average_gas_cost_usd` | FLOAT | Logarithm of average gas cost in USD |

## Implementation Guideline

The data pipeline for the empirics from this paper is implemented through Dune Analytics queries. The table below maps dataset columns to their corresponding Dune queries. Dune queries can be accessed from the links in the table below, and the raw SQL can be found in this repository.:

| Spread sheet name | Dune query number | Dune column name | Dune link |
|-------------------|-------------------|------------------|-----------|
| UniswapV3: WETH/USDC (5bp ES, $1K–1.5K) | 4511288 | swaes_1,000-1,500 USDC | [https://dune.com/queries/4511288](https://dune.com/queries/4511288) |
| UniswapV3: WETH/USDC (5bp ES, $5K–7.5K) | 4511288 | swaes_5,000-7,500 USDC | [https://dune.com/queries/4511288](https://dune.com/queries/4511288) |
| UniswapV3: WETH/USDC (5bp ES, $10K–15K) | 4511288 | swaes_10,000-15,000 USDC | [https://dune.com/queries/4511288](https://dune.com/queries/4511288) |
| UniswapV3: WETH/USDC (5bp ES, $50K–75K) | 4511288 | swaes_50,000-75,000 USDC | [https://dune.com/queries/4511288](https://dune.com/queries/4511288) |
| Uniswap: WETH/USDC (ES, $1K–1.5K) | 4522033 | swaes_1,000-1,500 USDC | [https://dune.com/queries/4522033](https://dune.com/queries/4522033) |
| Uniswap: WETH/USDC (ES, $5K–7.5K) | 4522033 | swaes_5,000-7,500 USDC | [https://dune.com/queries/4522033](https://dune.com/queries/4522033) |
| Uniswap: WETH/USDC (ES, $10K–15K) | 4522033 | swaes_10,000-15,000 USDC | [https://dune.com/queries/4522033](https://dune.com/queries/4522033) |
| Uniswap: WETH/USDC (ES, $50K–75K) | 4522033 | swaes_50,000-75,000 USDC | [https://dune.com/queries/4522033](https://dune.com/queries/4522033) |
| internalization_rate | 4514143 | internalization_rate | [https://dune.com/queries/4514143](https://dune.com/queries/4514143) |
| Trade Size Specific PMM Ratio ($1K–$1.5K) | 4575910 | ratio_range_1000_1500 | [https://dune.com/queries/4575910](https://dune.com/queries/4575910) |
| Trade Size Specific PMM Ratio ($5K–$7.5K) | 4575910 | ratio_range_5000_7500 | [https://dune.com/queries/4575910](https://dune.com/queries/4575910) |
| Trade Size Specific PMM Ratio ($10K–$15K) | 4575910 | ratio_range_10000_15000 | [https://dune.com/queries/4575910](https://dune.com/queries/4575910) |
| Trade Size Specific PMM Ratio ($50K–$75K) | 4575910 | ratio_range_50000_75000 | [https://dune.com/queries/4575910](https://dune.com/queries/4575910) |
| daily_log_return | 4420754 | daily_log_return | [https://dune.com/queries/4420754](https://dune.com/queries/4420754) |
| daily_realized_volatility | 4420754 | daily_realized_volatility | [https://dune.com/queries/4420754](https://dune.com/queries/4420754) |
| market_depth | 4884606 | log_market_depth_usd | [https://dune.com/queries/4884606](https://dune.com/queries/4884606) |
| log_average_gas_cost_usd | 4510136 | log_average_gas_cost_usd | [https://dune.com/queries/4510136](https://dune.com/queries/4510136) |
