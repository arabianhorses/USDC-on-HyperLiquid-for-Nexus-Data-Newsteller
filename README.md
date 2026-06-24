# USDC on HyperLiquid for Nexus Data Newsteller
How to reach HyperEVM's data and compute some metrics of USDC

## 1 — Reaching the data

 **Data source**
 
- **Provider:** purroofgroup HyperEVM RPC `https://rpc.purroofgroup.com`
    
- **What we pull:** every log emitted by the USDC contract `0xb88339cb7199b77e23db6e890353e22632ba630f` Transfer, Mint, Burn, etc.  across its full history, from its **first event (block `10,934,119`)** to the current chain head (**~`37,646,391`** at last sync, ≈ mid-June 2026).
    
- Standard `eth_getLogs`, so it's portable to any HyperEVM RPC — but purroofgroup was chosen because it returns the *complete* log set

**[`fetch_logs.py`](data_fetch/fetch_logs.py)** downloads those logs and writes them to `raw/` as Parquet shards (one per
  aligned 10,000-block chunk).

  ```bash
  pip install requests duckdb

  # full backfill: contract start -> chain head
  python fetch_logs.py
  # or a specific block range
  python fetch_logs.py <run_from> <run_to>
  ```

  Output: zero-padded shards in `raw/`, e.g. `010934119_010944118.parquet` (zero-padding
  makes lexicographic order == chronological order, which the analysis relies on). One row
  per log:

  | column | meaning |
  |---|---|
  | `blockNumber`, `blockTimestamp`, `logIndex` | log position + inline timestamp |
  | `transactionHash`, `transactionIndex` | transaction reference |
  | `address` | emitting contract (the USDC token) |
  | `topic0`–`topic3`, `data` | raw event topics + data (decoded downstream) |
  | `removed` | reorg flag |

  ## 2 - Processing Data and Visualizing it 

  Visualization related sections written by claude.ai 

  ### USDC supply — weekly mint / burn & total supply

  <img width="1950" height="975" alt="usdc_supply" src="https://github.com/user-attachments/assets/2558a8b4-e120-4e01-9634-956c95b72e88" />

[`mintBurnSupply_graph_generator.py`](analysis/mintBurnSupply_graph_generator.py) reads the raw log shards with **DuckDB** and:

- decodes the USDC `Mint` and `Burn` events (hex `data` → exact decimals via a small UDF);

- buckets them by week (`DATE_TRUNC('WEEK', …)`, UTC) → weekly **gross mint** (green) and **gross burn** (red);

- accumulates the running net (mint − burn) into **total supply** (the navy line);

- derives **QoQ growth** from the exact on-chain supply at each quarter boundary (not week-rounded).
- writes **[`usdc_supply.csv`](analysis/usdc_supply.csv)** — the exact weekly table behind the chart: one row per week with `mint`, `burn`, `netChange`, and `acumBal` (cumulative total supply), all in USDC amounts (divided by 10^6).

  ```bash
  pip install duckdb pandas matplotlib
  python mintBurnSupply_graph_generator.py   # reads raw/ -> writes usdc_supply.png
   ```

  ### Weekly balances - weekly snapshot of USDC held by address
  
<img width="1950" height="975" alt="usdc_balance_by_holder" src="https://github.com/user-attachments/assets/acd45515-fc30-4d61-963f-d4da6e472976" />
[`weeklyBalances_generator.py`](analysis/weeklyBalances_generator.py) reads the raw log shards with **DuckDB** and creates a view named as 'raw'. Then for each weeks' timestamp 

```python

[1756080000, 1756684800, 1757289600, 1757894400, 1758499200, 1759104000, 1759708800, 1760313600, 1760918400, 1761523200, 1762128000, 1762732800, 1763337600, 1763942400, 1764547200, 1765152000, 1765756800, 1766361600, 1766966400, 1767571200, 1768176000, 1768780800, 1769385600, 1769990400, 1770595200, 1771200000, 1771804800, 1772409600, 1773014400, 1773619200, 1774224000, 1774828800, 1775433600, 1776038400, 1776643200, 1777248000, 1777852800, 1778457600, 1779062400, 1779667200, 1780272000, 1780876800, 1781481600]

```
It run the query below 
```sql
WITH
balances AS (
SELECT 
DATE_TRUNC('WEEK', to_timestamp({week_timestamp})) AS week_
,  address
, (SUM(amount))/POW(10,6) AS balance
FROM (
    SELECT
    CONCAT('0x', SUBSTR(topic2, 27, 40)) AS 'address'
    , CAST(hexdec(data) AS HUGEINT) AS amount
    , blockNumber * POW(10,6) + logIndex AS gi
    , blockTimestamp
    FROM raw 
    WHERE address = '0xb88339cb7199b77e23db6e890353e22632ba630f'-- USDC 
    AND topic0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' --Transfer (index_topic_1 address from, index_topic_2 address to, uint256 value)
    AND blockTimestamp <= {week_timestamp}

    UNION ALL

    SELECT
    CONCAT('0x', SUBSTR(topic1, 27, 40)) AS 'address'
    , -1 * CAST(hexdec(data) AS HUGEINT) AS amount
    , blockNumber * POW(10,6) + logIndex AS gi
    , blockTimestamp
    FROM raw 
    WHERE address = '0xb88339cb7199b77e23db6e890353e22632ba630f'-- USDC 
    AND topic0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' --Transfer (index_topic_1 address from, index_topic_2 address to, uint256 value
    AND blockTimestamp <= {week_timestamp}
    )
GROUP BY 1, 2
)
, labels_ratios AS (
    SELECT *

    ,  CASE  
        WHEN address = '0xc20699185c15d0a2fd65779bb5d69f5b0b113c00' THEN 'Coinbase: Hyperliquid Deployer'
        WHEN address = '0x744e4f26ee30213989216e1632d9be3547c4885b' THEN 'HyperLend USDC Market'
        WHEN address = '0x462b95575cb2d56de9d1aaaaab452279b058aa06' THEN 'Hyperbeat'
        WHEN address = '0x6b9e773128f453f5c2c60935ee2de2cbc5390a24' THEN 'Circle: CCTP CoreDepositWallet'
        ELSE 'others'
       END AS addressOrOthers
    FROM (
            SELECT 
            week_, address, balance
            , SUM(balance) OVER(PARTITION BY week_ ORDER BY balance DESC) AS pay
            , SUM(balance) OVER(PARTITION BY week_) AS payda
            , ((SUM(balance) OVER(PARTITION BY week_ ORDER BY balance DESC) )/(SUM(balance) OVER(PARTITION BY week_))) * 100  AS ratio
            , ROW_NUMBER() OVER(ORDER BY balance DESC) AS rn
            FROM balances
            WHERE address != '0x0000000000000000000000000000000000000000'
        )
    order by balance DESC
            )
SELECT 
week_
, addressOrOthers
, SUM(balance) AS balance
FROM labels_ratios
GROUP BY 1, 2
""")
```
and record the output a data frame. Then with the help of hardcoded labels we are generating the graph above

