# USDC-on-HyperLiquid-for-Nexus-Data-Newsteller
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

  

