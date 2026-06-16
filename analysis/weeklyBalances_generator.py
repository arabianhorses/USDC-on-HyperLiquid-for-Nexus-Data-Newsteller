import os
import glob
import duckdb
import pandas as pd

RAW_DIR = '/Users/oguzkarabulut/Desktop/hyperliquid-working area/analysis/usdc_rebuild/raw'
CUTOFF  = '999999999999_026004118.parquet'   # read shards from the beginning up to (and including) this one

# shard filenames are zero-padded block ranges, so lexicographic <= == chronological <=
files = sorted(glob.glob(os.path.join(RAW_DIR, '*.parquet')))
files = [f for f in files if os.path.basename(f) <= CUTOFF]
if not files:
    raise SystemExit(f"no shards found <= {CUTOFF} in {RAW_DIR}")

file_list_sql = "[" + ",".join("'" + f.replace("'", "''") + "'" for f in files) + "]"

con = duckdb.connect()

# hex (uint256) -> exact decimal string. Python int is arbitrary precision, so this is
# lossless even for 2**256-1 (which overflows duckdb's HUGEINT). Cast to ::HUGEINT only
# when you know the value fits (Transfer/Mint/Burn amounts do; Approval may not).
con.create_function("hexdec", lambda h: None if h is None else str(int(h, 16)),
                    ["VARCHAR"], "VARCHAR")

# `raw` = all logs from the first shard up to the cutoff shard, concatenated
con.execute(f"CREATE VIEW raw AS SELECT * FROM read_parquet({file_list_sql})")



def compute_balances_before(week_timestamp):
    return con.sql(f"""
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
    --,  CASE WHEN ((ratio < 50) OR (rn<10)) THEN address ELSE 'others' END AS addressOrOthers
   -- ,  CASE WHEN  rn<3 THEN address ELSE 'others' END AS addressOrOthers
    ,  CASE  
        WHEN address = '0xc20699185c15d0a2fd65779bb5d69f5b0b113c00' THEN 'Coinbase: Hyperliquid Deployer'
        WHEN address = '0x744e4f26ee30213989216e1632d9be3547c4885b' THEN 'HyperLend USDC Market'
        WHEN address = '0x462b95575cb2d56de9d1aaaaab452279b058aa06' THEN 'Hyperbeat'
        WHEN address = '0x6b9e773128f453f5c2c60935ee2de2cbc5390a24' THEN 'Circle: CCTP CoreDepositWallet'
--        WHEN rn<3                                                   THEN address
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
""").df()


timestamps_list = list(range(1756080000, 1781481601, 604800))

"""
print (timestamps_list)
[1756080000, 1756684800, 1757289600, 1757894400, 1758499200, 1759104000, 1759708800, 1760313600, 1760918400, 1761523200, 1762128000, 1762732800, 1763337600, 1763942400, 1764547200, 1765152000, 1765756800, 1766361600, 1766966400, 1767571200, 1768176000, 1768780800, 1769385600, 1769990400, 1770595200, 1771200000, 1771804800, 1772409600, 1773014400, 1773619200, 1774224000, 1774828800, 1775433600, 1776038400, 1776643200, 1777248000, 1777852800, 1778457600, 1779062400, 1779667200, 1780272000, 1780876800, 1781481600]
"""


weekly_balances = []
for timestamp in timestamps_list:
    r = compute_balances_before(timestamp)
    weekly_balances.append(r)

weekly_balances = pd.concat(weekly_balances, ignore_index=True)

weekly_balances.to_json('weekly_balances_with_tags.json', orient='records', indent=2, date_format='iso')

# normalize week to a naive timestamp (matches the JSON / CSV representation)
weekly_balances["week_"] = pd.to_datetime(weekly_balances["week_"], utc=True).dt.tz_localize(None)

# data behind the chart -> CSV for the submission (exact weekly rows: week_, addressOrOthers, balance)
(weekly_balances.sort_values(["week_", "balance"], ascending=[True, False])
   [["week_", "addressOrOthers", "balance"]]
   .to_csv("usdc_balance_by_holder.csv", index=False))
print(f"wrote usdc_balance_by_holder.csv ({len(weekly_balances)} rows)")


# ---------------------------------------------------------------------------
# Polished stacked-bar chart (house style, shared with usdc_supply.png)
# ---------------------------------------------------------------------------
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter
from matplotlib.patches import Patch

NAVY, GREEN, GREY, GRID = "#0f172a", "#16a34a", "#6b7280", "#e5e7eb"
USDC_BLUE, AMBER, SLATE = "#2775ca", "#f59e0b", "#cbd5e1"

ORDER = [
    "Circle: CCTP CoreDepositWallet",
    "Coinbase: Hyperliquid Deployer",
    "HyperLend USDC Market",
    "Hyperbeat",
    "others",
]
COLORS = {
    "Circle: CCTP CoreDepositWallet": USDC_BLUE,
    "Coinbase: Hyperliquid Deployer": NAVY,
    "HyperLend USDC Market":          GREEN,
    "Hyperbeat":                      AMBER,
    "others":                         SLATE,
}
LEGEND = {  # original tag + shortened address; 'others' is an aggregate (no address)
    "Circle: CCTP CoreDepositWallet": "Circle: CCTP CoreDepositWallet(0x6b9...a24)",
    "Coinbase: Hyperliquid Deployer": "Coinbase: Hyperliquid Deployer(0xc20...c00)",
    "HyperLend USDC Market":          "HyperLend USDC Market(0x744...85b)",
    "Hyperbeat":                      "Hyperbeat(0x462...a06)",
    "others":                         "Everyone else",
}


def money(v, _):
    a = abs(v)
    if a >= 1e9: return f"${v/1e9:.0f}B"
    if a >= 1e6: return f"${v/1e6:.0f}M"
    return f"${v:.0f}"


pivot = (weekly_balances.pivot_table(index="week_", columns="addressOrOthers",
                                     values="balance", aggfunc="sum")
           .sort_index()
           .reindex(columns=ORDER)
           .fillna(0)
           .clip(lower=0))                                       # drop tiny negative dust
weeks = pivot.index.to_pydatetime()
BAR_W = 6   # days (matches usdc_supply.png)

fig, ax = plt.subplots(figsize=(13, 6.5))
bottom = np.zeros(len(pivot))
for col in ORDER:
    vals = pivot[col].to_numpy()
    ax.bar(weeks, vals, width=BAR_W, bottom=bottom, color=COLORS[col],
           linewidth=0, label=LEGEND[col])
    bottom += vals

total = bottom                       # top of each stack == total held that week
ax.set_ylim(0, total.max() * 1.12)   # headroom for the final-total annotation

ax.annotate(f"${total[-1]/1e9:.2f}B", xy=(weeks[-1], total[-1]),
            xytext=(0, 8), textcoords="offset points",
            ha="center", fontsize=10.5, fontweight="bold", color=NAVY)

# callout on the final-week Coinbase block — the headline event
cb_last = pivot["Coinbase: Hyperliquid Deployer"].iloc[-1]
cb_base = pivot["Circle: CCTP CoreDepositWallet"].iloc[-1]
ax.annotate(f"Coinbase 'Hyperliquid Deployer'\nwallet holds ${cb_last/1e9:.1f}B",
            xy=(weeks[-1], cb_base + cb_last * 0.55),
            xytext=(weeks[-11], total.max() * 0.78),
            ha="center", fontsize=9.5, fontweight="bold", color=NAVY,
            arrowprops=dict(arrowstyle="->", color=NAVY, lw=1.3))

ax.yaxis.set_major_formatter(FuncFormatter(money))
ax.set_ylabel("USDC balance held", color=GREY)
ax.grid(axis="y", color=GRID, lw=0.8)
ax.set_axisbelow(True)
ax.margins(x=0.02)
for s in ("top", "right"):
    ax.spines[s].set_visible(False)
ax.xaxis.set_major_locator(mdates.MonthLocator())
ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
for lbl in ax.get_xticklabels():
    lbl.set_fontsize(8)

# legend reads top-down in the same order the colours stack
handles = [Patch(color=COLORS[c], label=LEGEND[c]) for c in ORDER][::-1]
ax.legend(handles=handles, loc="upper left", frameon=False, fontsize=9.5)

# title block: bold headline + grey dek + source footer
fig.subplots_adjust(top=0.84, bottom=0.11, left=0.075, right=0.975)
fig.text(0.075, 0.945, "USDC on Hyperliquid — weekly balance by holder",
         fontsize=15.5, fontweight="bold", color=NAVY)
fig.text(0.075, 0.895,
         r"USDC held on HyperEVM, weekly snapshot — from ~\$0 at the Aug-2025 "
         r"launch to \$5.25B ten months later.",
         fontsize=10.5, color=GREY)
fig.text(0.075, 0.025,
         "Source: on-chain USDC Transfer events on HyperEVM  ·  balances at each week's close",
         fontsize=8, color=GREY)
fig.savefig("usdc_balance_by_holder.png", dpi=150)
print("wrote usdc_balance_by_holder.png ; weeks:", len(weeks),
      "; final total $%.2fB" % (total[-1] / 1e9))
