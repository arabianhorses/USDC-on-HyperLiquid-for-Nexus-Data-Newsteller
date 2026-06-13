import datetime as dt

import os
import glob
import json
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter, MultipleLocator
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

RAW_DIR = '/Users/oguzkarabulut/Desktop/hyperliquid-working area/analysis/usdc_rebuild/raw'
CUTOFF  = '999999999999_026004118.parquet'   # will drop cut_off for now too big value
CACHE   = 'usdc_supply_cache.json'           # computed weekly flows + quarter QoQ; delete to recompute

# ---------------------------------------------------------------------------
# Quarter boundaries (UTC midnight). Epoch is derived from the date so the two
# can never drift apart — just add a date here to extend. (No DB needed, static.)
# ---------------------------------------------------------------------------
_QBOUNDS = [dt.datetime(2025, 7, 1), dt.datetime(2025, 10, 1), dt.datetime(2026, 1, 1),
            dt.datetime(2026, 4, 1), dt.datetime(2026, 7, 1)]
BND = [(d, int(d.replace(tzinfo=dt.timezone.utc).timestamp())) for d in _QBOUNDS]


def qname(d):
    return f"Q{(d.month - 1)//3 + 1}'{d.year % 100:02d}"


# ---------------------------------------------------------------------------
# Data: load from JSON cache if present, otherwise scan the parquet shards once
# and write the cache. `g` = weekly mint/burn/acumBal (ascending); `quarters` =
# (name, bstart, bend, qoq, partial) per quarter band; `now_supply` = latest.
# ---------------------------------------------------------------------------
def build_from_raw():
    import duckdb

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
    con.execute(f"CREATE VIEW raw AS SELECT * FROM read_parquet({file_list_sql})")

    Mint_table = con.sql("""
    SELECT
    to_timestamp(blockTimestamp) AT TIME ZONE 'UTC' AS blockTime
    , blockTimestamp
    , 'mint' AS side
    , CAST(hexdec(data) AS HUGEINT) AS amount
    FROM raw
    WHERE address = '0xb88339cb7199b77e23db6e890353e22632ba630f'-- USDC
    AND topic0 = '0xab8530f87dc9b59234c4623bf917212bb2536d647574c8e7e5da92c2ede0c9f8' --Mint(address minter, address to, uint256 amount)
    """).df()

    Burn_table = con.sql("""
    SELECT
    to_timestamp(blockTimestamp) AT TIME ZONE 'UTC' AS blockTime
    , blockTimestamp
    , 'burn' AS side
    , CAST(hexdec(data) AS HUGEINT) AS amount
    FROM raw
    WHERE address = '0xb88339cb7199b77e23db6e890353e22632ba630f'-- USDC
    AND topic0 = '0xcc16f5dbb4873280815c1ee09dbd06736cffcc184412cf7a71a0fdb75d397ca5' -- Burn(address burner, uint256 amount)
     """).df()

    # register the materialized frames so the local connection's queries can see them
    con.register("Mint_table", Mint_table)
    con.register("Burn_table", Burn_table)

    balance_and_change = con.sql("""
    SELECT
      week_
    , SUM(CASE WHEN side = 'mint' THEN amount ELSE 0 END)/POW(10,6) AS mint   -- gross, +
    , -SUM(CASE WHEN side = 'burn' THEN amount ELSE 0 END)/POW(10,6) AS burn   -- gross magnitude, +
    , SUM(amount)/POW(10,6) AS netChange
    , (SUM(SUM(amount)) OVER(ORDER BY week_))/POW(10,6) AS acumBal
    FROM (
        SELECT
        DATE_TRUNC('WEEK', blockTime) AS week_
        , 'mint' AS side
        , SUM(CAST(amount AS HUGEINT)) AS amount
        FROM Mint_table
        GROUP BY 1, 2

        UNION ALL

        SELECT
        DATE_TRUNC('WEEK', blockTime) AS week_
        , 'burn' AS side
        ,  (-1 * SUM(CAST(amount AS HUGEINT))) AS amount
        FROM Burn_table
        GROUP BY 1, 2
         )
    GROUP BY 1
    ORDER BY week_ DESC
    """).df()

    # QoQ total-supply growth: exact supply at each quarter boundary (from raw
    # blockTimestamp, not week-rounded), then (end/start - 1).
    def supply_before(epoch):
        """Cumulative net supply (USDC) of all events strictly before `epoch` seconds."""
        return con.sql(f"""
            SELECT COALESCE(SUM(amt), 0)/POW(10,6) FROM (
                SELECT CAST(amount AS HUGEINT) AS amt FROM Mint_table WHERE blockTimestamp < {epoch}
                UNION ALL
                SELECT -CAST(amount AS HUGEINT)     FROM Burn_table WHERE blockTimestamp < {epoch}
            )
        """).fetchone()[0]

    g = balance_and_change.sort_values("week_").reset_index(drop=True)   # ascending for plotting
    xmin, xmax = g.week_.min().to_pydatetime(), g.week_.max().to_pydatetime()
    now_supply = float(g.acumBal.iloc[-1])

    quarters = []
    for (s, se), (e, ee) in zip(BND[:-1], BND[1:]):
        bstart, bend = max(s, xmin), min(e, xmax)
        if bend <= bstart:
            continue
        s0 = supply_before(se)
        partial = e > xmax
        s1 = now_supply if partial else supply_before(ee)
        qoq = None if s0 <= 0 else (s1 - s0) / s0 * 100
        quarters.append((qname(s), bstart, bend, qoq, partial))

    # persist for fast re-plots
    json.dump({
        "now_supply": now_supply,
        "weekly": json.loads(g.to_json(orient="records", date_format="iso")),
        "quarters": [{"name": n, "bstart": bs.isoformat(), "bend": be.isoformat(),
                      "qoq": q, "partial": p} for (n, bs, be, q, p) in quarters],
    }, open(CACHE, "w"), indent=2)
    return g, quarters, now_supply


def load_cache():
    obj = json.load(open(CACHE))
    g = pd.DataFrame(obj["weekly"])
    g["week_"] = pd.to_datetime(g["week_"]).dt.tz_localize(None)
    quarters = [(q["name"], dt.datetime.fromisoformat(q["bstart"]),
                 dt.datetime.fromisoformat(q["bend"]), q["qoq"], q["partial"])
                for q in obj["quarters"]]
    return g, quarters, float(obj["now_supply"])


_src = "cache" if os.path.exists(CACHE) else "raw"
g, quarters, now_supply = load_cache() if _src == "cache" else build_from_raw()
xmin, xmax = g.week_.min().to_pydatetime(), g.week_.max().to_pydatetime()

# data behind the chart -> CSV for the submission (same weekly rows as usdc_supply_cache.json)
g.to_csv("usdc_supply.csv", index=False)
print(f"wrote usdc_supply.csv ({len(g)} weekly rows)")

# ---------------------------------------------------------------------------
# Chart
# ---------------------------------------------------------------------------
GREEN, RED, NAVY, GREY, GRID = "#16a34a", "#dc2626", "#0f172a", "#6b7280", "#e5e7eb"
BAR_W = 6


def money(v, _):
    a = abs(v)
    if a >= 1e9: return f"{v/1e9:.1f}B"
    if a >= 1e6: return f"{v/1e6:.0f}M"
    return f"{v:.0f}"


fig, ax_f = plt.subplots(figsize=(13, 6.5))
ax_f.bar(g.week_, g.mint, width=BAR_W, color=GREEN, linewidth=0, label="Mint (week)")
ax_f.bar(g.week_, -g.burn, width=BAR_W, color=RED, linewidth=0, label="Burn (week)")
ax_f.axhline(0, color="#9ca3af", lw=0.8)
ax_f.set_ylabel("Weekly gross flow", color=GREY)
ax_f.yaxis.set_major_formatter(FuncFormatter(money))

ax_b = ax_f.twinx()
ax_b.fill_between(g.week_, 0, g.acumBal, color=NAVY, alpha=0.10, zorder=2, lw=0)
ax_b.plot(g.week_, g.acumBal, color=NAVY, lw=2.4, zorder=3, label="Total supply")
ax_b.set_ylabel("Total supply", color=NAVY)
ax_b.yaxis.set_major_formatter(FuncFormatter(money))
ax_b.yaxis.set_major_locator(MultipleLocator(500e6))               # tick every 500M
ax_b.set_ylim(0, (now_supply // 500e6) * 500e6 + 250e6)            # top tick = 5.5B
ax_b.set_zorder(ax_f.get_zorder() + 1)
ax_b.patch.set_visible(False)

# subtle quarter dividers (interior boundaries inside the data span)
for d, _ in BND:
    if xmin < d < xmax:
        ax_f.axvline(d, color="#9ca3af", ls="--", lw=1, alpha=0.7, zorder=0)

# QoQ growth labels at the top of each quarter band
for name, bstart, bend, qoq, partial in quarters:
    center = bstart + (bend - bstart) / 2
    if qoq is None:
        txt, col = f"{name} launch", GREY
    else:
        txt = f"{name} {qoq:+.0f}%" + (" (QTD)" if partial else "")
        col = GREEN if qoq >= 0 else RED
    ax_b.text(center, 0.965, txt, transform=ax_b.get_xaxis_transform(),
              ha="center", va="top", fontsize=10, fontweight="bold", color=col)

ax_f.grid(axis="y", color=GRID, lw=0.8)
ax_f.set_axisbelow(True)
ax_f.margins(x=0.02)
ax_f.spines["top"].set_visible(False)
ax_b.spines["top"].set_visible(False)
ax_f.xaxis.set_major_locator(mdates.MonthLocator())
ax_f.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
for lbl in ax_f.get_xticklabels():
    lbl.set_fontsize(8)
ax_f.legend(handles=[Line2D([0], [0], color=NAVY, lw=2.4, label="Total supply"),
                     Patch(color=GREEN, label="Mint (week)"),
                     Patch(color=RED, label="Burn (week)")],
            loc="lower left", frameon=False, fontsize=9.5)

# title block: bold headline + grey dek + source footer (newsletter house style)
fig.subplots_adjust(top=0.84, bottom=0.12, left=0.08, right=0.92)
fig.text(0.08, 0.945, "USDC supply — weekly gross mint/burn & QoQ total-supply growth",
         fontsize=15, fontweight="bold", color=NAVY)
fig.text(0.08, 0.895,
         "Weekly USDC minted (green) and burned (red) on HyperEVM, with cumulative total "
         "supply (line) compounding to ~\\$5.5B.",
         fontsize=10.5, color=GREY)
fig.text(0.08, 0.03,
         "Source: on-chain USDC Mint & Burn events on HyperEVM  ·  "
         "QoQ = total-supply growth vs. prior quarter (QTD = quarter-to-date)",
         fontsize=8, color=GREY)
fig.savefig("usdc_supply.png", dpi=150)
print("wrote usdc_supply.png ; weeks:", len(g), "; total supply $%.2fB" % (now_supply / 1e9),
      "; source:", _src)
