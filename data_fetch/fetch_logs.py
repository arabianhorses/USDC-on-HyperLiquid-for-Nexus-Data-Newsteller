"""
Fetch EVERY log emitted by the USDC contract 0xb88339...ba630f over full history,
from purroofgroup (the validated complete HyperEVM source — includes HyperCore->EVM
system-tx logs that HyperSync/official RPC drop). Lossless raw logs with TRUE chain
log_index. Fully independent of Envio.

Robust + resumable:
  - adaptive chunking: 10k-block chunks, recursively halve on result-cap errors
  - retries with backoff on transient/rate-limit errors
  - one parquet shard per aligned 10k chunk in raw/ ; existing shards are skipped on resume

Usage:  python fetch_logs.py [run_from] [run_to]   (defaults: contract start .. head)
"""
import os, sys, json, time, glob, re
import requests
import duckdb

RPC   = "https://rpc.purroofgroup.com"
USDC  = "0xb88339cb7199b77e23db6e890353e22632ba630f"
START = 10_934_119               # contract's first event (across all Envio parquets)
CHUNK = 10_000                   # aligned top-level chunk size (provider max range)
HERE  = os.path.dirname(os.path.abspath(__file__))
RAW   = os.path.join(HERE, "raw")
LOG   = os.path.join(HERE, "fetch.log")

os.makedirs(RAW, exist_ok=True)
session = requests.Session()
con = duckdb.connect()

SPLIT_HINTS = ("max results", "response is too big", "1048576", "exceeds", "too large", "too many")
RETRY_HINTS = ("rate", "limit exceeded", "timeout", "busy", "capacity", "try again", "-32005", "429", "503", "502")

def log(msg):
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(LOG, "a") as f:
        f.write(line + "\n")

SUBSPAN = 2000     # pre-split each 10k chunk into 2k sub-ranges -> stays under caps, rarely splits
SLEEP   = 0.12     # politeness between successful calls (avoid triggering throttling)

def rpc(method, params, tries=12):
    """Patient retry. Returns (result, None) | (None, err). Size-cap errors are tagged
    {'_split': True} so the caller splits; ALL other errors are retried (never amplified)."""
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    delay = 2.0
    for attempt in range(tries):
        try:
            r = session.post(RPC, json=payload, timeout=60)
            if r.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"http {r.status_code}")
            j = r.json()
        except Exception as e:
            if attempt == tries - 1:
                return None, {"message": f"exc:{e}"}
            time.sleep(delay); delay = min(delay * 1.6, 60); continue
        if "error" in j:
            msg = str(j["error"].get("message", "")).lower()
            if any(h in msg for h in SPLIT_HINTS):
                return None, {"_split": True, **j["error"]}   # size cap -> caller splits
            if attempt == tries - 1:
                return None, j["error"]
            time.sleep(delay); delay = min(delay * 1.6, 60); continue   # rate-limit/transient -> wait, retry SAME range
        return j["result"], None
    return None, {"message": "unreachable"}

def get_head():
    res, err = rpc("eth_blockNumber", [])
    if err: raise RuntimeError(err)
    return int(res, 16)

def fetch_subrange(fromb, tob):
    """Logs for [fromb,tob]; recursively halve ONLY on size-cap errors (never on rate-limit)."""
    res, err = rpc("eth_getLogs", [{"fromBlock": hex(fromb), "toBlock": hex(tob), "address": USDC}])
    if err is None:
        time.sleep(SLEEP)
        return res
    if err.get("_split") and tob > fromb:
        mid = (fromb + tob) // 2
        return fetch_subrange(fromb, mid) + fetch_subrange(mid + 1, tob)
    raise RuntimeError(f"range [{fromb},{tob}] failed after retries: {err}")

def fetch_range(fromb, tob):
    """Fetch a top-level chunk by walking fixed SUBSPAN sub-ranges (bounded, throttle-safe)."""
    logs, b = [], fromb
    while b <= tob:
        e = min(b + SUBSPAN - 1, tob)
        logs += fetch_subrange(b, e)
        b = e + 1
    return logs

SHARD_DDL = """CREATE OR REPLACE TEMP TABLE shard(
  blockNumber BIGINT, blockTimestamp BIGINT, logIndex BIGINT, transactionHash VARCHAR,
  transactionIndex BIGINT, address VARCHAR, topic0 VARCHAR, topic1 VARCHAR, topic2 VARCHAR,
  topic3 VARCHAR, data VARCHAR, removed BOOLEAN)"""

def write_shard(path, logs):
    """Explicit typed schema (identical across all shards regardless of null columns).
    blockTimestamp comes inline on every log from purroofgroup (no separate fetch needed)."""
    def tp(t, i):
        return t[i] if len(t) > i else None
    tuples = [(
        int(l["blockNumber"], 16),
        int(l["blockTimestamp"], 16) if l.get("blockTimestamp") else None,
        int(l["logIndex"], 16), l["transactionHash"],
        int(l["transactionIndex"], 16), l["address"],
        tp(l["topics"], 0), tp(l["topics"], 1), tp(l["topics"], 2), tp(l["topics"], 3),
        l["data"], bool(l.get("removed", False)),
    ) for l in logs]
    con.execute(SHARD_DDL)
    if tuples:
        con.executemany("INSERT INTO shard VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", tuples)
    con.execute(f"COPY shard TO '{path}' (FORMAT parquet, COMPRESSION zstd)")
    return len(tuples)

def prune_partial_shards():
    """Delete trailing partial shards (cend != cstart+CHUNK-1). The last chunk of a
    previous run was capped at the old head, so it must be re-fetched to the new head;
    otherwise the new (full-range) filename would coexist with it and duplicate blocks."""
    removed = []
    for p in glob.glob(os.path.join(RAW, "*.parquet")):
        m = re.match(r"(\d{9})_(\d{9})\.parquet$", os.path.basename(p))
        if m and int(m.group(2)) != int(m.group(1)) + CHUNK - 1:
            os.remove(p); removed.append(os.path.basename(p))
    if removed:
        log(f"pruned {len(removed)} partial shard(s) for clean resync: {removed}")

def main():
    head = get_head()
    run_from = int(sys.argv[1]) if len(sys.argv) > 1 else START
    run_to   = int(sys.argv[2]) if len(sys.argv) > 2 else head
    if len(sys.argv) == 1:          # full incremental sync -> safe to prune the tip
        prune_partial_shards()
    log(f"START={START} head={head} run=[{run_from},{run_to}] CHUNK={CHUNK}")

    chunks = list(range(START, run_to + 1, CHUNK))
    total = sum(1 for c in chunks if not (c + CHUNK - 1 < run_from or c > run_to))
    done = 0; logs_written = 0; t0 = time.time()
    for cstart in chunks:
        cend = min(cstart + CHUNK - 1, head)
        if cend < run_from or cstart > run_to:
            continue
        shard = os.path.join(RAW, f"{cstart:09d}_{cend:09d}.parquet")
        if os.path.exists(shard):
            done += 1; continue
        for chunk_try in range(4):                 # survive a transient chunk failure
            try:
                logs = fetch_range(cstart, cend); break
            except Exception as ex:
                if chunk_try == 3:
                    log(f"CHUNK FAILED [{cstart},{cend}]: {ex}"); raise
                log(f"chunk [{cstart},{cend}] retry {chunk_try+1}: {ex}"); time.sleep(30)
        tmp = shard + ".tmp"
        n = write_shard(tmp, logs)
        os.replace(tmp, shard)   # atomic: shard only appears when complete
        done += 1; logs_written += n
        if done % 20 == 0 or done == total:
            el = time.time() - t0
            rate = done / el if el else 0
            eta = (total - done) / rate / 60 if rate else 0
            log(f"{done}/{total} chunks ({100*done/total:.1f}%)  blk~{cend}  logs+={logs_written:,}  "
                f"{rate:.1f} chunk/s  ETA {eta:.0f} min")
    log(f"DONE run=[{run_from},{run_to}] chunks={done} logs_written={logs_written:,} in {(time.time()-t0)/60:.1f} min")

if __name__ == "__main__":
    main()
