#!/usr/bin/env python3
"""Snapshot resource usage of the download into distributed/usage/ (re-runnable).

1. Azure Monitor platform metrics of every VM in the resource group, hourly:
   Percentage CPU (average + max) and Network In/Out Total (bytes). Azure keeps
   these for 93 days and drops them when a VM is deleted, so run this again at
   the end of the job and BEFORE deleting any VM.
   -> usage/azure_metrics_hourly.csv
2. Per-chunk record of every host (incl. on-premise ones and deleted VMs) from
   the controller DB: host, crawl, finish time, WET bytes downloaded, docs kept.
   -> usage/chunks.csv
3. A per-host summary of both.
   -> usage/summary.md
"""
import argparse
import csv
import datetime as dt
import json
import os
import sqlite3
import subprocess
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "usage")
DB = os.path.join(HERE, "state", "state.db")
METRICS = ["Percentage CPU", "Network In Total", "Network Out Total"]
UTC = dt.timezone.utc


def az(*args):
    r = subprocess.run(["az", *args, "-o", "json"], capture_output=True, text=True)
    if r.returncode:
        raise RuntimeError(f"az {' '.join(args[:3])}: {r.stderr.strip()[-300:]}")
    return json.loads(r.stdout)


def azure_metrics(rg):
    rows = []
    for vm in az("vm", "list", "-g", rg):
        name, rid = vm["name"], vm["id"]
        # Azure gives 7 fractional digits, which older Pythons can't parse: keep seconds
        start = dt.datetime.strptime(vm["timeCreated"][:19], "%Y-%m-%dT%H:%M:%S").replace(
            tzinfo=UTC, minute=0, second=0)
        end = dt.datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
        by_ts = defaultdict(dict)
        t = start
        while t < end:  # 7-day windows keep each query well inside API limits
            t2 = min(t + dt.timedelta(days=7), end)
            res = az("monitor", "metrics", "list", "--resource", rid, "--metrics", *METRICS,
                     "--interval", "PT1H", "--aggregation", "Average", "Maximum", "Total",
                     "--start-time", t.isoformat(), "--end-time", t2.isoformat())
            for m in res["value"]:
                key = m["name"]["value"]
                for ts in m["timeseries"]:
                    for p in ts["data"]:
                        d = by_ts[p["timeStamp"]]
                        if key == "Percentage CPU":
                            d["cpu_avg_pct"] = p.get("average")
                            d["cpu_max_pct"] = p.get("maximum")
                        else:
                            d[key.replace(" Total", "").lower().replace(" ", "_") + "_bytes"] = p.get("total")
            t = t2
        for ts in sorted(by_ts):
            rows.append({"vm": name, "hour_utc": ts, **by_ts[ts]})
        print(f"{name}: {len(by_ts)} hours since {start:%Y-%m-%d %H:%M} UTC")
    return rows


def chunk_rows():
    con = sqlite3.connect(DB)
    rows = []
    for crawl, idx, host, started, finished, stats in con.execute(
            "SELECT crawl, idx, host, started, finished, stats FROM chunks "
            "WHERE stats IS NOT NULL ORDER BY finished"):
        s = json.loads(stats)
        rows.append({
            "crawl": crawl, "chunk": idx, "host": host,
            "finished_utc": dt.datetime.fromtimestamp(finished, UTC).isoformat(timespec="seconds"),
            "wet_files": s["n_paths"], "failed_files": s["n_failed"], "docs_kept": s["docs"],
            # bytes of the last run only: work done before a restart/resume is not counted
            "wet_bytes_last_run": s.get("bytes_downloaded_this_run", 0),
            "secs_last_run": s.get("secs_this_run", 0),
        })
    return rows


def write_csv(path, rows):
    keys = list(dict.fromkeys(k for r in rows for k in r))
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, keys)
        w.writeheader()
        w.writerows(rows)


def summary(metrics, chunks):
    tb = 1e12
    lines = [f"# Download resource usage (snapshot {dt.datetime.now(UTC):%Y-%m-%d %H:%M} UTC)", ""]
    lines += ["## Azure VMs (Azure Monitor, hourly)", "",
              "| VM | hours with data | network in (TB) | network out (TB) | mean CPU % | peak hourly CPU % |",
              "|---|---|---|---|---|---|"]
    by_vm = defaultdict(list)
    for r in metrics:
        by_vm[r["vm"]].append(r)
    for vm, rs in sorted(by_vm.items()):
        live = [r for r in rs if r.get("network_in_bytes")]
        cpu = [r["cpu_avg_pct"] for r in live if r.get("cpu_avg_pct") is not None]
        lines.append(f"| {vm} | {len(live)} | {sum(r['network_in_bytes'] for r in live) / tb:.1f} | "
                     f"{sum(r.get('network_out_bytes') or 0 for r in live) / tb:.2f} | "
                     f"{sum(cpu) / max(len(cpu), 1):.0f} | {max(cpu, default=0):.0f} |")
    lines += ["", "Network in includes retries, SSH and package installs; "
              "hours with no traffic (deallocated) are excluded from the CPU mean.", ""]
    lines += ["## All hosts (controller DB, per finished chunk)", "",
              "| host | chunks | WET files | WET TB (last run) | docs kept |", "|---|---|---|---|---|"]
    by_host = defaultdict(lambda: [0, 0, 0, 0])
    for r in chunks:
        h = by_host[r["host"]]
        h[0] += 1; h[1] += r["wet_files"]; h[2] += r["wet_bytes_last_run"]; h[3] += r["docs_kept"]
    tot = [0, 0, 0, 0]
    for host, h in sorted(by_host.items()):
        lines.append(f"| {host} | {h[0]} | {h[1]:,} | {h[2] / tb:.1f} | {h[3]:,} |")
        tot = [a + b for a, b in zip(tot, h)]
    lines.append(f"| **total** | {tot[0]} | {tot[1]:,} | {tot[2] / tb:.1f} | {tot[3]:,} |")
    lines += ["", "WET TB counts the final run of each chunk only, so bytes fetched before a "
              "job was restarted (and resumed from its part files) are missing; "
              "it is a lower bound on what was downloaded."]
    return "\n".join(lines) + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rg", default="CANTONESE-LLM")
    args = ap.parse_args()
    os.makedirs(OUT, exist_ok=True)
    metrics = azure_metrics(args.rg)
    chunks = chunk_rows()
    write_csv(os.path.join(OUT, "azure_metrics_hourly.csv"), metrics)
    write_csv(os.path.join(OUT, "chunks.csv"), chunks)
    with open(os.path.join(OUT, "summary.md"), "w") as f:
        f.write(summary(metrics, chunks))
    print(f"wrote {len(metrics)} metric rows, {len(chunks)} chunk rows -> {OUT}")


if __name__ == "__main__":
    main()
