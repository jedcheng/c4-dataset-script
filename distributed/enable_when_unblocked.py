#!/usr/bin/env python3
"""Re-enable a host in hosts.json once Common Crawl stops blocking this machine's IP.

Sends ONE small request every --every seconds (a burst of requests is what got the
IP blocked in the first place). After --need consecutive HTTP 200s it sets
"enabled": true for --host and exits; the running controller picks it up within a
minute. Run it in screen:

    screen -dmS cc-unblock python3 enable_when_unblocked.py --host office
"""
import argparse
import json
import os
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG = os.path.join(HERE, "hosts.json")
LOG = os.path.join(HERE, "state", "unblock_watch.log")
PROBE = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-05/wet.paths.gz"


def log(msg):
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}"
    print(line, flush=True)
    with open(LOG, "a") as f:
        f.write(line + "\n")


def probe():
    req = urllib.request.Request(PROBE, method="HEAD")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception as e:
        return f"error {type(e).__name__}"


def enable(host):
    with open(CONFIG) as f:
        cfg = json.load(f)
    for w in cfg["workers"]:
        if w["name"] == host:
            w["enabled"] = True
    tmp = CONFIG + ".tmp"
    with open(tmp, "w") as f:
        f.write(json.dumps(cfg, indent=2) + "\n")
    os.replace(tmp, CONFIG)  # atomic: the controller never sees half a file


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="office")
    ap.add_argument("--every", type=int, default=900, help="seconds between probes")
    ap.add_argument("--need", type=int, default=2, help="consecutive 200s before enabling")
    args = ap.parse_args()
    log(f"watching for the block to lift, then enabling '{args.host}' "
        f"(1 request every {args.every // 60} min, {args.need} x 200 needed)")
    ok = 0
    while True:
        status = probe()
        ok = ok + 1 if status == 200 else 0
        log(f"probe: {status} ({ok}/{args.need})")
        if ok >= args.need:
            enable(args.host)
            log(f"block lifted: set '{args.host}' enabled=true in hosts.json, exiting")
            return
        time.sleep(args.every)


if __name__ == "__main__":
    main()
