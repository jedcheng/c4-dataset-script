#!/usr/bin/env python3
"""Download + filter one chunk of WET files. Runs on a worker host.

Launched (detached) by controller.py. Reads the chunk's WET paths from
<chunk_dir>/paths.txt, writes one part file per WET file so that a killed or
restarted job resumes where it stopped, and finally packs everything into
<chunk_dir>/<name>.jsonl.gz plus a <name>.ok stats file. The controller treats
the existence of the .ok file as "chunk finished".

Requires only: requests, pyahocorasick, tqdm (no Spark / Java).
"""
import argparse
import gzip
import io
import itertools
import json
import logging
import os
import random
import shutil
import sys
import time
from multiprocessing import Pool, TimeoutError as PoolTimeout, Value

import requests

# tqdm bars from every pool process would flood the log; must be set before import
os.environ.setdefault("TQDM_DISABLE", "1")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import download_web_docs as dwd  # noqa: E402

CC_DOMAIN = "https://data.commoncrawl.org"
HERE = os.path.dirname(os.path.abspath(__file__))
# If no file of the chunk finishes for this long, assume the pool lost a task
# (multiprocessing.Pool never notices a worker that dies mid-task and then waits
# forever), discard the pool and re-run the missing files in a fresh one.
STALL_SECS = 30 * 60
MAX_POOL_ROUNDS = 4

_badwords = None
_sc_words = None
_rate = None  # bytes/s cap for the whole job (None = unlimited)
_rate_file, _rate_checked = None, 0.0
_active = None  # shared count of processes currently downloading


def _init_pool(badwords_path, sc_path, rate, rate_file, active):
    global _badwords, _sc_words, _rate, _rate_file, _active
    _rate, _rate_file, _active = rate, rate_file, active
    _badwords = dwd.build_badword_automaton(dwd.load_word_list(badwords_path))
    _sc_words = dwd.build_sc_matcher(dwd.load_word_list(sc_path))


def _current_rate():
    """This process's cap in bytes/s: the job cap shared by the processes that are
    downloading right now (the others are busy filtering). The controller rewrites
    the rate file (Mbit/s for the whole job, 0 = unlimited) as the day/night
    schedule changes; it is re-read every 30 s."""
    global _rate, _rate_checked
    if _rate_file and time.time() - _rate_checked > 30:
        _rate_checked = time.time()
        try:
            with open(_rate_file) as f:
                mbps = float(f.read())
            _rate = mbps * 1e6 / 8 if mbps > 0 else None
        except (OSError, ValueError):
            pass
    if _rate is None:
        return None
    return _rate / max(1, _active.value)


def _download(cc_path, give_up_after=3600):
    """Fetch one WET file into memory, paced to _rate.

    Two kinds of failure, two backoffs: the server saying "slow down" (503 etc.)
    gets a long exponential backoff (up to ~5 min), while network trouble on our
    side (DNS failures, unanswered connects, resets) is retried quickly (<= 30 s)
    so a short outage doesn't leave workers asleep long after it is over.
    Gives up after `give_up_after` seconds of failures."""
    url = f"{CC_DOMAIN}/{cc_path}"
    t_start = time.time()
    for attempt in itertools.count():
        server_busy = False
        try:
            r = requests.get(url, timeout=(10, 600), stream=True)
            if r.status_code == 200:
                with _active.get_lock():
                    _active.value += 1
                try:
                    buf, t_prev = bytearray(), time.time()
                    for block in r.iter_content(1 << 20):
                        buf += block
                        # pace block by block, since the share changes as others start/finish
                        rate = _current_rate()
                        if rate:
                            ahead = len(block) / rate - (time.time() - t_prev)
                            if ahead > 0:
                                time.sleep(ahead)
                        t_prev = time.time()
                finally:
                    with _active.get_lock():
                        _active.value -= 1
                return bytes(buf)
            r.close()
            if r.status_code == 404:
                raise FileNotFoundError(f"HTTP {r.status_code} {url}")
            err = f"HTTP {r.status_code}"
            # 403 from CloudFront = this IP is blocked/throttled, not a missing file
            server_busy = r.status_code in (403, 429, 500, 502, 503, 504)
        except requests.RequestException as e:
            err = repr(e)
        if time.time() - t_start > give_up_after:
            raise RuntimeError(f"giving up on {url} after {attempt + 1} tries: {err}")
        if server_busy:
            delay = min(300, 5 * 2 ** min(attempt, 6))
        else:
            delay = min(30, 2 * 2 ** min(attempt, 4))
        delay *= random.uniform(0.5, 1.5)
        logging.warning("download %s failed (%s), retry in %.0fs", cc_path, err, delay)
        time.sleep(delay)


def process_one(task):
    """Download + filter one WET file into parts/<idx>.jsonl. Returns stats dict."""
    idx, cc_path, parts_dir = task
    out = os.path.join(parts_dir, f"{idx:05d}.jsonl")
    if os.path.exists(out):
        return {"idx": idx, "skipped": True}
    t0 = time.time()
    try:
        for _ in range(5):
            data = _download(cc_path)
            try:
                pages = list(dwd.filter_wet_pages(io.BytesIO(data), _badwords, _sc_words))
                break
            except (EOFError, gzip.BadGzipFile, OSError) as e:
                logging.warning("corrupt gzip %s (%r), re-downloading", cc_path, e)
        else:
            raise RuntimeError(f"corrupt gzip 5 times: {cc_path}")
    except Exception as e:
        return {"idx": idx, "error": f"{type(e).__name__}: {e}", "path": cc_path}

    tmp = out + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for page in pages:
            f.write(json.dumps(page, ensure_ascii=False))
            f.write("\n")
    os.replace(tmp, out)
    return {"idx": idx, "docs": len(pages), "bytes_in": len(data), "secs": time.time() - t0}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--chunk-dir", required=True)
    ap.add_argument("--name", required=True, help="output basename, e.g. chunk_00017")
    ap.add_argument("--procs", type=int, default=os.cpu_count())
    ap.add_argument("--max-mbps", type=float, default=0,
                    help="download cap for this job in Mbit/s, shared by its processes (0 = none)")
    ap.add_argument("--rate-file", default=None,
                    help="file holding the current cap in Mbit/s; overrides --max-mbps when present")
    ap.add_argument("--badwords", default=os.path.join(HERE, "badwords", "zh"))
    ap.add_argument("--sc-words", default=os.path.join(HERE, "badwords", "SC_list.txt"))
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    chunk_dir = args.chunk_dir
    parts_dir = os.path.join(chunk_dir, "parts")
    os.makedirs(parts_dir, exist_ok=True)
    ok_file = os.path.join(chunk_dir, args.name + ".ok")
    if os.path.exists(ok_file):
        return

    with open(os.path.join(chunk_dir, "paths.txt")) as f:
        paths = [l.strip() for l in f if l.strip()]
    tasks = [(i, p, parts_dir) for i, p in enumerate(paths)]

    t0 = time.time()
    failed, docs, bytes_in = [], 0, 0
    rate = args.max_mbps * 1e6 / 8 if args.max_mbps else None
    answered = set()  # task indices that returned a result (ok, skipped or error)
    for round_no in range(MAX_POOL_ROUNDS):
        todo = [t for t in tasks if t[0] not in answered]
        if not todo:
            break
        if round_no:
            logging.warning("pool round %d: re-running %d unanswered files", round_no + 1, len(todo))
        # maxtasksperchild: fresh processes stay under per-process CPU-time ulimits (HPC login node)
        with Pool(args.procs, initializer=_init_pool,
                  initargs=(args.badwords, args.sc_words, rate, args.rate_file, Value("i", 0)),
                  maxtasksperchild=50) as pool:
            it = pool.imap_unordered(process_one, todo)
            while True:
                try:
                    res = it.next(timeout=STALL_SECS)
                except StopIteration:
                    break
                except PoolTimeout:
                    logging.error("no file finished for %d min, %d still outstanding: "
                                  "discarding the pool", STALL_SECS // 60,
                                  len(tasks) - len(answered))
                    break
                answered.add(res["idx"])
                n = len(answered)
                if "error" in res:
                    failed.append(res)
                    logging.error("FAILED %s: %s", res["path"], res["error"])
                elif not res.get("skipped"):
                    docs += res["docs"]
                    bytes_in += res["bytes_in"]
                    logging.info("[%d/%d] %05d docs=%d %.1fMB %.0fs", n, len(tasks), res["idx"],
                                 res["docs"], res["bytes_in"] / 1e6, res["secs"])
                with open(os.path.join(chunk_dir, "progress"), "w") as f:
                    # answered / total / succeeded (failed files are progress, not throughput)
                    f.write(f"{n} {len(tasks)} {n - len(failed)}\n")
        # leaving the with-block terminates the pool, including any hung worker
    for i, p, _ in tasks:
        if i not in answered:
            failed.append({"idx": i, "path": p, "error": "no result after all pool rounds"})
            logging.error("FAILED %s: no result after %d pool rounds", p, MAX_POOL_ROUNDS)

    # Pack the parts in path order into one gzip file.
    out = os.path.join(chunk_dir, args.name + ".jsonl.gz")
    total_docs = 0
    with gzip.open(out + ".tmp", "wt", encoding="utf-8", compresslevel=6) as g:
        for i in range(len(paths)):
            part = os.path.join(parts_dir, f"{i:05d}.jsonl")
            if os.path.exists(part):
                with open(part, encoding="utf-8") as f:
                    for line in f:
                        g.write(line)
                        total_docs += 1
    os.replace(out + ".tmp", out)

    stats = {
        "name": args.name,
        "n_paths": len(paths),
        "n_failed": len(failed),
        "failed": [r["path"] for r in failed],
        "docs": total_docs,
        "bytes_downloaded_this_run": bytes_in,
        "secs_this_run": round(time.time() - t0, 1),
        "host": os.uname().nodename,
    }
    with open(ok_file + ".tmp", "w") as f:
        json.dump(stats, f)
    os.replace(ok_file + ".tmp", ok_file)
    shutil.rmtree(parts_dir, ignore_errors=True)
    logging.info("chunk done: %s", stats)


if __name__ == "__main__":
    main()
