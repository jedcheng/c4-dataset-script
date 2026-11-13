#!/usr/bin/env python3
"""Distribute the Common Crawl download+filter step over several hosts.

Runs on one machine (the office PC) that can ssh into every worker and the HPC.
State lives in state/state.db (SQLite), so the controller can be killed and
restarted at any time; jobs on the workers run detached and survive it.

Work unit = a "chunk" of CHUNK_SIZE consecutive WET files of one crawl.
Scheduling is pull-based: every host keeps `slots` chunks in flight and gets a
new one as soon as one finishes, so faster hosts automatically take more work.
A host that stays unreachable longer than LEASE_HOURS loses its chunks back to
the queue. Finished chunks are pulled to state/staging/ and periodically
rsynced in one batch to the HPC as
    <hpc.root>/1_download_<YYYY_WW>/chunk_<NNNNN>.jsonl.gz (+ .ok stats).

Usage:
    python controller.py init        # list 2025/2026 crawls, build the queue
    python controller.py setup       # push code + create venvs on all workers
    python controller.py run         # main loop (run inside tmux / nohup)
    python controller.py status      # progress, per-host throughput, ETA
    python controller.py requeue-failed   # retry WET files that failed

hosts.json is re-read every loop: edit procs / slots / "enabled" live.
"""
import argparse
import gzip
import json
import logging
import os
import shlex
import sqlite3
import statistics
import subprocess
import sys
import threading
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
STATE = os.path.join(HERE, "state")
DB_PATH = os.path.join(STATE, "state.db")
STAGING = os.path.join(STATE, "staging")
WET_PATHS_DIR = os.path.join(STATE, "wet_paths")
CONFIG = os.path.join(HERE, "hosts.json")

POLL_SECS = 60
UPLOAD_EVERY_SECS = 600
LEASE_HOURS = 3
MAX_ATTEMPTS = 6
STUCK_SECS = 3600  # a running job whose progress counter hasn't moved this long is restarted
SSH_OPTS = ["-o", "BatchMode=yes", "-o", "ConnectTimeout=20", "-o", "ServerAliveInterval=30",
            "-o", "ServerAliveCountMax=4", "-o", "StrictHostKeyChecking=accept-new",
            "-o", "ControlMaster=auto", "-o", "ControlPath=~/.ssh/cm-cc-%r@%h:%p",
            "-o", "ControlPersist=10m"]
# Files shipped to every worker: (source in repo, name on worker)
WORKER_FILES = [
    ("distributed/wet_worker.py", "wet_worker.py"),
    ("c4_dataset_script/Chinese/download_web_docs.py", "download_web_docs.py"),
    ("c4_dataset_script/badwords/zh", "badwords/zh"),
    ("c4_dataset_script/badwords/SC_list.txt", "badwords/SC_list.txt"),
]

log = logging.getLogger("controller")


# --------------------------------------------------------------------------- config / hosts

_good_config = None
_config_lock = threading.Lock()


def load_config():
    """hosts.json is edited by hand while the controller runs: if an edit leaves it
    invalid, keep using the last valid version (and log) instead of crashing."""
    global _good_config
    with _config_lock:
        try:
            with open(CONFIG) as f:
                _good_config = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            if _good_config is None:
                raise
            log.error("hosts.json is invalid (%s); still using the previous version", e)
        return _good_config


def claim_order():
    """ORDER BY for handing out chunks: crawls of years listed first in
    hosts.json "years" go first, then crawl and chunk order."""
    years = [y for y in load_config().get("years", []) if str(y).isdigit()]
    case = " ".join(f"WHEN '{y}' THEN {i}" for i, y in enumerate(years))
    prio = f"CASE substr(crawl, 9, 4) {case} ELSE {len(years)} END, " if years else ""
    return prio + "crawl, idx"


class Host:
    """A machine reachable by ssh (or the local machine when ssh is null)."""

    def __init__(self, cfg, workers=()):
        self.name = cfg["name"]
        self.cfg = cfg
        self.local = not cfg.get("ssh")
        # "via": name of another worker to hop through (for hosts on its private LAN,
        # reachable only with that host's own ssh keys). "ssh" is then e.g. "192.168.2.10".
        self.jump = None
        if cfg.get("via"):
            self.jump = Host(next(w for w in workers if w["name"] == cfg["via"]), workers)
            self.target = cfg["ssh"]
            self.inner_ssh = "ssh -o BatchMode=yes -o ConnectTimeout=20 -o ServerAliveInterval=30"
        elif not self.local:
            toks = [os.path.expanduser(os.path.expandvars(t)) for t in shlex.split(cfg["ssh"])]
            if toks and toks[0] == "ssh":
                toks = toks[1:]
            self.target = toks[-1]
            self.ssh_args = SSH_OPTS + toks[:-1]
        self.workdir = cfg.get("workdir", "cc_worker")  # relative to $HOME
        self.night = None

    def sh(self, cmd, input=None, timeout=300, check=True):
        """Run a bash command on the host with cwd=$HOME."""
        if self.local:
            argv = ["bash", "-c", f"cd ~ && {cmd}"]
        elif self.jump:
            j = self.jump
            argv = ["ssh", *j.ssh_args, j.target,
                    f"{self.inner_ssh} {self.target} {shlex.quote(f'cd ~ && {cmd}')}"]
        else:
            argv = ["ssh", *self.ssh_args, self.target, f"cd ~ && {cmd}"]
        r = subprocess.run(argv, input=input, capture_output=True, text=True, timeout=timeout)
        if check and r.returncode != 0:
            raise RuntimeError(f"[{self.name}] `{cmd[:80]}` exit {r.returncode}: {r.stderr.strip()[-500:]}")
        return r.stdout

    def close_master(self):
        """Drop the shared ControlMaster connection, so the next command opens a
        fresh TCP connection (e.g. to pick up a congestion-control change)."""
        if not self.local and not self.jump:
            subprocess.run(["ssh", *self.ssh_args, "-O", "exit", self.target],
                           capture_output=True, timeout=30)

    def rsync_spec(self, path):
        """rsync path for a file under $HOME on the host."""
        if self.local:
            return os.path.join(os.path.expanduser("~"), path)
        return f"{self.target}:{path}"

    def rsync(self, src, dst, extra=(), timeout=3600):
        argv = ["rsync", "-a", "--partial", *extra]
        if self.jump:
            argv += ["-e", "ssh " + " ".join(shlex.quote(a) for a in self.jump.ssh_args)
                     + f" {self.jump.target} {self.inner_ssh}"]
        elif not self.local:
            argv += ["-e", "ssh " + " ".join(shlex.quote(a) for a in self.ssh_args)]
        argv += [src, dst]
        r = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
        if r.returncode != 0:
            raise RuntimeError(f"[{self.name}] rsync {src} -> {dst} failed: {r.stderr.strip()[-500:]}")


# --------------------------------------------------------------------------- database

def db():
    con = sqlite3.connect(DB_PATH, timeout=120, isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL")
    return con


SCHEMA = """
CREATE TABLE IF NOT EXISTS crawls (
    crawl TEXT PRIMARY KEY,          -- CC-MAIN-2025-05
    n_paths INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS chunks (
    crawl TEXT NOT NULL,
    idx INTEGER NOT NULL,            -- chunk number within the crawl
    start INTEGER NOT NULL,          -- slice [start, stop) of wet.paths, or -1 for a retry chunk
    stop INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',  -- pending | running | finished | fetched | done | error
    host TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    started REAL,
    finished REAL,
    stats TEXT,
    PRIMARY KEY (crawl, idx)
);
CREATE TABLE IF NOT EXISTS retry_paths (   -- WET paths of retry chunks (start = -1)
    crawl TEXT NOT NULL, idx INTEGER NOT NULL, path TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS failed_paths (
    crawl TEXT NOT NULL, path TEXT NOT NULL, requeued INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (crawl, path)
);
CREATE TABLE IF NOT EXISTS progress_log (  -- per-host WET files done (finished + in flight)
    host TEXT NOT NULL, ts REAL NOT NULL, files INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS progress_log_host_ts ON progress_log (host, ts);
CREATE TABLE IF NOT EXISTS crawl_sizes (    -- average WET file size per crawl (sampled), for a data-volume ETA
    crawl TEXT PRIMARY KEY, mb_per_file REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS host_pause (     -- circuit breaker: host takes no new chunks until `until`
    host TEXT PRIMARY KEY, until REAL NOT NULL, reason TEXT
);
CREATE TABLE IF NOT EXISTS heal_log (       -- automatic / manual VM restarts and redeploys
    host TEXT NOT NULL, ts REAL NOT NULL, action TEXT NOT NULL, reason TEXT, ok INTEGER
);
CREATE TABLE IF NOT EXISTS hosts (
    name TEXT PRIMARY KEY, last_seen REAL, last_error TEXT
);
"""


def crawl_tag(crawl):
    """CC-MAIN-2025-05 -> 2025_05 (matches the old 1_download_${CC_ID} naming)."""
    return crawl.replace("CC-MAIN-", "").replace("-", "_")


def chunk_name(idx):
    return f"chunk_{idx:05d}"


def job_id(crawl, idx):
    return f"{crawl_tag(crawl)}__{chunk_name(idx)}"


def parse_job_id(jid):
    tag, name = jid.split("__")
    return "CC-MAIN-" + tag.replace("_", "-"), int(name.split("_")[1])


_paths_cache = {}
_paths_lock = threading.Lock()


def wet_paths(crawl):
    with _paths_lock:
        if crawl not in _paths_cache:
            with gzip.open(os.path.join(WET_PATHS_DIR, f"{crawl}.wet.paths.gz"), "rt") as f:
                _paths_cache[crawl] = [l.strip() for l in f if l.strip()]
        return _paths_cache[crawl]


def chunk_paths(con, crawl, idx, start, stop):
    if start >= 0:
        return wet_paths(crawl)[start:stop]
    return [r[0] for r in con.execute(
        "SELECT path FROM retry_paths WHERE crawl=? AND idx=? ORDER BY rowid", (crawl, idx))]


# --------------------------------------------------------------------------- commands

def cmd_init(args):
    cfg = load_config()
    os.makedirs(WET_PATHS_DIR, exist_ok=True)
    con = db()
    con.executescript(SCHEMA)
    with urllib.request.urlopen("https://index.commoncrawl.org/collinfo.json", timeout=60) as r:
        crawls = sorted(c["id"] for c in json.load(r)
                        if c["id"].split("-")[2] in cfg["years"])
    if args.crawls:
        crawls = [c for c in crawls if c in args.crawls.split(",")]
    chunk_size = cfg.get("chunk_size", 500)
    for crawl in crawls:
        dest = os.path.join(WET_PATHS_DIR, f"{crawl}.wet.paths.gz")
        if not os.path.exists(dest):
            url = f"https://data.commoncrawl.org/crawl-data/{crawl}/wet.paths.gz"
            urllib.request.urlretrieve(url, dest + ".tmp")
            os.replace(dest + ".tmp", dest)
        n = len(wet_paths(crawl))
        con.execute("INSERT OR IGNORE INTO crawls VALUES (?, ?)", (crawl, n))
        for i, start in enumerate(range(0, n, chunk_size)):
            con.execute("INSERT OR IGNORE INTO chunks (crawl, idx, start, stop) VALUES (?,?,?,?)",
                        (crawl, i, start, min(n, start + chunk_size)))
        print(f"{crawl}: {n} WET files, {-(-n // chunk_size)} chunks")
    total = con.execute("SELECT COUNT(*), SUM(stop-start) FROM chunks WHERE start>=0").fetchone()
    print(f"queue: {total[0]} chunks, {total[1]} WET files")
    sample_crawl_sizes(con)


def sample_crawl_sizes(con, n=12):
    """Average WET file size of every crawl not sampled yet, from HEAD requests on
    `n` random files. WET files range from ~60 MB (2026) to ~170 MB (2016), so a
    files/h ETA alone is misleading."""
    import random
    from concurrent.futures import ThreadPoolExecutor
    todo = [r[0] for r in con.execute("SELECT crawl FROM crawls WHERE crawl NOT IN "
                                      "(SELECT crawl FROM crawl_sizes)")]

    def size(path):
        for _ in range(3):
            try:
                req = urllib.request.Request(f"https://data.commoncrawl.org/{path}", method="HEAD")
                with urllib.request.urlopen(req, timeout=30) as r:
                    return int(r.headers["Content-Length"])
            except Exception:
                time.sleep(2)
        return None

    for crawl in todo:
        paths = random.sample(wet_paths(crawl), min(n, len(wet_paths(crawl))))
        with ThreadPoolExecutor(2) as ex:  # gently: a burst of HEADs got an IP blocked
            sizes = [x for x in ex.map(size, paths) if x]
        time.sleep(1)
        if sizes:
            mb = sum(sizes) / len(sizes) / 1e6
            con.execute("INSERT OR REPLACE INTO crawl_sizes VALUES (?,?)", (crawl, mb))
            print(f"{crawl}: ~{mb:.0f} MB per WET file")


# BBR congestion control: far-away senders (Azure US -> Japan) collapse with CUBIC
BBR_SETUP = (
    'printf "tcp_bbr\\n" | sudo -n tee /etc/modules-load.d/bbr.conf >/dev/null && '
    'printf "net.core.default_qdisc=fq\\nnet.ipv4.tcp_congestion_control=bbr\\n'
    'net.core.wmem_max=67108864\\nnet.ipv4.tcp_wmem=4096 65536 67108864\\n" '
    '| sudo -n tee /etc/sysctl.d/90-bbr.conf >/dev/null && '
    'sudo -n modprobe tcp_bbr && sudo -n sysctl -q --system && '
    'sysctl -n net.ipv4.tcp_congestion_control')


def setup_host(h, code_only=False):
    """Push the worker bundle to a host and (unless code_only) build its venv.
    Returns a one-line summary; raises on failure."""
    stage = os.path.join(STATE, "worker_bundle", h.name)
    os.makedirs(os.path.join(stage, "badwords"), exist_ok=True)
    for src, dst in WORKER_FILES:
        subprocess.run(["cp", os.path.join(REPO, src), os.path.join(stage, dst)], check=True)
    h.sh(f"mkdir -p {h.workdir}/jobs")
    h.rsync(stage + "/", h.rsync_spec(h.workdir + "/"))
    if code_only:
        return "code updated"
    py = h.cfg.get("python", "3.11")
    out = h.sh(
        "export PATH=$HOME/.local/bin:$PATH; "
        "command -v uv >/dev/null || curl -LsSf https://astral.sh/uv/install.sh | sh; "
        f"cd {h.workdir} && (test -x venv/bin/python || uv venv -q --python {py} venv) && "
        "VIRTUAL_ENV=$PWD/venv uv pip install -q requests pyahocorasick tqdm && "
        "venv/bin/python -c 'import ahocorasick, requests, download_web_docs; print(\"ok\")' && nproc",
        timeout=900)
    msg = f"ready, nproc={out.split()[-1]}"
    if h.cfg.get("bbr"):
        msg += ", tcp=" + h.sh(BBR_SETUP).strip()
        # the multiplexed ssh connection predates BBR and would stay on CUBIC
        h.close_master()
    return msg


def cmd_setup(args):
    cfg = load_config()
    hosts = [Host(h, cfg["workers"]) for h in cfg["workers"] if not args.host or h["name"] in args.host]

    def one(h):
        try:
            print(f"[{h.name}] {setup_host(h, args.code_only)}")
        except Exception as e:
            print(f"[{h.name}] SETUP FAILED: {e}")

    threads = [threading.Thread(target=one, args=(h,)) for h in hosts]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


# Publishes the current per-job download cap (read live by running jobs), then prints
# one line per job dir on the host: <job_id> <ok|running|dead> <numa node|-> <progress>
REMOTE_STATUS = r"""
[ -x {wd}/venv/bin/python ] && [ -f {wd}/wet_worker.py ] || echo NEEDS_SETUP
echo {mbps} > {wd}/job_mbps 2>/dev/null
cd {wd}/jobs 2>/dev/null || exit 0
for d in */; do
  d=${{d%/}}; [ "$d" = "*" ] && continue
  if ls "$d"/*.ok >/dev/null 2>&1; then st=ok
  elif [ -f "$d/pid" ] && kill -0 "$(cat "$d/pid")" 2>/dev/null; then st=running
  else st=dead; fi
  echo "$d $st $(cat "$d/numa" 2>/dev/null || echo -) $(cat "$d/progress" 2>/dev/null | tr ' ' /)"
done
"""


def launch(h, con, crawl, idx, start, stop, procs, numa=None):
    """Start a chunk job detached on the host. With `numa`, the job is pinned to
    that NUMA node's CPUs (memory then stays node-local via first-touch)."""
    jid = job_id(crawl, idx)
    jdir = f"{h.workdir}/jobs/{jid}"
    paths = chunk_paths(con, crawl, idx, start, stop)
    h.sh(f"mkdir -p {jdir} && cat > {jdir}/paths.txt", input="\n".join(paths) + "\n")
    mbps = job_mbps(h)
    pin = f"nice -n {h.cfg['nice']} " if h.cfg.get("nice") else ""
    if numa is not None:
        h.sh(f"echo {numa} > {jdir}/numa")
        pin += f"taskset -c $(cat /sys/devices/system/node/node{numa}/cpulist) "
    # setsid/nohup/taskset all exec, so $! is the python PID and its process group
    h.sh(f"cd {jdir} && (setsid nohup {pin}../../venv/bin/python ../../wet_worker.py --chunk-dir . "
         f"--name {chunk_name(idx)} --procs {procs} --max-mbps {mbps} --rate-file ../../job_mbps >> worker.log 2>&1 < /dev/null "
         f"& echo $! > pid)")


BREAKER_FAILED_FRACTION = 0.2
BREAKER_PAUSE_SECS = 2 * 3600


def requeue_paths(con, crawl, paths, size):
    """Put specific WET paths back in the queue as retry chunks (idx >= 90000)."""
    nxt = max(90000, con.execute("SELECT MAX(idx)+1 FROM chunks WHERE crawl=?", (crawl,)).fetchone()[0])
    for k in range(0, len(paths), size):
        idx = nxt + k // size
        con.execute("INSERT INTO chunks (crawl, idx, start, stop) VALUES (?,?,-1,-1)", (crawl, idx))
        con.executemany("INSERT INTO retry_paths VALUES (?,?,?)",
                        [(crawl, idx, p) for p in paths[k:k + size]])
    con.executemany("UPDATE failed_paths SET requeued=1 WHERE crawl=? AND path=?",
                    [(crawl, p) for p in paths])


def record_failures(h, con, crawl, idx, stats):
    """Record a finished chunk's failed WET files. If many failed, the host itself is
    probably broken (e.g. its IP got blocked by CloudFront: every request -> 403):
    pause it so it stops burning through the queue, and requeue the files now."""
    for p in stats["failed"]:
        con.execute("INSERT OR IGNORE INTO failed_paths (crawl, path) VALUES (?,?)", (crawl, p))
    n, bad = stats["n_paths"], stats["n_failed"]
    if bad >= 10 and bad > BREAKER_FAILED_FRACTION * n:
        reason = f"{bad}/{n} files failed in {job_id(crawl, idx)}"
        con.execute("INSERT OR REPLACE INTO host_pause VALUES (?,?,?)",
                    (h.name, time.time() + BREAKER_PAUSE_SECS, reason))
        requeue_paths(con, crawl, stats["failed"], load_config().get("chunk_size", 500))
        log.error("[%s] CIRCUIT BREAKER: %s -> paused for %d h, failed files requeued",
                  h.name, reason, BREAKER_PAUSE_SECS // 3600)


def host_paused(con, name):
    row = con.execute("SELECT until, reason FROM host_pause WHERE host=?", (name,)).fetchone()
    return row if row and row[0] > time.time() else None


def fetch(h, con, crawl, idx):
    """Pull a finished chunk to staging, record stats, delete it on the worker."""
    jid, name = job_id(crawl, idx), chunk_name(idx)
    if h.cfg.get("output_dir"):  # worker shares the HPC filesystem: move in place
        return finish_in_place(h, con, crawl, idx)
    # Pull into a private dir first so the uploader never sees half a chunk.
    tmp = os.path.join(STATE, "incoming", jid)
    os.makedirs(tmp, exist_ok=True)
    src = f"{h.workdir}/jobs/{jid}"
    h.rsync(h.rsync_spec(f"{src}/{name}.jsonl.gz"), tmp + "/")
    h.rsync(h.rsync_spec(f"{src}/{name}.ok"), tmp + "/")
    subprocess.run(["gzip", "-t", os.path.join(tmp, name + ".jsonl.gz")], check=True)
    with open(os.path.join(tmp, name + ".ok")) as f:
        stats = json.load(f)
    dest = os.path.join(STAGING, f"1_download_{crawl_tag(crawl)}")
    os.makedirs(dest, exist_ok=True)
    os.replace(os.path.join(tmp, name + ".jsonl.gz"), os.path.join(dest, name + ".jsonl.gz"))
    os.replace(os.path.join(tmp, name + ".ok"), os.path.join(dest, name + ".ok"))
    os.rmdir(tmp)
    record_failures(h, con, crawl, idx, stats)
    con.execute("UPDATE chunks SET status='fetched', finished=?, stats=? WHERE crawl=? AND idx=?",
                (time.time(), json.dumps(stats), crawl, idx))
    h.sh(f"rm -rf {src}")
    log.info("[%s] fetched %s: %d docs, %d failed", h.name, jid, stats["docs"], stats["n_failed"])


def finish_in_place(h, con, crawl, idx):
    jid, name = job_id(crawl, idx), chunk_name(idx)
    src = f"{h.workdir}/jobs/{jid}"
    dest = f"{h.cfg['output_dir']}/1_download_{crawl_tag(crawl)}"
    stats = json.loads(h.sh(f"gzip -t {src}/{name}.jsonl.gz && cat {src}/{name}.ok"))
    h.sh(f"mkdir -p {dest} && mv {src}/{name}.jsonl.gz {src}/{name}.ok {dest}/ && rm -rf {src}")
    record_failures(h, con, crawl, idx, stats)
    con.execute("UPDATE chunks SET status='done', finished=?, stats=? WHERE crawl=? AND idx=?",
                (time.time(), json.dumps(stats), crawl, idx))
    log.info("[%s] finished %s in place: %d docs, %d failed", h.name, jid, stats["docs"], stats["n_failed"])


def in_window(window):
    """window = ["22:00", "07:00"] (controller's local time; may wrap midnight)."""
    if not window:
        return True
    now = time.strftime("%H:%M")
    start, end = window
    return start <= now < end if start <= end else (now >= start or now < end)


def job_mbps(h):
    """Current cap per chunk job: the host cap (night_max_mbps inside the global
    "night" window, else max_mbps; 0 = unlimited) split over the host's slots."""
    cap = h.cfg.get("max_mbps", 0)
    if "night_max_mbps" in h.cfg and h.night and in_window(h.night):
        cap = h.cfg["night_max_mbps"]
    return cap / max(1, h.cfg.get("slots", 2))


def log_progress(h, con, mine, remote, progress):
    """Record files done on this host (completed chunks + in-flight progress), so
    `status` can show an accurate short-window rate despite 500-file chunks."""
    # successfully processed files only: a host whose requests all fail must not look fast
    completed = con.execute(
        "SELECT COALESCE(SUM(CASE WHEN stats IS NOT NULL THEN json_extract(stats, '$.n_paths') "
        "- json_extract(stats, '$.n_failed') ELSE stop - start END), 0) FROM chunks "
        "WHERE host=? AND status IN ('finished', 'fetched', 'done')", (h.name,)).fetchone()[0]
    inflight = sum(progress.get(jid, (0, 0))[1] for jid, st in remote.items()
                   if st != "ok" and jid in mine and mine[jid][5] == "running")
    now = time.time()
    con.execute("INSERT INTO progress_log VALUES (?,?,?)", (h.name, now, completed + inflight))
    con.execute("DELETE FROM progress_log WHERE host=? AND ts<?", (h.name, now - 3 * 86400))


def recent_rate(con, host, secs):
    """Files/s on `host` over the last `secs`, from progress_log (None if too little data)."""
    first = con.execute("SELECT ts, files FROM progress_log WHERE host=? AND ts>=? "
                        "ORDER BY ts LIMIT 1", (host, time.time() - secs)).fetchone()
    last = con.execute("SELECT ts, files FROM progress_log WHERE host=? "
                       "ORDER BY ts DESC LIMIT 1", (host,)).fetchone()
    if not first or not last or last[0] - first[0] < 300:
        return None
    return max(0, last[1] - first[1]) / (last[0] - first[0])


_progress_seen = {}  # (host, job id) -> (progress value, time it last changed)


def kill_if_stuck(h, jid, value):
    """Backstop for a hung job (e.g. its main process deadlocked): kill it once its
    progress stops moving; the next cycle sees it dead and relaunches it, resuming
    from the finished part files."""
    key, now = (h.name, jid), time.time()
    prev = _progress_seen.get(key)
    if prev is None or prev[0] != value:
        _progress_seen[key] = (value, now)
        return
    if now - prev[1] > STUCK_SECS:
        log.warning("[%s] %s stuck at %s files for %d min, killing it to relaunch",
                    h.name, jid, value, (now - prev[1]) // 60)
        h.sh(f"cd {h.workdir}/jobs/{jid} && kill -- -$(cat pid)", check=False)
        _progress_seen.pop(key, None)


def host_cycle(h, con):
    """One scheduling round for one host."""
    out = h.sh(REMOTE_STATUS.format(wd=h.workdir, mbps=job_mbps(h)), timeout=120)
    if "NEEDS_SETUP" in out.split("\n", 1)[0]:
        # New host added to hosts.json without `setup`: bootstrap it before
        # launching anything, instead of burning chunks on jobs that die instantly.
        log.info("[%s] no worker venv, running setup", h.name)
        log.info("[%s] setup: %s", h.name, setup_host(h))
    con.execute("INSERT INTO hosts (name, last_seen) VALUES (?,?) ON CONFLICT(name) "
                "DO UPDATE SET last_seen=excluded.last_seen, last_error=NULL", (h.name, time.time()))
    remote, node_of, progress = {}, {}, {}
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 3 and "__" in parts[0]:
            remote[parts[0]] = parts[1]
            node_of[parts[0]] = None if parts[2] == "-" else int(parts[2])
            if len(parts) >= 4 and "/" in parts[3]:
                p = parts[3].split("/")
                progress[parts[0]] = (int(p[0]), int(p[2]) if len(p) > 2 else int(p[0]))

    # 'finished' = done on the worker, waiting for the fetch thread (no longer uses a slot)
    mine = {job_id(c, i): (c, i, s, e, a, st) for c, i, s, e, a, st in con.execute(
        "SELECT crawl, idx, start, stop, attempts, status FROM chunks "
        "WHERE status IN ('running', 'finished') AND host=?", (h.name,))}
    # "procs" is per host; each of its `slots` concurrent chunk jobs gets an equal share
    procs = max(1, h.cfg.get("procs", 8) // h.cfg.get("slots", 2))

    for jid, st in remote.items():
        if jid not in mine:  # reassigned elsewhere while this host was away -> discard
            log.info("[%s] removing stale job %s", h.name, jid)
            h.sh(f"cd {h.workdir}/jobs/{jid} && (kill -- -$(cat pid) 2>/dev/null; true) && "
                 f"cd .. && rm -rf {jid}")
            continue
        crawl, idx, start, stop, attempts, db_status = mine[jid]
        if db_status == "finished":
            continue
        if st == "running":
            kill_if_stuck(h, jid, progress.get(jid, (None,))[0])
        if st == "ok":
            if h.cfg.get("output_dir"):  # on the HPC filesystem: quick local mv
                fetch(h, con, crawl, idx)
            else:
                con.execute("UPDATE chunks SET status='finished' WHERE crawl=? AND idx=?", (crawl, idx))
        elif st == "dead" and (not h.cfg.get("enabled", True) or host_paused(con, h.name)):
            # don't restart work on a disabled / circuit-broken host: hand it to the others
            log.warning("[%s] %s not running and host is disabled/paused: requeueing", h.name, jid)
            con.execute("UPDATE chunks SET status='pending', host=NULL WHERE crawl=? AND idx=?",
                        (crawl, idx))
            h.sh(f"rm -rf {h.workdir}/jobs/{jid}", check=False)
        elif st == "dead":
            if attempts >= MAX_ATTEMPTS:
                log.error("[%s] %s died %d times, marking error (see worker.log)", h.name, jid, attempts)
                con.execute("UPDATE chunks SET status='error' WHERE crawl=? AND idx=?", (crawl, idx))
            else:
                log.warning("[%s] %s not running, relaunching (resumes from parts)", h.name, jid)
                con.execute("UPDATE chunks SET attempts=attempts+1 WHERE crawl=? AND idx=?", (crawl, idx))
                launch(h, con, crawl, idx, start, stop, procs, node_of[jid])

    for jid, (crawl, idx, *_rest) in mine.items():
        if jid not in remote:  # job dir vanished (disk wiped?) -> back to queue
            log.warning("[%s] %s missing on host, requeueing", h.name, jid)
            con.execute("UPDATE chunks SET status='pending', host=NULL WHERE crawl=? AND idx=?",
                        (crawl, idx))

    log_progress(h, con, mine, remote, progress)

    if (not h.cfg.get("enabled", True) or not in_window(h.cfg.get("window"))
            or host_paused(con, h.name)):
        return  # running chunks finish, no new ones
    running = con.execute("SELECT COUNT(*) FROM chunks WHERE status='running' AND host=?",
                          (h.name,)).fetchone()[0]
    numa_nodes = h.cfg.get("numa_nodes", 0)
    busy = [node_of[j] for j, st in remote.items() if j in mine and st == "running"]
    for _ in range(h.cfg.get("slots", 2) - running):
        con.execute("BEGIN IMMEDIATE")
        row = con.execute("SELECT crawl, idx, start, stop FROM chunks WHERE status='pending' "
                          f"ORDER BY {claim_order()} LIMIT 1").fetchone()
        if not row:
            con.execute("COMMIT")
            return
        con.execute("UPDATE chunks SET status='running', host=?, started=?, attempts=attempts+1 "
                    "WHERE crawl=? AND idx=?", (h.name, time.time(), row[0], row[1]))
        con.execute("COMMIT")
        try:
            numa = None
            if numa_nodes:  # least-loaded node
                numa = min(range(numa_nodes), key=busy.count)
                busy.append(numa)
            launch(h, con, *row, procs, numa)
        except Exception:
            con.execute("UPDATE chunks SET status='pending', host=NULL WHERE crawl=? AND idx=?",
                        (row[0], row[1]))
            raise
        log.info("[%s] launched %s", h.name, job_id(row[0], row[1]))


def duplicate_of(cfg, workers):
    """Name of an earlier hosts.json entry that points at the same machine, if any."""
    def key(c):
        return (c.get("via"), " ".join((c.get("ssh") or "local").split()))
    for other in workers:
        if other["name"] == cfg["name"]:
            return None
        if key(other) == key(cfg):
            return other["name"]
    return None


def host_loop(name, stop_evt):
    con = db()
    warned_dup = None
    while not stop_evt.is_set():
        conf = load_config()
        workers = conf["workers"]
        cfg = next((c for c in workers if c["name"] == name), None)
        if cfg is None:
            return
        h = Host(cfg, workers)
        h.night = conf.get("night")
        dup = duplicate_of(cfg, workers)
        if dup:
            # Two entries for one machine would each kill the other's jobs as "stale".
            if dup != warned_dup:
                log.error("[%s] same machine as '%s' in hosts.json (copy-paste?), skipping it",
                          name, dup)
            warned_dup = dup
            stop_evt.wait(POLL_SECS)
            continue
        warned_dup = None
        try:
            host_cycle(h, con)
        except Exception as e:
            log.error("[%s] %s", name, e)
            con.execute("INSERT INTO hosts (name, last_error) VALUES (?,?) ON CONFLICT(name) "
                        "DO UPDATE SET last_error=excluded.last_error", (name, str(e)[:500]))
            last = con.execute("SELECT last_seen FROM hosts WHERE name=?", (name,)).fetchone()
            if last and last[0] and time.time() - last[0] > LEASE_HOURS * 3600:
                n = con.execute("UPDATE chunks SET status='pending', host=NULL "
                                "WHERE status IN ('running', 'finished') AND host=?", (name,)).rowcount
                if n:
                    log.warning("[%s] unreachable > %dh, released %d chunks", name, LEASE_HOURS, n)
        stop_evt.wait(POLL_SECS)


def fetch_loop(name, stop_evt):
    """Transfers a host's finished chunks, separately from scheduling so a slow
    link (Azure US -> Japan) never keeps a slot idle. Up to `fetch_parallel`
    chunks are transferred at once."""
    from concurrent.futures import ThreadPoolExecutor
    in_progress = set()
    lock = threading.Lock()

    def one(h, crawl, idx):
        con = db()
        try:
            fetch(h, con, crawl, idx)
        except Exception as e:
            log.error("[%s] fetch %s failed (will retry): %s", h.name, job_id(crawl, idx), e)
        finally:
            with lock:
                in_progress.discard((crawl, idx))

    con = db()
    pool = None
    while not stop_evt.is_set():
        conf = load_config()
        cfg = next((c for c in conf["workers"] if c["name"] == name), None)
        if cfg is None:
            return
        if pool is None:
            pool = ThreadPoolExecutor(cfg.get("fetch_parallel", 3))
        h = Host(cfg, conf["workers"])
        for crawl, idx in con.execute("SELECT crawl, idx FROM chunks WHERE status='finished' "
                                      "AND host=? ORDER BY crawl, idx", (name,)).fetchall():
            with lock:
                if (crawl, idx) in in_progress or len(in_progress) >= cfg.get("fetch_parallel", 3):
                    continue
                in_progress.add((crawl, idx))
            pool.submit(one, h, crawl, idx)
        stop_evt.wait(15)


_marked_complete = set()


def upload_once(con, hpc):
    """Batch-rsync staging -> HPC, then mark the uploaded chunks done."""
    fetched = con.execute("SELECT crawl, idx FROM chunks WHERE status='fetched'").fetchall()
    root = hpc.cfg["root"]
    if fetched:
        hpc.sh(f"mkdir -p {shlex.quote(root)}")
        hpc.rsync(STAGING + "/", hpc.rsync_spec(root + "/"), extra=["--remove-source-files"],
                  timeout=6 * 3600)
        for crawl, idx in fetched:
            f = os.path.join(STAGING, f"1_download_{crawl_tag(crawl)}", chunk_name(idx) + ".jsonl.gz")
            if not os.path.exists(f):
                con.execute("UPDATE chunks SET status='done' WHERE crawl=? AND idx=? AND status='fetched'",
                            (crawl, idx))
        log.info("[hpc] uploaded %d chunks", len(fetched))
    # Mark crawls whose every chunk is done (also those finished in place on the HPC)
    for (crawl,) in con.execute(
            "SELECT crawl FROM chunks GROUP BY crawl HAVING SUM(status!='done')=0").fetchall():
        if crawl in _marked_complete:
            continue
        _marked_complete.add(crawl)
        d = f"{root}/1_download_{crawl_tag(crawl)}"
        failed = [r[0] for r in con.execute(
            "SELECT path FROM failed_paths WHERE crawl=? AND requeued=0", (crawl,))]
        hpc.sh(f"test -e {d}/_COMPLETE || cat > {d}/_COMPLETE",
               input=json.dumps({"crawl": crawl, "failed_wet_paths": failed}) + "\n")


def upload_loop(stop_evt):
    con = db()
    while not stop_evt.wait(UPLOAD_EVERY_SECS):
        try:
            upload_once(con, Host(dict(load_config()["hpc"], name="hpc")))
        except Exception as e:
            log.error("[hpc] upload failed (will retry): %s", e)


def release_removed_hosts(con, names):
    """A host deleted from hosts.json (e.g. a replaced VM) leaves its running and
    not-yet-fetched chunks behind with nobody to finish them: put them back in the queue."""
    marks = ",".join("?" * len(names))
    for host, n in con.execute(
            f"SELECT host, COUNT(*) FROM chunks WHERE status IN ('running', 'finished') "
            f"AND host IS NOT NULL AND host NOT IN ({marks}) GROUP BY host", names).fetchall():
        con.execute(f"UPDATE chunks SET status='pending', host=NULL WHERE status IN "
                    f"('running', 'finished') AND host=?", (host,))
        log.warning("[%s] no longer in hosts.json: requeued its %d unfinished chunks", host, n)


# ---------------------------------------------------------------------------- auto-heal (Azure)
# Azure VMs occasionally degrade (e.g. the accelerated-networking NIC fails during
# host maintenance and the VM limps on with dropped connections). A host with
# "auto_reboot": true plus "azure_vm"/"azure_rg" is healed when, for HEAL_BAD_SECS,
# its last-hour rate stays below HEAL_THRESHOLD x the median of the other auto_reboot
# hosts, or it is unreachable. First action: restart; if it needs healing again
# within a day: redeploy (moves the VM to different Azure hardware; IPs must be static).
HEAL_EVERY_SECS = 300
HEAL_BAD_SECS = 3600
HEAL_THRESHOLD = 0.5
HEAL_GRACE_SECS = 5400            # after an action, wait for fresh data before judging again
HEAL_MIN_GAP_SECS = 6 * 3600      # at most one action per host per 6 h
REDEPLOY_WITHIN_SECS = 24 * 3600  # restarted within this long and still bad -> redeploy


def heal_host(con, cfg, action, reason):
    """Restart or redeploy one Azure VM (falls back to `sudo reboot` over ssh)."""
    name = cfg["name"]
    log.warning("[%s] HEAL: %s (%s)", name, action, reason)
    ok = False
    if cfg.get("azure_vm") and cfg.get("azure_rg"):
        r = subprocess.run(["az", "vm", action, "-g", cfg["azure_rg"], "-n", cfg["azure_vm"],
                            "-o", "none"], capture_output=True, text=True, timeout=3600)
        ok = r.returncode == 0
        if not ok:
            log.error("[%s] az vm %s failed: %s", name, action, r.stderr.strip()[-300:])
    if not ok and action == "restart":
        h = Host(cfg, load_config()["workers"])
        h.sh("sudo -n systemctl reboot || sudo -n reboot", check=False, timeout=60)
        ok, action = True, "restart (ssh reboot)"
    con.execute("INSERT INTO heal_log VALUES (?,?,?,?,?)", (name, time.time(), action, reason, int(ok)))
    try:  # the multiplexed ssh connection died with the VM
        Host(cfg, load_config()["workers"]).close_master()
    except Exception:
        pass
    log.warning("[%s] HEAL: %s %s", name, action, "done" if ok else "FAILED")
    return ok


def heal_once(con, bad_since):
    workers = [w for w in load_config()["workers"] if w.get("auto_reboot") and w.get("enabled", True)]
    if len(workers) < 3:
        return  # need peers for a meaningful median
    now = time.time()
    rates = {w["name"]: recent_rate(con, w["name"], 3600) for w in workers}
    for w in workers:
        name = w["name"]
        last = con.execute("SELECT MAX(ts) FROM heal_log WHERE host=?", (name,)).fetchone()[0] or 0
        if now - last < HEAL_GRACE_SECS:
            bad_since.pop(name, None)
            continue
        peers = [r for n, r in rates.items() if n != name and r]
        if len(peers) < 2:
            continue
        median = statistics.median(peers)
        seen = con.execute("SELECT last_seen FROM hosts WHERE name=?", (name,)).fetchone()
        unreachable = not seen or not seen[0] or now - seen[0] > 1800
        rate = rates[name] or 0
        slow = rate < HEAL_THRESHOLD * median
        if not (slow or unreachable):
            bad_since.pop(name, None)
            continue
        start = bad_since.setdefault(name, now)
        if now - start < HEAL_BAD_SECS or now - last < HEAL_MIN_GAP_SECS:
            continue
        reason = ("unreachable for %d min" % ((now - seen[0]) // 60) if unreachable and seen and seen[0]
                  else "unreachable" if unreachable
                  else f"{rate * 3600:.0f} files/h vs peer median {median * 3600:.0f} for "
                       f"{(now - start) / 60:.0f} min")
        restarted = con.execute("SELECT COUNT(*) FROM heal_log WHERE host=? AND action LIKE 'restart%' "
                                "AND ts>?", (name, now - REDEPLOY_WITHIN_SECS)).fetchone()[0]
        heal_host(con, w, "redeploy" if restarted else "restart", reason)
        bad_since.pop(name, None)


def heal_loop(stop_evt):
    con = db()
    bad_since = {}
    while not stop_evt.wait(HEAL_EVERY_SECS):
        try:
            heal_once(con, bad_since)
        except Exception as e:
            log.error("[heal] %s", e)


def cmd_heal(args):
    """Manual heal: controller.py heal azure2 [--redeploy]"""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(),
                                  logging.FileHandler(os.path.join(STATE, "controller.log"))])
    con = db()
    con.executescript(SCHEMA)
    cfg = next(w for w in load_config()["workers"] if w["name"] == args.host)
    heal_host(con, cfg, "redeploy" if args.redeploy else "restart", "manual")


def cmd_run(args):
    os.makedirs(STAGING, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(),
                                  logging.FileHandler(os.path.join(STATE, "controller.log"))])
    db().executescript(SCHEMA)
    stop_evt = threading.Event()
    threads = {}
    up = threading.Thread(target=upload_loop, args=(stop_evt,), daemon=True)
    up.start()
    threading.Thread(target=heal_loop, args=(stop_evt,), daemon=True).start()
    try:
        while True:
            for cfg in load_config()["workers"]:  # pick up hosts added to hosts.json
                for kind, target in (("sched", host_loop), ("fetch", fetch_loop)):
                    t = threads.get((kind, cfg["name"]))
                    if t is None or not t.is_alive():
                        t = threading.Thread(target=target, args=(cfg["name"], stop_evt), daemon=True)
                        t.start()
                        threads[(kind, cfg["name"])] = t
            con = db()
            release_removed_hosts(con, [c["name"] for c in load_config()["workers"]])
            left = con.execute("SELECT COUNT(*) FROM chunks WHERE status NOT IN ('done','error')").fetchone()[0]
            if left == 0:
                log.info("all chunks done")
                break
            time.sleep(POLL_SECS)
    except KeyboardInterrupt:
        log.info("stopping controller (worker jobs keep running; restart to resume)")
    stop_evt.set()
    upload_once(db(), Host(dict(load_config()["hpc"], name="hpc")))


def cmd_status(args):
    con = db()
    con.executescript(SCHEMA)
    now = time.time()
    print("== crawls")
    for crawl, n, tot, done, run in con.execute(
            "SELECT crawl, COUNT(*), SUM(CASE WHEN start>=0 THEN stop-start ELSE 0 END), "
            "SUM(status='done'), SUM(status IN ('running','fetched')) FROM chunks "
            "GROUP BY crawl ORDER BY crawl"):
        bar = "#" * int(20 * done / n)
        print(f"  {crawl}  {bar:<20} {done:>4}/{n} chunks done, {run} in flight")
    first = con.execute("SELECT MIN(started) FROM chunks").fetchone()[0] or now
    win = max(600, min(6 * 3600, now - first))
    # average WET file size, to turn files/h into MB/s
    sized = [json.loads(s) for (s,) in con.execute(
        "SELECT stats FROM chunks WHERE stats IS NOT NULL ORDER BY finished DESC LIMIT 50")]
    mb_per_file = (sum(x["bytes_downloaded_this_run"] for x in sized) /
                   max(1, sum(x["n_paths"] for x in sized)) / 1e6) if sized else 105
    print(f"\n== hosts            {'last ' + format(win / 3600, '.1f') + 'h':>15} {'last 1h':>21}")
    total_1h = 0.0
    # only hosts currently in hosts.json, in its order (removed VMs drop out of the list)
    known = {r[0]: r[1:] for r in con.execute("SELECT name, last_seen, last_error FROM hosts")}
    for name in [w["name"] for w in load_config()["workers"]]:
        last_seen, err = known.get(name, (None, None))
        rows = con.execute("SELECT stats FROM chunks WHERE host=? AND finished>?",
                           (name, now - win)).fetchall()
        files_h = sum(json.loads(s)["n_paths"] for (s,) in rows) * 3600 / win
        r1 = recent_rate(con, name, 3600)
        total_1h += r1 or 0
        running, waiting = con.execute(
            "SELECT SUM(status='running'), SUM(status='finished') FROM chunks WHERE host=?",
            (name,)).fetchone()
        seen = f"{(now - last_seen) / 60:.0f}m ago" if last_seen else "never"
        one_h = "    (collecting)     " if r1 is None else \
            f"{r1 * 3600:>6.0f} files/h {r1 * mb_per_file:>5.1f} MB/s"
        flag = ""
        if r1 is not None and files_h > 0 and r1 * 3600 < 0.5 * files_h:
            flag = "  <-- SLOWER than usual"
        if last_seen and now - last_seen > 10 * 60:
            flag += "  <-- not seen for a while"
        pause = host_paused(con, name)
        if pause:
            flag += f"  PAUSED until {time.strftime('%H:%M', time.localtime(pause[0]))} ({pause[1]})"
        elif not next((w for w in load_config()["workers"] if w["name"] == name), {}).get("enabled", True):
            flag += "  (disabled)"
        print(f"  {name:<8} {files_h:>6.0f} files/h  | {one_h} | running={running or 0} "
              f"to_fetch={waiting or 0} seen={seen}{flag}" + (f"  ERR: {err[:80]}" if err else ""))
    c = dict(con.execute("SELECT status, COUNT(*) FROM chunks GROUP BY status").fetchall())
    print(f"\n== chunks: {c}")
    left_files = con.execute("SELECT SUM(stop-start) FROM chunks WHERE status IN ('pending','running') "
                             "AND start>=0").fetchone()[0] or 0
    recent = con.execute("SELECT stats FROM chunks WHERE finished>?", (now - win,)).fetchall()
    rate = sum(json.loads(s)["n_paths"] for (s,) in recent) / win
    if rate:
        print(f"   {left_files} WET files left")
        print(f"   ETA from last {win / 3600:.1f}h: {rate * 3600:>6.0f} files/h -> "
              f"{left_files / rate / 86400:.1f} days")
    if total_1h:
        print(f"   ETA from last 1h  : {total_1h * 3600:>6.0f} files/h -> "
              f"{left_files / total_1h / 86400:.1f} days  (assumes remaining files cost what current ones do)")
    # Data-volume ETA: remaining TB (sampled file size per crawl) / current MB/s
    left_mb = con.execute(
        "SELECT SUM((c.stop - c.start) * COALESCE(z.mb_per_file, ?)) FROM chunks c "
        "LEFT JOIN crawl_sizes z ON z.crawl = c.crawl "
        "WHERE c.status IN ('pending', 'running') AND c.start >= 0", (mb_per_file,)).fetchone()[0] or 0
    if total_1h and left_mb:
        mbps = total_1h * mb_per_file
        print(f"   ETA by data volume: {left_mb / 1e6:.0f} TB left at {mbps:.0f} MB/s -> "
              f"{left_mb / mbps / 86400:.1f} days")
    heals = con.execute("SELECT host, ts, action, reason, ok FROM heal_log WHERE ts>? ORDER BY ts",
                        (now - 86400,)).fetchall()
    if heals:
        print("\n== VM heals (last 24h)")
        for host, ts, action, reason, ok in heals:
            print(f"   {time.strftime('%m-%d %H:%M', time.localtime(ts))}  {host:<8} {action:<10} "
                  f"{'' if ok else 'FAILED '}{reason}")
    nf = con.execute("SELECT COUNT(*) FROM failed_paths WHERE requeued=0").fetchone()[0]
    if nf:
        print(f"   {nf} failed WET files (run `requeue-failed` once the queue is done)")


def cmd_requeue_failed(args):
    con = db()
    cfg = load_config()
    size = cfg.get("chunk_size", 500)
    for (crawl,) in con.execute("SELECT DISTINCT crawl FROM failed_paths WHERE requeued=0").fetchall():
        paths = [r[0] for r in con.execute(
            "SELECT path FROM failed_paths WHERE crawl=? AND requeued=0", (crawl,))]
        requeue_paths(con, crawl, paths, size)
        print(f"{crawl}: requeued {len(paths)} WET files")


def cmd_audit(args):
    """Check that every WET file of every crawl is accounted for, and that the HPC
    holds exactly the chunks the database says are done."""
    con = db()
    con.executescript(SCHEMA)
    bad = 0
    print("crawl            WET files  chunks done  failed  recovered  missing")
    expected = {}
    for crawl, n in con.execute("SELECT crawl, n_paths FROM crawls ORDER BY crawl").fetchall():
        rows = con.execute("SELECT idx, start, stop, status FROM chunks WHERE crawl=? AND start>=0 "
                           "ORDER BY start", (crawl,)).fetchall()
        issues, pos = [], 0
        for idx, a, b, st in rows:  # chunks must tile [0, n) exactly
            if a != pos:
                issues.append(f"gap/overlap at file {pos}..{a} (chunk {idx})")
            pos = b
        if pos != n:
            issues.append(f"chunks end at file {pos}, wet.paths has {n}")
        err = sum(1 for r in rows if r[3] == "error")
        if err:
            issues.append(f"{err} chunks in error (reset them to pending to retry)")
        failed = {r[0] for r in con.execute("SELECT path FROM failed_paths WHERE crawl=?", (crawl,))}
        recovered = {r[0] for r in con.execute(
            "SELECT r.path FROM retry_paths r JOIN chunks k ON k.crawl=r.crawl AND k.idx=r.idx "
            "WHERE r.crawl=? AND k.status='done'", (crawl,))}
        for (st,) in con.execute("SELECT stats FROM chunks WHERE crawl=? AND start<0 AND status='done'",
                                 (crawl,)):
            recovered -= set(json.loads(st)["failed"])
        missing = len(failed - recovered)
        done = con.execute("SELECT COUNT(*) FROM chunks WHERE crawl=? AND status='done'", (crawl,)).fetchone()[0]
        total = con.execute("SELECT COUNT(*) FROM chunks WHERE crawl=?", (crawl,)).fetchone()[0]
        expected[f"1_download_{crawl_tag(crawl)}"] = done
        if done == 0 and not failed and not issues:
            continue
        state = "" if done == total else "  (in progress)"
        print(f"{crawl}  {n:>9}  {done:>5}/{total:<5} {len(failed):>6}  {len(failed & recovered):>9}  "
              f"{missing:>7}{state}")
        for i in issues:
            print(f"   !! {i}")
        bad += len(issues) + (missing if done == total else 0)
    # HPC: every done chunk must be there with its .ok file
    hpc = Host(dict(load_config()["hpc"], name="hpc"))
    root = hpc.cfg["root"]
    listing = hpc.sh(f"cd {shlex.quote(root)} && for d in 1_download_*; do "
                     f"echo \"$d $(ls $d | grep -c '^chunk_.*\\.jsonl\\.gz$') $(ls $d | grep -c '^chunk_.*\\.ok$')\"; done",
                     timeout=600)
    have = {d: (int(g), int(o)) for d, g, o in (l.split() for l in listing.splitlines() if l.strip())}
    print("\nHPC check:")
    mismatch = 0
    for d, n in sorted(expected.items()):
        g, o = have.get(d, (0, 0))
        if n and (g < n or o < n):
            print(f"   !! {d}: database has {n} done chunks, HPC has {g} .jsonl.gz / {o} .ok")
            mismatch += 1
    print("   all crawl folders match the database" if not mismatch else f"   {mismatch} folders mismatch")
    bad += mismatch
    print("\nAUDIT OK" if not bad else f"\nAUDIT: {bad} problems (see !! lines)")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("init")
    p.add_argument("--crawls", help="comma-separated subset, e.g. CC-MAIN-2025-05")
    p = sub.add_parser("setup")
    p.add_argument("--host", nargs="*")
    p.add_argument("--code-only", action="store_true", help="only push code, skip venv")
    sub.add_parser("run")
    sub.add_parser("status")
    sub.add_parser("requeue-failed")
    sub.add_parser("audit", help="check every WET file is accounted for and on the HPC")
    p = sub.add_parser("heal", help="restart (or --redeploy) an Azure VM now")
    p.add_argument("host")
    p.add_argument("--redeploy", action="store_true")
    args = ap.parse_args()
    os.makedirs(STATE, exist_ok=True)
    {"init": cmd_init, "setup": cmd_setup, "run": cmd_run, "status": cmd_status,
     "requeue-failed": cmd_requeue_failed, "heal": cmd_heal, "audit": cmd_audit}[args.cmd](args)


if __name__ == "__main__":
    main()
