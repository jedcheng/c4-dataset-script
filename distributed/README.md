# Distributed download + filter

Spreads the step-1 download/filter of Common Crawl WET files over several
machines and collects the results on the HPC. Runs from the office PC.

```
office PC: controller.py  ──ssh──>  azure1, azure2, lab, office (itself), genkai login node (22–07h)
  (state/state.db)        ──ssh──>  lab ──ssh──> mpi1..3   ("via": "lab")
          │  pulls finished chunks to state/staging/
          └─ every 10 min, one rsync ──> HPC:<root>/1_download_<YYYY_WW>/chunk_NNNNN.jsonl.gz
```

* **Work unit**: a chunk = 500 consecutive WET files of one crawl (~50 GB
  downloaded, ~50–100 MB kept). Chunks are handed out in crawl order, so whole
  crawls land on the HPC one after another.
* **Pull scheduling**: each host keeps `slots` chunks in flight and gets a new
  one when one finishes, so faster hosts take more. Jobs run detached on the
  workers (`~/cc_worker/jobs/<id>`), write one part file per WET file, and resume
  where they stopped if killed. The controller can be stopped/restarted any time.
* **Failure handling**: dead jobs are relaunched (up to 6 times). A host that
  is unreachable for 3 h loses its chunks back to the queue. WET files that fail
  after all retries are listed in each chunk's `.ok` file; re-run them with
  `requeue-failed`. When every chunk of a crawl is on the HPC, a `_COMPLETE`
  file is written there.
* **Worker** (`wet_worker.py`) needs no Spark/Java, only a uv venv with
  `requests pyahocorasick tqdm`. It reuses the filters in
  `c4_dataset_script/Chinese/download_web_docs.py`.

## Commands

```bash
cd distributed
python3 controller.py init            # fetch wet.paths for `years`, build the queue (idempotent)
python3 controller.py setup           # push code + build venvs on all workers
python3 controller.py setup --code-only   # after changing the filter code
screen -dmS cc bash -c 'python3 controller.py run 2>&1 | tee -a state/screen.log'
python3 controller.py status          # progress, per-host files/h + MB/s, ETA
python3 controller.py requeue-failed  # when the main queue is finished
```

## hosts.json (re-read every minute, so edits apply live)

`hosts.json` holds real addresses and accounts, so it is not committed:
start from `cp hosts.example.json hosts.json` and fill in your hosts.

| key | meaning |
|---|---|
| `procs` | total worker processes on the host (split across its slots) |
| `slots` | chunks running at the same time on the host |
| `max_mbps` | download cap for the host in Mbit/s (0/absent = none). Shared by the processes that are downloading at that moment, so the host really runs at the cap |
| `night_max_mbps` | cap inside the global `"night"` window (e.g. `["21:00","09:00"]`), 0 = none. Pushed to running jobs within ~1 min |
| `window` | only take new chunks inside this time range (running chunk finishes); used for the HPC login node |
| `nice` | run the jobs at this nice level |
| `output_dir` | worker shares the HPC filesystem: finished chunks are `mv`ed there instead of rsynced via this PC |
| `fetch_parallel` | chunks transferred from the host at once (default 3) |
| `numa_nodes` | pin each chunk to one NUMA node with `taskset` (lab Xeon E5) |
| `via` | reach this host through another worker (the MPI nodes behind the lab PC) |
| `auto_reboot` | Azure VM: heal it automatically when it stays slow or unreachable (see below); needs `azure_vm` + `azure_rg` |
| `azure_vm`, `azure_rg` | the VM's Azure name and resource group, for `az vm restart/redeploy` |
| `enabled` | `false` = finish running chunks, take no new ones |

Throughput reference (after the SC-filter speedup, ~3.6 s CPU per WET file on
a modern core): an 8-vCPU Azure VM ≈ 1 Gbit/s worth of WET files, so the
on-premise hosts are capped (`max_mbps`) to keep the lab/office networks usable.

The genkai login node (max 8 cores for user compute) is pinned to one login
node by IP address: Genkai has two login nodes behind round-robin DNS, and the PID
liveness check only works on the node that started the job.

Transfers run in a separate fetch thread per host: a chunk that finishes on a
worker frees its slot immediately (status `finished`, shown as `to_fetch` in
`status`), so a slow link never leaves a worker idle.

The Azure VMs (US) send results to Japan over a 176 ms path where the default
CUBIC congestion control collapsed to ~100 KB/s. They are set to BBR
(`/etc/sysctl.d/90-bbr.conf`, `/etc/modules-load.d/bbr.conf`), which measured
32x faster (20 MB: 204 s -> 6.3 s). Do the same on any new far-away worker.

Region comparison for pulling Common Crawl (East vs Central vs West US):
see [REGION_BENCHMARK.md](REGION_BENCHMARK.md).

## Auto-heal for Azure VMs

Azure VMs sometimes degrade without failing outright (seen on azure2: the
accelerated-networking NIC `mana` failed during host maintenance, after which
the VM dropped ~8x more TCP connects and ran at 1/4 speed). Every 5 min the
controller compares each `auto_reboot` VM's last-hour rate with the median of
the others. If a VM stays below half the median, or is unreachable, for an hour:

1. `az vm restart`, falling back to `sudo reboot` over ssh if the CLI fails;
2. if it needs healing again within 24 h: `az vm redeploy`, which moves it to
   different Azure hardware (keeps the disk; public IPs must be **static**).

At most one action per VM per 6 h, then 90 min grace before it is judged again.
Jobs resume from their part files afterwards. Actions are listed in `status`
and logged in `state/controller.log` (`HEAL:`). Manual:
`python3 controller.py heal azure2 [--redeploy]`. Needs `az login` on this PC
(`az login --use-device-code` works over ssh).
