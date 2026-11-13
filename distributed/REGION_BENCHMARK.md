# Does the Azure region matter for downloading Common Crawl?

Measured 2026-10-27, while the three VMs were running the download/filter job.

## Setup

| VM | Azure region | CPU | vCPU / RAM |
|---|---|---|---|
| azure1 | East US | Intel Xeon 6973P-C (Granite Rapids) | 8 / 16 GB |
| azure2 | Central US | Intel Xeon 6973P-C (Granite Rapids) | 8 / 32 GB |
| azure3 | West US | Intel Xeon 6973P-C (Granite Rapids) | 8 / 32 GB |

Same CPU everywhere (the sponsor limits us to 8 cores per region), so only the
region differs. Common Crawl data lives in AWS S3 **us-east-1 (Northern
Virginia)** and is served through CloudFront at `data.commoncrawl.org`.

Method: at the same moment, each VM downloaded 4 *different* random WET files
of CC-MAIN-2026-43 (~63 MB each) one after another with `curl`, recording time to
first byte, single-stream speed, and the CloudFront POP / cache status from the
response headers. Different files per VM, so no VM benefits from another
warming the CDN cache. The VMs were busy with their normal jobs (same load on
all three), so the absolute numbers are a little pessimistic.

## 1. Single download, CDN cache miss (the fair comparison)

On a miss the CloudFront POP must fetch the file from S3 in Virginia.

| VM | CloudFront POP | TCP connect | time to first byte | speed (MB/s) |
|---|---|---|---|---|
| East US | IAD55 (Virginia) | 5 ms | 0.10 – 0.18 s | 95\*, 76, 94, 92 (avg ≈ 89) |
| Central US | ORD58 (Chicago) | 14 ms | 0.16 – 0.26 s | 77, 75, 63 (avg ≈ 71) |
| West US | SFO53 (San Francisco) | 3 ms | 0.20 – 0.37 s | 37, 51 (avg ≈ 44) |

\* first East US file was a `RefreshHit`, so treat as a miss-equivalent.

**West US gets about half the single-stream speed of East US on cache misses**:
the POP is next to the VM, but the POP-to-S3 leg crosses the continent.

## 2. CDN cache hits

About a third of the random files were already cached at the POP (presumably
fetched recently by other Common Crawl users). Then distance stops mattering:

| VM | cache status | speed (MB/s) |
|---|---|---|
| Central US | RefreshHit | 182 |
| West US | RefreshHit | 146 |
| West US | Hit | 274 (TTFB 0.03 s) |

## 3. Effect on real throughput

Average wall time per WET file per worker process (download + filtering), from
the last ~500–600 files in each VM's worker logs, 12 processes per VM:

| VM | s / WET file | load average (8 vCPU) |
|---|---|---|
| East US | 9.0 | 10.9 |
| Central US | 8.6 | 10.9 |
| West US | 10.2 | 9.8 |

* West US is **~13% slower per file**, and its CPU is not saturated (load 9.8 vs
  10.9): processes spend more time waiting on the network.
* East vs Central is within noise despite Central's slower downloads.
* The region costs **~10–15% throughput, not 2x**, because the job is mostly
  CPU-bound (~3.6 s of filtering per file on a fast core) and with more
  processes than vCPUs some processes download while others filter.

## Takeaways

1. **East US (Virginia) is the best region for pulling Common Crawl**: it sits
   next to the S3 bucket. Central is nearly as good in practice.
2. A far region costs latency per request, which you hide by running **more
   processes than vCPUs**. azure3 was raised from 12 to 16 processes for this
   (check its rate in `controller.py status` to see whether it caught up).
3. For sending results to Japan, West US is closer (RTT to Kyushu 125 ms vs
   176 ms from East US), but with BBR congestion control (see README) that link
   is no longer a bottleneck either way.
4. CDN hits are fast from anywhere, but you cannot plan around them: most WET
   files are fetched rarely enough to be cache misses.

## Reproduce

```bash
curl -s https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-43/wet.paths.gz | zcat | shuf -n 4 > paths.txt
while read p; do
  curl -s -o /dev/null -D h.txt \
    -w "ttfb=%{time_starttransfer} MBps=%{speed_download}\n" "https://data.commoncrawl.org/$p"
  grep -iE "x-amz-cf-pop|x-cache" h.txt
done < paths.txt
```
