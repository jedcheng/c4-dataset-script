# Download resource usage (snapshot 2026-11-13 04:56 UTC)

## Azure VMs (Azure Monitor, hourly)

| VM | hours with data | network in (TB) | network out (TB) | mean CPU % | peak hourly CPU % |
|---|---|---|---|---|---|
| common-crawl-downloader1 | 396 | 178.4 | 0.80 | 84 | 98 |
| common-crawl-downloader2 | 395 | 171.1 | 0.69 | 82 | 99 |
| common-crawl-downloader4 | 380 | 176.8 | 0.67 | 85 | 99 |
| common-crawl-downloader5 | 320 | 144.5 | 0.63 | 86 | 98 |

Network in includes retries, SSH and package installs; hours with no traffic (deallocated) are excluded from the CPU mean.

## All hosts (controller DB, per finished chunk)

| host | chunks | WET files | WET TB (last run) | docs kept |
|---|---|---|---|---|
| azure1 | 2861 | 1,427,492 | 164.6 | 166,797,205 |
| azure2 | 2797 | 1,397,244 | 158.1 | 160,660,489 |
| azure3 | 749 | 374,500 | 27.4 | 32,799,533 |
| azure4 | 2828 | 1,410,735 | 163.4 | 162,502,650 |
| azure5 | 2044 | 1,020,760 | 133.6 | 130,615,328 |
| genkai | 1907 | 953,107 | 108.1 | 120,938,756 |
| lab | 890 | 445,000 | 52.0 | 53,776,866 |
| mpi1 | 516 | 258,000 | 30.2 | 31,558,049 |
| mpi2 | 518 | 258,365 | 30.2 | 31,627,565 |
| mpi3 | 520 | 259,849 | 30.2 | 31,640,828 |
| office | 1642 | 819,688 | 93.3 | 98,806,329 |
| **total** | 17272 | 8,624,740 | 991.2 | 1,021,723,598 |

WET TB counts the final run of each chunk only, so bytes fetched before a job was restarted (and resumed from its part files) are missing; it is a lower bound on what was downloaded.
