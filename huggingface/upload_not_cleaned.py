"""Convert dumps to parquet and replace each dump's folder in a HF repo, one
commit per dump:
  --source download  downloaded (download-filtered, uncleaned) dumps -> NOT-Cleaned repo
  --source phase_a   stage 4 output of cleaning_phase_a.sh -> Filtered repo
  --source parquet   <input_dir>/<CC_ID>/*.parquet, rewritten with the common schema
                     (e.g. per_dump/cantonese, all/zh_cleaned); needs --repo_id

Resumable: a dump with <staging>/done/<CC_ID> is skipped. Run on the login
node (compute nodes reach the internet through it anyway):

    nohup python huggingface/upload_not_cleaned.py > upload.log 2>&1 &
"""

import argparse
import glob
import gzip
import json
import os
import shutil
import threading
import queue
from multiprocessing import Pool

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.json as paj
import pyarrow.parquet as pq
from huggingface_hub import HfApi

SCHEMA = pa.schema([(c, pa.string()) for c in
                    ["url", "timestamp", "content_language", "content_type", "text"]])


def read_chunk(path):
    try:
        return paj.read_json(
            pa.input_stream(path, compression="gzip"),
            read_options=paj.ReadOptions(use_threads=False, block_size=64 << 20),
            parse_options=paj.ParseOptions(explicit_schema=SCHEMA, unexpected_field_behavior="ignore"))
    except pa.ArrowInvalid:
        # A line longer than block_size, or an empty file: parse it in Python.
        rows = [json.loads(l) for l in gzip.open(path, "rt", encoding="utf-8") if l.strip()]
        return pa.Table.from_pylist([{k: r.get(k) for k in SCHEMA.names} for r in rows], schema=SCHEMA)


def convert_group(job):
    chunks, out_path = job
    tmp = out_path + ".tmp"
    n = 0
    with pq.ParquetWriter(tmp, SCHEMA, compression="snappy") as w:
        for c in chunks:
            t = read_chunk(c)
            if t.num_rows:
                w.write_table(t.select(SCHEMA.names), row_group_size=50_000)
            n += t.num_rows
    os.replace(tmp, out_path)
    return n


def normalize_parquet(job):
    """Rewrite one parquet file with SCHEMA: timestamps as strings, and a null
    content_language for the pre-2020 dumps that have none."""
    src, out_path = job
    f = pq.ParquetFile(src)
    tmp = out_path + ".tmp"
    with pq.ParquetWriter(tmp, SCHEMA, compression="snappy") as w:
        for batch in f.iter_batches(batch_size=50_000):
            cols = []
            for name in SCHEMA.names:
                if name not in batch.schema.names:
                    cols.append(pa.nulls(batch.num_rows, pa.string()))
                    continue
                col = batch.column(name)
                if pa.types.is_timestamp(col.type):
                    col = pc.strftime(pc.cast(col, pa.timestamp("s", tz="UTC")), format="%Y-%m-%dT%H:%M:%SZ")
                cols.append(pc.cast(col, pa.string()))
            w.write_table(pa.Table.from_arrays(cols, schema=SCHEMA), row_group_size=50_000)
    os.replace(tmp, out_path)
    return pq.ParquetFile(out_path).metadata.num_rows


def convert_parquet_dump(cc, args, pool):
    srcs = sorted(glob.glob(os.path.join(args.input_dir, cc, "*.parquet")))
    if not srcs:
        raise RuntimeError(f"{cc}: no parquet files in {args.input_dir}")
    expected = sum(pq.ParquetFile(s).metadata.num_rows for s in srcs)
    out_dir = os.path.join(args.staging, cc)
    shutil.rmtree(out_dir, ignore_errors=True)
    os.makedirs(out_dir)
    n = sum(pool.map(normalize_parquet, [(s, os.path.join(out_dir, os.path.basename(s))) for s in srcs]))
    if n != expected:
        raise RuntimeError(f"{cc}: wrote {n} rows, expected {expected}")
    return out_dir, n, len(srcs)


def phase_a_ready(cc, args):
    return os.path.exists(os.path.join(args.cleaned_dir, "done", f"{cc}.a"))


def list_input(cc, args):
    """Input files of one dump and the row count they must add up to."""
    if args.source == "download":
        chunks = sorted(glob.glob(os.path.join(args.download_dir, f"1_download_{cc}", "chunk_*.jsonl.gz")))
        expected = sum(json.load(open(c[:-len(".jsonl.gz")] + ".ok"))["docs"] for c in chunks)
        return chunks, expected
    chunks = sorted(glob.glob(os.path.join(args.cleaned_dir, "4_remove_duplicate_docs", cc, "unique_docs", "part-*.gz")))
    expected = None
    for line in open(os.path.join(args.cleaned_dir, "logs", f"{cc}.a.log"), errors="replace"):
        if line.startswith("Number of deduplicated documents:"):
            expected = int(line.split(":")[1])
    if expected is None:
        raise RuntimeError(f"{cc}: no document count in the phase A log")
    return chunks, expected


def convert_dump(cc, args, pool):
    chunks, expected = list_input(cc, args)
    groups, cur, size = [], [], 0
    for c in chunks:
        cur.append(c)
        size += os.path.getsize(c)
        if size >= args.group_bytes:
            groups.append(cur)
            cur, size = [], 0
    if cur:
        groups.append(cur)
    out_dir = os.path.join(args.staging, cc)
    shutil.rmtree(out_dir, ignore_errors=True)
    os.makedirs(out_dir)
    m = len(groups)
    jobs = [(g, os.path.join(out_dir, f"{cc}_{args.name}-{i + 1:05d}-of-{m:05d}.parquet"))
            for i, g in enumerate(groups)]
    n = sum(pool.map(convert_group, jobs))
    if n != expected:
        raise RuntimeError(f"{cc}: wrote {n} rows, expected {expected}")
    return out_dir, n, m


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--download_dir", default="/home/pj24001684/ku40000295/jc/projects/CommonCrawl")
    p.add_argument("--staging", default="/home/pj24001684/ku40000295/jc/projects/CommonCrawl_hf_staging")
    p.add_argument("--dumps_file", default=os.path.join(os.path.dirname(__file__), "..", "bash_script", "dumps.txt"))
    p.add_argument("--source", choices=["download", "phase_a", "parquet"], default="download",
        help="download: the downloaded dumps -> NOT-Cleaned repo; "
             "phase_a: stage 4 output of cleaning_phase_a.sh -> Filtered repo; "
             "parquet: <input_dir>/<CC_ID>/*.parquet -> --repo_id")
    p.add_argument("--input_dir", help="--source parquet: one folder of parquet files per dump")
    p.add_argument("--cleaned_dir", default="/home/pj24001684/ku40000295/jc/projects/CommonCrawl_cleaned")
    p.add_argument("--repo_id")
    p.add_argument("--name", help="Parquet file name after <CC_ID>_")
    p.add_argument("--commit_message", default="{cc}: replace with 2026 download and cleaning")
    p.add_argument("--group_bytes", type=int, default=1_500_000_000, help="gz input per parquet file")
    p.add_argument("--workers", type=int, default=8)
    args = p.parse_args()
    if args.source == "download":
        args.repo_id = args.repo_id or "jed351/Traditional-Chinese-Common-Crawl-NOT-Cleaned"
        args.name = args.name or "CC_zh_not_cleaned"
    elif args.source == "parquet":
        if not (args.repo_id and args.input_dir):
            p.error("--source parquet needs --repo_id and --input_dir")
    else:
        args.repo_id = args.repo_id or "jed351/Traditional-Chinese-Common-Crawl-Filtered"
        args.name = args.name or "C4_Traditional_Chinese"

    done_dir = os.path.join(args.staging, "done")
    os.makedirs(done_dir, exist_ok=True)
    dumps = [l.strip() for l in open(args.dumps_file) if l.strip()]
    todo = [d for d in dumps if not os.path.exists(os.path.join(done_dir, d))]
    if args.source == "phase_a":
        waiting = [d for d in todo if not phase_a_ready(d, args)]
        todo = [d for d in todo if phase_a_ready(d, args)]
        print(f"phase A not finished, skipped for now: {waiting}", flush=True)
    print(f"{len(todo)} of {len(dumps)} dumps to upload to {args.repo_id}", flush=True)

    api = HfApi()
    ready = queue.Queue(maxsize=1)  # convert one dump ahead of the upload

    def producer():
        with Pool(args.workers) as pool:
            for cc in todo:
                try:
                    convert = convert_parquet_dump if args.source == "parquet" else convert_dump
                    ready.put((cc, *convert(cc, args, pool)))
                except Exception as e:
                    print(f"{cc}: CONVERT FAILED {e!r}", flush=True)
        ready.put(None)

    threading.Thread(target=producer, daemon=True).start()
    while (item := ready.get()) is not None:
        cc, out_dir, n, m = item
        print(f"{cc}: {n} rows in {m} files, uploading", flush=True)
        for attempt in range(3):
            try:
                api.upload_folder(repo_id=args.repo_id, repo_type="dataset", folder_path=out_dir,
                                  path_in_repo=cc, delete_patterns="*",
                                  commit_message=args.commit_message.format(cc=cc))
                break
            except Exception as e:
                print(f"{cc}: upload attempt {attempt + 1} failed {e!r}", flush=True)
        else:
            continue
        shutil.rmtree(out_dir)
        open(os.path.join(done_dir, cc), "w").write(f"{n} {m}\n")
        print(f"{cc}: done", flush=True)


if __name__ == "__main__":
    main()
