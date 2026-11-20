"""Build the Cantonese-Web-Data set: the per-dump Cantonese subsets of every dump,
cleaned and near-duplicate-removed (MinHash) across all dumps.

    prep      per-dump parquet -> gzipped JSONL part files for remove_duplicate_minhash.py,
              newest dump first, so each group of near-duplicates keeps its newest copy
              (the MinHash script keeps the smallest document id).
    finalize  unique_docs/part-*.gz -> one parquet file.

Every row keeps its metadata: url, timestamp, content_language, content_type, dump.
See bash_script/cantonese_web_data.sh.
"""

import argparse
import glob
import gzip
import json
import os
import re

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

SCHEMA = pa.schema([(c, pa.string()) for c in
                    ["url", "timestamp", "content_language", "content_type", "dump", "text"]])
NAV = re.compile(r"^(上一篇|下一篇).*\n?", re.M)
BRACKETS = re.compile(r"【.*?】")
MIN_CHARS, MAX_CHARS = 100, 100_000


def normalize(table):
    """Common Crawl metadata as strings; pre-2020 dumps have no content_language."""
    cols = {}
    for name in ["url", "timestamp", "content_language", "content_type", "text"]:
        if name not in table.column_names:
            cols[name] = pa.nulls(table.num_rows, pa.string())
            continue
        col = table.column(name)
        if pa.types.is_timestamp(col.type):
            col = pc.strftime(pc.cast(col, pa.timestamp("s", tz="UTC")), format="%Y-%m-%dT%H:%M:%SZ")
        cols[name] = pc.cast(col, pa.string())
    return cols


def clean_text(text):
    """The 2025 final_processing.py rules: drop 上一篇/下一篇 lines and 【…】."""
    text = NAV.sub("", text)
    text = BRACKETS.sub("", text)
    n = len(text.replace(" ", "").replace("\n", ""))
    return text if MIN_CHARS <= n <= MAX_CHARS else None


def prep(args):
    dumps = [l.strip() for l in open(args.dumps_file) if l.strip()]
    os.makedirs(args.output)
    total = kept = 0
    for i, cc in enumerate(sorted(dumps, reverse=True)):
        cols = normalize(pq.read_table(os.path.join(args.input, cc, f"{args.subset}.parquet")))
        rows = {k: v.to_pylist() for k, v in cols.items()}
        with gzip.open(os.path.join(args.output, f"part-{i:05d}.gz"), "wt", encoding="utf-8") as f:
            for j, text in enumerate(rows["text"]):
                total += 1
                text = clean_text(text or "")
                if text is None:
                    continue
                kept += 1
                doc = {k: rows[k][j] for k in ["url", "timestamp", "content_language", "content_type"]}
                doc["dump"] = cc
                doc["text"] = text
                f.write(json.dumps(doc, ensure_ascii=False) + "\n")
    print(f"{args.subset}: {total} documents from {len(dumps)} dumps, {kept} after text cleaning")


def finalize(args):
    tables = []
    for path in sorted(glob.glob(os.path.join(args.input, "part-*.gz"))):
        rows = [json.loads(l) for l in gzip.open(path, "rt", encoding="utf-8")]
        if rows:
            tables.append(pa.Table.from_pylist(rows, schema=SCHEMA))
    table = pa.concat_tables(tables)
    table = table.take(pc.sort_indices(table, [("dump", "ascending"), ("url", "ascending")]))
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    pq.write_table(table, args.output, row_group_size=20_000, compression="zstd")
    chars = sum(len(t.replace(" ", "").replace("\n", "")) for t in table.column("text").to_pylist())
    print(f"Wrote {table.num_rows} documents, {chars} characters (excluding spaces and newlines) to {args.output}")


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("prep")
    a.add_argument("--input", required=True, help="per_dump/cantonese, one folder per dump")
    a.add_argument("--subset", required=True, help="e.g. no_quotes_domain_cleaned")
    a.add_argument("--dumps_file", required=True)
    a.add_argument("--output", required=True)
    b = sub.add_parser("finalize")
    b.add_argument("--input", required=True, help="unique_docs folder of remove_duplicate_minhash.py")
    b.add_argument("--output", required=True, help="parquet file")
    args = p.parse_args()
    {"prep": prep, "finalize": finalize}[args.cmd](args)


if __name__ == "__main__":
    main()
