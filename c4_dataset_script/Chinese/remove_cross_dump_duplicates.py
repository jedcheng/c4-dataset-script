"""Remove documents whose exact text also appears in a later (or earlier) dump.

Stages 3 and 4 only deduplicate within one dump. This step works in two passes
so that every dump can be processed in parallel:

  hash    After stage 4, save the sorted 64-bit text hashes of the dump's
          output as <hash_dir>/<cc_id>.npy.
  filter  Once every dump has a hash file, drop each doc whose hash appears in
          any dump listed *after* this one in --dumps_file (chronological
          order), so the latest crawl of a text is the one kept. With
          --keep earliest, drop it if it appears in any dump *before* this one
          instead. Either way each dump only reads hash files, so all dumps can
          run in parallel, and re-running a dump gives the same result.

Usage:
    python Chinese/remove_cross_dump_duplicates.py hash \
        --input work/2016_30/4_remove_duplicate_docs/unique_docs \
        --hash_dir cross_dump_hashes --cc_id 2016_30
    python Chinese/remove_cross_dump_duplicates.py filter \
        --input work/2016_30/4_remove_duplicate_docs/unique_docs \
        --output work/2016_30/4b_cross_dump_dedup \
        --hash_dir cross_dump_hashes --dumps_file dumps.txt --cc_id 2016_30
"""

import argparse
import glob
import gzip
import json
import os

import numpy as np
import xxhash


def text_hash(text):
    return xxhash.xxh64_intdigest(text.strip())


def iter_lines(input_dir):
    for path in sorted(glob.glob(os.path.join(input_dir, "part-*"))):
        opener = gzip.open if path.endswith(".gz") else open
        with opener(path, "rt", encoding="utf-8") as f:
            yield from f


def cmd_hash(args):
    os.makedirs(args.hash_dir, exist_ok=True)
    hashes = np.unique(np.fromiter(
        (text_hash(json.loads(line)["text"]) for line in iter_lines(args.input)),
        dtype=np.uint64))
    # Write to a temp name first so a crash never leaves a truncated hash file.
    tmp = os.path.join(args.hash_dir, f"{args.cc_id}.tmp.npy")
    np.save(tmp, hashes)
    os.replace(tmp, os.path.join(args.hash_dir, f"{args.cc_id}.npy"))
    print(f"{args.cc_id}: {len(hashes)} unique text hashes")


def load_other_hashes(hash_dir, dumps_file, cc_id, keep):
    dumps = [l.strip() for l in open(dumps_file) if l.strip()]
    i = dumps.index(cc_id)
    others = dumps[i + 1:] if keep == "latest" else dumps[:i]
    missing = [d for d in others if not os.path.exists(os.path.join(hash_dir, f"{d}.npy"))]
    if missing:
        raise SystemExit(f"Missing hash files (run the hash step first): {missing}")
    if not others:
        return np.empty(0, dtype=np.uint64)
    return np.unique(np.concatenate([np.load(os.path.join(hash_dir, f"{d}.npy")) for d in others]))


def cmd_filter(args):
    os.makedirs(args.output, exist_ok=True)
    seen = load_other_hashes(args.hash_dir, args.dumps_file, args.cc_id, args.keep)
    print(f"{args.cc_id}: {len(seen)} hashes from {'later' if args.keep == 'latest' else 'earlier'} dumps")

    kept_hashes = set()
    n_in = n_cross = n_self = n_out = 0
    out_idx = 0
    out = None

    for line in iter_lines(args.input):
        n_in += 1
        h = text_hash(json.loads(line)["text"])
        if h in kept_hashes:
            n_self += 1
            continue
        i = np.searchsorted(seen, h)
        if i < len(seen) and seen[i] == h:
            n_cross += 1
            continue
        kept_hashes.add(h)
        if out is None or n_out % args.docs_per_file == 0:
            if out:
                out.close()
            out = gzip.open(os.path.join(args.output, f"part-{out_idx:05d}.gz"), "wt", encoding="utf-8")
            out_idx += 1
        out.write(line if line.endswith("\n") else line + "\n")
        n_out += 1
    if out:
        out.close()

    print(f"Input docs: {n_in}")
    print(f"Removed, also in {'a later' if args.keep == 'latest' else 'an earlier'} dump: {n_cross} ({n_cross / max(n_in, 1):.1%})")
    print(f"Removed, exact duplicate within this dump: {n_self}")
    print(f"Kept: {n_out}")


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("hash")
    p.add_argument("--input", required=True, help="Directory of part-* JSONL(.gz) files")
    p.add_argument("--hash_dir", required=True)
    p.add_argument("--cc_id", required=True)
    p.set_defaults(func=cmd_hash)

    p = sub.add_parser("filter")
    p.add_argument("--input", required=True, help="Directory of part-* JSONL(.gz) files")
    p.add_argument("--output", required=True)
    p.add_argument("--hash_dir", required=True)
    p.add_argument("--dumps_file", required=True,
        help="All CC_IDs, one per line, in chronological order")
    p.add_argument("--cc_id", required=True)
    p.add_argument("--keep", choices=["latest", "earliest"], default="latest",
        help="Which crawl of a duplicated text to keep")
    p.add_argument("--docs_per_file", type=int, default=200_000)
    p.set_defaults(func=cmd_filter)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
