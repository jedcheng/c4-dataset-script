"""Remove documents whose exact text was already kept from another Common Crawl dump.

Stages 3 and 4 only deduplicate within one dump. This step keeps a persistent
index of 64-bit text hashes, one sorted .npy file per dump, and drops any doc
whose text hash appears in the index of a *different* dump. Run it once per
dump after stage 4; whichever dump is processed first keeps the document.
Re-running a dump replaces that dump's own index file, so it is idempotent.

Usage:
    python Chinese/remove_cross_dump_duplicates.py \
        --input 4_remove_duplicate_docs/unique_docs \
        --output 4b_cross_dump_dedup \
        --index_dir $SSD/cross_dump_index \
        --cc_id 2016_30
"""

import argparse
import glob
import json
import os

import numpy as np
import xxhash


def text_hash(text):
    return xxhash.xxh64_intdigest(text.strip())


def load_index(index_dir, exclude_cc_id):
    arrays = [np.load(f) for f in sorted(glob.glob(os.path.join(index_dir, "*.npy")))
              if os.path.basename(f) != f"{exclude_cc_id}.npy"]
    if not arrays:
        return np.empty(0, dtype=np.uint64)
    return np.unique(np.concatenate(arrays))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Directory of part-* JSONL files")
    parser.add_argument("--output", required=True)
    parser.add_argument("--index_dir", required=True)
    parser.add_argument("--cc_id", required=True)
    parser.add_argument("--docs_per_file", type=int, default=200_000)
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)
    os.makedirs(args.index_dir, exist_ok=True)

    seen = load_index(args.index_dir, args.cc_id)
    print(f"Index: {len(seen)} hashes from other dumps")

    kept_hashes = set()
    n_in = n_cross = n_self = n_out = 0
    out_idx = 0
    out = None

    for path in sorted(glob.glob(os.path.join(args.input, "part-*"))):
        with open(path, encoding="utf-8") as f:
            for line in f:
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
                    out = open(os.path.join(args.output, f"part-{out_idx:05d}"), "w", encoding="utf-8")
                    out_idx += 1
                out.write(line if line.endswith("\n") else line + "\n")
                n_out += 1
    if out:
        out.close()

    # Write to a temp name first so a crash never leaves a truncated index file.
    tmp = os.path.join(args.index_dir, f"{args.cc_id}.tmp.npy")
    np.save(tmp, np.sort(np.fromiter(kept_hashes, dtype=np.uint64, count=len(kept_hashes))))
    os.replace(tmp, os.path.join(args.index_dir, f"{args.cc_id}.npy"))

    print(f"Input docs: {n_in}")
    print(f"Removed, already kept from another dump: {n_cross} ({n_cross / max(n_in, 1):.1%})")
    print(f"Removed, exact duplicate within this dump: {n_self}")
    print(f"Kept: {n_out}")


if __name__ == "__main__":
    main()
