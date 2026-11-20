"""MinHash near-duplicate removal across many dumps, sharded so no step needs
the whole corpus in one Spark job. Same tokenization, shingles, permutations and
bands as remove_duplicate_minhash.py (stage 4), which only works within a dump.

  sign    One dump: the band hashes of every document, saved as
          <work>/sig/<cc>.npy (uint64, one row per document, one column per
          band), in the row order of the dump's parquet files.
  edges   All dumps: documents that share any band hash are linked; in each
          connected component the document of the latest dump (then the first
          row) is kept. Saves the rows to drop as <work>/remove/<cc>.npy.
  filter  One dump: rewrite its parquet files without those rows.

    python Chinese/global_minhash.py sign   --input_dir X/zh_cleaned --work W --cc_id 2025_05
    python Chinese/global_minhash.py edges  --work W --dumps_file dumps_2025_2026.txt
    python Chinese/global_minhash.py filter --input_dir X/zh_cleaned --work W --cc_id 2025_05 \
        --output_dir X/zh_cleaned_minhash

Docs ids in `edges` are (rank of the dump, newest first) << 40 | row, so the
smallest id of a component is its newest copy.
"""

import argparse
import glob
import os
import sys
from multiprocessing import Pool

import numpy as np
import pyarrow.parquet as pq
import xxhash

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from remove_duplicate_minhash import (  # noqa: E402
    DTYPE, MAX_HASH, MOD_PRIME, RNG, ngram_hashes, optimal_param)

ROW_BITS = 40


def parquet_files(input_dir, cc):
    files = sorted(glob.glob(os.path.join(input_dir, cc, "*.parquet")))
    if not files:
        raise SystemExit(f"{cc}: no parquet files in {input_dir}")
    return files


def setup(threshold, num_perm):
    b, r = optimal_param(threshold, num_perm)
    a, c = (RNG.randint(1, MOD_PRIME, size=(num_perm,), dtype=DTYPE),
            RNG.randint(0, MOD_PRIME, size=(num_perm,), dtype=DTYPE))
    return [(i * r, (i + 1) * r) for i in range(b)], (a.astype(np.uint64), c.astype(np.uint64))


def _init(args):
    global G
    G = dict(zip(["ranges", "perm", "ngram", "min_length"], args))


def band_hashes(texts):
    a, c = G["perm"]
    out = np.empty((len(texts), len(G["ranges"])), dtype=np.uint64)
    for i, text in enumerate(texts):
        h = np.fromiter(ngram_hashes(text or "", G["ngram"], G["min_length"]), dtype=np.uint64)
        if len(h) == 0:
            h = np.zeros(1, dtype=np.uint64)
        mins = (((np.outer(h, a) + c) % MOD_PRIME) & MAX_HASH).min(axis=0).astype(DTYPE)
        out[i] = [xxhash.xxh64_intdigest(mins[s:e].tobytes()) for s, e in G["ranges"]]
    return out


def text_batches(files, size=2000):
    for f in files:
        for batch in pq.ParquetFile(f).iter_batches(batch_size=size, columns=["text"]):
            yield batch.column(0).to_pylist()


def cmd_sign(args):
    ranges, perm = setup(args.threshold, args.num_perm)
    os.makedirs(os.path.join(args.work, "sig"), exist_ok=True)
    out = os.path.join(args.work, "sig", f"{args.cc_id}.npy")
    with Pool(args.workers, _init, ((ranges, perm, args.ngram_size, args.min_length),)) as pool:
        sig = np.concatenate(list(pool.imap(band_hashes, text_batches(parquet_files(args.input_dir, args.cc_id)))))
    np.save(out + ".tmp.npy", sig)
    os.replace(out + ".tmp.npy", out)
    print(f"{args.cc_id}: {len(sig)} documents, {sig.shape[1]} bands")


def cmd_edges(args):
    from scipy.sparse import coo_matrix
    from scipy.sparse.csgraph import connected_components

    dumps = [l.strip() for l in open(args.dumps_file) if l.strip()]
    newest_first = sorted(dumps, reverse=True)
    sigs = {cc: np.load(os.path.join(args.work, "sig", f"{cc}.npy"), mmap_mode="r") for cc in dumps}
    ids = np.concatenate([(np.uint64(rank) << np.uint64(ROW_BITS)) + np.arange(len(sigs[cc]), dtype=np.uint64)
                          for rank, cc in enumerate(newest_first)])
    print(f"{len(ids)} documents in {len(dumps)} dumps", flush=True)
    n_bands = sigs[dumps[0]].shape[1]
    src, dst = [], []
    for b in range(n_bands):
        h = np.concatenate([np.asarray(sigs[cc][:, b]) for cc in newest_first])
        order = np.argsort(h, kind="stable")  # stable: within a run, ids stay ascending
        hs = h[order]
        start = np.r_[True, hs[1:] != hs[:-1]]
        run_first = np.maximum.accumulate(np.where(start, np.arange(len(hs)), 0))
        dup = ~start
        src.append(order[dup])            # positions, linked to the first of their run
        dst.append(order[run_first[dup]])
        print(f"band {b}: {dup.sum()} links", flush=True)
        del h, order, hs, start, run_first, dup
    src, dst = np.concatenate(src), np.concatenate(dst)
    nodes, inv = np.unique(np.r_[src, dst], return_inverse=True)
    g = coo_matrix((np.ones(len(src), dtype=np.int8), (inv[:len(src)], inv[len(src):])),
                   shape=(len(nodes), len(nodes)))
    n_comp, label = connected_components(g, directed=False)
    # Keep the smallest position (newest dump, first row) of each component.
    keep = np.full(n_comp, np.iinfo(np.int64).max)
    np.minimum.at(keep, label, nodes)
    remove = np.sort(nodes[nodes != keep[label]])
    print(f"{len(nodes)} documents in {n_comp} groups of near-duplicates; removing {len(remove)} "
          f"({len(remove) / len(ids):.2%})", flush=True)
    os.makedirs(os.path.join(args.work, "remove"), exist_ok=True)
    rid = ids[remove]
    for rank, cc in enumerate(newest_first):
        rows = (rid[(rid >> np.uint64(ROW_BITS)) == rank] & np.uint64((1 << ROW_BITS) - 1)).astype(np.int64)
        np.save(os.path.join(args.work, "remove", f"{cc}.npy"), rows)
        print(f"{cc}: remove {len(rows)} of {len(sigs[cc])}", flush=True)


def cmd_filter(args):
    remove = np.load(os.path.join(args.work, "remove", f"{args.cc_id}.npy"))
    out_dir = os.path.join(args.output_dir, args.cc_id)
    os.makedirs(out_dir, exist_ok=True)
    offset = kept = 0
    for f in parquet_files(args.input_dir, args.cc_id):
        pf = pq.ParquetFile(f)
        n = pf.metadata.num_rows
        drop = remove[(remove >= offset) & (remove < offset + n)] - offset
        mask = np.ones(n, dtype=bool)
        mask[drop] = False
        t = pf.read().filter(mask)
        tmp = os.path.join(out_dir, os.path.basename(f) + ".tmp")
        pq.write_table(t, tmp, row_group_size=50_000)
        os.replace(tmp, os.path.join(out_dir, os.path.basename(f)))
        offset += n
        kept += t.num_rows
    print(f"{args.cc_id}: kept {kept} of {offset}, removed {offset - kept}")


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("sign")
    s.add_argument("--input_dir", required=True)
    s.add_argument("--cc_id", required=True)
    s.add_argument("--threshold", type=float, default=0.7)
    s.add_argument("--num_perm", type=int, default=250)
    s.add_argument("--ngram_size", type=int, default=5)
    s.add_argument("--min_length", type=int, default=10)
    s.add_argument("--workers", type=int, default=os.cpu_count())
    e = sub.add_parser("edges")
    e.add_argument("--dumps_file", required=True)
    f = sub.add_parser("filter")
    f.add_argument("--input_dir", required=True)
    f.add_argument("--cc_id", required=True)
    f.add_argument("--output_dir", required=True)
    for x in (s, e, f):
        x.add_argument("--work", required=True)
    args = p.parse_args()
    {"sign": cmd_sign, "edges": cmd_edges, "filter": cmd_filter}[args.cmd](args)


if __name__ == "__main__":
    main()
