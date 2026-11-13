import argparse
import os
import json
import re
import numpy as np
import xxhash
from pyspark.sql import SparkSession
import jieba

# Absolute path: Spark workers do not run this module's top-level code, so a
# set_dictionary() call here only affects the driver. jieba_cut() sets it lazily
# in whichever process does the cutting.
DICT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dict.txt.big"))


def jieba_cut(text):
    if jieba.dt.dictionary != DICT_PATH:
        jieba.set_dictionary(DICT_PATH)
    return jieba.lcut(text)

SEED = 42
RNG = np.random.RandomState(SEED)
# NON_ALPHA = re.compile(r"", re.UNICODE)
DTYPE = np.uint32
MAX_HASH = 4_294_967_295
MOD_PRIME = 4_294_967_291


def ngrams(sequence, n, min_length=5):
    if len(sequence) < min_length:
        return []
    if len(sequence) < n:
        # If sequence is shorter than n, but meets min_length, treat the whole sequence as one n-gram
        return [tuple(sequence)] if len(sequence) >= 1 else [] # Ensure sequence is not empty
    from itertools import tee
    iterables = tee(iter(sequence), n)
    for i, sub_iterable in enumerate(iterables):
        for _ in range(i):
            next(sub_iterable, None)
    return zip(*iterables)


def ngram_hashes(content, n, min_length=5):
    # Tokenize using jieba
    # Use cut_all=False for precise mode, which is generally preferred
    tokens = jieba_cut(content.lower())
    # Filter out spaces or single-character punctuation if desired, though jieba handles most.
    # For simplicity, we'll proceed with jieba's output.
    tokens = [token for token in tokens if token.strip()] # Optional: further clean tokens

    ng = {bytes(" ".join(t).lower(), "utf-8") for t in ngrams(tokens, n, min_length)}
    return {xxhash.xxh32_intdigest(n) for n in ng}


def generate_hash_values(content, idx, num_perm, ngram_size, min_length, hashranges, permutations):
    a, b = permutations
    # ngram_hashes now uses jieba
    hashes = np.array(list(ngram_hashes(content, ngram_size, min_length)), dtype=DTYPE)
    if len(hashes) == 0:
        # If no ngrams are generated (e.g., text too short or only punctuation),
        # assign a default hash to avoid errors.
        # Using a single, consistent hash for empty/invalid content ensures it can be processed,
        # though it might group all such items if many exist.
        hashes = np.array([0], dtype=DTYPE)
    # uint64: h * a overflows uint32 and would wrap before the modulo.
    p_hashes = (((np.outer(hashes.astype(np.uint64), a.astype(np.uint64)) + b.astype(np.uint64))
                 % MOD_PRIME) & MAX_HASH).astype(DTYPE)
    min_hashes = np.vstack([p_hashes, np.full(num_perm, MAX_HASH, dtype=DTYPE)]).min(axis=0)
    return [(band_idx, min_hashes[start:end].data.tobytes(), idx) for band_idx, (start, end) in enumerate(hashranges)]




def generate_edges(nodes):
    if len(nodes) <= 1:
        return []
    min_node = min(nodes)
    return [(n, min_node) for n in nodes if n != min_node]

def optimal_param(threshold, num_perm):
    from scipy.integrate import quad as integrate
    def false_positive_area(threshold, b, r):
        def area(s):
            return 1 - (1 - s ** float(r)) ** float(b)
        a, _ = integrate(area, 0.0, threshold)
        return a
    def false_negative_area(threshold, b, r):
        def area(s):
            return 1 - (1 - (1 - s ** float(r)) ** float(b))
        a, _ = integrate(area, threshold, 1.0)
        return a
    min_error = float("inf")
    opt = (0, 0)
    for b in range(1, num_perm + 1):
        max_r = int(num_perm / b)
        for r in range(1, max_r + 1):
            fp = false_positive_area(threshold, b, r)
            fn = false_negative_area(threshold, b, r)
            error = fp * 0.5 + fn * 0.5
            if error < min_error:
                min_error = error
                opt = (b, r)
    return opt


def ngrams_length_check(content, n, min_length=5): # n here is ngram_size
    # Tokenize using jieba
    tokens = jieba_cut(content.lower())
    tokens = [token for token in tokens if token.strip()] # further clean tokens
    # Check if the number of words is sufficient
    if len(tokens) < min_length: # min_length now refers to words
        return False
    # Also ensure there are enough tokens to form at least one n-gram of size n
    # This check is implicitly handled by ngrams function returning [] if len(tokens) < n
    # but an explicit check against n can be clearer if min_length is very small.
    if len(tokens) < n and min_length <= n: # If min_length is for n-grams themselves
        return False
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--threshold", type=float, default=0.7)
    parser.add_argument("--ngram_size", type=int, default=5) # Now refers to 5 words
    parser.add_argument("--min_length", type=int, default=10) # Now refers to 5 words minimum document length
    parser.add_argument("--num_perm", type=int, default=250)
    args = parser.parse_args()

    B, R = optimal_param(args.threshold, args.num_perm)
    HASH_RANGES = [(i * R, (i + 1) * R) for i in range(B)]
    PERMUTATIONS = (
        RNG.randint(1, MOD_PRIME, size=(args.num_perm,), dtype=DTYPE),
        RNG.randint(0, MOD_PRIME, size=(args.num_perm,), dtype=DTYPE),
    )


    spark = SparkSession.builder.appName("Remove duplicate docs with MinHash").getOrCreate()



    # Number the docs before repartition: repartition is not deterministic, so ids
    # assigned after it can change if Spark recomputes an evicted partition.
    docs = spark.sparkContext.textFile(os.path.join(args.input, "part-*"))\
        .map(lambda line: json.loads(line))\
        .zipWithIndex()\
        .repartition(120)

    # ngrams_length_check now uses jieba and word counts
    docs = docs.filter(
        lambda x: ngrams_length_check(x[0]["text"], args.ngram_size, args.min_length)
    ).cache()

    print(f"Total number of documents: {docs.count()}")

    # MinHash LSH
    edges = (
        docs.flatMap(
            lambda x: generate_hash_values(
                content=x[0]["text"],
                idx=x[1],
                num_perm=args.num_perm,
                ngram_size=args.ngram_size, # word n-gram size
                min_length=args.min_length, # min words for document
                hashranges=HASH_RANGES,
                permutations=PERMUTATIONS,
            )
        )
        .groupBy(lambda x: (x[0], x[1]))
        .flatMap(lambda x: generate_edges([ele[2] for ele in x[1]]))
        .distinct()
    )

    # Connected components (union-find over the nodes that have edges; docs with no
    # edge are unique and kept). Each component keeps its smallest doc id.
    def duplicate_ids(edges_rdd):
        parent = {}
        def find(u):
            parent.setdefault(u, u)
            while parent[u] != u:
                parent[u] = parent[parent[u]]
                u = parent[u]
            return u
        for u, v in edges_rdd.collect():
            pu, pv = find(u), find(v)
            if pu != pv:
                parent[max(pu, pv)] = min(pu, pv)
        return {u for u in parent if find(u) != u}

    to_remove = spark.sparkContext.broadcast(duplicate_ids(edges))
    print(f"Number of near-duplicate documents removed: {len(to_remove.value)}")

    dedup_docs = docs.filter(lambda x: x[1] not in to_remove.value).map(lambda x: x[0])

    print(f"Number of deduplicated documents: {dedup_docs.count()}")

    dedup_docs.map(lambda j: json.dumps(j, ensure_ascii=False)).saveAsTextFile(
        os.path.join(args.output, "unique_docs"),
        compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec",
    )

    # Optionally save duplicate documents as well in a single JSON file for easier inspection
    # dup_docs = docs.filter(lambda x: x[1] in to_remove.value).map(lambda x: x[0])
    # dup_docs_list = dup_docs.collect()
    # print(f"Number of duplicated documents: {len(dup_docs_list)}")
    # with open(os.path.join(args.output, "duplicate_docs.json"), "w", encoding="utf-8") as f:
    #     json.dump(dup_docs_list, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()