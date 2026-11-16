
import argparse
import hashlib
import os
import json

from pyspark.sql import SparkSession


MIN_NUM_SENTENCES = 3



def hash_text(text):
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def _remove_lines_from_text(el, min_num_sentences):
    url, join_values = el
    features = join_values["features"]
    text = features["text"]
    lines_to_keep = set(join_values["lines"])
    new_lines = []
    hashed_lines = set()
    removed_lines = []
    for line in text.split("\n"):
        hashed_line = hash_text(line.strip().lower())
        if hashed_line not in lines_to_keep or hashed_line in hashed_lines:
            removed_lines.append({"url": url, "line": line})
        else:
            new_lines.append(line)
            hashed_lines.add(hashed_line)
    new_text = "\n".join(new_lines)
    if not new_text or (min_num_sentences and len(new_text.splitlines()) < min_num_sentences):
        # If the doc is too short, treat all lines as removed
        removed_lines.extend([{"url": url, "line": l} for l in new_lines])
        return
    new_features = features.copy()
    new_features["text"] = new_text
    yield (url, new_features, removed_lines)

def remove_duplicate_text(docs, min_num_sentences=MIN_NUM_SENTENCES):
    docs = docs.map(lambda doc: (doc["url"], doc))

    # Only the line hash and the URL are needed to decide which doc keeps a
    # line. Shuffling the line text as well overflowed the node's local disk
    # on the larger dumps.
    def emit_url_to_lines(doc):
        url = doc["url"]
        for h in {hash_text(line.strip().lower()) for line in doc["text"].splitlines()}:
            yield h, url

    def pick_url(a, b):
        # Deterministic: the URL with the smallest hash keeps the line.
        return a if hash_text(a) <= hash_text(b) else b

    line_to_selected_url = docs\
        .flatMap(lambda url_doc: emit_url_to_lines(url_doc[1]))\
        .reduceByKey(pick_url)

    lines_to_keep = line_to_selected_url.map(lambda x: (x[1], x[0]))

    # Not cached: only clean_docs is written, and caching also kept every
    # removed line around.
    processed = docs.cogroup(lines_to_keep, numPartitions=1024)\
        .mapValues(lambda x: {"features": list(x[0])[0], "lines": list(x[1])})\
        .flatMap(lambda x: _remove_lines_from_text(list(x), min_num_sentences=min_num_sentences))

    clean_docs = processed.map(lambda x: x[1])
    removed_lines = processed.flatMap(lambda x: x[2])

    return clean_docs, removed_lines

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True,
        help="Path or glob of JSONL(.gz) files, e.g. 2_repetition_removal/clean_docs")
    parser.add_argument("--output", default="./rd_output")
    parser.add_argument("--min_num_lines", type=int, default=MIN_NUM_SENTENCES,
        help="Drop docs with fewer lines than this after line dedup (C4 uses 3)")
    args = parser.parse_args()

    spark = SparkSession.builder\
            .appName("Remove duplicate text")\
            .getOrCreate()

    docs = spark.sparkContext.textFile(args.input)\
        .map(lambda line: json.loads(line))\
        .repartition(120).cache()

    clean_docs, removed_lines = remove_duplicate_text(docs, min_num_sentences=args.min_num_lines)

    clean_docs.map(lambda j: json.dumps(j, ensure_ascii=False)) \
        .saveAsTextFile(os.path.join(args.output, "clean_docs"), compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")

    # removed_lines.map(lambda j: json.dumps(j, ensure_ascii=False)) \
    #     .saveAsTextFile(os.path.join(args.output, "removed_lines"))

if __name__ == "__main__":
    main()