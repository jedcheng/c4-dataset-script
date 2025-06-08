
import argparse
import hashlib
import os
import json

from pyspark.sql import SparkSession


MIN_NUM_SENTENCES = 5



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

    def emit_url_to_lines(doc):
        for line in doc["text"].splitlines():
            yield hash_text(line.strip().lower()), (doc["url"], line)

    def merge_duplicate_line_group(a, b):
        if hash_text(a[0]) > hash_text(b[0]):
            a, b = b, a
        a[-1] += b[-1]
        return a

    line_to_selected_url = docs\
        .flatMap(lambda url_doc: emit_url_to_lines(url_doc[1]))\
        .mapValues(lambda x: list(x) + [1])\
        .reduceByKey(lambda a, b: merge_duplicate_line_group(a, b))

    lines_to_keep = line_to_selected_url.map(lambda x: (x[1][0], x[0]))

    # Now, collect both cleaned docs and removed lines
    processed = docs.cogroup(lines_to_keep, numPartitions=1024)\
        .mapValues(lambda x: {"features": list(x[0])[0], "lines": list(x[1])})\
        .flatMap(lambda x: _remove_lines_from_text(list(x), min_num_sentences=min_num_sentences))\
        .cache()

    clean_docs = processed.map(lambda x: x[1])
    removed_lines = processed.flatMap(lambda x: x[2])

    return clean_docs, removed_lines

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", default="./rd_output")
    parser.add_argument("--array_index_lower_bound", type=int, default=0)
    parser.add_argument("--array_index_upper_bound", type=int, default=19)
    args = parser.parse_args()

    spark = SparkSession.builder\
            .appName("Remove duplicate text")\
            .getOrCreate()

    input_files = [
        f"{args.input}/repetition_removed_{i}/clean_docs/part-*"
        for i in range(args.array_index_lower_bound, args.array_index_upper_bound + 1)
    ]

    docs = None
    for file_ in input_files:
        file_rdd = spark.sparkContext.textFile(file_).map(lambda line: json.loads(line))
        docs = file_rdd if docs is None else docs.union(file_rdd)

    docs = docs.repartition(120).cache()

    clean_docs, removed_lines = remove_duplicate_text(docs)

    clean_docs.map(lambda j: json.dumps(j, ensure_ascii=False)) \
        .saveAsTextFile(os.path.join(args.output, "clean_docs"))
    removed_lines.map(lambda j: json.dumps(j, ensure_ascii=False)) \
        .saveAsTextFile(os.path.join(args.output, "removed_lines"))

if __name__ == "__main__":
    main()