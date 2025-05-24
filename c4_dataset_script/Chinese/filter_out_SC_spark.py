"""This script filter out simplified Chinese entries in the dataset.

```bash
spark-submit --master "local[*]" [filter_out_SC_lines.py](http://_vscodecontentref_/1) --SC_words_filepath ../badwords/SC_list.txt --input docs.jsonl --output clean_docs
"""

import argparse 
import sys 
import json 
import re 
import gzip 
import os 
from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser("Filter out SC lines.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--SC_words_filepath", default=None,
        help="The file path of the toxic word dictionary, if you set this "
        "argument, the program will filter out which document has over limit of"
        " toxic word count. The format of the dictionary file is one word per"
        "line."
    )
    parser.add_argument("--SC_words_ratio", default=0.01, type=float,
        help="Document filtering conditions, when the number of SC words in the document exceeds this ratio, it will be screened out.")

    args = parser.parse_args()

    return args





def is_SC_doc(doc_text, SC_words_list, ratio_threshold): 
    """Check if a document contains too many simplified Chinese words.""" 
    SC_words_character_count = 0 

    for SC_word in SC_words_list: 
        SC_word = SC_word.strip()
        if SC_word in doc_text: 
            SC_words_character_count += doc_text.count(SC_word)* len(SC_word)

    if SC_words_character_count / len(doc_text) > ratio_threshold:
        return True
    return False


def main():
    args = parse_args()
    input = args.input
    output = args.output
    SC_words_filepath = args.SC_words_filepath
    SC_words_ratio = args.SC_words_ratio

    with open(SC_words_filepath, 'r', encoding='utf-8') as f:
        SC_words_list = [line.strip() for line in f.readlines()]

    spark = SparkSession.builder \
        .appName("Filter out SC lines") \
        .getOrCreate()


    docs = spark.sparkContext.textFile(input).repartition(32)

    filtered = docs.map(lambda x: json.loads(x))\
        .filter(lambda x: not is_SC_doc(x['text'], SC_words_list, SC_words_ratio))


    filtered = filtered.map(lambda x: json.dumps(x, ensure_ascii=False))
    filtered.saveAsTextFile(os.path.join(output, "clean_docs"))

    return 


if __name__ == "__main__":
    main()