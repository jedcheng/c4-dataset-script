"""This script filter Cantonese entries in the dataset.

Powered by Cantonese Detect (https://github.com/CanCLID/cantonesedetect)
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
    parser.add_argument("--include_quotes", action="store_true", help="Cantonese Detect can detect if Cantonese quotes are present in the written Chinese text")
    parser.add_argument("--split", action="store_true", help="Cantonese Detect can increase precision (but decrease recall) by splitting the text into sentences and checking each sentence separately")
    args = parser.parse_args()

    return args

from cantonesedetect import CantoneseDetector
from cantonesedetect import JudgementTypes
import pandas as pd

cantonese_types = JudgementTypes.JudgementType.CANTONESE
cantonese_quotes_in_SWC_types = JudgementTypes.JudgementType.CANTONESE_QUOTES_IN_SWC


def is_cantonese(doc_text, detector):
    """Check if a document contains too many simplified Chinese words.""" 
    output = detector.judge(doc_text)

    if output == cantonese_types:
        return True
    else:
        return False
    
def is_cantonese_quotes_in_SWC(doc_text, detector):
    """Check if a document contains too many simplified Chinese words.""" 
    output = detector.judge(doc_text)

    if output == cantonese_quotes_in_SWC_types or output == cantonese_types:
        return True
    else:
        return False

def main():
    args = parse_args()
    input = args.input
    output = args.output
    
    
    split = args.split

    

    spark = SparkSession.builder \
        .appName("Filter Cantonese Entries") \
        .getOrCreate()


    docs = spark.sparkContext.textFile(os.path.join(input, "part-*")).repartition(120)


    detector = CantoneseDetector(split_seg=split, use_quotes=False)

    filtered_no_quotes = docs.map(lambda x: json.loads(x))\
        .filter(lambda x: is_cantonese(x['text'], detector))
    

    detector = CantoneseDetector(split_seg=split, use_quotes=True)
    filtered_with_quotes = docs.map(lambda x: json.loads(x))\
        .filter(lambda x: is_cantonese_quotes_in_SWC(x['text'], detector))

    # Collect and save filtered_no_quotes
    filtered_docs_no_quotes = filtered_no_quotes.map(lambda x: json.dumps(x, ensure_ascii=False)).collect()
    with open(os.path.join(output, "no_quotes.jsonl"), "w", encoding="utf-8") as f:
        for doc in filtered_docs_no_quotes:
            f.write(doc + "\n")
    df_no_quotes = pd.read_json(os.path.join(output, "no_quotes.jsonl"), lines=True)
    df_no_quotes.to_parquet(os.path.join(output, "no_quotes.parquet"), index=False)
    # Collect and save filtered_with_quotes
    filtered_docs_with_quotes = filtered_with_quotes.map(lambda x: json.dumps(x, ensure_ascii=False)).collect()
    with open(os.path.join(output, "with_quotes.jsonl"), "w", encoding="utf-8") as f:
        for doc in filtered_docs_with_quotes:
            f.write(doc + "\n")
    df_with_quotes = pd.read_json(os.path.join(output, "with_quotes.jsonl"), lines=True)
    df_with_quotes.to_parquet(os.path.join(output, "with_quotes.parquet"), index=False)



    # Filter out entries from specific domains. 
    # They should not be obtained via common crawl. 
    # Use other methods to obtain the full set of entries
    exclude_domains = [
        "wikipedia",
        "lihkg",
    ]

    def is_allowed_domain(url):
        for domain in exclude_domains:
            if domain in url:
                return False
        return True

    def filter_by_domain(docs):
        return [doc for doc in docs if is_allowed_domain(json.loads(doc).get("url", ""))]

    # Filter clean_docs_no_quotes by domain
    filtered_docs_no_quotes_domain = filter_by_domain(filtered_docs_no_quotes)
    with open(os.path.join(output, "no_quotes_domain_cleaned.jsonl"), "w", encoding="utf-8") as f:
        for doc in filtered_docs_no_quotes_domain:
            f.write(doc + "\n")
    
    filtered_df_no_quotes_domain = pd.read_json(os.path.join(output, "no_quotes_domain_cleaned.jsonl"), lines=True)
    filtered_df_no_quotes_domain.to_parquet(os.path.join(output, "no_quotes_domain_cleaned.parquet"), index=False)


    # Filter clean_docs_with_quotes by domain
    filtered_docs_with_quotes_domain = filter_by_domain(filtered_docs_with_quotes)
    with open(os.path.join(output, "with_quotes_domain_cleaned.jsonl"), "w", encoding="utf-8") as f:
        for doc in filtered_docs_with_quotes_domain:
            f.write(doc + "\n")
    
    filtered_df_with_quotes_domain = pd.read_json(os.path.join(output, "with_quotes_domain_cleaned.jsonl"), lines=True)
    filtered_df_with_quotes_domain.to_parquet(os.path.join(output, "with_quotes_domain_cleaned.parquet"), index=False)

    return 


if __name__ == "__main__":
    main()