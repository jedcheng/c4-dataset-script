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
    
    include_quotes = args.include_quotes
    split = args.split

    detector = CantoneseDetector(split_seg=split, use_quotes=include_quotes)

    spark = SparkSession.builder \
        .appName("Filter Cantonese Entries") \
        .getOrCreate()


    docs = spark.sparkContext.textFile(input).repartition(8)

    if include_quotes:
        filtered = docs.map(lambda x: json.loads(x))\
            .filter(lambda x: is_cantonese_quotes_in_SWC(x['text'], detector))
        filtered.map(lambda x: json.dumps(x, ensure_ascii=False))\
            .saveAsTextFile(os.path.join(output, "clean_docs"))
        
    else:
        filtered = docs.map(lambda x: json.loads(x))\
            .filter(lambda x: is_cantonese(x['text'], detector))
        filtered.map(lambda x: json.dumps(x, ensure_ascii=False))\
            .saveAsTextFile(os.path.join(output, "clean_docs"))

    return 


if __name__ == "__main__":
    main()