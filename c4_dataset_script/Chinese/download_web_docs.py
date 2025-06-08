# Copyright (c) 2022 Jianbin Chang

import argparse
import json
import gzip
import io
import logging
import time
import os

from pyspark.sql import SparkSession
import langdetect
import requests
from tqdm import tqdm

# import whois

CC_DOMAIN = "https://data.commoncrawl.org"

# WET file constants
_PAGE_DELIMITER = "WARC/1.0"
_URL_KEY = "WARC-Target-URI:"
_URL_DATE = "WARC-Date:"
_CONTENT_TYPE = "Content-Type:"
_CONTENT_LANGUAGE = "WARC-Identified-Content-Language:"
_METADATA_PREFIXES = ("WARC", "CONTENT-", "Content-")


def check_if_gz_file_corrupted(gz_file):
    chunksize = 10 * 1024 ** 2

    with gzip.open(gz_file, 'rb') as f:
        try:
            while f.read(chunksize) != b'':
                pass
            return False
        except:
            return True

import re

def is_bad_line(line):
    ending_punctuations = ["。", "！", "？", "……", "”", "："]
    if not any(line.endswith(punc) for punc in ending_punctuations):
        return True

    if len(line) < 5:
        return True

    ill_word_regex = "[-]|□|■|�"

    if re.search(ill_word_regex, line) != None:
        return True

    return False

def is_bad_doc(doc_text, badwords_list, ratio_threshold=0.05):
    """Check if text contains too many bad words."""
    if not badwords_list:
        return False
        
    bad_words_character_count = 0
    for bad_word in badwords_list:
        if bad_word in doc_text:
            bad_words_character_count += doc_text.count(bad_word) * len(bad_word)
    
    if bad_words_character_count / len(doc_text) > ratio_threshold:
        return True
    return False


def load_word_list(filepath):
    """Load a word list from a file."""
    if not filepath or not os.path.exists(filepath):
        return []
        
    with open(filepath, 'r') as f:
        return [line.strip() for line in f if line.strip()]

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


def split_wet_file(wet_file_path):
    def _validate_features(page):
        feature_list = ["url", "text", "timestamp"]
        for feature_name in feature_list:
            if feature_name not in page:
                return False

        return True

    page = dict()
    for i, line in enumerate(gzip.open(wet_file_path, "rt")):
        line = line.strip()
        if not line:
            continue

        if line == _PAGE_DELIMITER:
            if i > 0 and _validate_features(page):
                yield page
            page = dict()

        if line.startswith(_URL_KEY):
            page["url"] = line[len(_URL_KEY):].strip()

        if line.startswith(_URL_DATE):
            page["timestamp"] = line[len(_URL_DATE):].strip()

        if line.startswith(_CONTENT_TYPE):
            page["content_type"] = line[len(_CONTENT_TYPE):].strip()

        if line.startswith(_CONTENT_LANGUAGE):
            page["content_language"] = line[len(_CONTENT_LANGUAGE):].strip()

        if line.startswith(_METADATA_PREFIXES):
            continue

        if "text" in page:
            page["text"] += "\n"
        page["text"] = page.get("text", "") + line

    if _validate_features(page):
        yield page


def request_with_retry(connection_reset_retry=100, *args, **kwargs):
    retries = 0
    while True:
        try:
            response = requests.get(*args, **kwargs, timeout=3600)
            if response.status_code == 503:
                raise requests.exceptions.RequestException("503")
            return response
        except:
            if retries > connection_reset_retry:
                logging.info(f"{args}")
                raise
            time.sleep(2 * retries)
            retries += 1



def filter_and_process_text(text, badwords_list=None, 
                            bad_words_ratio=0.05,
                            filter_simplified_chinese=True,
                            SC_words_list=None, 
                            SC_words_ratio=0.01):
    """Filter lines from text and determine if document passes filters."""
    # Check if document has too many bad words
    if is_bad_doc(text, badwords_list, bad_words_ratio):
        return None


    if filter_simplified_chinese:
        if is_SC_doc(text, SC_words_list, SC_words_ratio):
            return None            

    # Process and filter individual lines
    output_lines = []
    for line in text.splitlines():
        line = line.strip()
        if not is_bad_line(line):
            output_lines.append(line)
    
    # If too few lines remain, reject the document
    if len(output_lines) <= 5:
        return None
        
    return "\n".join(output_lines)


def download_and_package(
    cc_path,
    badwords_list=None,
    chinese_filtering=True,
    bad_words_ratio=0.05,
    filter_simplified_chinese=True,
    SC_words_list=None,
    SC_words_ratio=0.01
    
):
    logging.basicConfig(level=logging.ERROR)

    for _ in range(10):
        response = request_with_retry(connection_reset_retry=100, url=f"{CC_DOMAIN}/{cc_path}")
        download_file = io.BytesIO(response.content)
        page_list = []
        try:
            for page in tqdm(split_wet_file(download_file), desc=f"split_wet_file {download_file}"):
                if chinese_filtering:
                    if "content_language" not in page:
                        try:
                            language = langdetect.detect(page["text"])
                        except langdetect.lang_detect_exception.LangDetectException:
                            continue
                        if language not in ["zh-tw", "zh-cn"]:
                            continue
                    elif "zho" not in page["content_language"].split(","):
                        continue

                    filtered_text = filter_and_process_text(page["text"],  badwords_list=badwords_list, 
                        bad_words_ratio=bad_words_ratio,
                        filter_simplified_chinese=filter_simplified_chinese,
                        SC_words_list=SC_words_list,
                        SC_words_ratio=SC_words_ratio)
                    
                    if filtered_text:
                        page["text"] = filtered_text
                        page_list.append(page)
            break
        except (EOFError, gzip.BadGzipFile):
            continue

    for page in page_list:
        yield page



    

def read_wet_paths_file(filepath):
    for line in gzip.open(filepath, "rt"):
        cc_path = line.strip()
        yield cc_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--wet-paths", nargs="+", required=True)
    parser.add_argument("--output", required=True)
    # parser.add_argument("--spark-sub-job", default=10, type=int,
    #     help="From the data dimention, divide the spark job into sub-jobs, reducing the loss of job failed.")
    parser.add_argument("--array_index", default=0, type=int)
    parser.add_argument("--badwords_filepath", default=None,
        help="Path to file containing bad words to filter out")
    parser.add_argument("--bad_words_ratio", default=0.05, type=float,
    help="Threshold ratio for filtering documents with bad words")
    parser.add_argument("--simplified_chinese_filtering", action="store_true",
        help="Whether to filter out documents that are not in simplified Chinese")
    parser.add_argument("--SC_words_filepath", default=None,
        help="The file path of the toxic word dictionary, if you set this "
        "argument, the program will filter out which document has over limit of"
        " toxic word count. The format of the dictionary file is one word per"
        "line."
    )
    parser.add_argument("--SC_words_ratio", default=0.01, type=float,
        help="Document filtering conditions, when the number of SC words in the document exceeds this ratio, it will be screened out.")

    
    args = parser.parse_args()

    spark = SparkSession.builder\
            .appName("Download Chinese web docs")\
            .getOrCreate()

    cc_paths = []
    for wet_path in args.wet_paths:
        for cc_path in read_wet_paths_file(wet_path):
            cc_paths.append(cc_path)
    
    
    array_index = args.array_index
    cc_paths = cc_paths[int(array_index * 5000):int((array_index + 1) * 5000)]
    
    
    output_dir_base = args.output + "_" + str(array_index)

    # if not os.path.exists(output_dir_base):
    #     os.makedirs(output_dir_base)
    

    rdd = spark.sparkContext.parallelize(cc_paths)\
        .repartition(4096)\
        .flatMap(lambda cc_path: download_and_package(cc_path, 
            badwords_list=load_word_list(args.badwords_filepath), 
            chinese_filtering=True,
            bad_words_ratio=args.bad_words_ratio,
            filter_simplified_chinese=args.simplified_chinese_filtering,
            SC_words_list=load_word_list(args.SC_words_filepath),
            SC_words_ratio=args.SC_words_ratio))\
        .map(lambda page: json.dumps(page, ensure_ascii=False))
    
    rdd.saveAsTextFile(output_dir_base)


if __name__ == "__main__":
    main()
