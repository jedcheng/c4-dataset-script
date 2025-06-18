# Copyright (c) 2022 Jianbin Chang
import ahocorasick
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

def build_sc_automaton(sc_words_list):
    """Builds and returns an Aho-Corasick automaton from a word list."""
    A = ahocorasick.Automaton()
    # Use a set to handle duplicates and strip whitespace
    clean_words = {word.strip() for word in sc_words_list if word.strip()}
    for index, keyword in enumerate(clean_words):
        A.add_word(keyword, len(keyword)) # Store word length for efficiency
    A.make_automaton()
    return A

def build_badword_automaton(badwords_list):
    """Builds and returns an Aho-Corasick automaton for case-sensitive matching."""
    A = ahocorasick.Automaton()
    # Use a set to remove duplicates and ignore empty strings
    bad_words = {word.strip() for word in badwords_list if word.strip()}
    for index, keyword in enumerate(bad_words):
        # Store the length of the keyword as the value, which we can sum up later.
        A.add_word(keyword, len(keyword))
    A.make_automaton()
    return A


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

ILL_WORD_REGEX = re.compile("[-□■�]") # Simplified the regex pattern as well

# 2. Define constants outside the function.
# 3. Use a tuple for `endswith`, which is faster than iterating a list.
ENDING_PUNCTUATIONS = ("。", "！", "？", "……", "”", "：")
MIN_LINE_LENGTH = 5

def is_bad_line(line: str) -> bool:
    """
    Optimized check for a bad line.
    Checks are ordered from cheapest to most expensive for fast short-circuiting.
    """
    # 4. Check cheapest condition first (length).
    if len(line) < MIN_LINE_LENGTH:
        return True

    # 5. Check next cheapest (endswith with a tuple).
    if not line.endswith(ENDING_PUNCTUATIONS):
        return True

    # 6. Check most expensive condition last (regex search).
    #    Using the pre-compiled regex object.
    #    The `if regex.search(line):` is a more idiomatic check than `!= None`.
    if ILL_WORD_REGEX.search(line):
        return True

    return False

def is_bad_doc(doc_text, badword_automaton, ratio_threshold=0.05):
    """Efficiently check for bad words using a pre-built Aho-Corasick automaton."""
    doc_len = len(doc_text)
    if doc_len == 0:
        return False
    
    # The .iter() method finds all non-overlapping matches in one pass.
    # The `value` we get is the length of the bad word we stored earlier.
    total_bad_chars = sum(value for end_index, value in badword_automaton.iter(doc_text))

    return (total_bad_chars / doc_len) > ratio_threshold

def load_word_list(filepath):
    """Load a word list from a file."""
    if not filepath or not os.path.exists(filepath):
        return []
        
    with open(filepath, 'r') as f:
        return [line.strip() for line in f if line.strip()]



def is_SC_doc(doc_text, sc_automaton, ratio_threshold):
    """
    Efficiently checks if a document contains a high ratio of Simplified Chinese words.
    
    Args:
        doc_text (str): The input HTML document text.
        sc_automaton (ahocorasick.Automaton): The pre-built automaton.
        ratio_threshold (float): The ratio of SC characters to total characters to exceed.
        
    Returns:
        bool: True if the ratio is exceeded, False otherwise.
    """
    clean_text_len = len(doc_text)

    if clean_text_len == 0:
        return False

    sc_character_count = sum(result[1] for result in sc_automaton.iter(doc_text))

    return (sc_character_count / clean_text_len) > ratio_threshold



def split_wet_file(wet_file_path):
    """
    Optimized WET file parser using a state machine and match-case.
    - Reduces string operations per line by tracking state (header vs. content).
    - Avoids running expensive checks for every content line.
    - Uses match-case for cleaner header parsing (Python 3.10+).
    """
    def _validate_features(page):
        return "url" in page and "text" in page and "timestamp" in page

    page = {}
    content_lines = []
    in_header = True

    with gzip.open(wet_file_path, "rt", encoding='utf-8', errors='ignore') as f:
        for line in f:
            if line.startswith(_PAGE_DELIMITER):
                # This delimiter marks the end of the previous record's content
                # and the start of a new record's header.
                if page and content_lines:
                    page["text"] = "\n".join(content_lines)
                    if _validate_features(page):
                        yield page
                
                # Reset for the new record
                page = {}
                content_lines = []
                in_header = True
                continue

            if in_header:
                # The header is separated from the content by a blank line.
                # Using match-case for cleaner parsing of header lines.
                match line:
                    case '\n':
                        in_header = False
                    case _ if line.startswith(_URL_KEY):
                        page["url"] = line[len(_URL_KEY):].strip()
                    case _ if line.startswith(_URL_DATE):
                        page["timestamp"] = line[len(_URL_DATE):].strip()
                    case _ if line.startswith(_CONTENT_TYPE):
                        page["content_type"] = line[len(_CONTENT_TYPE):].strip()
                    case _ if line.startswith(_CONTENT_LANGUAGE):
                        page["content_language"] = line[len(_CONTENT_LANGUAGE):].strip()
            else: # We are in the content part of the record
                content_lines.append(line.strip())

    # Yield the very last record in the file if it exists
    if page and content_lines:
        page["text"] = "\n".join(content_lines)
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


def filter_and_process_text(text, badwords=None, 
                            bad_words_ratio=0.05,
                            filter_simplified_chinese=True,
                            sc_words=None, 
                            SC_words_ratio=0.01):
    """Filter lines from text and determine if document passes filters."""
    # Check if document has too many bad words
    if is_bad_doc(text, badwords, bad_words_ratio):
        return None


    if filter_simplified_chinese:
        if is_SC_doc(text, sc_words, SC_words_ratio):
            return None            

    # Process and filter individual lines
    stripped_lines = (line.strip() for line in text.splitlines())
    output_lines = [line for line in stripped_lines if not is_bad_line(line)]
    
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
    badwords = build_badword_automaton(badwords_list)
    sc_words = build_badword_automaton(SC_words_list)

    logging.basicConfig(level=logging.ERROR)

    i = 0
    for _ in range(10):
        response = request_with_retry(connection_reset_retry=100, url=f"{CC_DOMAIN}/{cc_path}")
        download_file = io.BytesIO(response.content)
        page_list = []
        try:
            for page in tqdm(split_wet_file(download_file), desc=f"split_wet_file {cc_path}"):
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
                    filtered_text = filter_and_process_text(page["text"],  badwords=badwords, 
                        bad_words_ratio=bad_words_ratio,
                        filter_simplified_chinese=filter_simplified_chinese,
                        sc_words=sc_words,
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
    # cc_paths = cc_paths[0:5]

    output_dir_base = args.output + "_" + str(array_index)

    # if not os.path.exists(output_dir_base):
    #     os.makedirs(output_dir_base)

    rdd = spark.sparkContext.parallelize(cc_paths)\
        .repartition(5000)\
        .flatMap(lambda cc_path: download_and_package(cc_path, 
            badwords_list=load_word_list(args.badwords_filepath), 
            chinese_filtering=True,
            bad_words_ratio=args.bad_words_ratio,
            filter_simplified_chinese=args.simplified_chinese_filtering,
            SC_words_list=load_word_list(args.SC_words_filepath),
            SC_words_ratio=args.SC_words_ratio))\
        .map(lambda page: json.dumps(page, ensure_ascii=False))
    
    # rdd.saveAsTextFile(output_dir_base)
    collected_lines = rdd.collect()

    output_file = f"{args.output}/clean_docs_{args.array_index}.jsonl"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(collected_lines))
        # Ensure the file ends with a newline for POSIX compliance
        if collected_lines:
            f.write('\n')



if __name__ == "__main__":
    main()
