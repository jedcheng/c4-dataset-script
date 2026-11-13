# Copyright (c) 2022 Jianbin Chang
import ahocorasick
import argparse
import json
import gzip
import io
import logging
import time
import os

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

class CharSetCounter:
    """Counts occurrences of a set of single characters. Same count as an
    Aho-Corasick automaton of single-character keys, but runs in C via
    str.translate (~10x faster)."""

    def __init__(self, chars):
        self.table = {ord(c): None for c in chars}

    def count(self, text):
        return len(text) - len(text.translate(self.table))


def build_sc_matcher(sc_words_list):
    """CharSetCounter if every SC entry is one character, else an automaton."""
    words = {word.strip() for word in sc_words_list if word.strip()}
    if all(len(w) == 1 for w in words):
        return CharSetCounter(words)
    return build_badword_automaton(words)


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
# 」』 close quotes in Traditional Chinese, ⋯ is the TW/HK ellipsis, ～ is common in
# informal writing. Half-width !? (LOOSE_ENDINGS) also end many informal TC/Cantonese
# lines but end English boilerplate too ("Got it!"), so those lines must be mostly CJK.
ENDING_PUNCTUATIONS = ("。", "！", "？", "……", "”", "：", "」", "』", "⋯", "～")
LOOSE_ENDINGS = ("!", "?")
LOOSE_MIN_CJK = 0.4
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
        if not (line.endswith(LOOSE_ENDINGS)
                and len(CJK_PATTERN.findall(line)) >= LOOSE_MIN_CJK * len(line)):
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
        sc_automaton (ahocorasick.Automaton | CharSetCounter): from build_sc_matcher.
        ratio_threshold (float): The ratio of SC characters to total characters to exceed.
        
    Returns:
        bool: True if the ratio is exceeded, False otherwise.
    """
    clean_text_len = len(doc_text)

    if clean_text_len == 0:
        return False

    if isinstance(sc_automaton, CharSetCounter):
        return (sc_automaton.count(doc_text) / clean_text_len) > ratio_threshold

    # Stop at the first match that crosses the threshold: most "zho" pages are
    # Simplified and would otherwise have every SC character counted.
    sc_character_count = 0
    for _, length in sc_automaton.iter(doc_text):
        sc_character_count += length
        if (sc_character_count / clean_text_len) > ratio_threshold:
            return True

    return False



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
            response = requests.get(*args, **kwargs, timeout=600)
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


CJK_PATTERN = re.compile(r'[\u4e00-\u9fff]')
KANA_PATTERN = re.compile(r'[\u3040-\u30ff]')
MAX_KANA = 0.05  # Japanese pages pass the CJK density test on their kanji alone

def is_chinese(text, min_density=0.4):
    # CJK characters are never ASCII, so a text with too few non-ASCII characters
    # cannot reach the density. Counting them is one C call and rejects almost every
    # non-CJK page before the regex scan (same result, ~2x faster on pre-2018 crawls).
    n = len(text)
    if n and n - len(text.encode("ascii", "ignore")) < min_density * n:
        return False

    # Use the pre-compiled pattern
    chinese_chars = CJK_PATTERN.findall(text)
    
    density = len(chinese_chars) / len(text)
    if density < min_density:
        return False
    return len(KANA_PATTERN.findall(text)) <= MAX_KANA * n


def filter_wet_pages(
    wet_file,
    badwords,
    sc_words,
    chinese_filtering=True,
    bad_words_ratio=0.05,
    filter_simplified_chinese=True,
    SC_words_ratio=0.01,
    desc="split_wet_file",
):
    """Yield the filtered Chinese pages of one (gzipped) WET file object.

    May raise EOFError / gzip.BadGzipFile on a truncated download."""
    for page in tqdm(split_wet_file(wet_file), desc=desc):
        if chinese_filtering:
            if "content_language" not in page:
                try:
                    is_chinese_text = is_chinese(page["text"])
                except Exception as e:
                    logging.error(f"Error detecting language for {page['url']}: {e}")
                    continue

                if not is_chinese_text:
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
                yield page


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
    sc_words = build_sc_matcher(SC_words_list)

    logging.basicConfig(level=logging.ERROR)

    for _ in range(10):
        response = request_with_retry(connection_reset_retry=100, url=f"{CC_DOMAIN}/{cc_path}")
        download_file = io.BytesIO(response.content)
        try:
            page_list = list(filter_wet_pages(download_file, badwords, sc_words,
                chinese_filtering=chinese_filtering,
                bad_words_ratio=bad_words_ratio,
                filter_simplified_chinese=filter_simplified_chinese,
                SC_words_ratio=SC_words_ratio,
                desc=f"split_wet_file {cc_path}"))
            break
        except (EOFError, gzip.BadGzipFile):
            page_list = []
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

    from pyspark.sql import SparkSession

    spark = SparkSession.builder\
            .appName("Download Chinese web docs")\
            .getOrCreate()

    cc_paths = []
    for wet_path in args.wet_paths:
        for cc_path in read_wet_paths_file(wet_path):
            cc_paths.append(cc_path)
    
    
    array_index = args.array_index
    cc_paths = cc_paths[int(array_index * 5000):int((array_index + 1) * 5000)]
    # cc_paths = cc_paths[0:100]

    output_dir_base = os.path.join(args.output, str(array_index))

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
