"""
Adventure into implementing a better duplicate and boilerplate line removal script for Chinese text.
But it is just not good enough yet.
"""

import argparse
import hashlib
import os
import json
import re
from pyspark.sql import SparkSession

MIN_NUM_SENTENCES = 5
BOILERPLATE_THRESHOLD = 1
MIN_BOILERPLATE_LENGTH = 15
LONG_LINE_LENGTH = 15
LONG_LINE_THRESHOLD = 2

# Regexes for common boilerplate and copyright lines
BOILERPLATE_REGEXES = [
    # General copyright/legal
    re.compile(r"(版權所有|不得轉載|隱私權政策|服務條款|如有侵權|僅供(學習|參考|交流)|來源[:：]|文章來源|本網站.*?不承擔任何責任|法律聲明|免責聲明)", re.I),
    # Expanded copyright/learning/notice lines (catch more verbose forms)
    re.compile(r"(版權.*?歸.*?所有|僅供.*?(學習|交流|參考).*?如.*?侵權|如.*?侵權.*?請.*?(通知|聯繫)|我們將.*?及時.*?(刪除|處理)|轉載.*?僅供.*?(學習|交流|參考))", re.I),
    # Cookie/Javascript
    re.compile(r"(cookie|cookies|javascript).*?(啟用|開啟|使用|無法|體驗|服務|質素|品質|功能|設定|接受|同意)", re.I),
    # About/contact
    re.compile(r"(聯絡我們|關於我們|聯繫方式|聯絡方式)", re.I),
    # Shopping/membership
    re.compile(r"(購物車|shopping\s*cart|會員.*?(專屬|權益|優惠|福利|好禮)|加入會員|註冊會員|成為會員|立即登入|立即註冊)", re.I),
    # Navigation
    re.compile(r"(上一篇|下一篇|返回頂部|回到頂部|閱讀全文|查看更多|更多文章|相關文章|Previous\s*Article|Next\s*Article|Previous\s*Post|Next\s*Post|Prev\s*Article|Prev\s*Post|Read\s*More|More\s*Articles|Related\s*Articles)", re.I),
    # Social media
    re.compile(r"(分享到|分享至|微信|微博|Facebook|Twitter|Line|Instagram|關注我們)", re.I),
    # Website infrastructure/credits
    re.compile(r"(Powered by|技術支持|網站設計|系統提供)", re.I),
    # Login/Register prompts (often near comments or forms)
    re.compile(r"(登入|註冊|Login|Register|Sign in|Sign up).*?(發表|留言|評論|參與)", re.I),
    # Search related
    re.compile(r"(搜索|搜尋|站內搜索|站內搜尋|關鍵字)", re.I),
    # Advertisement disclaimers
    re.compile(r"(廣告|推廣|贊助商連結|AD)", re.I),
    # Common forum/comment actions
    re.compile(r"(回覆|引用|編輯|刪除|舉報|檢舉)", re.I),
    # Print/Email page
    re.compile(r"(打印本頁|電郵本文|友善列印)", re.I),
    # Common spam ads in case they slip through
    re.compile(r"(免費|賺錢|投資|賭博|成人|色情|藥品|減肥|增大|增長|保健品|保健食品|瑪卡|馬卡)", re.I),
]

def parse_args():
    parser = argparse.ArgumentParser("Remove duplicate and boilerplate lines.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--array_index_lower_bound", type=int, default=0)
    parser.add_argument("--array_index_upper_bound", type=int, default=19)
    return parser.parse_args()

def hash_text(text):
    return hashlib.md5(text.encode("utf-8")).hexdigest()

def emit_url_to_lines(doc):
    for line in doc["text"].splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        yield hash_text(stripped.lower()), (doc["url"], stripped)

def get_boilerplate_sets(docs, boilerplate_threshold, min_boilerplate_length, long_line_length, long_line_threshold):
    # Count in how many documents each line appears
    line_doc_counts = docs.flatMap(lambda url_doc: set(emit_url_to_lines(url_doc[1]))) \
        .map(lambda x: (x[0], 1)) \
        .reduceByKey(lambda a, b: a + b)

    # Short line boilerplate
    boilerplate_lines = line_doc_counts \
        .filter(lambda x: x[1] >= boilerplate_threshold) \
        .map(lambda x: x[0]) \
        .collect()
    boilerplate_set = set(boilerplate_lines)

    # Long line boilerplate 
    long_boilerplate_lines = line_doc_counts \
        .filter(lambda x: x[1] >= long_line_threshold) \
        .map(lambda x: x[0]) \
        .collect()
    long_boilerplate_set = set(long_boilerplate_lines)

    return boilerplate_set, long_boilerplate_set


def is_boilerplate(line, boilerplate_set, long_boilerplate_set, min_boilerplate_length, long_line_length):
    stripped = line.strip()
    lowered = stripped.lower()

    # Regex match for boilerplate
    for pattern in BOILERPLATE_REGEXES:
        if pattern.search(stripped):
            return True
    # Hash-based: short and frequent
    if len(stripped) < min_boilerplate_length:
        return hash_text(lowered) in boilerplate_set
    
    # Hash-based: long and extremely frequent 
    if len(stripped) >= long_line_length:
        return hash_text(lowered) in long_boilerplate_set
    return False


def filter_boilerplate(doc, boilerplate_set, long_boilerplate_set, min_num_sentences, min_boilerplate_length, long_line_length):
    lines = doc["text"].splitlines()
    new_lines = []
    removed = []
    for line in lines:
        # Remove first line if it matches the pattern: 【記者林重鎣台中報導】（中央社記者潘姿羽台北3日電）
        if len(new_lines) == 0:
            if re.match(r"^[【（][^】）]{0,30}[】）]", line):
                removed.append({"url": doc.get("url", ""), "line": line})
                continue
        if is_boilerplate(line, boilerplate_set, long_boilerplate_set, min_boilerplate_length, long_line_length):
            removed.append({"url": doc.get("url", ""), "line": line})
        else:
            new_lines.append(line)
    if len(new_lines) < min_num_sentences:
        return None, removed
    doc = doc.copy()
    doc["text"] = "\n".join(new_lines)
    return doc, removed

def remove_duplicate_text(
    docs,
    min_num_sentences=MIN_NUM_SENTENCES,
    boilerplate_threshold=BOILERPLATE_THRESHOLD,
    min_boilerplate_length=MIN_BOILERPLATE_LENGTH,
    long_line_length=LONG_LINE_LENGTH,
    long_line_threshold=LONG_LINE_THRESHOLD,
):
    docs = docs.map(lambda doc: (doc["url"], doc))
    boilerplate_set, long_boilerplate_set = get_boilerplate_sets(
        docs, boilerplate_threshold, min_boilerplate_length, long_line_length, long_line_threshold
    )
    filtered_and_removed = docs.map(lambda x: x[1]) \
        .map(lambda doc: filter_boilerplate(
            doc, boilerplate_set, long_boilerplate_set,
            min_num_sentences, min_boilerplate_length, long_line_length
        )) \
        .filter(lambda x: x[0] is not None)
    filtered_docs = filtered_and_removed.map(lambda x: x[0])
    removed_lines = filtered_and_removed.flatMap(lambda x: x[1])
    return filtered_docs, removed_lines

def main():
    args = parse_args()
    spark = SparkSession.builder.appName("Remove duplicate text").getOrCreate()

    input_files = [
        f"{args.input}/repetition_removed_{i}/clean_docs/part-*"
        for i in range(args.array_index_lower_bound, args.array_index_upper_bound + 1)
    ]

    docs = None
    for file_ in input_files:
        file_rdd = spark.sparkContext.textFile(file_).map(lambda line: json.loads(line))
        docs = file_rdd if docs is None else docs.union(file_rdd)

    docs = docs.repartition(120).cache()
    print(f"Total number of documents: {docs.count()}")

    clean_docs, removed_lines = remove_duplicate_text(docs)

    clean_docs.map(lambda j: json.dumps(j, ensure_ascii=False)) \
        .saveAsTextFile(os.path.join(args.output, "clean_docs"))
    removed_lines.map(lambda j: json.dumps(j, ensure_ascii=False)) \
        .saveAsTextFile(os.path.join(args.output, "removed_lines"))

if __name__ == "__main__":
    main()
