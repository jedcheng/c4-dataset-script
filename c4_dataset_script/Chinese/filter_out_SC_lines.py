"""This script filter out non-sentence lines and toxic text.

```bash
cat docs.jsonl | python filter_out_SC_lines.py --SC_words_filepath ../badwords/SC_list.txt > clean_docs.jsonl
```
"""

import argparse
import sys
import json
import re
import gzip

from tqdm import tqdm


def parse_args():
    parser = argparse.ArgumentParser("Filter out SC lines.")
    parser.add_argument("--SC_words_filepath", default=None,
        help="The file path of the toxic word dictionary, if you set this "
        "argument, the program will filter out which document has over limit of"
        " toxic word count. The format of the dictionary file is one word per"
        "line."
    )
    parser.add_argument("--output_SC_lines", default="SC_lines.jsonl.zst",
        help="output file for SC lines")
    parser.add_argument("--SC_words_ratio", default=0.1, type=float,
        help="Document filtering conditions, when the number of SC words in the document exceeds this ratio, it will be screened out.")

    args = parser.parse_args()

    return args





def is_SC_doc(args, doc, SCwords_filepath):
    SC_words_character_count = 0
    for SC_word in open(SCwords_filepath):
        SC_word = SC_word.strip()
        if SC_word in doc:
            SC_words_character_count += doc.count(SC_word) * len(SC_word)

    if SC_words_character_count / len(doc) > args.SC_words_ratio:
        return True

    return False


def main():
    args = parse_args()
    SC_lines_file = gzip.open(args.output_SC_lines, "wt")

    for line in tqdm(sys.stdin):
        try:
            j = json.loads(line)
        except:
            continue

        # if args.SC_words_filepath is not None:
        #     if is_SC_doc(args, j["text"], args.SC_words_filepath):
        #         print(json.dumps(j, ensure_ascii=False), file=SC_lines_file)
        #         continue

        output = []
        SC_lines = []
        for line in j["text"].splitlines():
            line = line.strip()
            
            output.append(line)

        if len(output) > 5:
            j["text"] = '\n'.join(output)
            print(json.dumps(j, ensure_ascii=False))
        else:
            SC_lines += output

        if len(SC_lines) > 0:
            j["text"] = '\n'.join(SC_lines)
            print(json.dumps(j, ensure_ascii=False), file=SC_lines_file)


if __name__ == "__main__":
    main()
