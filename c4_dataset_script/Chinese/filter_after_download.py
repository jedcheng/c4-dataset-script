import glob
import json
import os
import sys
import random

def filter_jsonl_files(input_dir, bad_words, output_file):
    # Find all .jsonl files in the directory (recursively)
    jsonl_files = glob.glob(os.path.join(input_dir, 'part-*'))
    jsonl_files = sorted(jsonl_files)
    filtered_entries = []

    # Prepare set for faster lookup
    bad_words_set = set(bad_words)

    for file_path in jsonl_files:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    entry = json.loads(line)
                    entry_str = entry["text"]
                    if not any(bad_word in entry_str for bad_word in bad_words_set):
                        filtered_entries.append(entry)

                    else:
                        # Randomly sample 10% of entries containing bad words
                        if random.random() < 0.01:
                            print(f"Skipping entry with bad word in {file_path}: {entry_str[:50]}...")  # Print first 50 chars
                except Exception:
                    continue  # skip malformed lines

    print(f"Filtered {len(filtered_entries)} entries from {len(jsonl_files)} files.")

    # Write filtered entries to output_file
    with open(output_file, 'w', encoding='utf-8') as out_f:
        for entry in filtered_entries:
            out_f.write(json.dumps(entry, ensure_ascii=False) + '\n')

if __name__ == "__main__":
    # Usage: python filter_after_download.py <input_dir> <output_file>
    # Define your list of bad words here
    BAD_WORDS = ['瑪卡', '葉黃素'] 

    if len(sys.argv) != 3:
        print("Usage: python filter_after_download.py <input_dir> <output_file>")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_file = sys.argv[2]
    filter_jsonl_files(input_dir, BAD_WORDS, output_file)