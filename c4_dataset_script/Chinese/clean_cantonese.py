import json
import argparse




parser = argparse.ArgumentParser(description="Clean Cantonese dataset")
parser.add_argument("--input_file", type=str, help="Path to the input JSONL file")
parser.add_argument("--output_file", type=str, help="Path to the output JSONL file")
args = parser.parse_args()


domains_to_remove = [
    "wikipedia",
    "lihkg",
]

# with open("cantonese.jsonl", "r", encoding="utf-8") as f:
with open(args.input_file, "r", encoding="utf-8") as f:
    lines = f.readlines()
    lines = [json.loads(line) for line in lines]


cleaned_lines = []

for line in lines:
    include = True

    for domain in domains_to_remove:
        if domain in line["url"]:
            include = False
            break

    if include:
        cleaned_lines.append(line)


# with open("cantonese_cleaned.jsonl", "w", encoding="utf-8") as f:
with open(args.output_file, "w", encoding="utf-8") as f:
    for line in cleaned_lines:
        f.write(json.dumps(line, ensure_ascii=False) + "\n")
    
print(f"Original number of lines: {len(lines)}")
print(f"Removed {len(lines) - len(cleaned_lines)} lines")
print(f"Number of lines after cleaning: {len(cleaned_lines)}")