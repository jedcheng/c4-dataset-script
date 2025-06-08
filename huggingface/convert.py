import pandas as pd
import json
import argparse
from os.path import join

def jsonl_to_parquet(input_file, output_file):

    
    # Read JSONL file line by line and convert to list of dictionaries
    data = []
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            data.append(json.loads(line.strip()))


    print(f"Read {len(data)} records from {input_file}")
    
    # Convert to pandas DataFrame
    df = pd.DataFrame(data)
    
    df.to_parquet(output_file, index=False)
    print(f"Converted {input_file} to {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert JSONL to Parquet.')
    parser.add_argument('--input_file', type=str, help='Input JSONL file')
    parser.add_argument('--output_path', type=str, help='Output path')
    parser.add_argument('--output_file', type=str, help='Output Parquet file_name')
    parser.add_argument('--number', type=int)
    parser.add_argument('--total_no_files', type=int)


    args = parser.parse_args()
    input_file = args.input_file
    output_path = args.output_path
    output_file_name = args.output_file
    number = args.number
    total_no_files = args.total_no_files

    output_file = join(output_path, f"{output_file_name}-{str(number+1).zfill(5)}-of-{str(total_no_files).zfill(5)}.parquet")


    jsonl_to_parquet(input_file, output_file)