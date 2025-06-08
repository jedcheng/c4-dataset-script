import os
import json
import pyarrow 
import argparse
from glob import glob
import pyarrow.parquet as pq

def read_jsonl_files(jsonl_files):
    for index, file_path in enumerate(jsonl_files):
        print(f"Reading file {index + 1}/{len(jsonl_files)}: {file_path}")
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                yield json.loads(line)

def chunked_iterable(iterable, chunk_size):
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk

def main(input_path, output_path, num_parquet, output_prefix):
    jsonl_files = glob(os.path.join(input_path, 'part*'))
    jsonl_files.sort()  

    print(f"Found {len(jsonl_files)} JSONL files in {input_path}")

    all_records = list(read_jsonl_files(jsonl_files))
    total = len(all_records)
    
    print(f"Total records read: {total}")

    chunk_size = (total + num_parquet - 1) // num_parquet  

    for idx, chunk in enumerate(chunked_iterable(all_records, chunk_size)):
        table = pyarrow.Table.from_pylist(chunk)
        out_path = os.path.join(output_path, f"{output_prefix}-{str(idx+1).zfill(5)}-of-{str(num_parquet).zfill(5)}.parquet")
        pq.write_table(table, out_path)
        print(f"Wrote {len(chunk)} records to {out_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", type=str, required=True, help="Input directory containing JSONL files")
    parser.add_argument('--output_path', type=str, help='Output path')
    parser.add_argument("--num_parquet", type=int, required=True, help="Number of output parquet files")
    parser.add_argument('--output_file', type=str, help='Output Parquet file_name')
    args = parser.parse_args()


    main(args.input_path, args.output_path, args.num_parquet, args.output_file)