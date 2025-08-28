#!/usr/bin/env python3
"""
Automated script to convert Common Crawl JSONL files to Parquet format for all dumps in a given year.
This script replaces the bash script functionality and directly runs the conversion process.
"""

import os
import subprocess
import argparse
import glob
from pathlib import Path
import re
import concurrent.futures
from typing import Dict
import time

def get_dump_info(base_path: str, year: str) -> Dict[str, int]:
    """
    Scan the directory for all dumps in a given year and count files in each dump.
    
    Args:
        base_path: Base path to the c4_dataset_script directory
        year: Year to process (e.g., '2021')
    
    Returns:
        Dictionary mapping dump_id to number of files
    """
    dump_info = {}
    
    # Find all directories matching the pattern 1_download_{year}_*
    pattern = f"1_download_{year}_*"
    dump_dirs = glob.glob(os.path.join(base_path, pattern))
    
    for dump_dir in dump_dirs:
        # Extract dump ID from directory name
        dir_name = os.path.basename(dump_dir)
        match = re.match(r'1_download_(\d{4}_\d{2})', dir_name)
        if match:
            dump_id = match.group(1)
            
            # Count JSONL files in the directory
            jsonl_files = glob.glob(os.path.join(dump_dir, "clean_docs_*.jsonl"))
            if jsonl_files:
                # Get the highest numbered file to determine total count
                max_num = max(int(re.search(r'clean_docs_(\d+)\.jsonl', f).group(1)) for f in jsonl_files)
                dump_info[dump_id] = max_num
                print(f"Found dump {dump_id} with {max_num + 1} files (0-{max_num})")
    
    return dump_info

def run_conversion(dump_id: str, file_num: int, total_files: int, 
                  base_path: str, output_base: str, python_path: str) -> bool:
    """
    Run conversion for a single file using Python subprocess.
    
    Args:
        dump_id: Dump ID
        file_num: File number to convert
        total_files: Total number of files
        base_path: Base path to the dataset
        output_base: Base output path
        python_path: Path to the Python executable
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Prepare paths
        input_file = os.path.join(base_path, f"1_download_{dump_id}/clean_docs_{file_num}.jsonl")
        output_path = os.path.join(output_base, f"zh_uncleaned/{dump_id}")
        
        # Check if input file exists
        if not os.path.exists(input_file):
            print(f"Warning: Input file does not exist: {input_file}")
            return False
        
        # Create output directory if it doesn't exist
        os.makedirs(output_path, exist_ok=True)
        
        # Run the conversion
        cmd = [
            python_path, "convert.py",
            "--input_file", input_file,
            "--output_path", output_path,
            "--output_file", f"{dump_id}_CC_zh_not_cleaned",
            "--number", str(file_num),
            "--total_no_files", str(total_files)
        ]
        
        print(f"Converting {dump_id} file {file_num}/{total_files-1}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✓ Successfully converted {dump_id} file {file_num}")
            return True
        else:
            print(f"✗ Error converting {dump_id} file {file_num}: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"Exception converting {dump_id} file {file_num}: {e}")
        return False

def convert_dump_parallel(dump_id: str, total_files: int, base_path: str, 
                         output_base: str, python_path: str, max_workers: int = 4) -> bool:
    """
    Convert all files in a dump using parallel processing.
    
    Args:
        dump_id: Dump ID
        total_files: Total number of files (max file number + 1)
        base_path: Base path to the dataset
        output_base: Base output path
        python_path: Path to the Python executable
        max_workers: Maximum number of parallel workers
    
    Returns:
        True if all conversions successful, False otherwise
    """
    print(f"Starting conversion of dump {dump_id} with {total_files} files (0-{total_files-1})")
    
    success_count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all conversion tasks
        future_to_file = {
            executor.submit(run_conversion, dump_id, file_num, total_files, 
                          base_path, output_base, python_path): file_num 
            for file_num in range(total_files)
        }
        
        # Wait for all tasks to complete
        for future in concurrent.futures.as_completed(future_to_file):
            file_num = future_to_file[future]
            try:
                success = future.result()
                if success:
                    success_count += 1
            except Exception as e:
                print(f"Error processing file {file_num} of dump {dump_id}: {e}")
    
    print(f"Completed dump {dump_id}: {success_count}/{total_files} files converted successfully")
    return success_count == total_files

def main():
    parser = argparse.ArgumentParser(description='Automate conversion of Common Crawl dumps to Parquet format')
    parser.add_argument('--year', type=str, required=True, help='Year to process (e.g., 2021)')
    parser.add_argument('--base_path', type=str, 
                       help='Base path to the c4-dataset-script directory')
    parser.add_argument('--output_base', type=str, 
                       help='Base output path')
    parser.add_argument('--python_path', type=str, 
                       default='$HOME/venv/c4/bin/python3.11',
                       help='Path to the Python executable')
    parser.add_argument('--max_workers', type=int, default=8,
                       help='Maximum number of parallel workers per dump')
    parser.add_argument('--dump_id', type=str, 
                       help='Process only a specific dump ID (e.g., 2021_49)')
    
    args = parser.parse_args()
    
    # Expand environment variables
    base_path = os.path.expandvars(args.base_path)
    output_base = os.path.expandvars(args.output_base)
    python_path = os.path.expandvars(args.python_path)
    
    print(f"Processing Common Crawl dumps for year {args.year}")
    print(f"Base path: {base_path}")
    print(f"Output base: {output_base}")
    print(f"Python executable: {python_path}")
    
    # Get dump information
    dump_info = get_dump_info(base_path, args.year)
    
    if not dump_info:
        print(f"No dumps found for year {args.year}")
        return
    
    # Filter for specific dump if requested
    if args.dump_id:
        if args.dump_id in dump_info:
            dump_info = {args.dump_id: dump_info[args.dump_id]}
        else:
            print(f"Dump {args.dump_id} not found")
            return
    
    print(f"Found {len(dump_info)} dumps to process")
    
    # Process all dumps
    successful_dumps = 0
    for dump_id, max_file_num in dump_info.items():
        total_files = max_file_num + 1  # Files are numbered 0 to max_file_num
        success = convert_dump_parallel(dump_id, total_files, base_path, 
                                      output_base, python_path, args.max_workers)
        if success:
            print(f"✓ Successfully converted dump {dump_id}")
            successful_dumps += 1
        else:
            print(f"✗ Failed to convert dump {dump_id}")
    
    print(f"\nConversion completed: {successful_dumps}/{len(dump_info)} dumps converted successfully")

if __name__ == "__main__":
    main()
