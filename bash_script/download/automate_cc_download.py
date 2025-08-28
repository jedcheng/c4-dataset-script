#!/usr/bin/env python3
import os
import sys
import gzip
import argparse
import subprocess
import requests
from pathlib import Path
import math
from concurrent.futures import ThreadPoolExecutor

def download_wet_paths(year):
    """
    Download the wet.paths.gz files for a given year by trying all possible weeks
    
    Args:
        year (str): The year to download, e.g. "2021"
    
    Returns:
        list: List of tuples (path, cc_id) for all successfully downloaded files
    """
    # Create directory for downloads if it doesn't exist
    base_dir = Path(os.environ.get("SSD", ".")) / "c4-dataset-script" / "wet_paths"
    base_dir.mkdir(parents=True, exist_ok=True)
    
    # Try all possible weeks (01 to 52)
    weeks = [f"{i:02d}" for i in range(1, 53)]
    successful_downloads = []
    
    print(f"Trying to download wet.paths.gz for all weeks in {year}...")
    
    # Helper function to try downloading a specific week
    def try_download_week(week):
        cc_id = f"{year}_{week}"
        wet_paths_url = f"https://data.commoncrawl.org/crawl-data/CC-MAIN-{year}-{week}/wet.paths.gz"
        output_path = base_dir / f"wet.paths.{cc_id}.gz"
        
        # Skip if file already exists
        if output_path.exists():
            print(f"File already exists: {output_path}")
            return str(output_path), cc_id
        
        print(f"Trying to download: {wet_paths_url}")
        
        try:
            response = requests.get(wet_paths_url, stream=True)
            if response.status_code == 200:
                print(f"Downloading wet.paths.gz for {cc_id}...")
                with open(output_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"✓ Successfully downloaded {cc_id}")
                return str(output_path), cc_id
            else:
                print(f"✗ No wet.paths.gz found for {cc_id}")
                return None
        except Exception as e:
            print(f"✗ Error trying {cc_id}: {e}")
            return None
    
    # Try downloading all weeks in parallel
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(try_download_week, weeks))
    
    # Filter out None results
    successful_downloads = [result for result in results if result is not None]
    
    if not successful_downloads:
        print(f"Could not find any valid wet.paths.gz files for year {year}")
    else:
        print(f"Successfully downloaded {len(successful_downloads)} wet.paths.gz files for {year}")
    
    return successful_downloads

def count_paths(wet_paths_file):
    """
    Count the number of paths in the wet.paths.gz file
    
    Args:
        wet_paths_file (str): Path to the wet.paths.gz file
    
    Returns:
        int: Number of paths in the file
    """
    count = 0
    try:
        with gzip.open(wet_paths_file, 'rt') as f:
            for _ in f:
                count += 1
        return count
    except Exception as e:
        print(f"Error counting paths in {wet_paths_file}: {e}")
        return 0

def calculate_jobs(total_paths, paths_per_job=5000):
    """
    Calculate the number of jobs needed
    
    Args:
        total_paths (int): Total number of paths
        paths_per_job (int): Number of paths per job
    
    Returns:
        int: Number of jobs needed
    """
    return math.ceil(total_paths / paths_per_job)

def process_cc_dump(wet_paths_file, cc_id, working_dir, no_workers, paths_per_job=5000):
    """
    Process a CommonCrawl dump directly without creating a bash script
    
    Args:
        wet_paths_file (str): Path to the wet.paths.gz file
        cc_id (str): The CC ID (e.g., "2021_21")
        paths_per_job (int): Number of paths per job
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Get the base filename without the path
    wet_paths_filename = os.path.basename(wet_paths_file)
    
    # Count paths to determine number of jobs
    total_paths = count_paths(wet_paths_file)
    if total_paths == 0:
        print(f"No paths found in {wet_paths_file}, skipping")
        return False
        
    num_jobs = calculate_jobs(total_paths, paths_per_job)
    print(f"Processing {cc_id}: {total_paths} paths, {num_jobs} jobs")
    
    # Set up output directory
    output_dir = f"1_download_{cc_id}"
    os.makedirs(output_dir, exist_ok=True)
    

    # c4_script_dir = Path(os.environ.get("SSD", ".")) / "c4-dataset-script" / "c4_dataset_script"
    c4_script_dir = Path(working_dir) / "c4_dataset_script"
    wet_paths_dest = c4_script_dir / wet_paths_filename
    
    if not wet_paths_dest.exists():
        subprocess.run(["cp", wet_paths_file, str(wet_paths_dest)], check=True)
        print(f"Copied {wet_paths_file} to {wet_paths_dest}")
    
    # Execute spark-submit commands for each job
    print(f"Starting {num_jobs} jobs for {cc_id}...")
    
    for job_idx in range(num_jobs):
        print(f"Running job {job_idx+1}/{num_jobs} for {cc_id}...")
        wet_paths_filename = c4_script_dir / wet_paths_filename
        # Execute spark-submit
        cmd = [
            "spark-submit", "--master", f"local[{no_workers}]",
            "Chinese/download_web_docs.py",
            "--wet-paths", wet_paths_filename,
            "--output", output_dir,
            "--array_index", str(job_idx),
            "--badwords_filepath", "./badwords/zh",
            "--simplified_chinese_filtering",
            "--SC_words_filepath", "./badwords/SC_list.txt"
        ]
        
        # Change to the c4_dataset_script directory before running
        current_dir = os.getcwd()
        os.chdir(c4_script_dir)
        
        try:
            # Run the command
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error running job {job_idx} for {cc_id}: {e}")
            os.chdir(current_dir)
            return False
        finally:
            os.chdir(current_dir)
    
    print(f"Completed processing {cc_id}")
    return True

def main():
    parser = argparse.ArgumentParser(description='Automate CC dump download and processing')
    parser.add_argument('--year', type=str, help='Year to download, e.g. "2021"')
    parser.add_argument('--paths-per-job', type=int, default=5000, help='Number of paths per job (default: 5000)')
    parser.add_argument('--download-only', action='store_true', help='Only download the wet.paths.gz files without processing')
    parser.add_argument('--specific-weeks', type=str, help='Comma-separated list of specific weeks to process, e.g. "21,35,47"')
    parser.add_argument('--no-workers', type=int, default=128, help='Number of Spark workers (default: 128)')
    parser.add_argument('--working-dir', type=str, default=os.getcwd(), help='Working directory for processing (default: current directory)')
    
    args = parser.parse_args()
    
    # Initialize variables
    wet_paths_files = []
    
    # Step 1: Download the wet.paths.gz files for the specified year
    if args.specific_weeks:
        # Process only specific weeks
        weeks = args.specific_weeks.split(',')
        print(f"Will try to download only these weeks: {weeks}")
        
        wet_paths_files = []
        for week in weeks:
            week = week.zfill(2)  # Ensure 2-digit format
            cc_id = f"{args.year}_{week}"
            base_dir = Path(os.environ.get("SSD", ".")) / "c4-dataset-script" / "wet_paths"
            output_path = base_dir / f"wet.paths.{cc_id}.gz"
            
            if output_path.exists():
                print(f"Using existing file: {output_path}")
                wet_paths_files.append((str(output_path), cc_id))
            else:
                # Try to download this specific week
                result = try_download_week(week, args.year)
                if result:
                    wet_paths_files.append(result)
    else:
        # Try all weeks
        wet_paths_files = download_wet_paths(args.year)
        
    if not wet_paths_files:
        print("Failed to download any wet.paths.gz files. Exiting.")

    
    # Print summary of files to process
    print(f"\nFound {len(wet_paths_files)} wet.paths.gz files to process:")
    for path, cc_id in wet_paths_files:
        print(f"  - {cc_id}: {path}")
    
    # Exit if download-only flag is set
    if args.download_only:
        print("\nDownload completed. Skipping processing as --download-only was specified.")
        sys.exit(0)
    
    # Step 2: Process each wet.paths.gz file
    print("\nStarting to process files...")
    
    for wet_paths_file, cc_id in wet_paths_files:
        print(f"\n{'='*80}")
        print(f"Processing {cc_id}")
        print(f"{'='*80}")
        
        # Activate the virtual environment
        activate_cmd = f"source $HOME/venv/c4/bin/activate"
        subprocess.run(activate_cmd, shell=True, executable="/bin/bash")
        
        # Process the dump
        process_cc_dump(wet_paths_file, cc_id, args.working_dir, args.paths_per_job)

# Helper function for downloading a specific week
def try_download_week(week, year):
    """Helper function to try downloading a specific week"""
    base_dir = Path(os.environ.get("SSD", ".")) / "c4-dataset-script" / "wet_paths"
    base_dir.mkdir(parents=True, exist_ok=True)
    
    cc_id = f"{year}_{week}"
    wet_paths_url = f"https://data.commoncrawl.org/crawl-data/CC-MAIN-{year}-{week}/wet.paths.gz"
    output_path = base_dir / f"wet.paths.{cc_id}.gz"
    
    print(f"Trying to download: {wet_paths_url}")
    
    try:
        response = requests.get(wet_paths_url, stream=True)
        if response.status_code == 200:
            print(f"Downloading wet.paths.gz for {cc_id}...")
            with open(output_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"✓ Successfully downloaded {cc_id}")
            return str(output_path), cc_id
        else:
            print(f"✗ No wet.paths.gz found for {cc_id}")
            return None
    except Exception as e:
        print(f"✗ Error trying {cc_id}: {e}")
        return None

if __name__ == "__main__":
    main()


# python3.11 automate_cc_download.py 2021 --working-dir $SSD/c4-dataset-script/bash_script/download