# Common Crawl to Parquet Conversion Automation

This directory contains scripts to automate the conversion of Common Crawl JSONL files to Parquet format for all dumps in a given year.

## Files

- `automate_conversion.py`: Main automation script
- `convert.py`: Original conversion script (unchanged)
- `convert_unclean.sh`: Original manual script (for reference)
- `usage_examples.sh`: Example usage commands
- `README.md`: This file

## Features

The automation script provides the following features:

1. **Automatic dump discovery**: Scans the directory for all dumps in a given year
2. **File counting**: Automatically counts the number of files in each dump
3. **Two modes of operation**:
   - **Script generation**: Creates bash scripts for job submission (default)
   - **Sequential processing**: Processes dumps directly with parallel file conversion

## Usage

### Basic Usage

```bash
# Generate bash scripts for all dumps in 2021
python3 automate_conversion.py --year 2021

# Process all dumps sequentially
python3 automate_conversion.py --year 2021 --sequential
```

### Advanced Usage

```bash
# Process only a specific dump
python3 automate_conversion.py --year 2021 --dump_id 2021_49 --sequential

# Use custom paths
python3 automate_conversion.py --year 2021 \
    --base_path /custom/path/to/c4-dataset-script \
    --output_base /custom/output/path \
    --venv_path /custom/venv/path \
    --sequential

# Control parallel processing
python3 automate_conversion.py --year 2021 --sequential --max_workers 8
```

## Command Line Arguments

- `--year`: Year to process (required, e.g., 2021)
- `--base_path`: Base path to the c4-dataset-script directory (default: current directory structure)
- `--output_base`: Base output path (default: `${SSD}/c4-dataset-script/huggingface`)
- `--venv_path`: Path to the virtual environment (default: `$HOME/venv/c4`)
- `--max_workers`: Maximum number of parallel workers per dump (default: 4)
- `--sequential`: Process dumps sequentially instead of generating scripts
- `--dump_id`: Process only a specific dump ID (e.g., 2021_49)

## Operation Modes

### 1. Script Generation Mode (Default)

This mode creates individual bash scripts for each dump that can be submitted to a job scheduler:

```bash
python3 automate_conversion.py --year 2021
```

This will create scripts like:
- `convert_2021_04.sh`
- `convert_2021_10.sh`
- `convert_2021_49.sh`
- etc.

Submit them with:
```bash
pjsub convert_2021_04.sh
pjsub convert_2021_10.sh
# etc.
```

### 2. Sequential Processing Mode

This mode processes all dumps one by one, with parallel processing within each dump:

```bash
python3 automate_conversion.py --year 2021 --sequential
```

This is useful for:
- Interactive processing
- Better resource control
- Debugging

## Directory Structure

The script expects the following directory structure:

```
c4-dataset-script/
├── c4_dataset_script/
│   ├── 1_download_2021_04/
│   │   ├── clean_docs_0.jsonl
│   │   ├── clean_docs_1.jsonl
│   │   └── ...
│   ├── 1_download_2021_10/
│   └── ...
└── huggingface/
    ├── automate_conversion.py
    ├── convert.py
    └── zh_uncleaned/
        ├── 2021_04/
        ├── 2021_10/
        └── ...
```

## Output

The converted Parquet files will be saved in:
```
${output_base}/zh_uncleaned/{dump_id}/{dump_id}_CC_zh_not_cleaned-{file_num}-of-{total_files}.parquet
```

For example:
```
${SSD}/c4-dataset-script/huggingface/zh_uncleaned/2021_49/2021_49_CC_zh_not_cleaned-00001-of-00013.parquet
```

## Performance Considerations

- The script uses parallel processing within each dump to speed up conversion
- Default is 4 workers per dump, but this can be adjusted with `--max_workers`
- In sequential mode, dumps are processed one at a time to avoid overwhelming the system
- In script generation mode, each dump can be processed independently on different nodes

## Error Handling

- The script checks for the existence of input files before processing
- Failed conversions are logged and reported
- In sequential mode, the script continues processing other files even if some fail
- Success/failure statistics are provided for each dump

## Examples

See `usage_examples.sh` for detailed examples of how to use the script in different scenarios.
