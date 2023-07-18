#!/bin/bash
#
## ask PBS for time (format hh:mm:ss)
#PBS -l walltime=72:00:00
#
#PBS -l select=1:ncpus=64:mem=300gb:cpu_type=rome
# 
module load anaconda3/personal
source activate c4_env

cd ${EPEHMERAL}/C4-DATASET-SCRIPT

python -m nltk.downloader -d $(which python | xargs dirname)/../nltk_data punkt

wget -r --no-parent https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-14/wet.paths.gz

python -m c4_dataset_script.c4 --wet-file-paths /content/data.commoncrawl.org/crawl-data/CC-MAIN-2023-14/wet.paths.gz

cd c4_dataset_script


wget -r --no-parent https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-14/wet.paths.gz

spark-submit --master local[4]  \
    Chinese/download_web_docs.py \
        --wet-paths ./data.commoncrawl.org/crawl-data/CC-MAIN-2023-14/wet.paths.gz \
        --output ./download-docs


