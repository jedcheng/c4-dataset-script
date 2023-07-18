#!/bin/bash
#
## ask PBS for time (format hh:mm:ss)
#PBS -l walltime=08:00:00
#
#PBS -l select=1:ncpus=64:mem=200gb:cpu_type=rome
#PBS -J 0-3
module load anaconda3/personal
source activate c4-env

cd ${EPHEMERAL}/c4-dataset-script_{$PBS_ARRAY_INDEX}/c4_dataset_script

#python -m nltk.downloader -d $(which python | xargs dirname)/../nltk_data punkt

wget -r --no-parent https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-14/wet.paths.gz

spark-submit --master local[64]  \
    Chinese/download_web_docs.py \
        --wet-paths ./data.commoncrawl.org/crawl-data/CC-MAIN-2023-14/wet.paths.gz \
        --output ./download-docs \
        --array_index $PBS_ARRAY_INDEX




