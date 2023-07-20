#!/bin/bash
#
## ask PBS for time (format hh:mm:ss)
#PBS -l walltime=08:00:00
#
#PBS -l select=1:ncpus=64:mem=200gb:cpu_type=rome
#PBS -J 0-7
module load anaconda3/personal
source activate c4-env


git clone https://github.com/jedcheng/c4-dataset-script

mv c4-dataset-script c4-dataset-script_$PBS_ARRAY_INDEX

cd c4-dataset-script_$PBS_ARRAY_INDEX/c4_dataset_script


wget -r --no-parent https://data.commoncrawl.org/crawl-data/CC-MAIN-2023-14/wet.paths.gz

spark-submit --master local[64]  \
    Chinese/download_web_docs.py \
        --wet-paths ./data.commoncrawl.org/crawl-data/CC-MAIN-2023-14/wet.paths.gz \
        --output ./download-docs \
        --array_index $PBS_ARRAY_INDEX



cd $TMPDIR

cp -r c4-dataset-script_$PBS_ARRAY_INDEX $EPHEMERAL
