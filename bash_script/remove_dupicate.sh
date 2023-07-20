#!/bin/bash
#
## ask PBS for time (format hh:mm:ss)
#PBS -l walltime=08:00:00
#
#PBS -l select=1:ncpus=32:mem=64gb:cpu_type=rome
#PBS -J 0-7
module load anaconda3/personal
source activate c4-env


cd $EPHEMERAL/c4-dataset-script_$PBS_ARRAY_INDEX/c4_dataset_script

cp -r $EPHEMERAL/c4-dataset-script_$PBS_ARRAY_INDEX/c4_dataset_script $TMPDIR


spark-submit --master local[20]  \
    Chinese/remove_duplicate_text.py \
        --input clean_docs.jsonl \
        --output ./deduplicated_text

cp -r deduplicated_text $EPHEMERAL/c4-dataset-script_$PBS_ARRAY_INDEX/c4_dataset_script