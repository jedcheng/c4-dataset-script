#!/bin/bash
#
## ask PBS for time (format hh:mm:ss)
#PBS -l walltime=08:00:00
#
#PBS -l select=1:ncpus=64:mem=128gb:cpu_type=rome
#PBS -J 0-7
module load anaconda3/personal
source activate c4-env



cd $EPHEMERAL/c4-dataset-script_$PBS_ARRAY_INDEX/c4_dataset_script


spark-submit --master local[64]  \
    Chinese/repetition_removal.py \
        --input clean_docs_deduplicated.jsonl \
        --output ./repetition_removal_output

