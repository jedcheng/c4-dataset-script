#!/bin/bash
#
## ask PBS for time (format hh:mm:ss)
#PBS -l walltime=08:00:00
#
#PBS -l select=1:ncpus=1:mem=2gb:cpu_type=rome
#PBS -J 0-7
module load anaconda3/personal
source activate c4-env


# cp -r $EPHEMERAL/c4-dataset-script_$PBS_ARRAY_INDEX $TMPDIR

# cd $TMPDIR/c4-dataset-script_$PBS_ARRAY_INDEX/c4_dataset_script

cd $EPHEMERAL/c4-dataset-script_$PBS_ARRAY_INDEX/c4_dataset_script

cat ./deduplicated_text/clean_docs/part-* | \
    python Chinese/filter_out_bad_lines.py \
        --badwords_filepath ./badwords/zh \
         > clean_docs_deduplicated.jsonl


# cp clean_docs4.jsonl $EPHEMERAL/c4-dataset-script_$PBS_ARRAY_INDEX/c4_dataset_script


