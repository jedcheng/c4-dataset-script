#!/bin/bash
#
## ask PBS for time (format hh:mm:ss)
#PBS -l walltime=08:00:00
#
#PBS -l select=1:ncpus=1:mem=2gb:cpu_type=rome
#PBS -J 0-7
module load anaconda3/personal
source activate c4-env


wget https://raw.githubusercontent.com/jedcheng/c4-dataset-script/master/c4_dataset_script/badwords/SC_list.txt
mv SC_list.txt badwords/SC_list.txt

wget https://raw.githubusercontent.com/jedcheng/c4-dataset-script/master/c4_dataset_script/Chinese/filter_out_SC_line.py
mv filter_out_SC_line.py Chinese/filter_out_SC_line.py

cd $EPHEMERAL/c4-dataset-script_$PBS_ARRAY_INDEX/c4_dataset_script

cat ./repetition_removal_output/clean_docs/part-* | \
    python Chinese/filter_out_SC_lines.py \
        --badwords_filepath ./badwords/SC_list.txt \
         > clean_docs_repetition_TC.jsonl

