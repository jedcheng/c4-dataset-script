#!/bin/bash
MY_PATH="$(dirname -- "${BASH_SOURCE[0]}")"

cd $MY_PATH/../..
source venv/bin/activate
cd c4_dataset_script

export CC_ID="2023_40"

rm -rf 1_download_${CC_ID}/

for i in $(seq 0 17); do
        python Chinese/download_web_docs.py \
                --wet-paths wet.paths.gz \
                --output 1_download_${CC_ID} \
                --array_index ${i} \
                --badwords_filepath ./badwords/zh \
                --simplified_chinese_filtering \
                --SC_words_filepath ./badwords/SC_list.txt
done
