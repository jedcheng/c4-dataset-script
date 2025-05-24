# C4 Dataset Script

[C4](https://www.tensorflow.org/datasets/catalog/c4) is a great way to get a colossal cleaned web corpus. Unfortunately, Google open-sourced c4 script highly depends on GCP and code mixed in a big repo. Therefore, it takes work to develop it freely. This repository extracts the processing logic and implements it to run on Spark. In addition, some helpful data process method in MassiveText is implemented in massivetext_utils.py.


The original [repo](https://github.com/shjwudp/c4-dataset-script) is amazing. I further customized based on the code to run on a PBS cluster as well as adding a filter to filter out simplified Chinese entries. 


## Run c4 script on Spark

Setup c4 work environment.

```bash
# 1. Create an independent Anaconda environment and install python dependencies
conda create -y -n c4-env conda-pack && conda activate c4-env
pip install git+https://github.com/shjwudp/c4-dataset-script

# 2. Download punkt tokenizer
python -m nltk.downloader -d $(which python | xargs dirname)/../nltk_data punkt

# 3. Run pyspark requires JAVA to be installed in your environment, you should
#    make sure you have JDK installed and JAVA_HOME configured.
```


## 1. Download the WET crawl archive index file

Common Crawl organized crawled data into some archives. You can browse the archives list from [here](https://commoncrawl.org/the-data/get-started/). In the next step, we will download text data (WET) as the input of processing. First, download the WET crawl archive index file.

```bash
cd c4_dataset_script
wget -r --no-parent https://data.commoncrawl.org/crawl-data/${CRAWL_ARCHIVE_ID}/wet.paths.gz
```
*You can get CRAWL_ARCHIVE_ID [here](https://commoncrawl.org/the-data/get-started/). For instance: CC-MAIN-2022-49.*


## 2. Run download and Chinese screening script on Spark

All the following commands are outdated. I am too lazy to update them.
Please use the bash scripts written for PBS cluster to do so by splitting the entire process into 20 parts.

There are 100,000 files in the WET crawl. Divide into 20 jobs each with 5,000 files.
Don't rush all the files at once, you will miss out a lot of files.


```bash
spark-submit --master ${SPARK_MASTER_ADDR} \
    Chinese/download_web_docs.py \
        --wet-paths ./data.commoncrawl.org/crawl-data/${CRAWL_ARCHIVE_ID}/wet.paths.gz \
        --output ./download-docs
```






## 3. Filter out non-sentence lines and toxic document

Refer to the c4 heuristics method. I used the following strategies for cleaning up Common Crawl's web-extracted text:

- Only retained lines that ended in a terminal punctuation mark or colon.
- Discarded any page with fewer than five sentences and only retained lines that
contained at least five words.
- Removed any page that contained any word on the "List of Dirty, Naughty, Obscene
or Otherwise Bad Words."
- Many of the scraped pages contained Chinese garbled, so we removed any line with the garbled characters. For example: "[-]|□|■|�".

```bash
cat ./download-docs/*/part-* | \
    python Chinese/filter_out_bad_lines.py \
        --badwords_filepath ./badwords/zh \
         > clean_docs.jsonl
```

*About 93.57% of documents are filtered out in this stage. You can see samples of filtered documents [here](data/Chinese_bad-lines_samples.jsonl).*


This part is yet to be parallelized. The script uses the old logic of cat and pipe, which is not efficient. I will migrate this to Spark just like the other parts.




## 4. Remove duplicated text

To eliminate duplicate text, I use the text deduplication strategy from C4. The algorithm divides the document into lines, hashes them, and removes any duplicate lines from the dataset. This effective approach is particularly useful for removing repeated header and footer content.

```bash
spark-submit --master ${SPARK_MASTER_ADDR} \
    Chinese/remove_duplicate_text.py \
        --input clean_docs.jsonl \
        --output ./deduplicated_text
```


*About 62.67% of documents are filtered out in this stage. You can see samples of filtered lines [here](data/Chinese_Remove-Duplicated-Text_samples.jsonl).*




## 5. Remove documents that are over self-repeating - Repetition Removal in DeepMind MassiveText

Check the percentage of duplicate content in the web document, and the program will remove documents whose duplicate proportion exceeds the preset threshold. This function implements "Repetition Removal" as described in [Gopher](https://arxiv.org/abs/2112.11446).

```bash
spark-submit --master ${SPARK_MASTER_ADDR} \
    Chinese/repetition_removal.py \
        --input clean_docs.jsonl \
        --output ./repetition_removal_output
```

*About 21.21% of documents are filtered out in this stage. You can see samples of filtered documents [here](data/Chinese_Repetition-Removal_samples.jsonl).*

## 6. Remove Simplified Chinese documents (optional)
This step is optional. I have optimized the script to use Spark to do the job in parallel.
Refer to the SC_filter folder for how I obtained the list of simplified Chinese characters (TLDR I manually removed words that are shared between simplified and traditional Chinese from a list of simplified Chinese characters). 



## 7. Filter Cantonese documents (optional)
As a researchers in Cantonese NLP, we are interested in Cantonese documents. Using [Cantonese Detect](https://github.com/CanCLID/cantonesedetect) to extract Cantonese documents for further research. 

The size of the dataset went from 21GB after removing simplified Chinese to 110MB. This is why Cantonese is a low resource language.

You can choose whether to include Cantonese quotes in written Chinese documents as well as splitting the documents in phases for higher accuracy but lower recall. 