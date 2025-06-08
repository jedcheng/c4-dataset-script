# Traditional Chinese Web Corpus Pipeline

This repository provides a Spark-based pipeline for building a high-quality Traditional Chinese web corpus, inspired by C4 and MassiveText, but fully re-engineered for efficiency, robustness, and Traditional Chinese focus.

## Updates since Last Version

- **Robust Downloading:** All WET files are downloaded with retry logic to handle 503 errors from the Common Crawl API.
- **Integrated Filtering:** Non-sentence line removal, toxic word filtering, garbled character removal, and Simplified Chinese filtering are all performed during download to save disk space and compute. (To also slow down the download speed to avoid 503 errors.)
- **Traditional Chinese Only:** By default, only Traditional Chinese documents are retained. This saves me CPU, RAM, disk, and time. (There are already better Simplified Chinese datasets available.)
- **Efficient Spark Pipeline:** All major steps are now parallelized with Spark for scalability.
- **Multi-level Deduplication:** Both line-level and document-level deduplication are performed, including MinHash-based near-duplicate detection.
- **Optional Cantonese Filtering:** Extract Cantonese documents for low-resource NLP research.

---

## Pipeline Overview

### 1. Download and Filter WET Files

- Downloads all WET files, retrying on failure (e.g., HTTP 503).
- For each document:
  - Removes lines that are not sentences (using C4 heuristics).
  - Removes lines with garbled characters.
  - Removes documents with excessive toxic/bad words.
  - **Filters out Simplified Chinese documents by default.**
- All keyword based filtering is done on-the-fly to minimize disk usage.

### 2. Remove Self-Repeating Documents (Local Repetition Removal)

- Removes documents with excessive internal repetition (e.g., spam, boilerplate, or copy-paste artifacts).
- Implements the "Repetition Removal" strategy from [Gopher](https://arxiv.org/abs/2112.11446).
- This step is performed **before** global deduplication to maximize quality.
- Removes ~20% of documents that are self-repeating. This is relatively computationally expensive, but it can be ran in batches to avoid OOM (the next 2 steps cannot be ran in batches).

### 3. Remove Duplicate and Boilerplate Lines

- Hash-based filtering lines that are occurs more than oce
- Especially effective at removing repeated headers, footers, and web boilerplate.
- I tried to improve this process using regexes in order to preserve more content, but it removed more than the original method while yielding a worse result.

### 4. Remove Near-Duplicate Documents (MinHash LSH)

- Uses MinHash and Locality Sensitive Hashing (LSH) to efficiently detect and remove near-duplicate documents.
- Handles low-effort template-based, copy-paste and multi-crawled content.
- Only one representative from each near-duplicate cluster is retained.

### 5. (Optional) Filter for Cantonese

- Optionally, extract Cantonese documents using [Cantonese Detect](https://github.com/CanCLID/cantonesedetect).
- Useful for low-resource Cantonese NLP research.

---

## Example Usage

**All steps are Spark jobs. Use the provided bash scripts if uncler (I am very lazy to write a proper readme).**

### Download and Filter

```bash
spark-submit --master ${SPARK_MASTER_ADDR} \
    Chinese/download_and_filter.py \
    --wet-paths ./data.commoncrawl.org/crawl-data/${CRAWL_ARCHIVE_ID}/wet.paths.gz \
    --output ./downloaded_and_filtered_docs
```

### Remove Self-Repeating Documents

```bash
spark-submit --master ${SPARK_MASTER_ADDR} \
    Chinese/repetition_removal.py \
    --input ./downloaded_and_filtered_docs \
    --output ./repetition_removed_docs
```

### Remove Duplicate Lines

```bash
spark-submit --master ${SPARK_MASTER_ADDR} \
    Chinese/remove_duplicate_text.py \
    --input ./repetition_removed_docs \
    --output ./deduplicated_text
```

### Remove Near-Duplicate Documents (MinHash)

```bash
spark-submit --master ${SPARK_MASTER_ADDR} \
    Chinese/remove_duplicate_minhash.py \
    --input ./deduplicated_text \
    --output ./minhash_deduped_docs \
    --threshold 0.3 # by default, removed 20% of documents
```

### (Optional) Filter for Cantonese

```bash
spark-submit --master ${SPARK_MASTER_ADDR} \
    Chinese/filter_cantonese.py \
    --input ./minhash_deduped_docs \
    --output ./cantonese_docs
```

---

## Notes

- **Simplified Chinese Filtering:** Always on by default. Only Traditional Chinese is kept.
- **Disk Usage:** All filtering is performed as early as possible to minimize disk and compute requirements.
- **PBS/Cluster:** For large-scale runs, use the provided PBS scripts to split the workload (e.g., 20 jobs × 5,000 files each).
- **Customization:** Regexes and thresholds for boilerplate and repetition removal can be easily adjusted in the scripts.

---

## Credits

- Based on [shjwudp/c4-dataset-script](https://github.com/shjwudp/c4-dataset-script) with extensive modifications.
- Inspired by C4 and DeepMind MassiveText pipelines.
- Cantonese detection via [CanCLID/cantonesedetect](https://github.com/CanCLID/cantonesedetect).

