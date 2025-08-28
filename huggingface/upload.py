from huggingface_hub import HfApi
api = HfApi()

# api.upload_folder(
#     folder_path="zh_cc",
#     repo_id="jed351/Chinese-Common-Crawl-Filtered",
#     repo_type="dataset",
# )


# api.upload_large_folder(
#      folder_path="/fast/pj24001684/c4-dataset-script/huggingface/zh_cleaned",
#      repo_id="jed351/Traditional-Chinese-Common-Crawl-Filtered",
#      repo_type="dataset",
# )

# api.upload_large_folder(
#     folder_path="/fast/pj24001684/c4-dataset-script/huggingface/cantonese",
#     repo_id="jed351/Cantonese_Common_Crawl_Filtered",
#     repo_type="dataset",
# )

api.upload_large_folder(
    folder_path="/fast/pj24001684/c4-dataset-script/huggingface/zh_uncleaned",
    repo_id="jed351/Traditional-Chinese-Common-Crawl-NOT-Cleaned",
    repo_type="dataset",
)