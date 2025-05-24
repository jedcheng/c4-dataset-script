from huggingface_hub import HfApi
api = HfApi()

api.upload_folder(
    folder_path="zh_cc",
    repo_id="jed351/Chinese-Common-Crawl-Filtered",
    repo_type="dataset",
)