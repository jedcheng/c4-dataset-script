from setuptools import find_packages
from setuptools import setup

REQUIRED_PKGS = [
    "pyspark>=3.0.0",
    "tensorflow-datasets==4.6.0",
    "tensorflow",
    "nltk",
    "langdetect",
    "apache_beam",
    "jieba",
    "ahocorasick",
    "cantonesedetect"
]

setup(
    name="c4-dataset-script",
    author="Jed Cheng",
    url="https://github.com/jedcheng/c4-dataset-script",
    license="MIT",
    packages=find_packages(),
    package_data={
        "c4_dataset_script": [
            "badwords/en",
        ]
    },
    install_requires=REQUIRED_PKGS,
    keywords="c4 datasets commoncrawl",
)
