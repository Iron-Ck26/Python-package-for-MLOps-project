from setuptools import setup, find_packages
from typing import List

with open('README.md', 'r', encoding='utf-8') as f:
    long_description = f.read()     
   

__version__ = "0.0.4"
REPO_NAME = "Python-package-for-MLOps-project"    # Git repo name
PKG_NAME= "Python_mongoDB_Connect"                # Package name for pipy page
AUTHOR_USER_NAME = "Chakrapani26"                 # Git user name
AUTHOR_EMAIL = "chakrapaniwaghmode81@gmail.com"   # Git mail

# this imformation is about to package, lick name of package and other..
setup(
    name=PKG_NAME,
    version=__version__,
    author=AUTHOR_USER_NAME,
    author_email=AUTHOR_EMAIL,
    description="A python package for connecting with database.",
    long_description=long_description,
    long_description_content="text/markdown",
    url=f"https://github.com/{AUTHOR_USER_NAME}/{REPO_NAME}",
    project_urls={
        "Bug Tracker": f"https://github.com/{AUTHOR_USER_NAME}/{REPO_NAME}/issues",
    },
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    )



