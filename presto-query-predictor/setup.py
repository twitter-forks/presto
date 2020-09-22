import re

from setuptools import find_packages
from setuptools import setup

with open("query_predictor/__init__.py", encoding="utf8") as f:
    version = re.search(r'__version__ = "(.*?)"', f.read()).group(1)

setup(
    name="presto-query-predictor",
    author="Twitter Presto Team",
    author_email="iq-dev@twitter.com",
    description="A query predictor pipeline and service to predict resource usages of Presto queries",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    version=version,
    url="https://github.com/twitter-forks/presto/tree/query-predictor/presto-query-predictor",
    packages=find_packages(exclude=["tests", "tests.*"]),
    include_package_data=True,
    zip_safe=False,
    license="Apache 2.0",
    keywords="presto sql ml",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: Database",
        "Topic :: Database :: Database Engines/Servers",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
    ],
    install_requires=[
        "Flask",
        "flask-cors",
        "joblib",
        "numpy",
        "pandas",
        "pyyaml",
        "scikit-learn",
        "xgboost",
    ],
    extras_require={"tests": ["pytest"], "tutorial": ["waitress"]},
    python_requires=">=3.7",
)
