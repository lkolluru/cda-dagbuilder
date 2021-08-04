#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from setuptools import find_packages, setup

# Package meta-data.
NAME = "cda-dagbuilder"
PKG_NAME = "cdadagbuilder"
DESCRIPTION = "Prepare Dags Programmatically from YAML/csv configuration files"
URL = "git@git.prod.pch.com:CustDataAnalytics/cdaflow/cdaairflow.git"
EMAIL = "lkolluru@pch.com"
AUTHOR = "Lakshmi Kolluru"
REQUIRES_PYTHON = ">=3.7.0"
VERSION = None

here = os.path.abspath(os.path.dirname(__file__))

REQUIRED = ["apache-airflow>=1.10.0", "pyyaml", "packaging"]
DEV_REQUIRED = ["black", "pytest", "pylint"]

# Load the package's __version__.py module as a dictionary.
about = {}
if not VERSION:
    with open(os.path.join(here, PKG_NAME, "__version__.py")) as f:
        exec(f.read(), about)
else:
    about["__version__"] = VERSION

setup(
    name=NAME,
    version=about["__version__"],
    description=DESCRIPTION,
    author=AUTHOR,
    author_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=find_packages(exclude=("tests",)),
    install_requires=REQUIRED,
    extras_require={"dev": DEV_REQUIRED},
    include_package_data=True,
    license="MIT",
    keywords="dagbuilder",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
)
