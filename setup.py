#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Setup.py for the Cube Airflow provider package."""

from setuptools import find_packages, setup

with open("README.md", "r") as fh:
    long_description = fh.read()

__version__ = "0.0.0"

"""Perform the package airflow-provider-cube setup."""
setup(
    name="airflow-provider-cube",
    version=__version__,
    description="A Cube Apache Airflow provider package built by Cube.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    entry_points={
        "apache_airflow_provider": [
            "provider_info=cube_provider.__init__:get_provider_info"
        ]
    },
    license="Apache License 2.0",
    packages=find_packages(exclude=["*tests.*", "*tests"]),
    install_requires=["apache-airflow>=2.3"],
    setup_requires=["setuptools", "wheel"],
    author="Artem Lytvynov",
    author_email="artem.lytvynov@cube.dev",
    url="https://cube.dev/",
    classifiers=[
        "Framework :: Apache Airflow",
        "Framework :: Apache Airflow :: Provider",
    ],
    python_requires="~=3.8",
)
