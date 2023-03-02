"""Setup.py for the Cube Airflow provider package."""

from setuptools import find_packages, setup

with open("README.md", "r") as fh:
    long_description = fh.read()

__version__ = "1.0.0"

"""Perform the package airflow-provider-cube setup."""
setup(
    name="airflow-provider-cube",
    version=__version__,
    description="A Cube Apache Airflow provider package built by Cube.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    entry_points={"apache_airflow_provider": ["provider_info=cube_provider.__init__:get_provider_info"]},
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
