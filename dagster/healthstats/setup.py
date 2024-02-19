from setuptools import find_packages, setup

setup(
    name="healthstats",
    packages=find_packages(exclude=["healthstats_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
