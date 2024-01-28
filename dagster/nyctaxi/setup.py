from setuptools import find_packages, setup

setup(
    name="nyctaxi",
    packages=find_packages(exclude=["nyctaxi_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
