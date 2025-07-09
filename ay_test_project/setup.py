from setuptools import find_packages, setup

setup(
    name="ay_test_project",
    packages=find_packages(exclude=["ay_test_project_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-duckdb",
        "dagster-duckdb-pandas", 
        "pandas",
        "requests",
        "requests-kerberos",
        "smartsheet-python-sdk",
        "pytz"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
