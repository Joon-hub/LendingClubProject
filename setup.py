from setuptools import setup, find_packages

setup(
    name="lending_club_project",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.0.0",
        "pytest>=6.0.0",
    ],
    python_requires=">=3.7",
) 