from setuptools import setup, find_packages
import os

with open(os.path.join(current_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name="beancount-importers-india",
    version="1.0.0",
    packages=find_packages(),
    author="Sufiyan Adhikari",
    license='GPLv3',
    long_description=long_description,
    python_requires='>3.8.0'
)
