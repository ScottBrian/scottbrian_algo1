#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 11 2020

@author: Scott Tuttle
"""

from pathlib import Path
from setuptools import setup, find_packages

with open('README.rst', 'r') as readme:
    long_description = readme.read()


def get_version(rel_path):
    target_path = Path(__file__).resolve().parent.joinpath(rel_path)
    with open(target_path, 'r') as file:
        for line in file:
            if line.startswith('__version__'):
                delimiter = '"' if '"' in line else "'"
                return line.split(delimiter)[1]
        else:
            raise RuntimeError("Unable to find version string.")


setup(
      name='scottbrian_algo1',
      version=get_version('src/scottbrian_algo1/__init__.py'),
      author='Scott Tuttle',
      description='Analyze and trade financial instruments',
      long_description=long_description,
      long_description_content_type='text/x-rst',
      url='https://github.com/ScottBrian/scottbrian_algo1.git',
      license='MIT',
      classifiers=[
          'License :: OSI Approved :: MIT License',
          'Development Status :: 5 - Production/Stable',
          'Intended Audience :: Developers',
          'Topic :: Utilities',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
          'Operating System :: POSIX :: Linux'
                  ],
      project_urls={
          'Documentation': 'https://scottbrian-algo1.readthedocs.io/en'
                           '/latest/',
          'Source': 'https://github.com/ScottBrian/scottbrian_algo1.git'},
      python_requires='>=3.6',
      packages=find_packages('src'),
      package_dir={'': 'src'},
      install_requires=['ibapi', 'pandas', 'scottbrian_utils'],
      package_data={"scottbrian_algo1": ["__init__.pyi", "py.typed"]},
      zip_safe=False
     )
