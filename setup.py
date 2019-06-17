#!/usr/bin/env/python
#

from setuptools import setup, find_packages

setup(name="avro_streamer",
	version="1.0.0",
        description="Python module for reading and modifying streamed Avro data files",
        url="https://github.com/CAIDA/",
        author="Shane Alcock",
        author_email="shane.alcock@waikato.ac.nz",
        license="Apache 2.0",
        packages=find_packages(),
        install_requires=['python-snappy', 'future'],
)


