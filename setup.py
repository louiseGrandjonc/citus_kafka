import os
import sys

from setuptools import setup, Command, find_packages

# Pull version from source without importing
# since we can't import something we haven't built yet :)
exec(open('kafka/version.py').read())


here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.md')) as f:
    README = f.read()

setup(
    name="kafka-citus",
    version=__version__,

    packages=find_packages(exclude=['test']),
    author="Louise Grandjonc",
    author_email="louise@citusdata.com",
    url="",
    description="Kafka consumer for citus",
    long_description=README,
    keywords="apache kafka citus postgres",
)
