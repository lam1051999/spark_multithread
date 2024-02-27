from setuptools import setup, find_packages

setup(
    name='spark_multithread',
    version='0.1.0',
    description='Library for Spark multithread',
    author='Lam Tran',
    author_email='lamtt26@fpt.com',
    packages=find_packages(exclude=['*tests*']),
)
