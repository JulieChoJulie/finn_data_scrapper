from setuptools import setup, find_packages

setup(
    name='parser',
    version='0.1.0',
    description='parser',
    author='hj',
    author_email='h32cho@gmail.com',
    packages=find_packages(exclude=['*tests*']),
)