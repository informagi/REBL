from setuptools import setup, find_packages

setup(
    name='LinkJsonlCollection',
    version='0.0.1',
    description='Package for entity linking an JSONL collection',
    author='Chris Kamphuis & Arjen P. de Vries',
    author_email='chris@cs.ru.nl',
    url='https://github.com/informagi/link-jsonl-collection',
    install_requires=['duckdb', 'pandas', 'pyarrow', 'flair', 'torch>=1.5.0,!=1.8.*',
                      'syntok @ git+git://github.com/informagi/syntok'],
    packages=find_packages(),
    license='MIT License'
)