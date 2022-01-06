from setuptools import setup, find_packages

setup(
    name='rebl',
    version='0.0.1',
    description='Package for batch entity linking an JSONL collection',
    author='Chris Kamphuis & Arjen P. de Vries',
    author_email='chris@cs.ru.nl',
    url='https://github.com/informagi/REBL',
    install_requires=['duckdb', 'pandas', 'pyarrow', 'flair', 'torch>=1.5.0,!=1.8.*',
                      'syntok @ git+git://github.com/informagi/syntok'],
    packages=find_packages(),
    license='MIT License'
)