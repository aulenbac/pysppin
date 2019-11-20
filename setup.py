from setuptools import setup

setup(
    name='pysppin',
    version='0.0.1',
    description='Processing modules for gathering information about biological species from linked information sources',
    url='http://github.com/usgs-bcb/pysppin',
    author='R. Sky Bristol',
    author_email='sbristol@usgs.gov',
    license='unlicense',
    packages=['pysppin'],
    install_requires=[
        'requests',
        'xmltodict',
        'geopandas',
        'owslib',
        'genson',
        'ftfy',
        'bs4',
        'sciencebasepy'
    ],
    zip_safe=False
)
