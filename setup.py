from setuptools import setup, find_packages
from pathlib import Path

setup(
    name='simplech',
    version='0.16.3',
    author='Dmitry Rodin',
    author_email='madiedinro@gmail.com',
    license='MIT',
    description='Simple ClickHouse client that simplify you interration with DBMS by using dicts as payload.',
    long_description=Path('README.md').read_text(),
    long_description_content_type='text/markdown',
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    url='https://github.com/madiedinro/simple-clickhouse',
    include_package_data=True,
    install_requires=[
        'ujson>=1.35,<2', 
        'aiohttp>=3,<4', 
        'pydantic>=0.18',
        'arrow>=0.12.1,<1'
    ],
    zip_safe=False,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    project_urls={  # Optional
        'Homepage': 'https://github.com/madiedinro/simple-clickhouse'
    }
)
