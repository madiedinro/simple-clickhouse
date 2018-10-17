from setuptools import setup, find_packages
from pathlib import Path

setup(
    name='simplech',
    version='0.3.3',
    author='Dmitry Rodin',
    author_email='madiedinro@gmail.com',
    license='MIT',
    description='Simple ClickHouse client that uses json as main format',
    long_description=Path('README.md').read_text(),
    long_description_content_type='text/markdown',
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    url='https://github.com/madiedinro/simple-clickhouse',
    include_package_data=True,
    install_requires=[
        'ujson'
    ],
    zip_safe=False,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    project_urls={  # Optional
        'Homepage': 'https://github.com/madiedinro/simple-clickhouse'
    }
)
