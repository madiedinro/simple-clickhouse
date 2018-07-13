from setuptools import setup, find_packages

setup(
    name='simplech',
    version='0.1',
    author='Dmitry Rodin',
    author_email='madiedinro@gmail.com',
    license='MIT',
    description='Simple ClickHouse client that uses json as main format',
    long_description="""
    """,
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    url='https://github.com/madiedinro',
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
        'Homepage': 'https://github.com/madiedinro'
    }
)
